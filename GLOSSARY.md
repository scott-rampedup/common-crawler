# Glossary — one word, one meaning

Written 2026-10-27 after "queued" was found to mean six different things in the operator-facing UI,
while the one state that is genuinely queued (a crawl row waiting for a worker) was the only one the
word was never used for. Every entry below was verified against the running system, not inferred from
the code. If you find a term used in a way this file does not describe, this file is the bug.

## The three places work can actually wait

Nothing else is a waiting state. If all three are zero, nothing is pending anywhere, whatever a screen says.

| # | Name on the dashboard | Where it physically is | How to measure it, without trusting the UI |
|---|---|---|---|
| 1 | `awaitingResolve` | S3 `monitor-queue/` objects newer than the `bio_feed_cursor` | `node /app/bio-feed.js --dry` |
| 2 | `awaitingExtract` | Postgres `crawl_queue` rows with `status='pending'` | `SELECT status, count(*) FROM crawl_queue GROUP BY status` |
| 3 | `awaitingLive` | S3 `bio-miss/` objects newer than the `bio_live_cursor` | `node /app/bio-live.js --dry` |

`--dry` advances no cursor and launches no machine. It is always safe to run.

## Terms that have been used loosely

- **queue** — ambiguous on its own. Always say which: *the push staging prefix* (S3 `monitor-queue/`),
  *the crawl table* (Postgres `crawl_queue`), or *the live-miss prefix* (S3 `bio-miss/`).
- **queued** — do not use for a state. A `crawl_queue` row waiting for a worker is **pending**.
  A URL handed to S3 but not yet resolved is **awaitingResolve**.
- **BIO URL List** — a **receipt**, not a job. It records that a push happened and is terminal the
  moment it is created. It has no progress and never had any; the crawling of its URLs belongs to the
  queue, not to that row. Before 2026-10-27 it was born with `status: 'queued'` and nothing ever moved
  it, so 38 finished pushes sat on the dashboard looking like untouched work.
- **running** (a job row) — means a process claimed it. It is *stored*, so a job killed mid-flight kept
  saying "running" forever; `loadJobs` now re-derives it from the work done and writes the answer back.
- **drain** — the legacy batch path (`bio-etl.js`, behind `/api/backlog/drain`). The current path is the
  continuous pump (`bio-pump.js` -> `bio-feed.js`). Both exist; they are not the same thing.
- **done** — for `crawl_queue`, a row was fetched and parsed. It does **not** mean contacts were found.
- **records** (a `crawl_queue` column) — extraction returned a person *object* for that page. It does
  **not** mean a contact was stored. A bio page carrying a name and no email address counts 1 here and
  nothing anywhere else, which is how a push shows "23 crawled, 9 with records, 0 people".
- **no_email** (a `crawl_queue` column, added 2026-10-28) — that person was then **discarded**, because
  identity in the contacts store IS the normalized email address. The gap between `records` and
  `no_email` is what was actually kept. This is the largest silent loss in the pipeline, and before the
  column existed it left no trace in any table: the record died at `rowValues()` in `db-pg.js` ahead of
  the sightings spool, so nothing recorded that a person had ever been seen.
- **sent to the crawler** (the blue badge on a receipt) — the URLs reached S3 and the pump. It says
  nothing about people. Click the badge for the per-URL outcome (`GET /api/list-outcome?listId=`),
  which reports URLs sent, already-known, waiting, crawled, found a person, and discarded.

## "0 people" — three different causes, and how to tell them apart

A push can report nobody for three unrelated reasons. They look identical on the dashboard and have
nothing in common, so always establish WHICH before changing anything. Click the receipt's badge
(`/api/list-outcome`); it separates them.

| What you see | What it means | Example |
|---|---|---|
| `found a person` > 0, `discarded — no email address` > 0 | The page HAD a person. Identity in the contacts store is the email address, so one without an address is thrown away. | lawgistics.co.uk, 2026-10-28 |
| `fetched, but no person in the page's HTML` (ledger `no-record`) | The fetch worked and the document contains no person at all — usually a CMS that renders staff data in the browser. The crawler does not run JavaScript. | benmays.spps.org (Finalsite), 2026-10-29 |
| `0 people` on the row with no outcome shown | **Not a measurement.** Before 2026-10-29 the receipt row printed `0 CC · 0 live · 0 none · 0 people` from hard-coded placeholder fields that nothing ever updated. | any push before that date |

## The two paths a pushed URL can take

An extension push does NOT all go to one place, and for bio pages the second path is the majority —
one real 15-URL push went 1 and 13. An outcome built from only the first is wrong.

| Path | When | Where it is recorded |
|---|---|---|
| Worker fleet | the page is already in the Common Crawl archive | Postgres `crawl_queue` (`status`, `records`, `no_email`) |
| Live fleet | it is not — `bio-feed` writes it to S3 `bio-miss/`, `bio-live` launches shards | OpenSearch `crawl_log`, `_id = sha1(url)`, `outcome` = `extracted` / `no-record` / `empty` / `error` |

## Which bio-directory term list is authoritative

Four files hold a version of "what a path segment has to say for the page to be a person's bio", and
only one of them is read at runtime. Editing the wrong one changes nothing; regenerating the wrong
one destroys work.

| File | Role | Careful |
|---|---|---|
| `extractor.js` `BIO_DIR_RAW` | **the only list the crawl reads.** 797 terms | edit here for a behaviour change |
| `bio-dirs.js` (Chrome extension) | generated copy, so the popup pre-ticks what the server will accept | regenerate after any `BIO_DIR_RAW` edit, with the extension's `norm()` (strips every non-alphanumeric), NOT `normalizeForMatching` (keeps separators as spaces) |
| `bio-terms-people-company.txt` | 744 terms; **hand-maintained since 2026-10-29** — 359 are not in the sheet | `tmp-gen-terms.js` rebuilds it from `Directories.xlsx` and would delete those 359 |
| `Directories.xlsx` sheet `Directory` | the curated source, with a `Directory` column | rows marked `company`, `locations`, `news` etc. are **not** people directories |

`pathIdFromUrl` matches **any** path segment, so one term that names an ordinary website section
classifies the whole web as bios. 22 terms are held out of `BIO_DIR_RAW` for that reason and the list
is in the comment beside them — `about`, `about-us`, `company`, `corporate`, `who-we-are`, `mortgage`,
`integrations`, `fa`, `lo` among them. With all of them added, 10 of 10 plain company pages became
BIO URLs (`/about/our-history` → Path ID "About").

## Gates — work that is waiting on purpose

A gate is not a fault. These hold work deliberately, and the dashboard now names the one that is holding.

| Gate | Default | What it holds | Override |
|---|---|---|---|
| `BIO_LIVE_MIN_URLS` | 2000 | `bio-live` **run standalone** will not wake a fleet for fewer misses. **Not in play on the continuous path:** `bio-pump.js:121` invokes it with `--min 1`, so the pump never waits for 2,000 — a claim to the contrary on 2026-10-27 was wrong. | any priority push launches at once |
| fleet size | `BIO_LIVE_SHARDS` 12 | not a gate, but it used to waste machines: the sizing branch hung off `BIO_LIVE_MIN_URLS`, which `--min 1` always satisfied, so 12 shards launched for 23 URLs of one domain. Sharding is by domain, so shards are now capped by distinct domains and `BIO_LIVE_PRIO_PER_SHARD` (250). | — |
| claim order | `priority DESC, created_at` | bulk (p0) is taken after urgent (p9) | `BIO_URL_EXT_PRIORITY`, or `priority` in the request body |
| `JOBS_AUTO_RESUME` | off | interrupted jobs do not restart themselves | set it, bounded by `JOBS_RESUME_MAX` (5) |
| `MONITOR_QUEUE` | on | when off, nothing is pushed or resolved at all | — |

## Machines that do NOT follow a deploy

`fly deploy` only moves the `app` process group. These are long-lived standalone machines and keep
running their old image until updated by hand with `fly machine update --image <new>`:

`cq-live-0..3` (the crawl workers), `bio-pump-0` (the continuous feed), `scheduler-0` (launches
`bio-live`), `os-sync-0`. Forgetting this means new code appears to have no effect.
