# Enterprise Scaling Roadmap

**Goal:** evolve the contact pipeline from a single-machine, run-once tool into an enterprise
system that (1) crawls **thousands** of sites/pages/sitemaps, (2) **maximizes the BIO-URL catalog**
per domain, and (3) **monitors** those domains on a schedule to **detect new employees** (and
departures).

This is a phased evolution of the existing system — **not a rewrite**. Each phase ships value on
its own. Effort estimates assume one focused engineer; cost ranges are rough monthly figures to
budget against, not quotes.

---

## 1. Where we are today (baseline)

| Area | Today | Implication for scale |
|---|---|---|
| Compute | **1 Fly machine** (8GB / 4 perf vCPU, always-on, `min_machines_running=1`) | All work runs in one process on one box — no horizontal scaling |
| Database | **`node:sqlite` (DatabaseSync), WAL, single Fly volume** `/data`; ~232k contacts | Single-writer; a Fly volume **can't be shared across machines** → blocks a worker fleet |
| Jobs | In-memory `jobs` Map; each job holds all records in a `recordsByEmail` Map; persisted to `jobs/<id>.json`; coarse resume on restart | Large batches blow up RAM; no per-URL retry; no distributed workers |
| Scheduling | **None** — every job is one-shot | No recurrence, no change detection |
| Throughput knobs | `DOMAIN_CONCURRENCY=48`, `IN_SITE_CONCURRENCY=4`, `HOST_CONCURRENCY=3`, `CC_CONCURRENCY=1` | CC index lookups serialized (polite); bulk per-domain cache mitigates |
| Discovery | Domain crawl · Webpage · Sitemap · CC bio-discovery · Site Search (serper) · site adapters · Sheet sync | Strong primitives; not yet **unioned** into a durable per-domain catalog |
| Strengths to keep | CC-first (bypasses live blocks, cheap), email-keyed dedup, `normalize.js` durable rules, email modelling, site adapters, auto-resume | The crawler core is sound; scaling is about the layers around it |

**Three hard ceilings:** (a) SQLite-on-one-volume blocks a worker fleet; (b) in-memory job Maps
cap batch size; (c) no queue / scheduler / diff engine for monitoring.

---

## 2. Target architecture (overview)

```
                ┌─────────────┐        ┌──────────────────────────┐
   UI / API ───▶│  web machine│──enqueue──▶│   task queue (Postgres) │
                └─────────────┘        └──────────────────────────┘
                                              │  claim (SKIP LOCKED)
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                     worker 1            worker 2     …       worker N   (Fly process group, autoscaled)
                          │  crawl (CC-first → live via proxy) → extract → normalize
                          ▼
                ┌──────────────────────────────────────────────┐
                │              Postgres (Master DB)              │
                │  contacts · bio_urls registry · observations  │
                │  crawl_tasks · schedules · change_feed         │
                └──────────────────────────────────────────────┘
                          ▲ scheduler tick (recurring re-crawls)
                          ▼ diff engine → new-employee feed → alerts/export
```

Web stays a single machine (UI/API + enqueue). Workers are a horizontally-scaled Fly **process
group**. Postgres is the shared system of record **and** the queue/schedule/feed store.

---

## 3. Phases

### Phase 0 — Domain acquisition *(top of the funnel)*

**Why:** the app takes "a CSV of domains" as input; at scale that list must be *generated*. This is
the upstream stage that feeds everything else. Bias toward where extraction wins — small/local/
professional-services firms with bio pages (realtors, insurance/financial advisors, lawyers,
accountants), which are also the easiest domains to source in bulk.

**Scope — a `domains` table** (`domain, source, vertical, rank, discovered_at, status`), deduped by
**registrable domain** (public-suffix list; strip `www`), junk-filtered (parked pages, aggregators
we don't want crawled as one domain). Populated by, in priority order:

1. **Vertical directory harvesting** *(highest ROI — reuses existing crawler/adapters)*: directories
   we already crack (century21/remax/HomeSmart/Morgan Stanley) are structured lists of thousands of
   firms + their own sites. Output `(company, domain, vertical)`.
2. **Common Crawl domain/host index** *(broadest, free)*: the **columnar URL index** (Athena/DuckDB on
   S3) → hosts whose URLs contain bio-path signals (`/team`, `/our-people`, `/agents`, `/attorneys`,
   `/about-us`); the **host/domain web graph** → the full domain universe, rankable by centrality.
3. **Name → domain resolution** *(when starting from names)*: **serper.dev** (already integrated) —
   search the company name, take the top organic domain; Clearbit-style APIs as a higher-accuracy upgrade.
4. **Public registries / aggregators**: state license boards (real estate / insurance / law / medical),
   SEC EDGAR, Google Maps/Yelp by category+geo, Crunchbase, association member directories.
5. **Link-graph expansion**: from known domains, follow CC's web graph / on-page links ("partners,"
   association badges) to adjacent firms in the same vertical.

**Recommended mix:** (1) directories + (2) CC bio-path filtering + (3) serper. The `domains` table
becomes the source the crawl queue (Phase 2) pulls from. See [[cc-scale-and-discovery]], [[site-search]].

**Effort:** ~1–2 weeks for the framework + ~0.5–1 week per directory/registry source · **Cost:** serper
(~\$ per 1k searches, already in use); CC Athena ~\$5/TB scanned · **Risk:** Low–Medium (dedup/junk
filtering quality; aggregator-vs-target classification).

### Phase 1 — Postgres migration *(the keystone)*
**Why:** `node:sqlite` on one volume is the single thing preventing a worker fleet. Everything
else depends on a shared, multi-writer store.

**Scope:**
- Stand up Fly Postgres (or managed PG). Rewrite the `db.js` layer (`upsertMany`, `query`,
  `facets`, `stats`, `existingUrls`, etc.) against `pg`. **Keep `normalize.js` + email-keyed upsert
  + sheet-sync semantics identical** at the write layer.
- New tables beyond `contacts`: `bio_urls` (per-domain URL registry) and `observations` (what was
  seen, when) — lays the groundwork for Phases 3–4.
- One-time migration of the ~232k contacts; dual-read/verify before cutover.

**Effort:** ~1.5–2 weeks · **Cost:** ~$30–150/mo (starter→mid PG) · **Risk:** Medium (query rewrites,
data migration, txn semantics). **Mitigation:** migrate read paths first, shadow-write, verify row
counts + spot-check, then cut over.

**Alternative considered:** LiteFS (distributed SQLite) — lighter migration, but writes still
serialize on one primary; wrong fit for many concurrent crawl-writers. **Recommend Postgres.**

### Phase 2 — Queue + worker fleet *(throughput)*
**Why:** move from one in-process job to many durable, retryable per-URL tasks across machines.

**Scope:**
- `crawl_tasks` table claimed with `SELECT … FOR UPDATE SKIP LOCKED` (no extra infra), **or**
  Redis + BullMQ (richer scheduling/retries, adds a Redis dependency). **Recommend Postgres-queue
  first** — one less moving part; revisit BullMQ only if we need its features.
- Split Fly into **web** + **worker** process groups; scale workers with `fly scale count` (later,
  autoscale on queue depth). Unit of work = one URL/domain; workers write straight to Postgres
  (no in-RAM `recordsByEmail` accumulation).
- Per-task attempts/backoff replaces coarse whole-job resume; preserve the existing concurrency
  caps per worker (`HOST_CONCURRENCY`, etc.) for politeness.

**Effort:** ~2–3 weeks · **Cost:** +~$30–60/mo per worker machine (scale to fleet) · **Risk:** Medium
(claim/visibility-timeout correctness, idempotent writes). **Mitigation:** idempotent upserts make
re-delivery safe; start with 2–3 workers and measure.

### Phase 3 — Discovery union + coverage *(maximize BIO URLs)*
**Why:** turn 5 separate discovery modes into one growing, deduped per-domain catalog.

**Scope:**
- Per domain, union: **sitemap-first → CC bio-discovery → site-search (serper) gaps → whole-domain
  crawl fallback**, dedup by canonical URL into the `bio_urls` registry
  (`domain, url, first_seen, last_crawled, last_status, contact_email`).
- **Coverage metric** per domain + trend over time (bio URLs found, % yielding a contact).
- **At scale:** evaluate the **Common Crawl columnar index (Athena/S3)** for bulk URL discovery
  instead of per-domain CDX calls — directly lifts the `CC_CONCURRENCY=1` discovery ceiling.

**Effort:** ~1.5–2 weeks (+~1 week if adding Athena) · **Cost:** ~$0 (serper already in use); Athena
~\$5/TB scanned (targeted queries are cheap) · **Risk:** Low–Medium (canonicalization/dedup quality).

### Phase 4 — Scheduler + diff engine + new-employee feed *(the monitoring product)*
**Why:** this is the enterprise differentiator and the stated end goal.

> **MVP SHIPPED (2026-06-25) — sitemap-diff new-hire detection, on the current SQLite box.**
> `sitemap-monitor.js` watches the **bio-dedicated child sitemaps** (found by `cc-engine.discoverBioSitemaps`,
> which scores each child's bio-URL ratio — `agents-sitemap.xml` qualifies, `blog-sitemap.xml` doesn't),
> and on a schedule (`MONITOR_ENABLED=1`, `MONITOR_INTERVAL_HOURS=24`) diffs each child's URL set vs a
> stored baseline. New URL → candidate **new hire** (auto-extracted via the CC-first webpage pipeline →
> Master DB); disappeared URL → candidate **departure**. Two cost levers keep a pass ~free: we watch only
> bio-dedicated children, and a child whose `<lastmod>` in its parent index is unchanged is **not refetched**
> (content-hash fallback when no lastmod). New tables in `db.js`: `watched_sitemaps`, `bio_urls` (baseline),
> `observations` (change feed). UI at **`/monitor`** (add domains/sitemaps, watchlist, change feed, run-now);
> API under `/api/monitor/*`. Selftests: `npm run selftest:monitor` (15) + 5 new engine tests. Still TODO
> below: configurable per-domain schedules, departure→contact reconciliation, alerts/webhook, and lifting
> off SQLite onto the Phase 1/2 worker fleet for columnar-scale watch counts.

**Scope:**
- **Scheduler:** `schedules` table + a tick worker (or Fly scheduled machines) → recurring re-crawl
  per domain (weekly/monthly, configurable).
- **Diff engine:** on each run, compare fresh discovery + extraction against the registry/contacts:
  - **new bio URL** (in sitemap/CC, not seen before) → likely **new hire** → extract + flag `new`
  - **new contact** (email/name not in DB) → flag `new employee`
  - **disappeared** URL/contact → likely **departure**
- **Change feed + alerts:** `change_feed` table → "new employees this week" view + CSV export +
  optional webhook/email/Slack.
- **Killer-feature note:** *sitemap-diff* — re-fetch the sitemap, diff the URL set; new URLs are new
  bios at near-zero extra cost. Highest signal-per-dollar of the whole system.

**Effort:** ~2–3 weeks · **Cost:** minimal beyond the recurring crawl compute/proxy · **Risk:**
Low–Medium (mostly straightforward once P1/P3 data model exists).

---

## 4. Infra decisions (recommendations)

| Decision | Options | Recommendation |
|---|---|---|
| Master DB | SQLite (today) · **Postgres** · LiteFS | **Postgres** — only option that supports a multi-writer worker fleet |
| Queue | **Postgres `SKIP LOCKED`** · Redis/BullMQ | **Postgres-queue** first (no new infra); BullMQ only if needed |
| Bulk URL discovery | per-domain CDX (today) · **CC columnar/Athena** | Add **Athena** in Phase 3 when domain count makes CDX the bottleneck |
| Worker scaling | fixed count · **autoscale on queue depth** | Start fixed (2–3), autoscale later |
| Live fetch at scale | direct (blocked) · **NetNut proxy** | Proxy + **CC-first** to minimize paid bandwidth |

---

## 5. Rough cost model at scale (thousands of domains, weekly monitoring)

- **Fly compute:** web (1×8GB) + worker pool (3–8 × 4GB perf) ≈ **$150–500/mo** (scales with fleet)
- **Postgres:** **$50–300/mo** (millions of contacts is small for PG; sized for IOPS/connections)
- **Proxy (NetNut):** the **big variable** — bandwidth-based. CC-first keeps live fetches low;
  model from the % of pages not in Common Crawl. Could range **$hundreds–low-thousands/mo** under
  heavy live volume; much less if mostly CC-served.
- **Common Crawl:** free data; Athena ~\$5/TB scanned (targeted = cheap).
- **Ballpark infra (excl. proxy):** **~$250–1,000/mo**, scaling with the worker fleet.

> The dominant lever on cost is **CC-first coverage** (free) vs **live fetch** (paid proxy bandwidth).
> Maximizing CC reads is both a performance and a cost strategy.

---

## 6. Cross-cutting concerns

- **Politeness / robots / ToS:** keep per-host rate limits (`HOST_CONCURRENCY`); honor robots (already
  done in live crawl); enterprise scale raises legal/ToS scrutiny — keep crawls polite and auditable.
- **Data quality at scale:** dedup is already email-keyed; add canonical-URL dedup for the registry;
  keep email **modelling** clearly labelled `Modelled` (never mistaken for verified).
- **Observability:** metrics for URLs/sec, coverage %, task failures, proxy spend, new-employees/week.
- **Idempotency:** all writes are upserts → safe under task re-delivery and re-crawls.

---

## 7. Sequencing & milestones

1. **P1 Postgres** → foundation (worker fleet becomes possible).
2. **P2 Queue + workers** → horizontal throughput for thousands of sites.
3. **P3 Discovery union + coverage** → grow/measure the BIO-URL catalog.
4. **P4 Scheduler + diff + feed** → the monitoring product (new-employee detection).

**Total:** ~8–12 weeks of focused work for the full system, delivering value at each phase.
A faster proof-of-value path: a **sitemap-diff monitoring MVP** can run on the *current* SQLite
system (Phase 4 logic, narrow scope) to validate the new-employee detection before the migration.

---

## 8. Success metrics

- **Throughput:** domains/day, URLs/sec sustained.
- **Coverage:** BIO URLs per domain; % of known employees captured; trend over time.
- **Monitoring:** new employees detected/week; detection latency (hire → flagged); false-positive rate.
- **Cost efficiency:** CC-served % (free) vs live-fetched % (paid); $ per 1k contacts refreshed.
