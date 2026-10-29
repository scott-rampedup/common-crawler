/**
 * crawl-queue.js — Postgres work queue for the extraction worker fleet.
 * ---------------------------------------------------------------------
 * One row per bio URL to extract, carrying its Common Crawl WARC pointer so a worker can fetch the
 * archived page directly (the free WARC fast path — no per-URL index lookup, no proxy). Many worker
 * machines pull concurrently via `FOR UPDATE SKIP LOCKED`, so no two workers ever grab the same URL.
 *
 * This is the Phase-2 keystone of SCALING-ROADMAP: the unit of work is ONE URL. Built to ingest the
 * full columnar harvest (3.73M bio URLs) by scaling worker machines horizontally.
 *
 *   const q = await makeQueue(pool);            // share db-pg's pool
 *   await q.enqueueMany(pointers);              // {url,filename,offset,length,timestamp,priority?}[]
 *   await q.enqueueMany(ptrs, { priority: 9 }); // 9 = someone is waiting on these (extension push)
 *   await q.raisePriority(urls, 9);             // lift rows that are ALREADY pending
 *   const batch = await q.claimBatch(200, id);  // atomic, skip-locked, highest priority first
 *   await q.markDoneMany([{url, records}]);     // or q.markError(url, msg)
 */

const STATUSES = ['pending', 'claimed', 'done', 'error'];

// Claim order is priority DESC, created_at ASC. 0 is ordinary bulk work — a Maps export, a sitemap
// sweep, a monthly refresh — and 9 is "someone is waiting on this": a Chrome-extension push, where a
// person has just hit the button on a page and expects the contact to appear. Without this the queue
// was strictly FIFO, so an extension push made behind a 863,965-URL Maps backlog sat behind all of it.
const MAX_PRIORITY = 9;
const PRIORITY = { bulk: 0, extension: 9 };
const clampPrio = (v) => Math.max(0, Math.min(MAX_PRIORITY, Math.trunc(Number(v) || 0)));

async function makeQueue(pool, opts = {}) {
  if (!pool) throw new Error('makeQueue: a pg Pool is required');
  const maxAttempts = Number(opts.maxAttempts) || 3;
  const q = (text, params) => pool.query(text, params);

  // --- schema (idempotent) ---
  await q(`CREATE TABLE IF NOT EXISTS crawl_queue (
    url            TEXT PRIMARY KEY,
    warc_filename  TEXT,
    warc_offset    TEXT,
    warc_length    TEXT,
    warc_timestamp TEXT,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INT  NOT NULL DEFAULT 0,
    worker_id      TEXT,
    claimed_at     TIMESTAMPTZ,
    done_at        TIMESTAMPTZ,
    records        INT  DEFAULT 0,
    error          TEXT,
    created_at     TIMESTAMPTZ DEFAULT now()
  )`);
  await q(`ALTER TABLE crawl_queue ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0`);
  // Additive, and deliberately SEPARATE from `records`. `records` says extraction found a person;
  // this says that person was then discarded for having no email address, which is the largest silent
  // loss in the pipeline. DEFAULT 0 means a worker still on the previous image simply never writes it,
  // so a mixed-image fleet under-reports rather than reporting something false.
  await q(`ALTER TABLE crawl_queue ADD COLUMN IF NOT EXISTS no_email INT NOT NULL DEFAULT 0`);
  // Partial index keeps claim() fast: it only ever scans pending rows. It must LEAD with priority, or
  // the new claim order costs a sort over every pending row — 800k of them during a Maps backlog.
  await q(`CREATE INDEX IF NOT EXISTS idx_queue_pending_prio ON crawl_queue (priority DESC, created_at) WHERE status = 'pending'`);
  // Superseded: (priority DESC, created_at) answers everything the created_at-only index did, and a
  // second partial index on the same rows is pure write cost on the hot insert/claim path. Dropped
  // after the replacement exists, so there is no window where claim() has no index to use.
  await q(`DROP INDEX IF EXISTS idx_queue_pending`);
  await q(`CREATE INDEX IF NOT EXISTS idx_queue_status  ON crawl_queue (status)`);

  // Insert pointers, skipping URLs already queued (idempotent re-loads). pointers:
  // [{url, filename, offset, length, timestamp}]. Returns { inserted }.
  //
  // Bulk path: load into a SESSION-LOCAL temp staging table (uncontended — no index, invisible to the
  // fleet), then do ONE `INSERT ... SELECT ... ON CONFLICT` merge into crawl_queue. This replaces
  // thousands of small INSERTs that each fought the 12 workers' claims/marks for locks on the live
  // table — the load-vs-drain contention that capped bulk loads at ~150/s. Staging rows go in via
  // native `unnest` arrays (5 array params, no 65535-param cap → big batches). Call with a large
  // LOAD_CHUNK (e.g. 100000) so the merge runs few, big passes.
  //
  // `priority` sets the claim order for everything in this call; a pointer carrying its own `priority`
  // overrides it, which is how one feed pass loads a mixed batch (an extension push that arrived while
  // a Maps export was still draining) in a single merge. An URL ALREADY in the table keeps its row and
  // its priority — see raisePriority() for the path that lifts one.
  async function enqueueMany(pointers, { priority = PRIORITY.bulk } = {}) {
    const rows = [];
    const seen = new Set();
    for (const p of (pointers || [])) {
      const url = String(p && p.url || '').trim();
      if (!url || seen.has(url)) continue;
      seen.add(url);
      rows.push([url, p.filename || null,
        p.offset == null ? null : String(p.offset),
        p.length == null ? null : String(p.length),
        p.timestamp || null,
        clampPrio(p.priority == null ? priority : p.priority)]);
    }
    if (!rows.length) return { inserted: 0 };
    const { from: copyFrom } = require('pg-copy-streams');
    // COPY text-format escaping: NULL -> \N; escape backslash/newline/CR/tab in values.
    const esc = (v) => (v == null ? '\\N' : String(v).replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t'));
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`CREATE TEMP TABLE _stage_q (url TEXT, warc_filename TEXT, warc_offset TEXT,
        warc_length TEXT, warc_timestamp TEXT, priority INT) ON COMMIT DROP`);
      // Stream rows into staging via COPY — no per-row bind params or array-literal parsing (the cap
      // on the unnest path). Then ONE ON CONFLICT merge into crawl_queue.
      const cp = client.query(copyFrom(`COPY _stage_q (url, warc_filename, warc_offset, warc_length, warc_timestamp, priority) FROM STDIN`));
      await new Promise((resolve, reject) => {
        cp.on('error', reject);
        cp.on('finish', resolve);
        (async () => {
          for (const r of rows) {
            if (!cp.write(r.map(esc).join('\t') + '\n')) await new Promise((res) => cp.once('drain', res));
          }
          cp.end();
        })().catch(reject);
      });
      const res = await client.query(
        `INSERT INTO crawl_queue (url, warc_filename, warc_offset, warc_length, warc_timestamp, priority)
         SELECT url, warc_filename, warc_offset, warc_length, warc_timestamp, COALESCE(priority, 0) FROM _stage_q
         ON CONFLICT (url) DO NOTHING`);
      await client.query('COMMIT');
      return { inserted: res.rowCount };
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
  }

  // Atomically claim up to `n` pending URLs for this worker. FOR UPDATE SKIP LOCKED lets many workers
  // claim disjoint batches with zero coordination. Returns [{url, filename, offset, length, timestamp}].
  //
  // ORDER BY priority DESC first, created_at second: within one priority it is still strict FIFO, so
  // bulk work keeps its old behaviour exactly and only an explicitly-raised URL jumps. A batch in
  // flight is NOT interrupted — the highest priority pending work is taken by the next claim, which is
  // at most one batch (200 URLs, seconds) away.
  async function claimBatch(n, workerId) {
    const lim = Math.max(1, Math.min(1000, Number(n) || 100));
    const res = await q(
      `UPDATE crawl_queue SET status='claimed', worker_id=$1, claimed_at=now(), attempts=attempts+1
       WHERE url IN (
         SELECT url FROM crawl_queue WHERE status='pending'
         ORDER BY priority DESC, created_at FOR UPDATE SKIP LOCKED LIMIT ${lim}
       )
       RETURNING url, warc_filename, warc_offset, warc_length, warc_timestamp`,
      [String(workerId || 'worker')]);
    return res.rows.map((r) => ({
      url: r.url, filename: r.warc_filename, offset: r.warc_offset,
      length: r.warc_length, timestamp: r.warc_timestamp,
    }));
  }

  // Mark a batch done (status='done'), recording how many records each URL yielded. ONE bulk UPDATE
  // from a VALUES list instead of 200 round-trips/batch — the fetch was cross-region-latency-bound on
  // these per-row UPDATEs once the S3 fetch path removed the download bottleneck.
  async function markDoneMany(items) {
    const byUrl = new Map();
    for (const it of (items || [])) if (it && it.url) byUrl.set(it.url, [it.records | 0, it.noEmail | 0]);
    const entries = [...byUrl.entries()];
    if (!entries.length) return 0;
    const maxRows = 20000;   // 3 params/row now, stay under the 65535 bind-param cap
    let n = 0;
    for (let i = 0; i < entries.length; i += maxRows) {
      const chunk = entries.slice(i, i + maxRows);
      const params = [];
      const tuples = chunk.map(([url, [records, noEmail]]) => {
        params.push(url, records, noEmail);
        // cast the FIRST tuple so Postgres infers the VALUES column types (text, int, int)
        return params.length === 3
          ? `($1::text, $2::int, $3::int)`
          : `($${params.length - 2}, $${params.length - 1}, $${params.length})`;
      });
      const res = await q(
        `UPDATE crawl_queue AS c SET status='done', done_at=now(), records=v.records,
                no_email=v.no_email, error=NULL
         FROM (VALUES ${tuples.join(', ')}) AS v(url, records, no_email)
         WHERE c.url = v.url`,
        params);
      n += res.rowCount;
    }
    return n;
  }

  // Lift pending rows to a higher priority. Returns how many moved.
  //
  // This is not a nicety, it is the half of prioritisation that is easy to miss. bio-feed drops URLs it
  // finds already in crawl_queue BEFORE resolving them (no point paying Athena for a pointer the table
  // already holds), so a push of a URL that is already sitting in a 800k backlog would otherwise be
  // "deduplicated" straight into that backlog's place in the line — queued, acknowledged, and days away.
  // The row is already there; the only thing to change is where it sits.
  //
  // Only `pending` rows move: a claimed row is being worked on right now, and a done row is finished.
  async function raisePriority(urls, priority) {
    const p = clampPrio(priority);
    const list = [...new Set((urls || []).map((u) => String(u || '').trim()).filter(Boolean))];
    if (!list.length || p <= 0) return 0;
    let n = 0;
    for (let i = 0; i < list.length; i += 10000) {
      const res = await q(
        `UPDATE crawl_queue SET priority = $2
         WHERE url = ANY($1::text[]) AND status = 'pending' AND priority < $2`,
        [list.slice(i, i + 10000), p]);
      n += res.rowCount || 0;
    }
    return n;
  }

  // A URL failed. Retry (back to 'pending') until maxAttempts, then park it as 'error'.
  async function markError(url, message) {
    if (!url) return;
    await q(
      // claimed_at is left as now() rather than nulled, so it records WHEN this row was last tried.
      // Nulling it is what defeated the requeueErrors cooldown: that gate treats a null claimed_at as
      // "never attempted, eligible now", and since every failure cleared the column, every error row
      // was eligible on every pass. The pump requeued the same 36 rows every 25 seconds for two days.
      // requeueStale only looks at status='claimed', so a timestamp on an error/pending row is inert.
      `UPDATE crawl_queue
       SET status = CASE WHEN attempts >= $2 THEN 'error' ELSE 'pending' END,
           error = $3, worker_id = NULL, claimed_at = now()
       WHERE url = $1`,
      [url, maxAttempts, String(message || '').slice(0, 500)]);
  }

  // Reclaim rows stuck in 'claimed' (a worker died mid-batch) older than `minutes` -> back to pending.
  // Error rows are NOT a terminal state, and leaving them as one is how a queue grows zombies.
  //
  // markError() parks a row after maxAttempts, and nothing ever looked at it again: 1,871 rows sat there,
  // and their reasons were 'terminated', 'aborted' and 'batch: deadlock detected' -- a worker machine
  // killed mid-batch and a transient Postgres deadlock. None of that says the PAGE is bad, so parking
  // them permanently discarded work for reasons that had nothing to do with the work.
  //
  // So: give them back to the pool, and only give up on a row that has genuinely been tried and tried.
  // Rows at or past `ceiling` are DELETED rather than left sitting -- a row nothing will ever process
  // should not be in a queue of things to process.
  // `cooldownMin` stops the pump and the workers fighting. Without it the loop was: worker fails a row
  // at maxAttempts -> pump resets attempts to 0 -> worker picks it straight back up -> fails again,
  // every 25 seconds forever. The logs showed "recov 144" on repeat, burning slots on rows that were
  // never going to succeed this minute. A row now has to sit still for a while before it is eligible,
  // which turns a hot loop into a slow retry.
  //
  // The cooldown reads claimed_at as "when this row was last tried", which is why markError now leaves
  // a timestamp there instead of NULL. The `claimed_at IS NULL` branch below is only for rows that
  // failed BEFORE that change: they get one free pass, after which they carry a timestamp like the
  // rest and are gated normally. Without that branch those rows would never be retried again.
  async function requeueErrors({ ceiling = 12, limit = 50000, cooldownMin = 30 } = {}) {
    const dead = await q(`DELETE FROM crawl_queue WHERE status='error' AND attempts >= $1`, [ceiling]);
    const back = await q(
      `UPDATE crawl_queue SET status='pending', attempts=0, worker_id=NULL, claimed_at=NULL, error=NULL
         WHERE url IN (SELECT url FROM crawl_queue
                        WHERE status='error' AND attempts < $1
                          AND (claimed_at IS NULL OR claimed_at < now() - ($3 || ' minutes')::interval)
                        LIMIT $2)`,
      [ceiling, limit, String(cooldownMin)]);
    return { requeued: back.rowCount || 0, deleted: dead.rowCount || 0 };
  }

  async function requeueStale(minutes = 30) {
    const res = await q(
      `UPDATE crawl_queue SET status='pending', worker_id=NULL, claimed_at=NULL
       WHERE status='claimed' AND claimed_at < now() - ($1 || ' minutes')::interval`,
      [String(Math.max(1, minutes | 0))]);
    return res.rowCount;
  }

  // Counts by status + total records extracted (for progress/throughput reporting).
  async function stats() {
    const rows = (await q(`SELECT status, COUNT(*) c FROM crawl_queue GROUP BY status`)).rows;
    const out = { pending: 0, claimed: 0, done: 0, error: 0, total: 0 };
    for (const r of rows) { out[r.status] = Number(r.c); out.total += Number(r.c); }
    out.records = Number((await q(`SELECT COALESCE(SUM(records),0) s FROM crawl_queue WHERE status='done'`)).rows[0].s);
    // Pending split by priority, so "is the thing I just pushed at the front?" is answerable without
    // writing SQL. Uses the claim index, and only ever touches pending rows.
    out.pendingByPriority = {};
    for (const r of (await q(`SELECT priority, COUNT(*) c FROM crawl_queue WHERE status='pending'
                              GROUP BY priority ORDER BY priority DESC`)).rows) {
      out.pendingByPriority[Number(r.priority)] = Number(r.c);
    }
    return out;
  }

  return { enqueueMany, claimBatch, markDoneMany, markError, raisePriority, requeueStale, requeueErrors, stats, _pool: pool, STATUSES };
}

module.exports = { makeQueue, STATUSES, PRIORITY, MAX_PRIORITY, clampPrio };
