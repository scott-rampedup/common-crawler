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
 *   await q.enqueueMany(pointers);              // {url,filename,offset,length,timestamp}[]
 *   const batch = await q.claimBatch(200, id);  // atomic, skip-locked
 *   await q.markDoneMany([{url, records}]);     // or q.markError(url, msg)
 */

const STATUSES = ['pending', 'claimed', 'done', 'error'];

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
  // Partial index keeps claim() fast: it only ever scans pending rows.
  await q(`CREATE INDEX IF NOT EXISTS idx_queue_pending ON crawl_queue (created_at) WHERE status = 'pending'`);
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
  async function enqueueMany(pointers) {
    const rows = [];
    const seen = new Set();
    for (const p of (pointers || [])) {
      const url = String(p && p.url || '').trim();
      if (!url || seen.has(url)) continue;
      seen.add(url);
      rows.push([url, p.filename || null,
        p.offset == null ? null : String(p.offset),
        p.length == null ? null : String(p.length),
        p.timestamp || null]);
    }
    if (!rows.length) return { inserted: 0 };
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`CREATE TEMP TABLE _stage_q (url TEXT, warc_filename TEXT, warc_offset TEXT,
        warc_length TEXT, warc_timestamp TEXT) ON COMMIT DROP`);
      const CH = 50000;                                        // keep each unnest array a sane size
      for (let i = 0; i < rows.length; i += CH) {
        const chunk = rows.slice(i, i + CH);
        const c0 = [], c1 = [], c2 = [], c3 = [], c4 = [];
        for (const r of chunk) { c0.push(r[0]); c1.push(r[1]); c2.push(r[2]); c3.push(r[3]); c4.push(r[4]); }
        await client.query(
          `INSERT INTO _stage_q (url, warc_filename, warc_offset, warc_length, warc_timestamp)
           SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])`,
          [c0, c1, c2, c3, c4]);
      }
      const res = await client.query(
        `INSERT INTO crawl_queue (url, warc_filename, warc_offset, warc_length, warc_timestamp)
         SELECT url, warc_filename, warc_offset, warc_length, warc_timestamp FROM _stage_q
         ON CONFLICT (url) DO NOTHING`);
      await client.query('COMMIT');
      return { inserted: res.rowCount };
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
  }

  // Atomically claim up to `n` pending URLs for this worker. FOR UPDATE SKIP LOCKED lets many workers
  // claim disjoint batches with zero coordination. Returns [{url, filename, offset, length, timestamp}].
  async function claimBatch(n, workerId) {
    const lim = Math.max(1, Math.min(1000, Number(n) || 100));
    const res = await q(
      `UPDATE crawl_queue SET status='claimed', worker_id=$1, claimed_at=now(), attempts=attempts+1
       WHERE url IN (
         SELECT url FROM crawl_queue WHERE status='pending'
         ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT ${lim}
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
    for (const it of (items || [])) if (it && it.url) byUrl.set(it.url, it.records | 0);
    const entries = [...byUrl.entries()];
    if (!entries.length) return 0;
    const maxRows = 30000;   // 2 params/row, stay under the 65535 bind-param cap
    let n = 0;
    for (let i = 0; i < entries.length; i += maxRows) {
      const chunk = entries.slice(i, i + maxRows);
      const params = [];
      const tuples = chunk.map(([url, records]) => {
        params.push(url, records);
        // cast the FIRST tuple so Postgres infers the VALUES column types (text, int)
        return params.length === 2 ? `($1::text, $2::int)` : `($${params.length - 1}, $${params.length})`;
      });
      const res = await q(
        `UPDATE crawl_queue AS c SET status='done', done_at=now(), records=v.records, error=NULL
         FROM (VALUES ${tuples.join(', ')}) AS v(url, records)
         WHERE c.url = v.url`,
        params);
      n += res.rowCount;
    }
    return n;
  }

  // A URL failed. Retry (back to 'pending') until maxAttempts, then park it as 'error'.
  async function markError(url, message) {
    if (!url) return;
    await q(
      `UPDATE crawl_queue
       SET status = CASE WHEN attempts >= $2 THEN 'error' ELSE 'pending' END,
           error = $3, worker_id = NULL, claimed_at = NULL
       WHERE url = $1`,
      [url, maxAttempts, String(message || '').slice(0, 500)]);
  }

  // Reclaim rows stuck in 'claimed' (a worker died mid-batch) older than `minutes` -> back to pending.
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
    return out;
  }

  return { enqueueMany, claimBatch, markDoneMany, markError, requeueStale, stats, _pool: pool, STATUSES };
}

module.exports = { makeQueue, STATUSES };
