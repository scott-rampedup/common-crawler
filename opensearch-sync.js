/**
 * opensearch-sync.js — keep the OpenSearch production store current with the Postgres processing DB.
 * -----------------------------------------------------------------------------------------------------
 * The worker fleet writes new/updated contacts to Postgres; this background syncer ships those deltas
 * into OpenSearch so the UI search reflects them without a full re-migration. It keyset-scans by
 * (updated_at, email) — every insert/edit bumps updated_at, so the scan picks them up in order — and
 * persists a watermark in a tiny sync_state table, so an app restart resumes instead of re-scanning.
 *
 * Deletes and email-changes are handled separately by an immediate dual-write at the UI endpoints
 * (a vanished row can't be seen by an updated_at scan). This syncer only ADDS/UPDATES.
 *
 *   const { startSync } = require('./opensearch-sync');
 *   const h = startSync({ endpoint, connectionString, ssl });   // h.stop() to halt
 *
 * Env knobs: OS_SYNC_INTERVAL_MS (idle poll, default 20000), OS_SYNC_BATCH (default 2000),
 * OS_SYNC_FROM (initial watermark updated_at when sync_state is empty; default '' = full one-time catch-up).
 */
const { Pool } = require('pg');
const { makeClient, rowToDoc, bulkUpsert } = require('./opensearch');

async function ensureState(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS sync_state (k TEXT PRIMARY KEY, v TEXT)`);
}
async function loadWatermark(pool) {
  const r = await pool.query(`SELECT v FROM sync_state WHERE k = 'os_sync'`);
  if (r.rows[0] && r.rows[0].v) { try { return JSON.parse(r.rows[0].v); } catch (_) { /* corrupt -> reseed */ } }
  return null;
}
async function saveWatermark(pool, wm) {
  await pool.query(
    `INSERT INTO sync_state (k, v) VALUES ('os_sync', $1) ON CONFLICT (k) DO UPDATE SET v = $1`,
    [JSON.stringify(wm)]);
}

function startSync(opts = {}) {
  const endpoint = opts.endpoint || process.env.OPENSEARCH_ENDPOINT;
  const connectionString = opts.connectionString || process.env.DATABASE_URL;
  if (!endpoint || !connectionString) {
    console.error('[os-sync] missing OPENSEARCH_ENDPOINT / DATABASE_URL — sync DISABLED');
    return { stop() {} };
  }
  const intervalMs = Number(opts.intervalMs || process.env.OS_SYNC_INTERVAL_MS || 20000);
  const batch = Number(opts.batch || process.env.OS_SYNC_BATCH || 2000);
  const pool = new Pool({ connectionString, max: 2, ssl: opts.ssl || !!process.env.PGSSL });
  const os = makeClient(endpoint);
  let wm = null, stopped = false, timer = null;

  async function cycle() {
    if (stopped) return;
    try {
      if (!wm) {
        await ensureState(pool);
        wm = await loadWatermark(pool);
        if (!wm) {
          wm = { updated_at: process.env.OS_SYNC_FROM || '', email: '' };
          console.log(`[os-sync] no watermark — catching up from updated_at > '${wm.updated_at}'`);
        } else {
          console.log(`[os-sync] resuming from watermark updated_at=${wm.updated_at} email=${wm.email}`);
        }
      }
      let moved = 0;
      for (;;) {
        if (stopped) break;
        // Row-value keyset: strict compound (updated_at, email) cursor guarantees forward progress even
        // across rows that share an updated_at timestamp (no re-scan loop, no skips).
        const rows = (await pool.query(
          `SELECT * FROM contacts WHERE (updated_at, email) > ($1, $2)
           ORDER BY updated_at, email LIMIT $3`, [wm.updated_at, wm.email, batch])).rows;
        if (!rows.length) break;
        await bulkUpsert(os, rows.map(rowToDoc));
        const last = rows[rows.length - 1];
        wm = { updated_at: last.updated_at || '', email: last.email };
        await saveWatermark(pool, wm);
        moved += rows.length;
        if (rows.length < batch) break;                 // caught up to the tail
        await new Promise((r) => setTimeout(r, 25));     // breathe between catch-up batches (PG is under fleet load)
      }
      if (moved) console.log(`[os-sync] +${moved} rows -> OpenSearch (watermark ${wm.updated_at})`);
    } catch (e) {
      console.error('[os-sync] cycle error:', e.message);
    }
    if (!stopped) timer = setTimeout(cycle, intervalMs);
  }
  cycle();
  return { stop() { stopped = true; if (timer) clearTimeout(timer); pool.end().catch(() => {}); } };
}

module.exports = { startSync };
