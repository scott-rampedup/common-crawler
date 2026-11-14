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
 * OS_SYNC_LAG_MS (how far short of now the cursor stops, default 120000 -- see the note on the query),
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
  // How far behind "now" the cursor is allowed to read. Must exceed the longest write transaction;
  // bulk merges are the long ones. 120s is ample for those and still well inside a poll cycle.
  const lagMs = Number(opts.lagMs || process.env.OS_SYNC_LAG_MS || 120000);
  const pool = new Pool({ connectionString, max: 2, ssl: opts.ssl || !!process.env.PGSSL });
  // An IDLE pg client can emit 'error' on its own -- the server restarts, a NAT drops the connection, a
  // reaper kills a long-lived session. node treats an unhandled 'error' event as fatal, so without this
  // listener the whole process exits. That is not theoretical: the syncer died at 2026-10-13T19:49:17Z
  // with exit_code=1 after 9,930 pool uses, printing a pg Client dump and nothing else, and because its
  // machine runs restart:no it stayed dead for 18 hours. The pool replaces the broken client by itself;
  // all that was missing was somebody listening.
  pool.on('error', (e) => console.error('[os-sync] idle pg client error (pool will replace it):', e.message));
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
        // across rows that share an updated_at timestamp (no re-scan loop).
        //
        // IT DOES NOT GUARANTEE NO SKIPS, which the previous version of this comment claimed. updated_at
        // is stamped when a row is WRITTEN; the row becomes visible when its transaction COMMITS. A bulk
        // merge that stamps 05:48:23 and commits at 05:48:41 is invisible to every read in between -- and
        // if the cursor passed 05:48:23 during those 18 seconds, that row is behind the watermark the
        // moment it appears and is never read again. Measured 2026-11-14: 0.87% of merged contacts were
        // in Postgres, counted in a job's receipt, and absent from OpenSearch -- so they did not exist
        // as far as the Contact Crawler UI was concerned. Every example was stamped two days earlier.
        //
        // The cursor therefore stops short of the present by `lagMs`. A transaction that commits within
        // that window is still ahead of the watermark when it lands, so it is picked up normally. The
        // cost is that a new contact takes up to lagMs longer to become searchable, which is a trade
        // worth making against losing it silently and permanently.
        // .toISOString() is not cosmetic. `updated_at` is a TEXT column holding ISO-8601 UTC, so this
        // comparison is lexicographic, not chronological -- the two only agree while every value has
        // the same shape. Handing node-postgres a Date lets IT choose the serialization, and outside a
        // UTC container that is a local-offset string: '...T11:06:44.277-05:00' sorts below a stored
        // '...T16:04:55.751Z' on the hour digits alone, so the guard would exclude rows it should pass
        // and quietly stall the cursor. Pin the format to the one the column is written in.
        const safeTo = new Date(Date.now() - lagMs).toISOString();
        const rows = (await pool.query(
          `SELECT * FROM contacts WHERE (updated_at, email) > ($1, $2) AND updated_at <= $4
           ORDER BY updated_at, email LIMIT $3`, [wm.updated_at, wm.email, batch, safeTo])).rows;
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
