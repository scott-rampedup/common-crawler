/**
 * os-sync-run.js — the Postgres -> OpenSearch delta sync, on its OWN machine.
 * ---------------------------------------------------------------------------
 * Step 2 of the rebuild: nothing that reads the whole contacts table belongs inside the process that
 * serves the UI. This is the same `startSync` the web app used to host, lifted out verbatim -- the sync
 * logic is unchanged, only its address is. The web app disables its copy with OS_SYNC=0, so exactly one
 * syncer owns the `sync_state.os_sync` watermark at a time.
 *
 * Why it had to move, concretely: ui-server.js already aborted twice with a heap error while running the
 * jobs it hosts, and searches time out when a drain is in flight. The syncer is a tight loop that pulls
 * every changed contact row out of Postgres and bulk-writes it to OpenSearch; under the 16-worker fleet
 * that is tens of thousands of rows a minute sharing four CPUs with the request path.
 *
 *   node os-sync-run.js
 *
 * Env: OPENSEARCH_ENDPOINT, DATABASE_URL (both app secrets, so a launched machine has them).
 *      OS_SYNC_INTERVAL_MS (default 5000 here -- the web app used 20000 to stay out of its own way,
 *                           which it no longer has to)
 *      OS_SYNC_BATCH       (default 5000)
 *      PGSSL=1             to enable TLS
 *      OS_SYNC_PORT        Postgres port (default 5433 -- direct, not the 5432 pooler, because this is a
 *                          long-lived connection doing continuous work)
 *
 * Runs until killed. Logs a heartbeat so an idle syncer is distinguishable from a dead one -- the
 * failure mode this whole exercise is about.
 */
const { startSync } = require('./opensearch-sync');

const PORT = process.env.OS_SYNC_PORT || '5433';
const raw = process.env.DATABASE_URL || '';
// Long-running work goes direct to Postgres, not through the :5432 pooler.
const connectionString = raw.replace(/:(5432|5433)\//, ':' + PORT + '/');

if (!process.env.OPENSEARCH_ENDPOINT || !raw) {
  console.error('os-sync-run: need OPENSEARCH_ENDPOINT and DATABASE_URL');
  process.exit(1);
}

const intervalMs = Number(process.env.OS_SYNC_INTERVAL_MS) || 5000;
const batch = Number(process.env.OS_SYNC_BATCH) || 5000;

console.log(`os-sync-run: starting — pg port ${PORT}, interval ${intervalMs}ms, batch ${batch}`);

const handle = startSync({
  endpoint: process.env.OPENSEARCH_ENDPOINT,
  connectionString,
  ssl: !!process.env.PGSSL,
  intervalMs,
  batch,
});

// A syncer that has caught up prints nothing (startSync only logs when it moved rows), which is
// indistinguishable from a syncer that has died. Say so periodically instead.
const HEARTBEAT_MS = Number(process.env.OS_SYNC_HEARTBEAT_MS) || 300000;
const t0 = Date.now();
const beat = setInterval(() => {
  console.log(`os-sync-run: alive, ${Math.round((Date.now() - t0) / 60000)}m — see [os-sync] lines for rows moved`);
}, HEARTBEAT_MS);

function shutdown(sig) {
  console.log(`os-sync-run: ${sig} — stopping sync`);
  clearInterval(beat);
  try { handle.stop(); } catch (e) { /* already down */ }
  // Give the in-flight batch a moment to finish its watermark write, then go.
  setTimeout(() => process.exit(0), 2000);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
