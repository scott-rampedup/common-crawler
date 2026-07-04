/**
 * migrate-to-opensearch.js — bulk-load the Postgres contacts into the OpenSearch production store.
 * Keyset-paginates by email (the PK) so it streams millions without a cursor, and bulk-upserts in
 * chunks. Idempotent (score-gated upsert) — safe to re-run / resume.
 *
 *   OPENSEARCH_ENDPOINT=… DATABASE_URL=… node migrate-to-opensearch.js
 */
const { Pool } = require('pg');
const { makeClient, ensureIndex, rowToDoc, bulkUpsert } = require('./opensearch');

(async () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL, max: 2 });
  const os = makeClient(process.env.OPENSEARCH_ENDPOINT);
  console.log('index', (await ensureIndex(os)) ? 'created' : 'exists');

  const BATCH = 5000;
  let last = process.env.RESUME_FROM || '', total = 0, errs = 0;
  const t0 = Date.now();
  for (;;) {
    const rows = (await pool.query(
      'SELECT * FROM contacts WHERE email > $1 ORDER BY email LIMIT $2', [last, BATCH])).rows;
    if (!rows.length) break;
    const r = await bulkUpsert(os, rows.map(rowToDoc));
    total += rows.length; errs += r.errors;
    last = rows[rows.length - 1].email;
    if (total % 50000 === 0 || rows.length < BATCH) {
      const secs = (Date.now() - t0) / 1000;
      console.log(`  ${total.toLocaleString()} indexed | ${errs} err | ${Math.round(total / secs)}/s | last=${last.slice(0, 40)}`);
    }
  }
  console.log(`DONE: ${total.toLocaleString()} contacts -> OpenSearch, ${errs} errors, ${Math.round((Date.now() - t0) / 1000)}s`);
  await pool.end();
})().catch((e) => { console.error('migrate error:', e); process.exit(1); });
