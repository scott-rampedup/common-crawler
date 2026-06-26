/**
 * migrate-to-pg.js — one-time copy of the SQLite contacts store into Postgres.
 * ---------------------------------------------------------------------------
 * Reads every row from the SQLite `contacts` table (db.js) and upserts it into the shared Postgres
 * store (db-pg.js). Idempotent: the score-gated email-keyed upsert merges, so re-running is safe and
 * never downgrades a richer Postgres row (e.g. one the worker fleet already wrote). Run it where the
 * SQLite file lives AND DATABASE_URL is set — i.e. ON the Fly app machine:
 *
 *   flyctl ssh console -a common-crawler -C "node migrate-to-pg.js"
 *   flyctl ssh console -a common-crawler -C "node migrate-to-pg.js --verify"   # counts only
 *
 * Env: DATA_DIR (SQLite dir), DATABASE_URL, PGSSL=1 for TLS, MIGRATE_CHUNK (default 2000).
 */
const path = require('path');

(async () => {
  const DATA_DIR = process.env.DATA_DIR || __dirname;
  const chunkSize = Number(process.env.MIGRATE_CHUNK) || 2000;
  const verifyOnly = process.argv.includes('--verify');

  const sqlite = require('./db').makeDb(DATA_DIR);
  const pg = await require('./db-pg').makeDb({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL });

  const sqliteTotal = sqlite.count();
  const pgBefore = await pg.count();
  console.log(`SQLite contacts: ${sqliteTotal.toLocaleString()} | Postgres before: ${pgBefore.toLocaleString()}`);
  if (verifyOnly) { await pg.close(); return; }

  // Some legacy rows hold invalid UTF-8 (lone UTF-16 surrogates from bad source encoding) that a
  // UTF-8 Postgres rejects. Round-tripping each string through a Buffer replaces those with U+FFFD,
  // yielding valid UTF-8 without dropping the record.
  const scrub = (rec) => {
    const o = {};
    for (const k of Object.keys(rec)) { const v = rec[k]; o[k] = typeof v === 'string' ? Buffer.from(v, 'utf8').toString('utf8') : v; }
    return o;
  };
  // SQLite each() is synchronous — collect rows, then upsert in async batches.
  const all = [];
  sqlite.each({}, (rec) => all.push(scrub(rec)));
  console.log(`Read ${all.length.toLocaleString()} record(s) from SQLite; upserting into Postgres (chunk ${chunkSize})…`);

  const t0 = Date.now();
  let processed = 0, added = 0, skipped = 0;
  for (let i = 0; i < all.length; i += chunkSize) {
    const chunk = all.slice(i, i + chunkSize);
    try {
      const r = await pg.upsertMany(chunk);
      processed += r.processed; added += r.added;
    } catch (e) {
      // one bad row shouldn't abort the whole migration — retry the chunk row-by-row, skip offenders
      for (const rec of chunk) {
        try { const r = await pg.upsertMany([rec]); processed += r.processed; added += r.added; }
        catch (e2) { skipped++; if (skipped <= 10) console.warn(`  skipped 1 row: ${String(e2.message).slice(0, 90)}`); }
      }
    }
    if (i % (chunkSize * 10) === 0) console.log(`  ${i.toLocaleString()}/${all.length.toLocaleString()} … (pg +${added.toLocaleString()} new, ${skipped} skipped)`);
  }
  const pgAfter = await pg.count();
  const secs = Math.round((Date.now() - t0) / 1000);
  console.log(`Done: upserted ${processed.toLocaleString()} row(s), +${added.toLocaleString()} new, ${skipped} skipped. Postgres now ${pgAfter.toLocaleString()} (was ${pgBefore.toLocaleString()}). ${secs}s.`);
  await pg.close();
})().catch((e) => { console.error('migrate-to-pg failed:', e); process.exit(1); });
