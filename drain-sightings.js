/**
 * drain-sightings.js — ship sightings_spool into the Public Prospects ledger, then clear it.
 *
 * WHY THIS EXISTS
 * `contacts` is email-keyed, so every page beyond the first that an address appears on is
 * discarded — once by the in-batch Map dedupe in db-pg.upsertMany, again by
 * `ON CONFLICT ... WHERE EXCLUDED.score >= contacts.score`. Each discarded row carried its own
 * 'Web Source URL' + 'Time Stamp', i.e. a citable sighting, and those are unrecoverable once
 * dropped. upsertMany now appends them to `sightings_spool` first; this drains that spool.
 *
 * WHY A SPOOL AND NOT A DIRECT WRITE
 * Extraction must never gain a runtime dependency on a second database. If public-prospects-db is
 * down, sightings queue up in phase1's own Postgres and nothing in the crawl path notices.
 * The spool is NOT a store — leave it undrained and it grows without bound inside the production
 * DB, so run this on a schedule.
 *
 *   DATABASE_URL=…            phase1 Postgres (source of the spool)
 *   PP_DATABASE_URL=…         public-prospects-db (destination ledger)
 *   node drain-sightings.js [--apply] [--batch 5000]     (default = dry run)
 */
const { Pool } = require('pg');

const APPLY = process.argv.includes('--apply');
const bIdx = process.argv.indexOf('--batch');
const BATCH = bIdx > -1 ? Math.max(100, parseInt(process.argv[bIdx + 1], 10) || 5000) : 5000;

// The crawler reads Common Crawl first and falls back to a live fetch. That distinction matters
// downstream: a CC-served page carries a real capture date (a citation), a live fetch only carries
// the time we happened to look. Map it onto the ledger's source ids so the tier stays honest.
function sourceIdFor(src) {
  return /common\s*crawl|^cc\b|archive/i.test(String(src || '')) ? 'common_crawl' : 'contact_crawl';
}

(async () => {
  if (!process.env.DATABASE_URL) { console.error('need DATABASE_URL'); process.exit(1); }
  if (!process.env.PP_DATABASE_URL) { console.error('need PP_DATABASE_URL'); process.exit(1); }

  const src = new Pool({ connectionString: process.env.DATABASE_URL, max: 4 });
  const dst = new Pool({ connectionString: process.env.PP_DATABASE_URL, max: 4 });

  const { rows: [{ pending }] } = await src.query(
    `SELECT count(*)::bigint AS pending FROM sightings_spool WHERE synced_at IS NULL`);
  console.log(`spool: ${Number(pending).toLocaleString()} unsynced row(s)`);
  if (!APPLY) { console.log('DRY RUN — pass --apply to drain'); await src.end(); await dst.end(); return; }

  let moved = 0, skipped = 0;
  for (;;) {
    const { rows } = await src.query(
      `SELECT id, email, page_url, captured_at, source, domain, payload
         FROM sightings_spool WHERE synced_at IS NULL ORDER BY id LIMIT $1`, [BATCH]);
    if (!rows.length) break;

    // Normalization and hashing happen in Postgres via pp_normalize_email(), so the ledger has ONE
    // definition of a canonical address rather than one per producer — and the hash is taken of the
    // NORMALIZED form, so these rows join the bulk-loaded ones correctly.
    const params = [];
    const rowSql = rows.map((r) => {
      const p = r.payload || {};
      const q = (v) => { params.push(v === undefined ? null : v); return '$' + params.length; };
      return `(${[
        `'${sourceIdFor(r.source)}'`,
        `pp_normalize_email(${q(r.email)})`,
        q(r.email),
        `sha256(pp_normalize_email(${q(r.email)})::bytea)`,
        `pp_registrable(split_part(pp_normalize_email(${q(r.email)}), '@', 2))`,
        q(r.page_url),
        `pp_registrable(${q(r.page_url)})`,
        `pp_registrable(${q(r.page_url)}) = pp_registrable(split_part(pp_normalize_email(${q(r.email)}), '@', 2))`,
        q(p['Email Type'] === 'Role-Based' ? 'role' : 'personal'),
        `'corporate'`,
        `'B'`,
        `nullif(${q(r.captured_at)}, '')::timestamptz`,
        q(p['First'] || p['Last'] ? `${p['First'] || ''} ${p['Last'] || ''}`.trim() : null),
        q(p['Title'] || p['Position'] || null),
        q(p['Description'] || null),
      ].join(', ')})`;
    }).join(', ');

    const sql = `INSERT INTO sightings
      (source_id, email_norm, email_raw, email_sha256, email_registrable,
       page_url, page_registrable, domain_aligned, mailbox_type, domain_type,
       provenance_tier, captured_at, person_name, job_title, context)
      VALUES ${rowSql}`;

    const client = await dst.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql, params);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      client.release();
    }

    const ids = rows.map((r) => r.id);
    // Delete rather than just stamping synced_at: the spool is transient, and letting it
    // accumulate is how a production volume fills up quietly.
    await src.query(`DELETE FROM sightings_spool WHERE id = ANY($1::bigint[])`, [ids]);
    moved += rows.length;
    if (moved % 50000 < BATCH) console.log(`  drained ${moved.toLocaleString()}`);
  }

  console.log(`DONE: drained ${moved.toLocaleString()} sighting(s), skipped ${skipped}`);
  await src.end();
  await dst.end();
})().catch((e) => { console.error(e); process.exit(1); });
