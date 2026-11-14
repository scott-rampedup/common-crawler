#!/usr/bin/env node
//
// os-reconcile.js — find contacts that are in Postgres but NOT in the OpenSearch store the Contact
// Crawler UI searches, and put them there.
//
// WHY THIS EXISTS. A contact is only delivered when it is SEARCHABLE. Everything upstream can report
// success -- the crawl finishes, the job's receipt says merged, the row is in Postgres -- and the
// person still does not exist as far as anyone using the product is concerned. That gap is not
// theoretical: on 2026-11-14, 0.87% of merged contacts were in Postgres and absent from OpenSearch,
// every sampled one stamped two days earlier and sitting behind the sync watermark where the keyset
// cursor could never reach it again.
//
// opensearch-sync.js now holds its cursor short of the present so a transaction that commits late is
// still ahead of the watermark when it lands. That stops NEW rows being lost. It does nothing for the
// ones already behind the cursor, which is this tool's job: walk Postgres, ask OpenSearch what it
// actually holds, and index whatever is missing.
//
// It is a reconciler, not a re-migration. It reads every row but writes only the absent ones, so it is
// safe to run at any time and cheap to run again -- the second run should find nothing.
//
//   node os-reconcile.js                          # dry run, whole table
//   node os-reconcile.js --write
//   node os-reconcile.js --write --from 2026-11-01 # only rows updated on/after a date
//   node os-reconcile.js --write --limit 50000     # bound the scan
const { Client } = require('pg');
const { makeClient, rowToDoc, bulkUpsert, INDEX } = require('./opensearch');

const argv = process.argv.slice(2);
const arg = (n, d) => { const i = argv.indexOf('--' + n); return i > -1 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : d; };
const has = (f) => argv.includes('--' + f);

const WRITE = has('write');
const FROM = arg('from', '');
const LIMIT = Number(arg('limit', 0)) || 0;
const PAGE = Number(arg('page', 2000)) || 2000;
const CHECK = 500;                                   // mget batch; OpenSearch is happy well above this
const f = (n) => Number(n || 0).toLocaleString();

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT || !process.env.DATABASE_URL) {
    console.error('need OPENSEARCH_ENDPOINT and DATABASE_URL'); process.exit(1);
  }
  const os = makeClient(process.env.OPENSEARCH_ENDPOINT);
  const pg = new Client({ connectionString: process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/'),
    ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false });
  await pg.connect();
  await pg.query("SET statement_timeout='300s'");

  const total = Number((await pg.query('SELECT count(*)::bigint n FROM contacts' + (FROM ? ' WHERE updated_at >= $1' : ''),
    FROM ? [FROM] : [])).rows[0].n);
  console.log(`${WRITE ? 'WRITE' : 'DRY RUN'} — ${f(total)} Postgres contact(s) to verify`
    + (FROM ? ` (updated_at >= ${FROM})` : '') + `\n`);

  // Keyset by (updated_at, email): the same ordering the syncer uses, and the only one that pages a
  // moving table without skipping or repeating.
  let wmAt = FROM || '1970-01-01T00:00:00.000Z', wmEmail = '';
  const st = { scanned: 0, present: 0, missing: 0, indexed: 0, errors: 0, noEmail: 0 };
  const t0 = Date.now();
  const examples = [];

  for (;;) {
    if (LIMIT && st.scanned >= LIMIT) break;
    const rows = (await pg.query(
      `SELECT * FROM contacts WHERE (updated_at, email) > ($1, $2)
       ORDER BY updated_at, email LIMIT $3`, [wmAt, wmEmail, PAGE])).rows;
    if (!rows.length) break;
    const last = rows[rows.length - 1];
    wmAt = last.updated_at; wmEmail = last.email;

    // Only rows with an email can be checked or written — the store is email-keyed.
    const usable = rows.filter((r) => String(r.email || '').trim());
    st.noEmail += rows.length - usable.length;
    st.scanned += rows.length;

    const missing = [];
    for (let i = 0; i < usable.length; i += CHECK) {
      const chunk = usable.slice(i, i + CHECK);
      const r = await os.mget({ index: INDEX, body: { ids: chunk.map((x) => String(x.email).toLowerCase()) }, _source: false });
      ((r.body || r).docs || []).forEach((d, k) => { if (d.found) st.present++; else missing.push(chunk[k]); });
    }
    st.missing += missing.length;
    for (const m of missing) if (examples.length < 8) examples.push(`${String(m.updated_at && m.updated_at.toISOString ? m.updated_at.toISOString() : m.updated_at).slice(0, 19)}  ${m.email}`);

    if (missing.length && WRITE) {
      try {
        const res = await bulkUpsert(os, missing.map(rowToDoc));
        const errs = (res && res.errors) || 0;
        st.indexed += missing.length - errs;
        st.errors += errs;
      } catch (e) { st.errors += missing.length; console.error('  bulk failed: ' + e.message); }
    }

    if (st.scanned % 50000 < PAGE) {
      const rate = Math.round(st.scanned / Math.max(1, (Date.now() - t0) / 1000));
      console.log(`  ${f(st.scanned)} / ${f(total)} scanned · ${f(st.missing)} missing`
        + (WRITE ? ` · ${f(st.indexed)} indexed` : '') + ` · ${rate}/s`);
    }
    if (rows.length < PAGE) break;
  }

  if (WRITE && st.indexed) { try { await os.indices.refresh({ index: INDEX }); } catch (e) { /* best-effort */ } }

  console.log(`\nscanned            ${f(st.scanned)}`);
  console.log(`  already present  ${f(st.present)}`);
  console.log(`  no email (skipped) ${f(st.noEmail)}`);
  console.log(`  MISSING from the Contact Crawler  ${f(st.missing)}`
    + (st.scanned ? `  (${((st.missing / st.scanned) * 100).toFixed(2)}%)` : ''));
  if (WRITE) console.log(`  indexed          ${f(st.indexed)}${st.errors ? `   errors ${f(st.errors)}` : ''}`);
  if (examples.length) { console.log('\n  examples:'); for (const e of examples) console.log('    ' + e); }
  if (!WRITE) console.log('\n(dry run — pass --write to index them)');
  await pg.end();
})().catch((e) => { console.error('FAILED:', e.message, e.stack); process.exit(1); });
