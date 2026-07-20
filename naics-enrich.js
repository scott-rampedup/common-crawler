/**
 * naics-enrich.js — associate a NAICS 6 code + title onto every company from its Category (or, when
 * Category is blank, its Industry) using the curated "Category to NAICS.csv" lookup. Adds two fields to
 * the companies index: naics_code, naics_title.
 *
 *   OPENSEARCH_ENDPOINT=… node naics-enrich.js --dry                 # coverage report only, no writes
 *   OPENSEARCH_ENDPOINT=… node naics-enrich.js --max 200            # scoped test (server-side, sync)
 *   OPENSEARCH_ENDPOINT=… node naics-enrich.js --async [--rps 800]  # full backfill (async task, throttled)
 *
 * Match is case-insensitive on the trimmed category/industry text. Re-runnable: skips docs that already
 * have naics_code, so a resumed/partial run continues where it left off.
 */
const fs = require('fs');
const path = require('path');
const co = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes(f);
const CSV = arg('--csv', path.join(__dirname, '..', 'Category to NAICS.csv'));
const MAX = Number(arg('--max', '0')) || 0;
const RPS = Number(arg('--rps', '0')) || 0;

// ---- minimal quoted-CSV parser (fields may contain commas inside double quotes) ----
function parseCsv(text) {
  const rows = []; let row = [], field = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
    else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows;
}

// Build { normalizedKey -> [code, title] }. First mapping wins on duplicate keys (file lists the
// higher-signal Google Maps rows first). Columns: Source, Category or Key Word, NAICS 6 Code, NAICS 6 Title.
function loadMap() {
  const rows = parseCsv(fs.readFileSync(CSV, 'utf8'));
  const header = rows.shift();
  const m = {};
  let dupes = 0;
  for (const r of rows) {
    const key = String(r[1] || '').trim().toLowerCase();
    const code = String(r[2] || '').trim();
    const title = String(r[3] || '').trim();
    if (!key || !code) continue;
    if (m[key]) { dupes++; continue; }
    m[key] = [code, title];
  }
  return { m, count: Object.keys(m).length, dupes, header };
}

const SCRIPT = `
  String cat = ctx._source.category;
  if (cat == null || cat.trim().isEmpty()) { cat = ctx._source.industry; }
  if (cat == null || cat.trim().isEmpty()) { ctx.op = 'noop'; return; }
  String key = cat.trim().toLowerCase();
  if (params.m.containsKey(key)) { def v = params.m[key]; ctx._source.naics_code = v[0]; ctx._source.naics_title = v[1]; }
  else { ctx.op = 'noop'; }
`;
// only touch docs that have a category/industry and don't already have a naics_code
const QUERY = { bool: {
  must_not: [{ exists: { field: 'naics_code' } }],
  should: [{ exists: { field: 'category' } }, { exists: { field: 'industry' } }], minimum_should_match: 1,
} };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const { m, count, dupes } = loadMap();
  console.error(`loaded ${count.toLocaleString()} category→NAICS mappings (${dupes} dup keys skipped)`);
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);

  if (has('--dry')) {
    // coverage: how many companies would match, via a terms-set style estimate on distinct categories present
    const total = (await client.count({ index: co.INDEX })).body.count;
    const withCat = (await client.count({ index: co.INDEX, body: { query: { exists: { field: 'category' } } } })).body.count;
    // sample the top categories and report hit/miss against the map
    const agg = (await client.search({ index: co.INDEX, body: { size: 0, aggs: { c: { terms: { field: 'category.keyword', size: 40 } } } } })).body;
    console.log(`\nindex total ${total.toLocaleString()} | with category ${withCat.toLocaleString()}`);
    console.log('top categories — mapped? :');
    for (const b of agg.aggregations.c.buckets) {
      const key = String(b.key).trim().toLowerCase();
      const hit = m[key];
      console.log(`  ${hit ? '✓ ' + hit[0] : '✗ (no map)'}`.padEnd(18), b.doc_count.toLocaleString().padStart(10), ' ', b.key);
    }
    return;
  }

  const body = { query: QUERY, script: { lang: 'painless', source: SCRIPT, params: { m } } };
  if (MAX) {
    const r = await client.updateByQuery({ index: co.INDEX, conflicts: 'proceed', max_docs: MAX, refresh: true, body }, { requestTimeout: 120000 });
    const b = r.body || r;
    console.log(`scoped test: ${b.updated} updated / ${b.total} scanned (max ${MAX})`);
    return;
  }
  // full async backfill (server-side task); throttle with --rps to stay gentle during peak
  const params = { index: co.INDEX, conflicts: 'proceed', wait_for_completion: false, slices: 'auto', body };
  if (RPS) params.requests_per_second = RPS;
  const r = await client.updateByQuery(params, { requestTimeout: 120000 });
  console.log('backfill submitted as task:', (r.body || r).task, RPS ? `(throttled ${RPS}/s)` : '(unthrottled)');
  console.log('track: GET _tasks/<task> | this only sets naics on matches, noops the rest');
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 400) : (e.message || e)); process.exit(1); });
