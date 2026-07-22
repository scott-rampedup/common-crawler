/**
 * gender-enrich.js — backfill Gender onto contacts that have a first name but no gender, using the expanded
 * names-genders.csv map. Server-side update_by_query (painless + param map), like naics-enrich.
 *   OPENSEARCH_ENDPOINT=… node gender-enrich.js --dry            # coverage estimate
 *   OPENSEARCH_ENDPOINT=… node gender-enrich.js --max 300        # scoped test (sync)
 *   OPENSEARCH_ENDPOINT=… node gender-enrich.js --async [--rps N]# full backfill (async task)
 * Re-runnable: only touches docs still missing a gender.
 */
const path = require('path');
const os = require('./opensearch');
const ex = require('./extractor');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes(f);
const MAX = Number(arg('--max', '0')) || 0;
const RPS = Number(arg('--rps', '0')) || 0;

const gmap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));   // { name(lower): 'M'|'F' }

const SCRIPT = `
  if (ctx._source.gender != null && !ctx._source.gender.isEmpty()) { ctx.op = 'noop'; return; }
  String f = ctx._source.first;
  if (f == null || f.trim().isEmpty()) { ctx.op = 'noop'; return; }
  String key = f.trim().toLowerCase();
  if (params.g.containsKey(key)) { ctx._source.gender = params.g[key]; }
  else { ctx.op = 'noop'; }
`;
// no-gender docs that have a (non-blank) first name
const QUERY = { bool: {
  must: [{ bool: { must_not: [{ term: { 'first.kw': '' } }] } }],
  should: [{ term: { gender: '' } }, { bool: { must_not: [{ exists: { field: 'gender' } }] } }], minimum_should_match: 1,
} };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  console.error(`gender map: ${Object.keys(gmap).length.toLocaleString()} names`);
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);

  if (has('--dry')) {
    const noG = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
    console.log(`no-gender-with-name docs: ${noG.toLocaleString()}`);
    return;
  }
  const body = { query: QUERY, script: { lang: 'painless', source: SCRIPT, params: { g: gmap } } };
  if (MAX) {
    const r = await client.updateByQuery({ index: os.INDEX, conflicts: 'proceed', max_docs: MAX, refresh: true, body }, { requestTimeout: 120000 });
    const b = r.body || r;
    console.log(`scoped test: ${b.updated} gendered / ${b.total} scanned (max ${MAX})`);
    return;
  }
  const params = { index: os.INDEX, conflicts: 'proceed', wait_for_completion: false, slices: 'auto', body };
  if (RPS) params.requests_per_second = RPS;
  const r = await client.updateByQuery(params, { requestTimeout: 120000 });
  console.log('backfill submitted as task:', (r.body || r).task, RPS ? `(throttled ${RPS}/s)` : '');
  console.log('track: GET _tasks/<task>');
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 400) : (e.message || e)); process.exit(1); });
