/**
 * enrich-firmographics.js — append Company Data (industry / size / HQ / founded / LinkedIn / name) to contacts
 * by joining each contact's domain to the LIVE companies index (msearch, so Google/Bing companies are
 * included — unlike the raw-PDL build-company-lookup path). Streams contacts MISSING company data, batch-
 * resolves their domains, and bulk-updates matches. Doubles as the ongoing catch-up sweep (only touches
 * contacts without `industry`, so re-runs process just the new delta).
 *   OPENSEARCH_ENDPOINT=… node enrich-firmographics.js --dry [--limit N]   (matchable count)
 *   OPENSEARCH_ENDPOINT=… node enrich-firmographics.js [--limit N]         (apply)
 */
const os = require('./opensearch');
const co = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const CO_SRC = ['name', 'industry', 'size', 'locality', 'region', 'country', 'founded', 'linkedin_url'];
const firmo = (c) => ({
  industry: c.industry || '', company_size: c.size || '',
  company_hq: [c.locality, c.region, c.country].filter(Boolean).join(', '),
  company_country: c.country || '', company_founded: c.founded || null,
  company_linkedin: c.linkedin_url || '', company_name: c.name || '',
});

// batch-resolve domains -> firmographics from the LIVE companies index (msearch, retry+reconnect)
async function resolveDomains(coClient, domains) {
  const map = new Map();
  for (let i = 0; i < domains.length; i += 300) {
    const chunk = domains.slice(i, i + 300);
    const body = []; for (const d of chunk) { body.push({ index: co.INDEX }); body.push({ size: 1, query: { term: { domain: d } }, _source: CO_SRC }); }
    for (let a = 0; a < 6; a++) { try {
      const r = await coClient.msearch({ body }); const resp = (r.body || r).responses || [];
      for (let j = 0; j < chunk.length; j++) { const h = resp[j] && resp[j].hits && resp[j].hits.hits && resp[j].hits.hits[0]; if (h) map.set(chunk[j], h._source); }
      break;
    } catch (e) { if (a === 5) throw e; if (a === 2) coClient = co.makeClient(process.env.OPENSEARCH_ENDPOINT); await sleep(Math.min(8000, 300 * 2 ** a)); } }
  }
  return map;
}

// Enrich contacts missing company data by joining domain -> live companies index. Reusable: the app calls
// this on a schedule (the ongoing sweep) and the CLI calls it once. Returns { scanned, matched, updated }.
const QUERY = { bool: { must: [{ bool: { must_not: [{ term: { domain: '' } }] } }, { exists: { field: 'domain' } }], must_not: [{ exists: { field: 'industry' } }] } };
async function enrichMissing({ client, coClient, endpoint, limit = 0, dry = false, log = () => {} } = {}) {
  endpoint = endpoint || process.env.OPENSEARCH_ENDPOINT;
  let cl = client || os.makeClient(endpoint);
  const coCl = coClient || co.makeClient(endpoint);
  let scanned = 0, matched = 0, updated = 0, after = null; let batch = []; const t0 = Date.now();
  const bulkUpd = async (actions) => { for (let a = 0; a < 6; a++) { try { await cl.bulk({ body: actions, refresh: false }); return; } catch (e) { if (a === 5) throw e; if (a === 2 && endpoint) cl = os.makeClient(endpoint); await sleep(500 * 2 ** a); } } };
  const processBatch = async () => {
    if (!batch.length) return; const rows = batch; batch = [];
    const map = await resolveDomains(coCl, [...new Set(rows.map((r) => r.domain))]);
    const actions = [];
    for (const r of rows) { const c = map.get(r.domain); if (!c) continue; matched++; if (!dry) actions.push({ update: { _index: os.INDEX, _id: r.id } }, { doc: firmo(c) }); }
    if (actions.length) { await bulkUpd(actions); updated += actions.length / 2; }
  };
  for (;;) {
    const bq = { size: 5000, query: QUERY, _source: ['domain'], sort: [{ _doc: 'asc' }] };
    if (after) bq.search_after = after;
    const hits = (await cl.search({ index: os.INDEX, body: bq })).body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) { scanned++; if (h._source.domain) batch.push({ id: h._id, domain: h._source.domain }); if (limit && scanned >= limit) break; }
    after = hits[hits.length - 1].sort;
    if (batch.length >= 5000) await processBatch();
    if (scanned % 100000 === 0) log(`  scanned ${scanned.toLocaleString()} | matched ${matched.toLocaleString()} | ${Math.round(scanned / ((Date.now() - t0) / 1000))}/s`);
    if (limit && scanned >= limit) break;
  }
  await processBatch();
  return { scanned, matched, updated };
}
module.exports = { enrichMissing };

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.error(`contacts missing company data (with a domain): ${total.toLocaleString()}`);
  const r = await enrichMissing({ client, endpoint: process.env.OPENSEARCH_ENDPOINT, limit: LIMIT, dry: DRY, log: console.error });
  console.error(`DONE: scanned ${r.scanned.toLocaleString()} | matched ${r.matched.toLocaleString()}${DRY ? ' [DRY]' : ` | ${r.updated.toLocaleString()} enriched`}`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.message || e)); process.exit(1); });
