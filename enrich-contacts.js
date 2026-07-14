// Enrich contacts with company data: load the domain->company lookup, scroll every contact, and bulk-update
// the ones whose domain matches with industry / company_size / hq / founded / linkedin / company_name.
const fs = require('fs');
const readline = require('readline');
const os = require('./opensearch');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const map = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[2]), crlfDelay: Infinity });
    for await (const l of rl) { if (!l) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.domain) map.set(o.domain, o); } }
  console.error('lookup: ' + map.size.toLocaleString() + ' domains');

  const INDEX = 'contacts';
  async function bulkUpdate(actions) {
    for (let a = 0; ; a++) {
      try { const res = await client.bulk({ body: actions }); const r = res.body || res; let e = 0; if (r.errors) for (const it of r.items) if (it.update && it.update.error) e++; return e; }
      catch (err) { if (a >= 6) throw err; await sleep(Math.min(16000, 500 * 2 ** a)); }
    }
  }
  // MISSING_ONLY=1 → only scan contacts that don't yet have `industry` (efficient catch-up pass).
  const query = process.env.MISSING_ONLY === '1' ? { bool: { must_not: [{ exists: { field: 'industry' } }] } } : { match_all: {} };
  let after = null, scanned = 0, matched = 0, errs = 0;
  const t0 = Date.now();
  for (;;) {
    const body = { size: 5000, _source: ['domain'], query, sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const res = await client.search({ index: INDEX, body });
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    const actions = [];
    for (const h of hits) {
      scanned++;
      const dom = h._source && h._source.domain;
      const co = dom && map.get(dom);
      if (!co) continue;
      matched++;
      actions.push({ update: { _index: INDEX, _id: h._id } });
      actions.push({ doc: {
        industry: co.industry || '', company_size: co.company_size || '', company_hq: co.hq_location || '',
        company_country: co.company_country || '', company_founded: co.founded || null,
        company_linkedin: co.company_linkedin || '', company_name: co.company_name || '',
      } });
    }
    if (actions.length) errs += await bulkUpdate(actions);
    after = hits[hits.length - 1].sort;
    if (scanned % 500000 < 5000) { const s = (Date.now() - t0) / 1000; console.error(`  scanned ${scanned.toLocaleString()} | enriched ${matched.toLocaleString()} | ${errs} err | ${Math.round(scanned / s)}/s`); }
  }
  console.error(`DONE: scanned ${scanned.toLocaleString()}, enriched ${matched.toLocaleString()}, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('enrich error:', e.message); process.exit(1); });
