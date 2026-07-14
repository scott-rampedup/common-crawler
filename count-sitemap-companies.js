// Batch: set contact_count (# contacts sharing the root domain) and sitemap_url (discovered bio sitemap)
// on the company docs. Inputs: contact-counts.tsv (domain<TAB>count) + the Sitemap Master List CSV
// (Website -> SiteMap, Count). Scrolls the companies index and bulk-updates only matched domains.
//   OPENSEARCH_ENDPOINT=… node count-sitemap-companies.js contact-counts.tsv "Sitemap Master List.csv"
const fs = require('fs');
const readline = require('readline');
const co = require('./companies');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function parseCsvLine(line) {
  const out = []; let cur = '', q = false;
  for (let i = 0; i < line.length; i++) { const c = line[i];
    if (q) { if (c === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += c; }
    else if (c === '"') q = true; else if (c === ',') { out.push(cur); cur = ''; } else cur += c; }
  out.push(cur); return out;
}

(async () => {
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  try { await client.indices.putMapping({ index: co.INDEX, body: { properties: { contact_count: { type: 'integer' }, sitemap_url: { type: 'keyword' } } } }); }
  catch (e) { console.error('putMapping:', e.message); }

  // 1) domain -> contact_count
  const counts = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[2]), crlfDelay: Infinity });
    for await (const l of rl) { const i = l.indexOf('\t'); if (i < 0) continue; const d = l.slice(0, i), n = Number(l.slice(i + 1)); if (d) counts.set(d, n); } }
  console.error('contact-count domains: ' + counts.size.toLocaleString());

  // 2) domain -> discovered sitemap (keep the highest-Count sitemap per domain)
  const sm = new Map(), smCount = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[3]), crlfDelay: Infinity });
    let header = true, col = {};
    for await (const line of rl) {
      const cells = parseCsvLine(line);
      if (header) { header = false; cells.forEach((c, i) => { const k = c.trim().toLowerCase(); if (!(k in col)) col[k] = i; }); continue; }
      const web = cells[col.website], site = cells[col.sitemap], cnt = Number(cells[col.count] || 0);
      const d = co.normDomain(web || ''); if (!d || !site || !site.trim()) continue;
      if (!sm.has(d) || cnt > (smCount.get(d) || 0)) { sm.set(d, site.trim()); smCount.set(d, cnt); }
    } }
  console.error('discovered-sitemap domains: ' + sm.size.toLocaleString());

  // 3) scroll companies, bulk-update matched
  const INDEX = co.INDEX;
  async function bulkUpd(actions) {
    for (let a = 0; ; a++) { try { const res = await client.bulk({ body: actions }); const r = res.body || res; let e = 0; if (r.errors) for (const it of r.items) if (it.update && it.update.error) e++; return e; }
      catch (err) { if (a >= 6) throw err; await sleep(Math.min(16000, 500 * 2 ** a)); } }
  }
  let after = null, scanned = 0, updated = 0, errs = 0;
  const t0 = Date.now();
  for (;;) {
    const body = { size: 5000, _source: ['domain'], query: { match_all: {} }, sort: [{ id: 'asc' }] };
    if (after) body.search_after = after;
    const res = await client.search({ index: INDEX, body });
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    const actions = [];
    for (const h of hits) {
      scanned++; const d = h._source && h._source.domain; if (!d) continue;
      const cc = counts.get(d), site = sm.get(d);
      if (cc === undefined && site === undefined) continue;
      const doc = {}; if (cc !== undefined) doc.contact_count = cc; if (site !== undefined) doc.sitemap_url = site;
      actions.push({ update: { _index: INDEX, _id: h._id } }, { doc });
    }
    if (actions.length) { errs += await bulkUpd(actions); updated += actions.length / 2; }
    after = hits[hits.length - 1].sort;
    if (scanned % 1000000 < 5000) { const s = (Date.now() - t0) / 1000; console.error(`  scanned ${scanned.toLocaleString()} | updated ${updated.toLocaleString()} | ${errs} err | ${Math.round(scanned / s)}/s`); }
  }
  console.error(`DONE: scanned ${scanned.toLocaleString()}, updated ${updated.toLocaleString()}, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('error:', e.message); process.exit(1); });
