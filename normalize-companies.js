// Phase-1 normalization batch for the companies index:
//  - contact_count: roll the per-host contact counts up to the REGISTRABLE domain (includes sub-domains)
//  - country:  infer from the domain TLD when blank
//  - industry: infer from the TLD when blank (edu->higher ed, gov->govt admin, org->non-profit)
//  - size:     bump the employee band up when contact_count exceeds it (never down)
//   OPENSEARCH_ENDPOINT=… node normalize-companies.js contact-counts.tsv
const fs = require('fs');
const readline = require('readline');
const co = require('./companies');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const COMPOUND_TLD = new Set(['co.uk', 'org.uk', 'net.au', 'com.au', 'co.nz', 'org.nz', 'co.za', 'com.br', 'co.jp', 'co.in', 'com.mx', 'co.il', 'com.sg', 'com.hk', 'com.au', 'gov.uk', 'ac.uk', 'edu.au', 'gov.au']);
function registrable(host) {
  if (!host) return '';
  const p = host.split('.');
  if (p.length <= 2) return host;
  return COMPOUND_TLD.has(p.slice(-2).join('.')) ? p.slice(-3).join('.') : p.slice(-2).join('.');
}
// public-suffix (last label, or compound) -> country
const TLD_COUNTRY = { uk: 'united kingdom', 'co.uk': 'united kingdom', 'ac.uk': 'united kingdom', 'gov.uk': 'united kingdom',
  ca: 'canada', au: 'australia', 'com.au': 'australia', nz: 'new zealand', ie: 'ireland', us: 'united states',
  in: 'india', de: 'germany', fr: 'france', es: 'spain', it: 'italy', nl: 'netherlands', se: 'sweden', no: 'norway',
  dk: 'denmark', fi: 'finland', ch: 'switzerland', at: 'austria', be: 'belgium', pt: 'portugal', pl: 'poland',
  za: 'south africa', br: 'brazil', mx: 'mexico', jp: 'japan', sg: 'singapore', hk: 'hong kong', ae: 'united arab emirates', il: 'israel' };
function tldOf(host) { const p = host.split('.'); const c2 = p.slice(-2).join('.'); return COMPOUND_TLD.has(c2) ? c2 : p[p.length - 1]; }
function countryFromTld(host) { return TLD_COUNTRY[tldOf(host)] || ''; }
function industryFromTld(host) {
  const t = tldOf(host); const last = t.split('.').pop();
  if (last === 'edu' || t === 'edu.au' || t === 'ac.uk') return 'higher education';
  if (last === 'gov' || t === 'gov.uk' || t === 'gov.au') return 'government administration';
  if (last === 'org') return 'non-profit organization management';
  return '';
}
const BANDS = [['1-10', 10], ['11-50', 50], ['51-200', 200], ['201-500', 500], ['501-1000', 1000], ['1001-5000', 5000], ['5001-10000', 10000], ['10001+', Infinity]];
function bandFor(n) { for (const [b, mx] of BANDS) if (n <= mx) return b; return '10001+'; }
function adjustSize(size, count) {
  if (!count) return size;
  const need = bandFor(count), needMax = BANDS.find((b) => b[0] === need)[1];
  const cur = BANDS.find((b) => b[0] === size);
  if (!cur) return need;                 // unknown/blank size -> set to the count's band
  return needMax > cur[1] ? need : size; // only bump up
}

(async () => {
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  // roll per-host counts up to registrable domain
  const roll = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[2]), crlfDelay: Infinity });
    for await (const l of rl) { const i = l.indexOf('\t'); if (i < 0) continue; const host = l.slice(0, i), n = Number(l.slice(i + 1)); const r = registrable(host); if (r) roll.set(r, (roll.get(r) || 0) + n); } }
  console.error('registrable domains with contacts: ' + roll.size.toLocaleString());

  const INDEX = co.INDEX;
  async function bulk(actions) { for (let a = 0; ; a++) { try { const res = await client.bulk({ body: actions }); const r = res.body || res; let e = 0; if (r.errors) for (const it of r.items) if (it.update && it.update.error) e++; return e; } catch (err) { if (a >= 6) throw err; await sleep(Math.min(16000, 500 * 2 ** a)); } } }
  let after = null, scanned = 0, updated = 0, errs = 0;
  const t0 = Date.now();
  for (;;) {
    const body = { size: 5000, _source: ['domain', 'country', 'industry', 'size', 'contact_count'], query: { match_all: {} }, sort: [{ id: 'asc' }] };
    if (after) body.search_after = after;
    const res = await client.search({ index: INDEX, body });
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    const actions = [];
    for (const h of hits) {
      scanned++; const s = h._source; const dom = s.domain; if (!dom) continue;
      const reg = registrable(dom);
      const cc = roll.get(reg) || 0;
      const doc = {};
      if (cc !== (s.contact_count || 0)) doc.contact_count = cc;
      if (!s.country) { const c = countryFromTld(dom); if (c) doc.country = c; }
      if (!s.industry) { const ind = industryFromTld(dom); if (ind) doc.industry = ind; }
      const ns = adjustSize(s.size, cc); if (ns && ns !== s.size) doc.size = ns;
      if (Object.keys(doc).length) actions.push({ update: { _index: INDEX, _id: h._id } }, { doc });
    }
    if (actions.length) { errs += await bulk(actions); updated += actions.length / 2; }
    after = hits[hits.length - 1].sort;
    if (scanned % 2000000 < 5000) { const t = (Date.now() - t0) / 1000; console.error(`  scanned ${scanned.toLocaleString()} | updated ${updated.toLocaleString()} | ${errs} err | ${Math.round(scanned / t)}/s`); }
  }
  console.error(`DONE: scanned ${scanned.toLocaleString()}, updated ${updated.toLocaleString()}, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('error:', e.message); process.exit(1); });
