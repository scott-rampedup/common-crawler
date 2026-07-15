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

// A compound public suffix is <second-level>.<2-letter ccTLD>: co.uk, com.au, com.br, ac.uk, org.uk,
// gov.uk, net.au, co.nz, co.in, com.mx, co.za, co.jp, edu.au … So the registrable domain is the label
// BEFORE that suffix (ox.ac.uk, not ac.uk). This is what the contact->company rollup groups on.
const SLD = new Set(['co', 'com', 'org', 'net', 'ac', 'gov', 'edu', 'mil', 'ltd', 'plc', 'me', 'sch', 'nhs', 'or', 'ne', 'go', 'gr', 'gob']);
function isCompound(host) { const p = host.split('.'); return p.length >= 3 && p[p.length - 1].length === 2 && SLD.has(p[p.length - 2]); }
function registrable(host) {
  if (!host) return '';
  const p = host.split('.');
  if (p.length <= 2) return host;
  return isCompound(host) ? p.slice(-3).join('.') : p.slice(-2).join('.');
}
const TLD_COUNTRY = { uk: 'united kingdom', ca: 'canada', au: 'australia', us: 'united states', nz: 'new zealand',
  ie: 'ireland', in: 'india', de: 'germany', fr: 'france', es: 'spain', it: 'italy', nl: 'netherlands', se: 'sweden',
  no: 'norway', dk: 'denmark', fi: 'finland', ch: 'switzerland', at: 'austria', be: 'belgium', pt: 'portugal',
  pl: 'poland', za: 'south africa', br: 'brazil', mx: 'mexico', jp: 'japan', sg: 'singapore', hk: 'hong kong',
  ae: 'united arab emirates', il: 'israel', kr: 'south korea', tr: 'turkey', id: 'indonesia', ph: 'philippines',
  my: 'malaysia', th: 'thailand', vn: 'vietnam', cn: 'china' };
function countryFromTld(host) {
  const last = host.split('.').pop();
  if (last.length === 2) return TLD_COUNTRY[last] || '';         // ccTLD (incl. compound like ox.ac.uk -> uk)
  if (last === 'edu' || last === 'gov' || last === 'mil') return 'united states';
  return '';                                                     // generic .com/.org/.net -> no country
}
// The "type" label of the public suffix: the second-level for a compound suffix (org.au -> org,
// ac.uk -> ac, gov.au -> gov), else the last label (.org, .edu, .gov).
function tldType(host) { const p = host.split('.'); return isCompound(host) ? p[p.length - 2] : p[p.length - 1]; }
function industryFromTld(host) {
  const t = tldType(host);
  if (t === 'edu' || t === 'ac') return 'higher education';
  if (t === 'gov') return 'government administration';
  if (t === 'org') return 'non-profit organization management';
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
  // contact_count = the company's OWN domain count (host-exact; NO sub-domain rollup, per Scott 2026-07-15)
  const counts = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[2]), crlfDelay: Infinity });
    for await (const l of rl) { const i = l.indexOf('\t'); if (i < 0) continue; const host = l.slice(0, i), n = Number(l.slice(i + 1)); if (host) counts.set(host, n); } }
  console.error('domains with contacts: ' + counts.size.toLocaleString());

  const INDEX = co.INDEX;
  async function bulk(actions) { for (let a = 0; ; a++) { try { const res = await client.bulk({ body: actions }); const r = res.body || res; let e = 0; if (r.errors) for (const it of r.items) if (it.update && it.update.error) e++; return e; } catch (err) { if (a >= 6) throw err; await sleep(Math.min(16000, 500 * 2 ** a)); } } }
  async function searchRetry(body) { for (let a = 0; ; a++) { try { return await client.search({ index: INDEX, body }); } catch (err) { if (a >= 20) throw err; await sleep(Math.min(60000, 1000 * 2 ** a)); } } }
  const CKPT = process.argv[3] || '';                    // resume cursor file (survives a fatal error -> re-run continues)
  let after = null, scanned = 0, updated = 0, errs = 0;
  if (CKPT) { try { after = JSON.parse(fs.readFileSync(CKPT, 'utf8')); console.error('resuming from checkpoint'); } catch (e) { after = null; } }
  const t0 = Date.now();
  for (;;) {
    const body = { size: 5000, _source: ['domain', 'country', 'industry', 'size', 'contact_count'], query: { match_all: {} }, sort: [{ id: 'asc' }] };
    if (after) body.search_after = after;
    const res = await searchRetry(body);
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    const actions = [];
    for (const h of hits) {
      scanned++; const s = h._source; const dom = s.domain; if (!dom) continue;
      const cc = counts.get(dom) || 0;                     // host-exact (the company's own domain)
      const doc = {};
      if (cc !== (s.contact_count || 0)) doc.contact_count = cc;
      if (!s.country) { const c = countryFromTld(dom); if (c) doc.country = c; }
      if (!s.industry) { const ind = industryFromTld(dom); if (ind) doc.industry = ind; }
      const ns = adjustSize(s.size, cc); if (ns && ns !== s.size) doc.size = ns;
      if (Object.keys(doc).length) actions.push({ update: { _index: INDEX, _id: h._id } }, { doc });
    }
    if (actions.length) { errs += await bulk(actions); updated += actions.length / 2; }
    after = hits[hits.length - 1].sort;
    if (CKPT) { try { fs.writeFileSync(CKPT, JSON.stringify(after)); } catch (e) { /* best effort */ } }
    if (scanned % 2000000 < 5000) { const t = (Date.now() - t0) / 1000; console.error(`  scanned ${scanned.toLocaleString()} | updated ${updated.toLocaleString()} | ${errs} err | ${Math.round(scanned / t)}/s`); }
  }
  if (CKPT) { try { fs.unlinkSync(CKPT); } catch (e) { /* gone */ } }
  console.error(`DONE: scanned ${scanned.toLocaleString()}, updated ${updated.toLocaleString()}, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('error:', e.message); process.exit(1); });
