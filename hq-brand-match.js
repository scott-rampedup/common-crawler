/**
 * hq-brand-match.js — second opinion on the hosts hq-name-cardinality.js wanted to split apart.
 * ---------------------------------------------------------------------------------------------
 * Name cardinality alone is not sufficient, and the failure is not subtle. Measured on the index:
 *
 *   hertz.com          7,743 records, 7,503 names (ratio 0.97)  -> ONE company, branches named per site
 *   choicehotels.com   4,717 records, 4,485 names (ratio 0.95)  -> ONE company, properties are SUB-BRANDS
 *   br.bluepillow.com 14,113 records, 13,731 names (ratio 0.97) -> thousands of strangers' listings
 *
 * All three look identical by cardinality. Brand containment separates the first from the third
 * (hertz 59% of records carry "hertz", bluepillow 0%) but not the second: Choice Hotels' properties are
 * called Comfort Inn and Quality Inn, so the franchisor's own token appears on 1% of them. Same for
 * wyndhamhotels (0%), napaonline (0%, stores trade as "NAPA Auto Parts"), ameripriseadvisors (0%,
 * records are advisors' names).
 *
 * So this emits a THREE-way verdict instead of pretending to a two-way one:
 *   merge      low cardinality, or the brand appears on most records            -> safe to roll up
 *   split      low brand share AND near-unique names                           -> safe to keep apart
 *   review     high cardinality, low brand share, but plausibly one company    -> LEFT ALONE
 *
 * A `review` host keeps whatever company_type its records already have. Nothing is merged and nothing
 * is separated, so an unresolved host costs us the status quo rather than a wrong irreversible answer.
 *
 *   node hq-brand-match.js --in card.json --out verdict.json
 */
const co = require('./companies');
const psl = require('./psl-suffix');
const fs = require('fs');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('in', '');
const OUT = arg('out', '');
const BATCH = Number(arg('batch', '')) || 40;
const BRAND_MERGE = Number(arg('brand-merge', '')) || 0.5;   // brand on this share of records -> one company
const N = (x) => Number(x || 0).toLocaleString();

const squash = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
function brandOf(domain) {
  const reg = psl.registrableOrHost(domain) || String(domain || '');
  const suffix = psl.publicSuffix(reg);
  const label = suffix && reg.endsWith('.' + suffix) ? reg.slice(0, reg.length - suffix.length - 1) : reg.split('.')[0];
  return squash(String(label).split('.').pop());
}

(async () => {
  if (!IN || !OUT) { console.error('usage: node hq-brand-match.js --in card.json --out verdict.json'); process.exit(1); }
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const card = JSON.parse(fs.readFileSync(IN, 'utf8'));
  const toCheck = Object.entries(card).filter(([, v]) => v && v.decision === 'split').map(([d]) => d);
  console.log('hosts the cardinality pass wanted to split: ' + N(toCheck.length));

  const out = { ...card };
  let merge = 0, split = 0, review = 0, done = 0;
  const t0 = Date.now();
  for (let i = 0; i < toCheck.length; i += BATCH) {
    const chunk = toCheck.slice(i, i + BATCH);
    const body = [];
    for (const dom of chunk) {
      body.push({ index: co.INDEX });
      body.push({ size: 0, track_total_hits: true, query: { term: { domain: dom } },
                  aggs: { top: { terms: { field: 'name.kw', size: 200 } } } });
    }
    let res = null;
    for (let a = 0; ; a++) {
      try { res = await c.msearch({ body }, { requestTimeout: 600000 }).then((x) => (x.body || x)); break; }
      catch (e) {
        if (a >= 6) throw e;
        console.error('  retry ' + (a + 1) + '/6: ' + (e.message || e).slice(0, 100));
        await new Promise((s) => setTimeout(s, Math.min(60000, 3000 * 2 ** a)));
      }
    }
    (res.responses || []).forEach((r, j) => {
      const dom = chunk[j];
      const prev = out[dom] || {};
      if (!r || r.error || !r.hits) { out[dom] = { ...prev, decision: 'review', why: 'query failed' }; review++; return; }
      const brand = brandOf(dom);
      const buckets = r.aggregations.top.buckets || [];
      // weighted by doc_count: what share of RECORDS carry the brand, not what share of spellings
      let hit = 0, seen = 0;
      for (const b of buckets) { seen += b.doc_count; if (brand && squash(b.key).includes(brand)) hit += b.doc_count; }
      const brandShare = seen ? hit / seen : 0;
      let decision, why;
      if (brandShare >= BRAND_MERGE) { decision = 'merge'; why = 'brand on ' + Math.round(brandShare * 100) + '% of records'; merge++; }
      else if ((prev.ratio || 0) >= 0.9 && brandShare < 0.05) { decision = 'split'; why = 'near-unique names, brand absent'; split++; }
      else { decision = 'review'; why = 'high name variety, brand on only ' + Math.round(brandShare * 100) + '% — could be sub-brands'; review++; }
      out[dom] = { ...prev, brand, brandShare: Number(brandShare.toFixed(3)), decision, why };
    });
    done += chunk.length;
    if (done % 2000 < BATCH) console.log('  ' + N(done) + ' / ' + N(toCheck.length) + '  (' + Math.round((Date.now() - t0) / 1000) + 's)');
  }

  fs.writeFileSync(OUT, JSON.stringify(out, null, 1));
  console.log('\nof the ' + N(toCheck.length) + ' re-examined:');
  console.log('  merge  (brand present on most records) : ' + N(merge));
  console.log('  split  (strangers sharing a website)   : ' + N(split));
  console.log('  review (ambiguous — left alone)        : ' + N(review));
  const rows = Object.entries(out).filter(([, v]) => v.decision === 'review' && v.recs).sort((a, b) => b[1].recs - a[1].recs);
  console.log('\nbiggest hosts needing a human call:');
  for (const [d, v] of rows.slice(0, 20)) {
    console.log('    ' + d.padEnd(30) + N(v.recs).padStart(8) + ' recs, ' + N(v.names).padStart(7) + ' names, brand ' +
                String(Math.round((v.brandShare || 0) * 100)).padStart(3) + '%  ' + JSON.stringify(String(v.topName || '').slice(0, 34)));
  }
  console.log('\nwrote ' + OUT);
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
