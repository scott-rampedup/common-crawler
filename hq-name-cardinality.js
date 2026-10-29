/**
 * hq-name-cardinality.js — decide the one case HQ grouping cannot decide from structure alone.
 * ---------------------------------------------------------------------------------------------
 * The dry run leaves ~12,467 hosts where every record sits on the ONE bare domain, with no subdomains
 * to tell them apart. Two opposite things look identical there:
 *
 *   a franchise network   mcdonalds.com      9,285 records,    60 names, "McDonald's" on 98%
 *   an aggregator         br.bluepillow.com 14,113 records, 13,731 names, top name under 1%
 *
 * Merging is right for the first and wrong for the second, and the record COUNT says nothing about
 * which is which. The number of DISTINCT COMPANY NAMES does, decisively — measured on the live index:
 *   cvs.com 0.006 names/record · chick-fil-a.com 0.004 · credit-agricole.fr 0.000 (one name, 100%)
 *   bluepillow 0.973 · glossgenius 0.956 · alibaba 0.999 · business.site 1.000
 * There is two orders of magnitude between the populations, so the threshold is not a close call.
 *
 *   node hq-resolve-dryrun.js --fold DUMP --emit-candidates cand.json
 *   node hq-name-cardinality.js --in cand.json --out card.json
 *   node hq-resolve-dryrun.js --fold DUMP --cardinality card.json
 *
 * Names come from name.kw (company_name.keyword is empty across the whole index).
 */
const co = require('./companies');
const fs = require('fs');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('in', '');
const OUT = arg('out', '');
const BATCH = Number(arg('batch', '')) || 60;
const N = (x) => Number(x || 0).toLocaleString();

// Ratios between these two get merged (the default, since a shared website with one brand is the
// commoner case) but are reported so they can be eyeballed.
const SPLIT_AT = Number(arg('split-at', '')) || 0.5;
const MERGE_UNDER = Number(arg('merge-under', '')) || 0.15;

(async () => {
  if (!IN || !OUT) { console.error('usage: node hq-name-cardinality.js --in cand.json --out card.json'); process.exit(1); }
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const cands = JSON.parse(fs.readFileSync(IN, 'utf8'));
  console.log('candidates: ' + N(cands.length));

  const out = {};
  let split = 0, merge = 0, grey = 0, done = 0;
  const t0 = Date.now();
  for (let i = 0; i < cands.length; i += BATCH) {
    const chunk = cands.slice(i, i + BATCH);
    const body = [];
    for (const x of chunk) {
      body.push({ index: co.INDEX });
      body.push({ size: 0, track_total_hits: true,
                  query: { term: { domain: x.domain } },
                  aggs: { names: { cardinality: { field: 'name.kw', precision_threshold: 40000 } },
                          top: { terms: { field: 'name.kw', size: 1 } } } });
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
      const dom = chunk[j].domain;
      if (!r || r.error || !r.hits) { out[dom] = { error: true }; return; }
      const recs = r.hits.total.value;
      const names = r.aggregations.names.value;
      const top = (r.aggregations.top.buckets || [])[0];
      const ratio = recs ? names / recs : 0;
      const decision = ratio >= SPLIT_AT ? 'split' : (ratio < MERGE_UNDER ? 'merge' : 'grey');
      if (decision === 'split') split++; else if (decision === 'merge') merge++; else grey++;
      out[dom] = { recs, names, ratio: Number(ratio.toFixed(4)), decision,
                   topName: top ? String(top.key).slice(0, 60) : '', topShare: top && recs ? Number((top.doc_count / recs).toFixed(3)) : 0 };
    });
    done += chunk.length;
    if (done % 3000 < BATCH) console.log('  ' + N(done) + ' / ' + N(cands.length) + '  (' + Math.round((Date.now() - t0) / 1000) + 's)');
  }

  fs.writeFileSync(OUT, JSON.stringify(out, null, 1));
  console.log('\nsplit (aggregator, keep separate) : ' + N(split));
  console.log('merge (one brand, many branches)  : ' + N(merge));
  console.log('grey  (between ' + MERGE_UNDER + ' and ' + SPLIT_AT + ', merged)   : ' + N(grey));
  const rows = Object.entries(out).filter(([, v]) => v.decision === 'split').sort((a, b) => b[1].recs - a[1].recs);
  console.log('\nbiggest hosts that would stay SEPARATE:');
  for (const [d, v] of rows.slice(0, 15)) console.log('    ' + d.padEnd(34) + N(v.recs).padStart(8) + ' recs, ' + N(v.names).padStart(8) + ' names, ratio ' + v.ratio);
  const mrows = Object.entries(out).filter(([, v]) => v.decision === 'merge').sort((a, b) => b[1].recs - a[1].recs);
  console.log('\nbiggest hosts that would MERGE:');
  for (const [d, v] of mrows.slice(0, 15)) console.log('    ' + d.padEnd(34) + N(v.recs).padStart(8) + ' recs, ' + N(v.names).padStart(8) + ' names, ' + JSON.stringify(v.topName) + ' ' + Math.round(v.topShare * 100) + '%');
  console.log('\nwrote ' + OUT);
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
