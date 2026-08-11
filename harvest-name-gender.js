/**
 * harvest-name-gender.js — reconstitute the names→gender table from genders the pipeline itself has
 * established, so each crawl makes the next one better.
 *
 *   OPENSEARCH_ENDPOINT=… node harvest-name-gender.js --dry [--min-count 8] [--min-agree 0.9]
 *   OPENSEARCH_ENDPOINT=… node harvest-name-gender.js --out names-genders-additions.csv
 *
 * WHERE THE NEW EVIDENCE COMES FROM: bio-signals now assigns gender from the person's own pronouns and
 * from schema.org, both of which work on names the 131k map has never seen — non-Western names, rare
 * spellings, new transliterations. Those genders sit on individual contacts. This aggregates them back
 * into first-name level facts: a first name the map does not know, seen on enough contacts, with near-
 * unanimous agreement, becomes a new map entry.
 *
 * DELIBERATELY CONSERVATIVE. The map is consulted for every contact ever crawled, so a wrong entry is a
 * systematic error, not a one-off. A candidate needs volume AND near-unanimity, and genuinely unisex
 * names (Jordan, Casey, Alex) are meant to fail the agreement test and stay out.
 *
 * Writes a CSV of ADDITIONS rather than editing names-genders.csv in place — review it, then append.
 */
const fs = require('fs');
const path = require('path');
const os = require('./opensearch');
const ex = require('./extractor');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const MIN_COUNT = Math.max(2, Number(arg('--min-count', '8')) || 8);
const MIN_AGREE = Math.min(1, Math.max(0.6, Number(arg('--min-agree', '0.9')) || 0.9));
const OUT = arg('--out', path.join(__dirname, 'names-genders-additions.csv'));
const TOP = Number(arg('--top', '40000')) || 40000;

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const known = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  console.error(`known names: ${Object.keys(known).length.toLocaleString()}${DRY ? '  [DRY RUN]' : ''}`);
  console.error(`  thresholds: >=${MIN_COUNT} contacts per name, >=${(MIN_AGREE * 100).toFixed(0)}% agreement\n`);

  // COMPOSITE, not a flat terms agg: there are far more than 65,535 distinct first names, and a terms agg
  // with a sub-aggregation blows the bucket ceiling outright (observed: "too many buckets ... was 65536").
  // Composite pages through every key instead of trying to materialize them at once.
  const QUERY = { bool: { must_not: [{ term: { gender: '' } }, { term: { 'first.kw': '' } }] } };
  const t0 = Date.now();
  const buckets = [];
  let afterKey = null;
  for (;;) {
    const body = {
      size: 0, query: QUERY,
      aggs: { names: { composite: { size: 2000, sources: [{ n: { terms: { field: 'first.kw' } } }], ...(afterKey ? { after: afterKey } : {}) },
        aggs: { g: { terms: { field: 'gender', size: 4 } } } } },
    };
    const agg = ((await client.search({ index: os.INDEX, body })).body || {}).aggregations.names;
    const page = agg.buckets || [];
    if (!page.length) break;
    for (const b of page) buckets.push({ key: b.key.n, doc_count: b.doc_count, g: b.g });
    afterKey = agg.after_key;
    if (!afterKey) break;
    if (buckets.length % 20000 < 2000) console.error(`  paged ${buckets.length.toLocaleString()} distinct first name(s)…`);
    if (buckets.length >= TOP) break;
  }
  console.error(`distinct gendered first names seen: ${buckets.length.toLocaleString()}\n`);

  const additions = [];
  const tally = { seen: buckets.length, alreadyKnown: 0, tooFew: 0, contested: 0, added: 0 };
  for (const b of buckets) {
    const name = String(b.key || '').trim().toLowerCase();
    if (!name || !/^[a-zà-ÿ][a-zà-ÿ'’-]{1,23}$/.test(name)) continue;
    if (typeof known[name] === 'string' && known[name]) { tally.alreadyKnown++; continue; }
    const g = {};
    for (const gb of (b.g.buckets || [])) g[gb.key] = gb.doc_count;
    const m = g.M || 0, f = g.F || 0, n = m + f;
    if (n < MIN_COUNT) { tally.tooFew++; continue; }
    const winner = m >= f ? 'M' : 'F';
    const agree = Math.max(m, f) / n;
    if (agree < MIN_AGREE) { tally.contested++; continue; }   // unisex names are supposed to land here
    additions.push({ name, gender: winner, n, agree });
    tally.added++;
  }

  additions.sort((a, b) => b.n - a.n);
  console.error('top additions:');
  for (const a of additions.slice(0, 20)) console.error(`  ${a.name.padEnd(22)} ${a.gender}  n=${String(a.n).padStart(6)}  agree=${(a.agree * 100).toFixed(0)}%`);
  console.error(`\n  already in the map : ${tally.alreadyKnown.toLocaleString()}`);
  console.error(`  too few contacts   : ${tally.tooFew.toLocaleString()}`);
  console.error(`  contested (unisex) : ${tally.contested.toLocaleString()}`);
  console.error(`  NEW entries        : ${tally.added.toLocaleString()}`);

  if (!DRY && additions.length) {
    fs.writeFileSync(OUT, 'name,gender\n' + additions.map((a) => `${a.name},${a.gender}`).join('\n') + '\n', 'utf8');
    console.error(`\nwrote ${additions.length.toLocaleString()} addition(s) -> ${OUT}`);
    console.error('Review, then append to names-genders.csv (it is loaded fresh on every process start).');
  }
  console.error(`\n${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.stack || e.message || e)); process.exit(1); });
