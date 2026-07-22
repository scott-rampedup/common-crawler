/**
 * junk-remove.js — delete near-miss "contacts" whose first name is a common English word (Board, Admin,
 * The, Forum, Executive, Sales, Info, …) and that STILL have no gender after the gender backfill. These are
 * page-title / nav words mistaken for people, not real contacts. Real name-words (Grace, June, Mark) were
 * gendered by the backfill so they're excluded; genuine uncommon names (Demba, Sibel) aren't common words
 * so they're spared. Also honors the opt-out registry implicitly (they're already removed).
 *   OPENSEARCH_ENDPOINT=… node junk-remove.js --dry   (report only)
 *   OPENSEARCH_ENDPOINT=… node junk-remove.js         (delete)
 */
const fs = require('fs');
const path = require('path');
const os = require('./opensearch');
const ex = require('./extractor');

const common = new Set(fs.readFileSync(path.join(__dirname, 'data-common-words.txt'), 'utf8').split(/\r?\n/).map((w) => w.trim().toLowerCase()).filter((w) => w.length >= 2));
const gmap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
// SAFETY: some common words are also real surnames/names — never delete these even if no-gender, to avoid
// dropping real people (esp. an ethnic-surname bias). Better to keep a few near-misses than lose real records.
const KEEP = new Set(['chen', 'wang', 'li', 'lee', 'kim', 'park', 'singh', 'patel', 'ng', 'xu', 'wu', 'liu', 'zhang', 'yang', 'huang', 'gao', 'guo', 'lin', 'ho', 'yu',
  'brown', 'green', 'black', 'white', 'gray', 'grey', 'young', 'king', 'bell', 'cook', 'hall', 'wood', 'ford', 'long', 'stone', 'reed', 'red', 'blue', 'rose', 'moon', 'sky', 'river', 'rain',
  'pastor', 'major', 'duke', 'earl', 'chief', 'grant', 'gross', 'love', 'star', 'sun', 'may', 'june']);
const noG = { bool: {
  must: [{ bool: { must_not: [{ term: { 'first.kw': '' } }] } }],
  should: [{ term: { gender: '' } }, { bool: { must_not: [{ exists: { field: 'gender' } }] } }], minimum_should_match: 1,
} };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  // distinct still-no-gender first names, keep those that are common words and NOT real names in the map
  const agg = (await client.search({ index: os.INDEX, body: { size: 0, query: noG, aggs: { f: { terms: { field: 'first.kw', size: 20000 } } } } })).body;
  const junk = []; let junkDocs = 0;
  for (const b of agg.aggregations.f.buckets) {
    const k = String(b.key).trim().toLowerCase();
    if (common.has(k) && !gmap[k] && !KEEP.has(k)) { junk.push(b.key); junkDocs += b.doc_count; }
  }
  console.log(`junk near-miss names: ${junk.length.toLocaleString()} distinct | ${junkDocs.toLocaleString()} docs`);
  console.log('top:', agg.aggregations.f.buckets.filter((b) => { const k = String(b.key).trim().toLowerCase(); return common.has(k) && !gmap[k]; }).slice(0, 20).map((b) => `${b.key}(${b.doc_count})`).join(', '));
  if (process.argv.includes('--dry') || !junk.length) return;
  // delete in batches of 1000 names (terms cap) via delete_by_query
  let deleted = 0;
  for (let i = 0; i < junk.length; i += 1000) {
    const names = junk.slice(i, i + 1000);
    const q = { bool: { must: [noG], filter: [{ terms: { 'first.kw': names } }] } };
    const r = await client.deleteByQuery({ index: os.INDEX, conflicts: 'proceed', refresh: false, body: { query: q } }, { requestTimeout: 300000 });
    deleted += (r.body || r).deleted || 0;
    console.log(`  batch ${i / 1000 + 1}: total deleted ${deleted.toLocaleString()}`);
  }
  console.log(`DONE: deleted ${deleted.toLocaleString()} junk near-miss contacts`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 400) : (e.message || e)); process.exit(1); });
