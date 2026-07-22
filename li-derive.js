/**
 * li-derive.js — FREE name recovery for blank-name contacts that already carry a linkedin.com/in URL, by
 * parsing the profile slug: first the separator parser (firstname-lastname-123), then a dictionary split of
 * separator-less slugs (angelomarino -> Angelo Marino) using the 131k names map as the first-name lexicon.
 * Conservative (no serper cost): no-separator slugs only, first name ≥4 chars, org-word + length guards, so
 * org/vanity slugs (superior-negotiators, txlender) are skipped. Then assigns Gender from the name.
 *   OPENSEARCH_ENDPOINT=… node li-derive.js --dry [--limit N] | node li-derive.js [--limit N]
 */
const path = require('path');
const os = require('./opensearch');
const che = require('./cc-home-enrich');
const ex = require('./extractor');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const gm = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
const cap = (s) => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
const ORG = /loan|lender|mortgage|realt|group|team|llc|inc|onlus|negotiat|insurance|agency|propert|homes|lawyer|consult|solutions|services|marketing|digital|media|studio|academy|foundation|network|global|partners|associates|capital|ventures|advisor|financial/;

function splitConcat(slug) {
  const raw = String(slug || '').toLowerCase();
  if (/[-_.]/.test(raw)) return null;                       // has separators -> the separator parser owns it
  const s = raw.replace(/[^a-z]/g, '');
  if (s.length < 6 || s.length > 22 || ORG.test(s)) return null;
  for (let i = Math.min(s.length - 3, 12); i >= 4; i--) {   // longest first-name prefix ≥4, last ≥3
    const f = s.slice(0, i), l = s.slice(i);
    if (gm[f] && l.length >= 3 && l.length <= 15) return { first: cap(f), last: cap(l) };
  }
  return null;
}
function nameFromLi(url) {
  const via = che.nameFromLinkedin(url);
  if (via.first && via.last) return { first: cap(via.first), last: cap(via.last) };
  const m = String(url || '').match(/linkedin\.com\/in\/([^/?#]+)/i);
  return m ? splitConcat(m[1]) : null;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  let client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const QUERY = { bool: { must: [{ term: { 'first.kw': '' } }], must_not: [{ term: { linkedin_url: '' } }] } };
  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.error(`blank-name + linkedin_url: ${total.toLocaleString()}`);

  let scanned = 0, derived = 0, gendered = 0, updated = 0, after = null, buf = []; const t0 = Date.now();
  const flush = async () => { if (!buf.length) return; const body = buf; buf = [];
    for (let a = 0; a < 5; a++) { try { await client.bulk({ body, refresh: false }); updated += body.length / 2; return; } catch (e) { if (a === 2) client = os.makeClient(process.env.OPENSEARCH_ENDPOINT); await new Promise((r) => setTimeout(r, 400 * 2 ** a)); } } };
  for (;;) {
    const bq = { size: 5000, query: QUERY, _source: ['linkedin_url'], sort: [{ _doc: 'asc' }] };
    if (after) bq.search_after = after;
    const hits = (await client.search({ index: os.INDEX, body: bq })).body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const nm = nameFromLi(h._source.linkedin_url);
      if (nm && nm.first && nm.last) {
        derived++; const g = gm[nm.first.toLowerCase()] || ''; if (g) gendered++;
        if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: h._id } }, { doc: { first: nm.first, last: nm.last, name: `${nm.first} ${nm.last}`.trim(), gender: g } }); if (buf.length >= 4000) await flush(); }
      }
      if (LIMIT && scanned >= LIMIT) break;
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 50000 === 0) console.error(`  scanned ${scanned.toLocaleString()} | derived ${derived.toLocaleString()} (${gendered.toLocaleString()} gendered)`);
    if (LIMIT && scanned >= LIMIT) break;
  }
  if (!DRY) await flush();
  console.error(`DONE: scanned ${scanned.toLocaleString()} | derived ${derived.toLocaleString()} (${gendered.toLocaleString()} gendered)${DRY ? ' [DRY]' : ` | ${updated.toLocaleString()} updated`} | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.message || e)); process.exit(1); });
