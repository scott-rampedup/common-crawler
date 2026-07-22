/**
 * name-derive.js — for contacts with a BLANK first name, derive First/Last from the email (firstname.lastname
 * pattern) or a linkedin.com/in profile, then assign Gender from the names map. Client-side scan + bulk
 * partial-update (nameFromEmail/nameFromLinkedin aren't expressible in regex-less painless).
 *   OPENSEARCH_ENDPOINT=… node name-derive.js --dry [--limit N]   (report est. yield)
 *   OPENSEARCH_ENDPOINT=… node name-derive.js [--limit N]         (apply)
 */
const path = require('path');
const os = require('./opensearch');
const che = require('./cc-home-enrich');
const ex = require('./extractor');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const gmap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
const cap = (s) => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';

// derive {first,last,gender} from a record's email / linkedin, or null
function derive(email, linkedin) {
  const ne = che.nameFromEmail(email || '');
  let nm = (ne.first && ne.last) ? ne : null;
  if (!nm && linkedin) { const nl = che.nameFromLinkedin(linkedin); if (nl.first && nl.last) nm = nl; }
  if (!nm) return null;
  const first = cap(nm.first), last = cap(nm.last);
  return { first, last, name: `${first} ${last}`.trim(), gender: gmap[first.toLowerCase()] || '' };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  let client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const QUERY = { term: { 'first.kw': '' } };
  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.error(`blank-first contacts: ${total.toLocaleString()} | map ${Object.keys(gmap).length.toLocaleString()} names`);

  let scanned = 0, derived = 0, gendered = 0, updated = 0, after = null; let buf = []; const t0 = Date.now();
  const flush = async () => {
    if (!buf.length) return; const body = buf; buf = [];
    for (let a = 0; a < 5; a++) { try { const r = await client.bulk({ body, refresh: false }); const b = r.body || r; if (b.items) for (const it of b.items) if (it.update && it.update.error) {} updated += body.length / 2; return; }
      catch (e) { if (a === 2) client = os.makeClient(process.env.OPENSEARCH_ENDPOINT); await new Promise((r) => setTimeout(r, 400 * 2 ** a)); } }
  };
  for (;;) {
    const bodyq = { size: 5000, query: QUERY, _source: ['email', 'linkedin_url'], sort: [{ _doc: 'asc' }] };
    if (after) bodyq.search_after = after;
    const res = await client.search({ index: os.INDEX, body: bodyq });
    const hits = (res.body || res).hits.hits; if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const d = derive(h._source.email, h._source.linkedin_url);
      if (d) { derived++; if (d.gender) gendered++;
        if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: h._id } }, { doc: { first: d.first, last: d.last, name: d.name, gender: d.gender } }); if (buf.length >= 4000) await flush(); }
      }
      if (LIMIT && scanned >= LIMIT) break;
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 100000 === 0) console.error(`  scanned ${scanned.toLocaleString()} | derived ${derived.toLocaleString()} (${gendered.toLocaleString()} gendered) | ${Math.round(scanned / ((Date.now() - t0) / 1000))}/s`);
    if (LIMIT && scanned >= LIMIT) break;
  }
  if (!DRY) await flush();
  console.error(`DONE: scanned ${scanned.toLocaleString()} | derived ${derived.toLocaleString()} names (${gendered.toLocaleString()} gendered)${DRY ? ' [DRY]' : ` | ${updated.toLocaleString()} updated`} | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.message || e)); process.exit(1); });
