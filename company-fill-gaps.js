/**
 * company-fill-gaps.js — fill two Company Crawler gaps from data already on the record.
 *
 *   OPENSEARCH_ENDPOINT=… node company-fill-gaps.js [--industry] [--location] [--prefix-fallback]
 *                                                   [--dry-run] [--limit N]
 *   (no --industry/--location flag = do both)
 *
 * 1. INDUSTRY from NAICS. naics-industry-crosswalk.csv maps a 6-digit NAICS code to an industry name.
 *    Measured against the live index: 4,921,099 companies carry a NAICS code with no industry, and the
 *    crosswalk covers 3,287,368 of them exactly (66.8%). A further 1,316,580 (26.8%) share only a 4-digit
 *    prefix with a crosswalk entry — that is the NAICS industry GROUP rather than the national industry,
 *    so it is an inference, not the crosswalk, and it stays behind --prefix-fallback.
 *
 * 2. LOCATION from the full address. The UI renders Location as locality + region + country, and 2,437,667
 *    companies have a full_address but no locality — almost all from the Google Maps import, in the shape
 *    "1984 Tobacco Rd, Augusta, GA 30906, United States".
 *
 * Only ever fills BLANKS. Nothing already populated is touched, so this is safe to re-run.
 */
const fs = require('fs');
const path = require('path');
const co = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const num = (f, d) => Number(arg(f, '')) || d;
const DRY = process.argv.includes('--dry-run');
const PREFIX_FALLBACK = process.argv.includes('--prefix-fallback');
const LIMIT = num('--limit', 0);
let DO_IND = process.argv.includes('--industry');
let DO_LOC = process.argv.includes('--location');
if (!DO_IND && !DO_LOC) { DO_IND = true; DO_LOC = true; }

function loadCrosswalk() {
  const p = path.join(__dirname, 'naics-industry-crosswalk.csv');
  const map = new Map();
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/).slice(1)) {
    if (!line.trim()) continue;
    const i = line.indexOf(',');
    const code = line.slice(0, i).trim();
    const ind = line.slice(i + 1).trim().replace(/^"|"$/g, '');
    if (code && ind) map.set(code, ind);
  }
  return map;
}

// "1984 Tobacco Rd, Augusta, GA 30906, United States" -> { locality:'Augusta', region:'GA', country:'United States' }
//
// Deliberately conservative: an address that does not clearly yield a field leaves that field blank rather
// than guessing. A wrong city is worse than an empty one, and the Location column is read as fact.
const US_STATES = new Set(('AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV ' +
  'NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR VI GU AS MP').split(' '));
const US_REGION_ZIP = /^([A-Z]{2})\s+(\d{5})(-\d{4})?$/;              // "GA 30906"
const CA_REGION_ZIP = /^([A-Z]{2})\s+([A-Z]\d[A-Z]\s*\d[A-Z]\d)$/i;   // "ON M5V 3A8"
function parseAddress(addr) {
  const out = { locality: '', region: '', country: '' };
  const parts = String(addr || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (parts.length < 3) return out;                                   // too little structure to be sure
  const last = parts[parts.length - 1];
  // Very common shape with the country omitted entirely: "401 Biscayne Blvd N-120, Miami, FL 33132".
  // A US state abbreviation followed by a 5-digit ZIP is unambiguous -- Canadian postcodes are
  // alphanumeric, and no other country pairs a 2-letter subdivision with a bare 5-digit code in this
  // position -- so the country can be filled with confidence rather than left blank. Measured: this shape
  // was 7.9% of the addresses the first version declined, roughly 193,000 records.
  const mUS = US_REGION_ZIP.exec(last);
  if (mUS && US_STATES.has(mUS[1].toUpperCase())) {
    out.region = mUS[1].toUpperCase();
    out.country = 'United States';
    const city = parts[parts.length - 2] || '';
    if (city && !/\d/.test(city)) out.locality = city;
    return out;
  }
  // A country is words, not digits. "…, GA 30906" that is not a US state+ZIP cannot be placed.
  if (/\d/.test(last) || last.length < 3) return out;
  out.country = last;
  const mid = parts[parts.length - 2];
  let m = US_REGION_ZIP.exec(mid) || CA_REGION_ZIP.exec(mid);
  if (m) {
    out.region = m[1].toUpperCase();
    out.locality = parts[parts.length - 3] || '';
  } else if (!/\d/.test(mid)) {
    // "…, Manchester, United Kingdom" — the middle is the locality, region unknown.
    out.locality = mid;
  } else {
    // "…, Sydney NSW 2000, Australia" — strip a trailing postcode and take what is left.
    const stripped = mid.replace(/\s+[A-Z0-9][A-Z0-9\s-]{2,}$/i, '').trim();
    if (stripped && !/\d/.test(stripped)) out.locality = stripped;
  }
  if (out.locality && /\d/.test(out.locality)) out.locality = '';     // a street number is not a city
  return out;
}

async function fillIndustry(client) {
  const cw = loadCrosswalk();
  console.error(`\n══════ INDUSTRY from NAICS ══════`);
  console.error(`crosswalk: ${cw.size} code(s)`);
  const byPrefix = new Map();
  if (PREFIX_FALLBACK) for (const [c, ind] of cw) { const p = c.slice(0, 4); if (!byPrefix.has(p)) byPrefix.set(p, ind); }

  const q = { bool: { filter: [{ exists: { field: 'naics_code' } }], must_not: [{ exists: { field: 'industry' } }] } };
  const total = (await client.count({ index: co.INDEX, body: { query: q } })).body.count;
  console.error(`companies with NAICS but no industry: ${total.toLocaleString()}${PREFIX_FALLBACK ? ' [prefix fallback ON]' : ''}`);

  let after = null, scanned = 0, matched = 0, viaPrefix = 0, updated = 0, errors = 0, buf = [];
  const flush = async () => {
    if (!buf.length || DRY) { buf = []; return; }
    const body = [];
    for (const it of buf) body.push({ update: { _index: co.INDEX, _id: it.id } }, { doc: { industry: it.industry } });
    buf = [];
    try {
      const r = await client.bulk({ body, refresh: false }, { requestTimeout: 180000 });
      const b = r.body || r;
      if (b.errors) for (const x of (b.items || [])) if (x.update && x.update.error) errors++;
      updated += body.length / 2;
    } catch (e) { errors += body.length / 2; console.error('  bulk failed:', e.message); }
  };

  const t0 = Date.now();
  for (;;) {
    const body = { size: 2000, query: q, _source: ['naics_code'], sort: [{ _doc: 'asc' }] };
    if (after) body.search_after = after;
    const r = await client.search({ index: co.INDEX, body }, { requestTimeout: 180000 });
    const hits = (r.body || r).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const code = String(h._source.naics_code || '').trim();
      let ind = cw.get(code);
      if (!ind && PREFIX_FALLBACK) { ind = byPrefix.get(code.slice(0, 4)); if (ind) viaPrefix++; }
      if (!ind) continue;
      matched++;
      buf.push({ id: h._id, industry: ind });
      if (buf.length >= 1000) await flush();
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 200000 < 2000) console.error(`  scanned ${scanned.toLocaleString()} | matched ${matched.toLocaleString()} | ${Math.round(scanned / Math.max(1, (Date.now() - t0) / 1000))}/s`);
    if (LIMIT && scanned >= LIMIT) break;
  }
  await flush();
  console.error(`INDUSTRY DONE: scanned ${scanned.toLocaleString()}, matched ${matched.toLocaleString()}${PREFIX_FALLBACK ? ` (${viaPrefix.toLocaleString()} via 4-digit prefix)` : ''}, ${DRY ? 'would update' : 'updated'} ${DRY ? matched.toLocaleString() : updated.toLocaleString()}, ${errors} error(s)`);
}

async function fillLocation(client) {
  console.error(`\n══════ LOCATION from full_address ══════`);
  const q = { bool: {
    filter: [{ exists: { field: 'full_address' } }],
    must_not: [{ exists: { field: 'locality' } }, { term: { 'full_address.keyword': '' } }] } };
  const total = (await client.count({ index: co.INDEX, body: { query: q } })).body.count;
  console.error(`companies with an address but no locality: ${total.toLocaleString()}`);

  let after = null, scanned = 0, parsed = 0, unparsed = 0, updated = 0, errors = 0, buf = [];
  const samples = [];
  const flush = async () => {
    if (!buf.length || DRY) { buf = []; return; }
    const body = [];
    for (const it of buf) body.push({ update: { _index: co.INDEX, _id: it.id } }, { doc: it.doc });
    buf = [];
    try {
      const r = await client.bulk({ body, refresh: false }, { requestTimeout: 180000 });
      const b = r.body || r;
      if (b.errors) for (const x of (b.items || [])) if (x.update && x.update.error) errors++;
      updated += body.length / 2;
    } catch (e) { errors += body.length / 2; console.error('  bulk failed:', e.message); }
  };

  const t0 = Date.now();
  for (;;) {
    const body = { size: 2000, query: q, _source: ['full_address', 'region', 'country'], sort: [{ _doc: 'asc' }] };
    if (after) body.search_after = after;
    const r = await client.search({ index: co.INDEX, body }, { requestTimeout: 180000 });
    const hits = (r.body || r).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const p = parseAddress(h._source.full_address);
      if (!p.locality) { unparsed++; if (samples.length < 5) samples.push(h._source.full_address); continue; }
      parsed++;
      const doc = { locality: p.locality };
      if (p.region && !h._source.region) doc.region = p.region;        // never overwrite what is there
      if (p.country && !h._source.country) doc.country = p.country;
      buf.push({ id: h._id, doc });
      if (buf.length >= 1000) await flush();
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 200000 < 2000) console.error(`  scanned ${scanned.toLocaleString()} | parsed ${parsed.toLocaleString()} | ${Math.round(scanned / Math.max(1, (Date.now() - t0) / 1000))}/s`);
    if (LIMIT && scanned >= LIMIT) break;
  }
  await flush();
  console.error(`LOCATION DONE: scanned ${scanned.toLocaleString()}, parsed ${parsed.toLocaleString()}, unparsed ${unparsed.toLocaleString()}, ${DRY ? 'would update' : 'updated'} ${DRY ? parsed.toLocaleString() : updated.toLocaleString()}, ${errors} error(s)`);
  if (samples.length) { console.error('  addresses left alone (too little structure to be sure):'); for (const s of samples) console.error(`    ${s}`); }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  if (DRY) console.error('[dry-run: nothing will be written]');
  if (DO_IND) await fillIndustry(client);
  if (DO_LOC) await fillLocation(client);
  try { await client.indices.refresh({ index: co.INDEX }); } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
