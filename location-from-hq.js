/**
 * location-from-hq.js — give a contact a Location from its company's HQ when nothing better is known.
 *
 *   OPENSEARCH_ENDPOINT=… node location-from-hq.js --dry [--limit N]
 *   OPENSEARCH_ENDPOINT=… node location-from-hq.js
 *
 * WHY: Location sits at 27.5% because it is derived almost entirely from a phone number, and only 21.2%
 * of contacts have a phone. But 56.1% already carry company firmographics including company_hq. Using it
 * roughly doubles location coverage without a single new fetch.
 *
 * PROVENANCE IS THE WHOLE POINT. A national firm's HQ is NOT where the individual sits, so this must not
 * be laundered into the same field as a phone-derived city. Three fields, three meanings, kept apart:
 *
 *   phone_location  DERIVED from the person's own phone — untouched here, ever
 *   work_location   the person's stated work location (schema.org workLocation) — what "Location" shows
 *   company_hq      the company's registered HQ — already populated by enrich-firmographics
 *
 * This only writes work_location, and only when the contact has NO location of any kind: no
 * work_location and no phone_location. Anything with a real signal keeps it.
 */
const os = require('./opensearch');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const PAGE = Number(process.env.PAGE || 5000);

// Contacts with a company HQ and no location signal of their own.
const QUERY = {
  bool: {
    must: [{ exists: { field: 'company_hq' } }],
    must_not: [
      { term: { company_hq: '' } },
      { bool: { must: [{ exists: { field: 'work_location' } }], must_not: [{ term: { work_location: '' } }] } },
      { bool: { must: [{ exists: { field: 'phone_location' } }], must_not: [{ term: { phone_location: '' } }] } },
    ],
  },
};

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  let client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  const all = (await client.count({ index: os.INDEX })).body.count;
  console.error(`contacts with a company HQ and no location of their own: ${total.toLocaleString()} of ${all.toLocaleString()}`
    + `${DRY ? '  [DRY RUN — no writes]' : ''}`);
  if (!total) { console.error('nothing to fill.'); process.exit(0); }

  const t0 = Date.now();
  const tally = { scanned: 0, filled: 0, updated: 0 };
  const samples = [];
  let buf = [], after = null;

  const flush = async () => {
    if (!buf.length) return;
    const body = buf; buf = [];
    for (let a = 0; a < 5; a++) {
      try { await client.bulk({ body, refresh: false }); tally.updated += body.length / 2; return; }
      catch (e) { if (a === 2) client = os.makeClient(process.env.OPENSEARCH_ENDPOINT); await new Promise((r) => setTimeout(r, 400 * 2 ** a)); }
    }
    console.error('  bulk failed after retries — continuing');
  };

  for (;;) {
    const body = { size: PAGE, query: QUERY, _source: ['email', 'company_hq', 'company_name', 'domain'], sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const hits = (await client.search({ index: os.INDEX, body })).body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      tally.scanned++;
      const hq = String(h._source.company_hq || '').trim();
      if (!hq) continue;
      tally.filled++;
      if (samples.length < 10) samples.push({ email: h._source.email, hq, co: h._source.company_name || h._source.domain || '' });
      if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: h._id } }, { doc: { work_location: hq } }); if (buf.length >= 4000) await flush(); }
      if (LIMIT && tally.scanned >= LIMIT) break;
    }
    after = hits[hits.length - 1].sort;
    if (tally.scanned % 100000 < PAGE) console.error(`  scanned ${tally.scanned.toLocaleString()} | filled ${tally.filled.toLocaleString()}`);
    if (LIMIT && tally.scanned >= LIMIT) break;
  }
  if (!DRY) await flush();

  if (samples.length) {
    console.error('\nsamples:');
    for (const s of samples) console.error(`  ${String(s.email).padEnd(38)} ${s.hq}   (${s.co})`);
  }
  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: scanned ${tally.scanned.toLocaleString()} | Location filled from HQ ${tally.filled.toLocaleString()}`
    + `${DRY ? '' : ` | ${tally.updated.toLocaleString()} updated`} | ${Math.round((Date.now() - t0) / 1000)}s`);
  console.error('phone_location was not modified — a phone-derived city stays the stronger, person-level signal.');
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.stack || e.message || e)); process.exit(1); });
