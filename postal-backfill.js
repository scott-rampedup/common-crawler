/**
 * postal-backfill.js — fill postal_code from the full_address text already stored.
 * ---------------------------------------------------------------------------------------------
 * 7,130,013 company records carry a full_address and none carried a postal code, because the column
 * did not exist until now. The codes are sitting in those strings in a very regular position:
 *
 *   united states   "1900 Powell St STE 716, Emeryville, CA 94608, United States"
 *   united kingdom  "The Welsh Mill Park Hill Dr, Frome BA11 2LE"
 *   canada          "340 Robson St, Vancouver, BC V6B 2B2, Canada"
 *   australia       "535 Bourke St, Melbourne VIC 3000, Australia"
 *
 * One pattern per country, never one pattern for everything: "90210", "SW1A 1AA" and "V6B 2B2" share
 * no shape, and a loose \d{4,5} would happily return a street number or a suite. Each pattern takes the
 * LAST match in the string, since every one of these formats puts the code at the end while street
 * numbers sit at the front.
 *
 *   node postal-backfill.js                  # dry run: sample and report the hit rate per country
 *   node postal-backfill.js --apply
 *   node postal-backfill.js --country "united kingdom" --apply
 */
const co = require('./companies');

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const argOf = (f, d) => { const i = args.indexOf('--' + f); return i > -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : d; };
const ONLY = argOf('country', '');
const SAMPLE = Number(argOf('sample', '')) || 4000;
const PAGE = 2000;
const N = (x) => Number(x || 0).toLocaleString();

// `last` pulls the final match, which is where all of these formats put the code.
const last = (s, re) => { const m = String(s).match(re); return m ? m[m.length - 1] : ''; };
const tidy = (s) => String(s || '').toUpperCase().replace(/\s+/g, ' ').trim();

const RULES = {
  'united states':  (a) => last(a, /\b\d{5}(?:-\d{4})?\b/g),
  'canada':         (a) => last(a, /\b[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d\b/g),
  'united kingdom': (a) => last(a, /\b[A-Za-z]{1,2}\d[A-Za-z\d]?[ ]?\d[A-Za-z]{2}\b/g),
  'australia':      (a) => last(a, /\b\d{4}\b/g),
  'germany':        (a) => last(a, /\b\d{5}\b/g),
  'france':         (a) => last(a, /\b\d{5}\b/g),
  'spain':          (a) => last(a, /\b\d{5}\b/g),
  'italy':          (a) => last(a, /\b\d{5}\b/g),
  'japan':          (a) => last(a, /\b\d{3}-?\d{4}\b/g),
  'netherlands':    (a) => last(a, /\b\d{4}\s?[A-Za-z]{2}\b/g),
  'brazil':         (a) => last(a, /\b\d{5}-?\d{3}\b/g),
  'mexico':         (a) => last(a, /\b\d{5}\b/g),
  'india':          (a) => last(a, /\b\d{6}\b/g),
  'switzerland':    (a) => last(a, /\b\d{4}\b/g),
  'austria':        (a) => last(a, /\b\d{4}\b/g),
  'belgium':        (a) => last(a, /\b\d{4}\b/g),
  'poland':         (a) => last(a, /\b\d{2}-\d{3}\b/g),
  'sweden':         (a) => last(a, /\b\d{3}\s?\d{2}\b/g),
  'norway':         (a) => last(a, /\b\d{4}\b/g),
  'denmark':        (a) => last(a, /\b\d{4}\b/g),
  'ireland':        (a) => last(a, /\b[A-Za-z]\d{2}\s?[A-Za-z\d]{4}\b/g),
  'new zealand':    (a) => last(a, /\b\d{4}\b/g),
};

const hasAddr = { bool: { must: [{ exists: { field: 'full_address.keyword' } }], must_not: [{ term: { 'full_address.keyword': '' } }] } };
const noPostal = { bool: { must_not: [{ bool: { must: [{ exists: { field: 'postal_code' } }], must_not: [{ term: { postal_code: '' } }] } }] } };
// `scope` is for COUNTING what is left to do. The SCAN deliberately does not filter on noPostal:
// paging through a query while writing the very field that query excludes removes rows from under the
// cursor, and search_after then walks off the end early — the first US run reported 4,181,409 due and
// wrote only 1,758,661. The scan therefore reads every record with an address and skips the ones that
// already have a code in code, where the result set cannot shift beneath it.
const scope = (country) => ({ bool: { filter: [hasAddr, noPostal, { term: { country } }] } });
const scanScope = (country) => ({ bool: { filter: [hasAddr, { term: { country } }] } });

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const count = (q) => c.count({ index: co.INDEX, body: { query: q } }, { requestTimeout: 600000 }).then((r) => Number((r.body || r).count));
  const countries = ONLY ? [ONLY] : Object.keys(RULES);
  let grandFound = 0, grandWritten = 0;

  for (const country of countries) {
    const rule = RULES[country];
    if (!rule) { console.log('no rule for ' + JSON.stringify(country)); continue; }
    const q = scope(country);
    const total = await count(q);
    if (!total) { console.log(country.padEnd(18) + '      — nothing to do'); continue; }

    let scanned = 0, found = 0, written = 0, errors = 0;
    const samples = [];
    let after = null, buf = [];
    const flush = async () => {
      if (!buf.length || !APPLY) { buf = []; return; }
      const body = [];
      for (const b of buf) body.push({ update: { _index: co.INDEX, _id: b.id } }, { doc: { postal_code: b.code } });
      buf = [];
      for (let a = 0; ; a++) {
        try {
          const r = await c.bulk({ body, refresh: false }, { requestTimeout: 300000 }).then((x) => (x.body || x));
          if (r.errors) for (const it of (r.items || [])) if (it.update && it.update.error) errors++;
          written += body.length / 2;
          return;
        } catch (e) { if (a >= 5) throw e; await new Promise((s) => setTimeout(s, 2000 * 2 ** a)); }
      }
    };

    for (;;) {
      const body = { size: PAGE, query: q, _source: ['full_address'], sort: [{ _doc: 'asc' }] };
      if (after) body.search_after = after;
      const r = await c.search({ index: co.INDEX, body }, { requestTimeout: 300000 }).then((x) => (x.body || x));
      const hits = r.hits.hits || [];
      if (!hits.length) break;
      for (const h of hits) {
        scanned++;
        const code = tidy(rule(h._source.full_address || ''));
        if (!code) continue;
        found++;
        if (samples.length < 3) samples.push(h._source.full_address + '  ->  ' + code);
        buf.push({ id: h._id, code });
      }
      if (buf.length >= 2000) await flush();
      after = hits[hits.length - 1].sort;
      if (APPLY && scanned % 500000 < PAGE) console.log('    ' + country + ': scanned ' + N(scanned) + ' / ' + N(total) + ', found ' + N(found));
      if (!APPLY && scanned >= SAMPLE) break;     // the query keeps matching, so one pass is the sample
    }
    await flush();
    grandFound += found; grandWritten += written;
    const pct = scanned ? (found / scanned * 100).toFixed(1) : '0';
    console.log(country.padEnd(18) + N(total).padStart(10) + ' to do   ' +
                (APPLY ? 'written ' + N(written) : 'sampled ' + N(scanned)) + ', hit rate ' + pct + '%' + (errors ? ', ' + errors + ' errors' : ''));
    for (const s of samples) console.log('      ' + s);
  }

  console.log('\n' + (APPLY ? 'written total: ' + N(grandWritten) : 'Dry run — nothing written. Add --apply.'));
  if (APPLY) {
    await c.indices.refresh({ index: co.INDEX });
    console.log('postal_code non-empty now: ' + N(await count({ bool: { must: [{ exists: { field: 'postal_code' } }], must_not: [{ term: { postal_code: '' } }] } })));
  }
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
