/**
 * canon-place-repair.js — lower-case country / region / locality / industry on the records that were
 * written with the source's own casing.
 * ---------------------------------------------------------------------------------------------
 * Why: companies.buildQuery lower-cases the value you filter on, and shapeRow title-cases for display,
 * so lower-case IS the stored form. The Google/Bing Location path passed the source casing through, so
 * ~3.19M records sat in buckets no filter could reach: the country facet showed "united states
 * 7,709,088" AND "United States 2,400,640", and clicking the second returned the first one's documents.
 * companies.canonPlace now fixes the write path; this fixes what is already in the index.
 *
 *   node canon-place-repair.js                 # dry run: enumerate the bad values, count the records
 *   node canon-place-repair.js --apply         # rewrite them
 *   node canon-place-repair.js --field country # one field only
 *
 * Selection is by EXACT VALUE, not by script. A `script` query comparing doc['f'].value to its
 * lower-case form costs ~20s of CPU per shard on 26M docs: the cluster's search backpressure cancels
 * it mid-flight, and _count then returns a half-counted total without raising (country came back
 * 1,595,255 against a true 3,186,625). A composite aggregation walks the distinct values for a few
 * hundred ms instead, and `terms` on those values is an ordinary cheap filter.
 *
 * Safe to re-run: the update script sets ctx.op='noop' when a value is already lower-case, and the
 * terms list is rebuilt from the index each run, so a second pass finds nothing.
 */
const co = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const APPLY = process.argv.includes('--apply');
const ONLY = arg('field', '');
const CHUNK = Number(arg('chunk', '')) || 500;      // bad values per update_by_query call
// Composite page size for enumerating distinct values. `locality` has 310,757 of them and its paging
// got rejected outright — "heap usage exceeded [2.2gb >= 40.9mb]" — while country's 569 never blinked.
// Smaller pages cost more round trips and ask for far less heap per request.
const AGG_PAGE = Number(arg('agg-page', '')) || 2000;
// Unthrottled by default, and keep each call SHORT — that second part is what actually matters.
// Measured on this index, with the live fleet writing to it throughout:
//   country  1 call  over 3,186,625 docs, 1171s -> reported 33,240 updated, 416,457 conflicts
//   industry 1 call  over 3,200,441 docs,  540s -> reported 167,995 updated, 941,319 conflicts
//   locality 25 calls over 3,190,521 docs, 625s -> reported 3,190,521 updated, 0 conflicts
// All three ended with the field fully canonical, so the writes always landed — but a long-running
// call takes its snapshot once and then collides with every concurrent write for the rest of its life,
// and its own tally comes back wrong. Short calls are exact. Prefer more chunks over bigger ones; the
// throttle only makes a call last longer, which is the opposite of what helps.
const RPS = Number(arg('rps', '')) || -1;

// `locality` and `name` are text+`kw` in this mapping; the others are plain keyword. Aggregating or
// filtering on the wrong one of those returns zero buckets without erroring, which is how this class
// of bug stays invisible — country.keyword does not exist, so an audit on it reports a clean index.
const DV = { country: 'country', region: 'region', locality: 'locality.kw', industry: 'industry' };
const FIELDS = (ONLY ? [ONLY] : ['country', 'region', 'locality', 'industry']).filter((f) => DV[f]);

const N = (x) => Number(x || 0).toLocaleString();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function withRetry(fn, label) {
  for (let a = 0; ; a++) {
    try { return await fn(); }
    catch (e) {
      if (a >= 5) throw e;
      console.error('    retry ' + (a + 1) + '/5 ' + label + ': ' + (e.message || e).slice(0, 120));
      await sleep(Math.min(30000, 2000 * 2 ** a));
    }
  }
}

// Walk every distinct value of one field and keep the ones that are not already lower-case.
async function badValues(c, field) {
  const out = [];
  let after = null, docs = 0;
  for (;;) {
    const body = { size: 0, aggs: { v: { composite: { size: AGG_PAGE, sources: [{ k: { terms: { field: DV[field] } } }], ...(after ? { after } : {}) } } } };
    // A page missing a shard would leave bad values out of the list and silently under-repair, so a
    // partial page is never accepted — but it is retried rather than fatal. Losing a whole pass to one
    // shed search is what ended the first run two fields in, with the work already done.
    let r = null;
    for (let attempt = 1; ; attempt++) {
      const res = await c.search({ index: co.INDEX, body }, { requestTimeout: 300000 })
        .then((x) => (x.body || x))
        .catch((e) => ({ _error: e }));
      const failed = res._error ? -1 : ((res._shards && res._shards.failed) || 0);
      if (!failed) { r = res; break; }
      const why = res._error ? (res._error.message || String(res._error)).slice(0, 140) : failed + ' shard(s) failed';
      if (attempt >= 8) throw new Error('giving up on the ' + field + ' value list after ' + attempt + ' attempts (' + why + ')');
      const wait = Math.min(60000, 3000 * 2 ** (attempt - 1));
      console.error('    retry ' + attempt + '/8 on the ' + field + ' value list in ' + Math.round(wait / 1000) + 's: ' + why);
      await sleep(wait);
    }
    const agg = r.aggregations.v, bs = agg.buckets || [];
    if (!bs.length) break;
    for (const b of bs) {
      const k = String(b.key.k == null ? '' : b.key.k);
      if (!k || k === k.toLowerCase()) continue;
      out.push(k); docs += b.doc_count;
    }
    after = agg.after_key;
    if (!after) break;
  }
  return { values: out, docs };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const plan = [];

  console.log('--- scope ---');
  for (const f of FIELDS) {
    const { values, docs } = await badValues(c, f);
    plan.push({ field: f, values, docs });
    console.log('  ' + f.padEnd(10) + N(values.length).padStart(8) + ' non-lower-case value(s)   ' + N(docs).padStart(11) + ' record(s)');
  }
  console.log('  (a record wrong in several fields is counted once per field, and rewritten once per field)');

  if (!APPLY) {
    console.log('\nworst offenders');
    for (const p of plan) {
      if (!p.values.length) continue;
      console.log('  ' + p.field + ': ' + p.values.slice(0, 8).map((v) => JSON.stringify(v)).join(', ') + (p.values.length > 8 ? ' …' : ''));
    }
    console.log('\nDry run — nothing written. Re-run with --apply.');
    process.exit(0);
  }

  for (const p of plan) {
    if (!p.values.length) { console.log('\n' + p.field + ': already clean'); continue; }
    console.log('\n' + p.field + ': rewriting ' + N(p.docs) + ' record(s) in ' + Math.ceil(p.values.length / CHUNK) + ' chunk(s)');
    let updated = 0, noops = 0, conflicts = 0, failed = 0;
    const t0 = Date.now();
    for (let i = 0; i < p.values.length; i += CHUNK) {
      const slice = p.values.slice(i, i + CHUNK);
      const res = await withRetry(() => c.updateByQuery({
        index: co.INDEX,
        conflicts: 'proceed',          // the crawler and the ETL write to this index while we run
        slices: 'auto',
        wait_for_completion: true,
        requests_per_second: RPS,
        body: {
          query: { terms: { [DV[p.field]]: slice } },
          script: {
            lang: 'painless',
            // noop rather than a blind write, so re-running costs nothing and the refresh stays small
            source: `if (ctx._source.${p.field} instanceof String && ctx._source.${p.field} != ctx._source.${p.field}.toLowerCase()) { ctx._source.${p.field} = ctx._source.${p.field}.toLowerCase(); } else { ctx.op = 'noop'; }`,
          },
        },
      }, { requestTimeout: 3600000 }), p.field + ' chunk ' + (i / CHUNK + 1)).then((x) => (x.body || x));
      updated += res.updated || 0; noops += res.noops || 0;
      conflicts += res.version_conflicts || 0; failed += (res.failures || []).length;
      if ((i / CHUNK) % 10 === 0 || i + CHUNK >= p.values.length) {
        console.log('    ' + N(updated) + ' updated, ' + N(noops) + ' noop, ' + N(conflicts) + ' conflict, ' + failed + ' failed  (' + Math.round((Date.now() - t0) / 1000) + 's)');
      }
    }
    const left = await badValues(c, p.field);
    console.log('  done: ' + N(updated) + ' updated, ' + N(conflicts) + ' version conflict(s), ' +
                N(left.docs) + ' record(s) still non-canonical' + (left.docs ? ' (re-run to pick up conflicts)' : ''));
  }
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
