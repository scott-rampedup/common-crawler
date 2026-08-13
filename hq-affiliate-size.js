/**
 * hq-affiliate-size.js — size the HQ -> Affiliate consolidation before running it.
 *
 *   OPENSEARCH_ENDPOINT=… node hq-affiliate-size.js [--type HQ] [--page 10000]
 *
 * "HQ is reserved for a unique webpage — but not a unique root domain. adobe.com has 14 HQ's."
 *
 * That is the defect behind the wrong firmographics on contacts: a contact resolves its company by root
 * domain, finds N records all claiming HQ, and picks one arbitrarily. The fix is one winner per domain and
 * the rest demoted to Affiliate. This measures how many records that actually moves, so the migration is
 * a known quantity rather than a surprise.
 *
 * Two earlier attempts at this died: a cardinality agg over 41M docs timed out twice. A composite
 * aggregation pages through the `domain` keyword with an after_key and constant memory, so it finishes.
 * Nothing is kept in memory but counters — the bucket distribution is accumulated as it streams.
 *
 * Read-only. Writes nothing, changes nothing.
 */
const co = require('/app/companies');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const TYPE = arg('type', 'HQ');
const PAGE = Number(arg('page', '10000')) || 10000;

const N = (n) => Number(n || 0).toLocaleString();

(async () => {
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const has = (f) => ({ bool: { must: [{ exists: { field: f } }], must_not: [{ term: { [f]: '' } }] } });
  const Q = { bool: { must: [{ term: { 'company_type.keyword': TYPE } }], filter: [has('domain')] } };

  const total = (await c.count({ index: co.INDEX, body: { query: { match_all: {} } } })).body.count;
  const typed = (await c.count({ index: co.INDEX, body: { query: { term: { 'company_type.keyword': TYPE } } } })).body.count;
  const withDomain = (await c.count({ index: co.INDEX, body: { query: Q } })).body.count;
  console.log(`companies total            ${N(total)}`);
  console.log(`  company_type = ${TYPE.padEnd(10)}   ${N(typed)}`);
  console.log(`  ...of those, with domain ${N(withDomain)}   (${((withDomain / Math.max(1, typed)) * 100).toFixed(1)}%)\n`);
  console.log(`paging distinct domains ${N(PAGE)} at a time…\n`);

  let after = null, domains = 0, docs = 0, pages = 0, biggest = { d: '', n: 0 };
  // Bucket-size distribution: how concentrated the duplication is decides whether this is a broad
  // one-extra-per-domain trim or a handful of pathological domains carrying most of the excess.
  const bands = [['1 (already unique)', 1, 1], ['2', 2, 2], ['3-5', 3, 5], ['6-10', 6, 10],
    ['11-50', 11, 50], ['51-200', 51, 200], ['201-1000', 201, 1000], ['1000+', 1001, Infinity]];
  const bandDomains = bands.map(() => 0), bandDocs = bands.map(() => 0);
  const t0 = Date.now();

  for (;;) {
    const src = { domain: { terms: { field: 'domain' } } };
    const comp = { size: PAGE, sources: [src] };
    if (after) comp.after = after;
    // requestTimeout is a CLIENT option — passing it inside the search params makes OpenSearch reject
    // the request outright as an unrecognized query-string parameter.
    const r = await c.search(
      { index: co.INDEX, body: { size: 0, query: Q, aggs: { byDomain: { composite: comp } } } },
      { requestTimeout: 300000 },
    );
    const agg = (r.body || r).aggregations.byDomain;
    const buckets = agg.buckets || [];
    if (!buckets.length) break;

    for (const b of buckets) {
      const n = b.doc_count;
      domains++; docs += n;
      if (n > biggest.n) biggest = { d: b.key.domain, n };
      for (let i = 0; i < bands.length; i++) {
        if (n >= bands[i][1] && n <= bands[i][2]) { bandDomains[i]++; bandDocs[i] += n; break; }
      }
    }
    after = agg.after_key;
    if (++pages % 25 === 0) console.log(`  ${N(domains)} domains / ${N(docs)} docs  (${Math.round((Date.now() - t0) / 1000)}s)`);
    if (!after) break;
  }

  const affiliates = docs - domains;         // one winner survives per domain; every other record demotes
  console.log(`\n=== ${TYPE} consolidation, ${Math.round((Date.now() - t0) / 1000)}s ===`);
  console.log(`  records with a domain      ${N(docs)}`);
  console.log(`  distinct root domains      ${N(domains)}    <- surviving ${TYPE} records`);
  console.log(`  would become Affiliate     ${N(affiliates)}    ${((affiliates / Math.max(1, docs)) * 100).toFixed(1)}% of the population`);
  console.log(`  mean records per domain    ${(docs / Math.max(1, domains)).toFixed(2)}`);
  console.log(`  worst offender             ${biggest.d} (${N(biggest.n)})`);
  console.log(`\n  records per domain:`);
  console.log(`    band                 domains        docs     excess`);
  for (let i = 0; i < bands.length; i++) {
    if (!bandDomains[i]) continue;
    const excess = bandDocs[i] - bandDomains[i];
    console.log(`    ${bands[i][0].padEnd(18)} ${N(bandDomains[i]).padStart(10)}  ${N(bandDocs[i]).padStart(10)}  ${N(excess).padStart(9)}`);
  }
})().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
