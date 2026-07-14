// Extract every unique contact `domain` from OpenSearch via a composite aggregation (paged), one per line.
const os = require('./opensearch');
const fs = require('fs');
const c = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
const out = fs.createWriteStream(process.argv[2] || 'contact-domains.txt');
(async () => {
  let after = null, total = 0, page = 0;
  for (;;) {
    const composite = { size: 10000, sources: [{ domain: { terms: { field: 'domain' } } }] };
    if (after) composite.after = after;
    const r = await c.search({ index: 'contacts', body: { size: 0, aggs: { d: { composite } } } });
    const agg = (r.body || r).aggregations.d;
    for (const b of agg.buckets) { if (b.key.domain) { out.write(b.key.domain + '\n'); total++; } }
    page++;
    if (page % 20 === 0) console.error(`  ${total.toLocaleString()} domains (page ${page})`);
    if (!agg.buckets.length || !agg.after_key) break;
    after = agg.after_key;
  }
  await new Promise((res) => out.end(res));
  console.error(`DONE: ${total.toLocaleString()} unique domains`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
