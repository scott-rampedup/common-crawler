// Retire People sitemaps that have NEVER produced a bio URL.
//
// Rule: last_seen_urls = 0 AND total_new = 0. The second clause is what keeps this safe -- 9,026 sitemaps
// also fetched 0 on the final pass but have produced before (avg 366 URLs, max 87,040). A zero-fetch and a
// hard block are indistinguishable in monitor_note, so "fetched nothing once" is not evidence of a dead
// sitemap. Only "never produced anything, across every pass we have ever run" is.
//
// --apply writes; default is a dry run. Every id is exported first so the change is reversible.
const fs = require('fs');
const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');

const APPLY = process.argv.includes('--apply');
const OUT = process.argv.includes('--out') ? process.argv[process.argv.indexOf('--out') + 1] : null;
const NOTE = 'retired 2026-11-03: 0 bio URLs on every pass ever run';

const client = new Client({
  ...AwsSigv4Signer({ region: 'us-east-1', service: 'es', getCredentials: () => defaultProvider()() }),
  node: 'https://search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com',
});

const QUERY = {
  bool: {
    filter: [{ term: { kind: 'People' } }, { term: { last_seen_urls: 0 } }, { term: { total_new: 0 } }],
    must_not: [{ term: { monitored: false } }, { term: { status: 'inactive' } }],
  },
};

(async () => {
  const out = OUT ? fs.createWriteStream(OUT) : null;
  let after = null, seen = 0, written = 0, errors = 0, batch = [];

  const flush = async () => {
    if (!batch.length || !APPLY) { batch = []; return; }
    const body = [];
    for (const id of batch) {
      body.push({ update: { _index: 'sitemaps', _id: id } });
      body.push({ doc: { monitored: false, monitor_note: NOTE } });
    }
    const r = await client.bulk({ body, refresh: false });
    for (const it of (r.body.items || [])) { if (it.update && it.update.error) errors++; else written++; }
    batch = [];
  };

  for (;;) {
    const body = { size: 2000, query: QUERY, _source: ['sitemap_url', 'domain', 'total_new', 'last_checked'],
      sort: [{ sitemap_url: 'asc' }] };
    if (after) body.search_after = after;
    const r = await client.search({ index: 'sitemaps', body });
    const hits = r.body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      seen++;
      if (out) out.write(JSON.stringify({ id: h._id, ...h._source }) + '\n');
      batch.push(h._id);
    }
    if (batch.length >= 2000) await flush();
    after = hits[hits.length - 1].sort;
    if (seen % 20000 === 0) console.log('  ' + seen.toLocaleString() + ' scanned, ' + written.toLocaleString() + ' retired');
  }
  await flush();
  if (out) await new Promise((res) => out.end(res));

  console.log((APPLY ? 'APPLIED' : 'DRY RUN') + ': ' + seen.toLocaleString() + ' sitemap(s) matched'
    + (APPLY ? ', ' + written.toLocaleString() + ' retired, ' + errors.toLocaleString() + ' error(s)' : ''));
  if (out) console.log('restore list: ' + OUT);
  if (APPLY) { try { await client.indices.refresh({ index: 'sitemaps' }); } catch (e) { /* best-effort */ } }
})();
