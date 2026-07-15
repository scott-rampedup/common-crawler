/**
 * dump-company-urls.js — write home-page URLs (https://{domain}/) for the companies matching a filter,
 * plus a domain->company map, to feed the Athena resolve + the CC-enrich step.
 *   OPENSEARCH_ENDPOINT=… node dump-company-urls.js '{"industry":"real estate","contactMin":"1"}' CAP urls.txt targets.ndjson
 */
const co = require('./companies');
const fs = require('fs');
(async () => {
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const filter = process.argv[2] ? JSON.parse(process.argv[2]) : { contactMin: '1' };
  const CAP = Number(process.argv[3]) || 1000000;
  const urlsOut = fs.createWriteStream(process.argv[4] || 'home-urls.txt');
  const mapOut = fs.createWriteStream(process.argv[5] || 'home-targets.ndjson');
  const query = co.buildQuery(filter);
  let after = null, n = 0;
  const seen = new Set();
  for (;;) {
    const body = { size: 5000, _source: ['domain', 'website', 'full_address', 'phone'], query, sort: [{ contact_count: 'desc' }, { id: 'asc' }] };
    if (after) body.search_after = after;
    const res = await client.search({ index: co.INDEX, body });
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      const s = h._source; const d = s.domain;
      if (!d || seen.has(d)) continue;
      seen.add(d);
      urlsOut.write('https://' + d + '/\n');
      mapOut.write(JSON.stringify({ domain: d, id: h._id, website: s.website || '', full_address: s.full_address || '', phone: s.phone || '' }) + '\n');
      if (++n >= CAP) break;
    }
    after = hits[hits.length - 1].sort;
    if (n >= CAP) break;
  }
  await new Promise((r) => urlsOut.end(r)); await new Promise((r) => mapOut.end(r));
  console.error('dumped ' + n.toLocaleString() + ' company home-page URLs');
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
