/**
 * atp-email-backfill.js — stamp each ATP Library brand with how many of its Corporate Places have a
 * non-empty email, so the "Has email" filter works without re-fetching any GeoJSON. Aggregates the
 * already-indexed corporate_places by spider (email != '') and writes `emails` onto the atp_library docs.
 *
 *   OPENSEARCH_ENDPOINT=… node atp-email-backfill.js
 */
const atp = require('./atp');
const cp = require('./corporate-places');

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = atp.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await atp.ensureIndex(client);

  // Count non-empty-email places per brand (keyed by spider = atp_library _id).
  const r = await client.search({ index: cp.INDEX, body: {
    size: 0, query: { bool: { must_not: [{ term: { email: '' } }] } },
    aggs: { b: { terms: { field: 'spider', size: 5000 } } },
  } });
  const buckets = ((r.body || r).aggregations.b.buckets || []).filter((x) => x.key);
  console.error(`${buckets.length} brand(s) have >=1 email`);

  let updated = 0, errors = 0;
  for (let i = 0; i < buckets.length; i += 500) {
    const body = [];
    for (const b of buckets.slice(i, i + 500)) body.push({ update: { _index: atp.INDEX, _id: b.key } }, { doc: { emails: b.doc_count } });
    const res = await client.bulk({ body, refresh: false });
    for (const it of (((res.body || res).items) || [])) { const u = it.update || {}; if (u.error || (u.status && u.status >= 400)) errors++; else updated++; }
  }
  try { await client.indices.refresh({ index: atp.INDEX }); } catch (e) { /* */ }
  console.error(`DONE: stamped emails on ${updated} brand(s), ${errors} error(s)`);
  try {
    const withEmail = (await client.count({ index: atp.INDEX, body: { query: { range: { emails: { gt: 0 } } } } })).body.count;
    console.error(`ATP Library brands with emails>0 now: ${withEmail}`);
  } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
