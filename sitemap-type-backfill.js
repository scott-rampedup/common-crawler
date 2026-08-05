/**
 * sitemap-type-backfill.js — one-time: stamp the `type` field (Parent | Child | Sub-Domain) on every
 * existing Sitemap Library doc, derived via sitemaps.deriveType (subdomain > has-parent > top-level).
 * Ensures the `type` keyword mapping exists first so the Type filter/facet work.
 *   OPENSEARCH_ENDPOINT=… node sitemap-type-backfill.js
 */
const sitemaps = require('./sitemaps');

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const INDEX = sitemaps.INDEX;
  try { await client.indices.putMapping({ index: INDEX, body: { properties: { type: { type: 'keyword' } } } }); }
  catch (e) { console.error('putMapping:', e.message); }

  let resp = await client.search({ index: INDEX, scroll: '5m', size: 1000, _source: ['sitemap_url', 'domain', 'parent_url'], body: { query: { match_all: {} } } });
  let sid = (resp.body || resp)._scroll_id;
  let scanned = 0, updated = 0, errors = 0, batch = [];
  const counts = { Parent: 0, Child: 0, 'Sub-Domain': 0 };
  const flush = async () => { if (!batch.length) return; const r = await client.bulk({ body: batch, refresh: false }); for (const it of (((r.body || r).items) || [])) { const u = it.update || {}; if (u.error) errors++; else updated++; } batch = []; };

  for (;;) {
    const hits = (resp.body || resp).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const t = sitemaps.deriveType(h._source.sitemap_url, h._source.domain, h._source.parent_url);
      counts[t] = (counts[t] || 0) + 1;
      batch.push({ update: { _index: INDEX, _id: h._id } }, { doc: { type: t } });
    }
    if (batch.length >= 2000) await flush();
    if (scanned % 20000 === 0) console.error(`  scanned ${scanned.toLocaleString()} | updated ${updated.toLocaleString()}`);
    resp = await client.scroll({ scroll_id: sid, scroll: '5m' }); sid = (resp.body || resp)._scroll_id;
  }
  await flush();
  try { await client.clearScroll({ body: { scroll_id: [sid] } }); } catch (e) { /* */ }
  await client.indices.refresh({ index: INDEX });
  console.error(`DONE: scanned ${scanned.toLocaleString()} | updated ${updated.toLocaleString()} | errors ${errors} | ${JSON.stringify(counts)}`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
