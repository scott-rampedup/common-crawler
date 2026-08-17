/**
 * normalize-websites.js — reclassify company Website fields: when a company's Website is really a
 * Facebook / Instagram / Google-Maps URL, or matches the admin Alternate-Websites list, move it out of
 * Website into the right field (Facebook / Instagram / Map / Alternate Websites) and blank the Website.
 *
 * This is the standalone/backfill version of cc-home-enrich.reclassifyWebsite (which only runs during CC
 * enrichment). Pre-filters to candidate websites so it only reads/writes the small matching subset.
 *
 *   OPENSEARCH_ENDPOINT=… node normalize-websites.js [--dry-run]
 */
const co = require('./companies');
const che = require('./cc-home-enrich');

(async () => {
  const DRY = process.argv.includes('--dry-run');
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const altList = await co.getAltWebsites(client);
  // Prefilter to candidates. `domain` holds the FULL host, so an exact terms[] match only ever finds
  // companies sitting on the bare pattern host and silently skips every subdomain of it. That is not a
  // small gap: 57,842 companies are on <name>.wixsite.com and 0 are on wixsite.com itself, so the whole
  // largest category was invisible to this backfill while the comment claimed the filter was
  // "over-inclusive". Each pattern host therefore needs both an exact term and a *.host wildcard.
  const DOMAINS = [...new Set(['facebook.com', 'fb.com', 'instagram.com', 'google.com', 'goo.gl', 'maps.app.goo.gl', 'bing.com', ...altList.map((a) => co.normDomain(a))].filter(Boolean).map((d) => d.toLowerCase()))];
  const query = { bool: { should: [
    { terms: { domain: DOMAINS } },
    ...DOMAINS.map((d) => ({ wildcard: { domain: { value: '*.' + d } } })),
  ], minimum_should_match: 1 } };
  console.error(`prefilter: ${DOMAINS.length} host(s), each matched exactly and as a subdomain`);
  console.error(`  ${DOMAINS.join(', ')}`);

  const total = (await client.count({ index: co.INDEX, body: { query } })).body.count;
  console.error(`candidate companies (Website looks like fb/ig/maps/alt): ${total.toLocaleString()}${DRY ? ' [dry-run]' : ''}`);
  if (DRY) { console.error('dry-run: not writing. Re-run without --dry-run to apply.'); return; }

  let buf = [], errs = 0;
  async function flush() {
    if (!buf.length) return; const body = [];
    for (const it of buf) body.push({ update: { _index: co.INDEX, _id: it.id } }, { doc: it.doc });
    buf = [];
    try { const r = await client.bulk({ body, refresh: false }); const b = r.body || r; if (b.errors) for (const x of b.items) if (x.update && x.update.error) errs++; }
    catch (e) { errs++; }
  }

  let after = null, scanned = 0, updated = 0; const t0 = Date.now();
  for (;;) {
    const body = { size: 2000, query, sort: [{ 'name.kw': 'asc' }, { id: 'asc' }], _source: ['id', 'website', 'facebook', 'instagram', 'map', 'alternate_websites'] };
    if (after) body.search_after = after;
    const res = await client.search({ index: co.INDEX, body });
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      const d = h._source; scanned++;
      const rc = che.reclassifyWebsite(d.website, altList);
      if (!('website' in rc)) continue;                       // not a candidate after the precise check
      const doc = { website: '', domain: '' };                // blank the (mis-classified) website
      if (rc.facebook && !d.facebook) doc.facebook = rc.facebook;
      if (rc.instagram && !d.instagram) doc.instagram = rc.instagram;
      if (rc.map && !d.map) doc.map = rc.map;
      if (rc.altMove) { const cur = String(d.alternate_websites || '').split(/[\s;]+/).filter(Boolean); doc.alternate_websites = [...new Set([...cur, rc.altMove])].join('; '); }
      buf.push({ id: d.id, doc }); updated++;
      if (buf.length >= 500) await flush();
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 20000 === 0) console.error(`  scanned ${scanned.toLocaleString()} | reclassified ${updated.toLocaleString()} | ${Math.round(scanned / ((Date.now() - t0) / 1000))}/s`);
  }
  await flush();
  console.error(`DONE: scanned ${scanned.toLocaleString()}, ${updated.toLocaleString()} websites reclassified, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
