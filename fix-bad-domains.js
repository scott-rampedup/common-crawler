/**
 * fix-bad-domains.js — repair Company Crawler records whose `domain` is a free-mail/social/shared host
 * (facebook.com, gmail.com, sites.google.com, …) instead of a real web source. These came from Google
 * Maps / AllThePlaces where the business's "website" was a social/free page. The source fields are
 * ALREADY in the record (name, full_address, phone, category) — so we enrich in place:
 *   - Location docs: parse full_address -> locality/region/country (GM leaves them blank), and CLEAR the
 *     bad domain + website (there is no real web source), keeping the business as a clean location POI.
 *   - the synthetic HQ doc for a bad domain (e.g. one "facebook.com company" merging 114k businesses): DELETE.
 *
 *   OPENSEARCH_ENDPOINT=… node fix-bad-domains.js [--apply]      (default = dry run)
 */
const co = require('./companies');
const APPLY = process.argv.includes('--apply');
const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
const INDEX = co.INDEX || 'companies';

const COUNTRIES = new Set(['united states', 'usa', 'us', 'united kingdom', 'uk', 'canada', 'australia', 'ireland', 'new zealand']);
function parseAddr(addr) {
  const parts = String(addr || '').split(',').map((s) => s.trim()).filter(Boolean);
  let country = '', region = '', city = '';
  if (parts.length && COUNTRIES.has(parts[parts.length - 1].toLowerCase())) country = parts.pop();
  const stzip = parts[parts.length - 1] || '';
  const m = stzip.match(/^([A-Za-z]{2})\s+\w/); if (m) { region = m[1]; if (!country) country = 'United States'; }
  city = parts[parts.length - 2] || '';
  return { city, region, country };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  // ONE `terms` query, not a bool.should of one `term` per host. The should-form built a clause per
  // domain and the list is ~4,900 long, so every run died on
  //   query_shard_exception: maxClauseCount is set to 1024
  // before reading a single record. That limit counts BOOL CLAUSES; a `terms` query is a single clause
  // holding many values, bounded instead by index.max_terms_count (65,536 by default), so the same
  // list fits with room to spare. This tool has therefore been unable to run since the list passed
  // 1,024 entries -- which is part of why the platform-domain records it exists to clean are still
  // in the index.
  const query = { terms: { domain: [...co.NON_COMPANY_DOMAINS] } };
  const total = (await client.count({ index: INDEX, body: { query } })).body.count;
  console.error(`bad-domain company records: ${total.toLocaleString()} | mode: ${APPLY ? 'APPLY' : 'DRY RUN'}`);

  let scanned = 0, enriched = 0, geoFilled = 0, mergeShells = 0, providerSelf = 0, batch = [];

  // THE PROVIDER IS ITSELF A COMPANY, and it has a record in here like anyone else.
  //
  // non-company-domains splits its list in two and tests them differently: PLATFORM_HOSTS get the
  // mailbox test, because "an @facebook.com address really does work at Facebook", while FREEMAIL_HOSTS
  // are cleared OUTRIGHT on the reasoning that nobody is employed by their mailbox provider. That
  // reasoning holds for a contact. It does not hold for a COMPANY record, where the provider itself is
  // one of the companies -- and there is no mailbox here to test with.
  //
  // Measured 2026-11-22 over all 171,207 matched records: 670 (0.39%) are the provider itself, 336 of
  // them HQ. Without this guard --apply would blank the real corporate domain of Telefonica Moviles
  // Espana (telefonica.es), Frontier (104 records), Sky (97), Cox (67), O2 (7) and the National
  // University of Singapore (19), and the field would simply be gone.
  //
  // The company-side equivalent of the mailbox test: the record IS the provider when the first token of
  // its name matches the domain's first label. Deliberately narrow -- it only protects a record that
  // names itself after the host, which is exactly the case the outright rule gets wrong. It will not
  // catch a provider under a different trading name (Deutsche Telekom on t-online.de), so 670 is a
  // floor, not a ceiling; widen it only with a measurement in hand.
  const firstLabel = (d) => String(d || '').toLowerCase().replace(/^www\./, '').split('.')[0];
  const nameHead = (s) => (String(s || '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').trim().split(/\s+/)[0] || '');
  const flush = async () => { if (APPLY && batch.length) { const r = await client.bulk({ body: batch, refresh: false }); if ((r.body || r).errors) { /* ignore per-item */ } } batch = []; };

  let resp = await client.search({ index: INDEX, scroll: '5m', size: 1000,
    _source: ['company_type', 'full_address', 'locality', 'region', 'country', 'domain', 'website', 'name', 'company_name', 'location_count'], body: { query } });
  let sid = (resp.body || resp)._scroll_id; const samples = [];
  for (;;) {
    const hits = (resp.body || resp).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++; const s = h._source; const id = h._id;
      // KEEP every record (real businesses). Strip the non-website domain/source; parse the address into
      // locality/region/country when GM left them blank. Only flag the mass-merge shell (an HQ that
      // aggregated many businesses under one bad host, location_count huge) for reporting — not deletion.
      if (s.company_type === 'HQ' && Number(s.location_count) > 50) mergeShells++;
      // Leave the provider's own record exactly as it is -- domain, website and all.
      const lab = firstLabel(s.domain);
      if (lab && nameHead(s.name || s.company_name) === lab) { providerSelf++; continue; }
      const doc = { domain: '', website: '' };
      if (s.name && !s.company_name) doc.company_name = s.name;          // apply Company Name from the source business name
      if (!s.locality && !s.region && s.full_address) { const a = parseAddr(s.full_address); if (a.city || a.region) { doc.locality = co.titleCase ? co.titleCase(a.city) : a.city; doc.region = (a.region || '').toUpperCase(); doc.country = a.country || 'United States'; geoFilled++; } }
      enriched++; batch.push({ update: { _index: INDEX, _id: id } }, { doc });
      if (samples.length < 5) samples.push({ name: s.name, type: s.company_type, was: s.domain, addr: s.full_address, geo: [doc.locality, doc.region].filter(Boolean).join(', ') });
    }
    if (batch.length >= 2000) await flush();
    if (scanned % 20000 === 0) console.error(`  scanned ${scanned.toLocaleString()} | fixed ${enriched.toLocaleString()} (geo-filled ${geoFilled.toLocaleString()})`);
    resp = await client.scroll({ scroll_id: sid, scroll: '5m' }); sid = (resp.body || resp)._scroll_id;
  }
  await flush();
  try { await client.clearScroll({ body: { scroll_id: [sid] } }); } catch (e) { /* */ }
  console.error('\nsample changes:'); for (const x of samples) console.error('  ', JSON.stringify(x));
  console.error(`\nDONE: scanned ${scanned.toLocaleString()} | ${APPLY ? 'fixed' : 'would fix'} ${enriched.toLocaleString()} records (domain/website cleared, geo-filled ${geoFilled.toLocaleString()}) | kept all; ${providerSelf.toLocaleString()} left untouched as the provider's own record; ${mergeShells} mass-merge shell(s) flagged${APPLY ? '' : '  (dry run — add --apply)'}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
