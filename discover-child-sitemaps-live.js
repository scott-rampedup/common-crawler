/**
 * discover-child-sitemaps-live.js — Phase 2 (LIVE layer) of the People/Location sitemap discovery tool.
 *
 * Common Crawl only captures a sitemap for ~1% of domains, so this layer goes to the sites directly: for
 * each Company-Crawler domain (industry + English-speaking country), fetch robots.txt + the common sitemap
 * paths through the Evomi proxy, then recurse/classify with cc-engine.discoverSitemaps (index -> children,
 * People/Location by lexicon + name ratio). People/Location results upsert into the Library as
 * monitored=true (source='live'); the Library monitor then populates the Contact Crawler.
 *
 *   OPENSEARCH_ENDPOINT=… PROXY_URL=… PROXY_FALLBACK_URL=… node discover-child-sitemaps-live.js \
 *     [--industry "real estate"] [--country "…"] [--limit-domains 2000] [--conc 12] [--skip-existing] [--dry]
 *
 * Detach for big runs (nohup … &). Skips platform hosts (medium.com, wordpress.com, …) that cause
 * ratio false-positives. Idempotent: upsert by sitemap_url; --skip-existing skips domains already in the Library.
 */
const companies = require('./companies');
const sitemaps = require('./sitemaps');
const ccEngine = require('./cc-engine');
const { loadGenderMap } = require('./extractor');
const fs = require('fs');
const path = require('path');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

// Blog/site-builder platforms whose per-user subdomains masquerade as company people sitemaps.
const PLATFORMS = new Set(['medium.com', 'wordpress.com', 'blogspot.com', 'wixsite.com', 'weebly.com', 'squarespace.com',
  'godaddysites.com', 'substack.com', 'tumblr.com', 'shopify.com', 'myshopify.com', 'sites.google.com', 'blogger.com']);
const isPlatform = (d) => PLATFORMS.has(d) || [...PLATFORMS].some((p) => d.endsWith('.' + p));

function loadNameSet(file) { try { return new Set(fs.readFileSync(path.join(__dirname, file), 'utf8').split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name'))); } catch (e) { return new Set(); } }
async function pool(items, n, fn) { let i = 0; const workers = Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const idx = i++; try { await fn(items[idx], idx); } catch (e) { /* per-domain */ } } }); await Promise.all(workers); }

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const industry = arg('industry', 'real estate');
  const country = arg('country', 'united states,united kingdom,canada,australia,new zealand,ireland');
  const limitDomains = Number(arg('limit-domains', '2000')) || 2000;
  const conc = Number(arg('conc', '12')) || 12;
  const skipExisting = has('skip-existing');
  const dry = has('dry');

  const coClient = companies.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const smClient = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(smClient);
  const genderMap = loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const bioNames = loadNameSet('Sitemap extensions.csv');
  const locNames = loadNameSet('Sitemap extensions - locations.csv');
  if (!process.env.PROXY_URL && !process.env.PROXY_FALLBACK_URL) console.error('WARNING: no PROXY_URL set — live fetches go direct (may be blocked).');

  // 1) Target domains (bounded for the pilot).
  console.error(`Selecting up to ${limitDomains.toLocaleString()} domains: industry="${industry}", country="${country}"…`);
  const domains = [];
  const seen = new Set();
  await companies.each(coClient, { industry, country }, (row) => {
    const d = String(row.domain || '').toLowerCase().trim().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, '');
    if (d && d.includes('.') && !d.includes(' ') && !isPlatform(d) && !seen.has(d)) { seen.add(d); domains.push(d); }
  }, limitDomains * 3);   // over-scan a bit; we cap after skip-existing
  let targets = domains.slice(0, skipExisting ? domains.length : limitDomains);

  // Optionally skip domains already represented in the Library.
  if (skipExisting) {
    const have = new Set();
    for (let i = 0; i < targets.length; i += 1024) { const chunk = targets.slice(i, i + 1024); const h = await sitemaps.existingDomains(smClient, chunk); for (const d of h) have.add(d); }
    targets = targets.filter((d) => !have.has(d)).slice(0, limitDomains);
    console.error(`  ${have.size.toLocaleString()} already in Library; ${targets.length.toLocaleString()} to probe.`);
  } else { targets = targets.slice(0, limitDomains); console.error(`  ${targets.length.toLocaleString()} to probe.`); }
  if (!targets.length) { console.error('Nothing to probe.'); process.exit(0); }

  // 2) Probe each domain: robots.txt + common paths -> discoverSitemaps (recurse + classify, via proxy).
  //    Results are FLUSHED to the Library incrementally (every ~300 sitemaps) — so a kill/deploy/restart
  //    never loses work, and re-running with --skip-existing resumes where it left off.
  const CANDIDATES = (d) => [`https://${d}/sitemap.xml`, `https://${d}/sitemap_index.xml`, `https://${d}/sitemap-index.xml`, `https://${d}/wp-sitemap.xml`];
  const seenUrl = new Set();
  let pending = [];
  const tally = { probed: 0, withSitemap: 0, people: 0, location: 0, errors: 0, upserted: 0, monitored: 0 };
  let done = 0, flushing = false;

  async function flushPending() {
    if (dry || flushing || !pending.length) return;
    flushing = true;
    const batch = pending.splice(0, pending.length);
    try {
      const now = new Date().toISOString();
      const r = await sitemaps.bulkUpsert(smClient, batch, now); tally.upserted += r.upserted;
      const ids = batch.map((x) => x.sitemap_url);
      for (let i = 0; i < ids.length; i += 500) { const u = await sitemaps.bulkUpdate(smClient, ids.slice(i, i + 500), { monitored: 'true' }); tally.monitored += u.updated; }
    } catch (e) { console.error('flush error:', e.message); }
    flushing = false;
  }

  await pool(targets, conc, async (d) => {
    tally.probed++;
    let smUrls = [];
    for (const ru of [`https://${d}/robots.txt`, `https://www.${d}/robots.txt`]) {
      try { const txt = await ccEngine.fetchDoc(ru); if (txt) { const p = ccEngine.parseRobots(txt); if (p.sitemaps && p.sitemaps.length) { smUrls = p.sitemaps; break; } } } catch (e) { /* */ }
    }
    if (!smUrls.length) smUrls = CANDIDATES(d);
    let watches = [];
    try {
      const r = await ccEngine.discoverSitemaps({ urls: smUrls.slice(0, 8), directoryRules: {}, genderMap, bioSitemapNames: bioNames, locationSitemapNames: locNames });
      watches = (r.watches || []).filter((w) => w.kind === 'People' || w.kind === 'Location');
    } catch (e) { tally.errors++; }
    if (watches.length) {
      tally.withSitemap++;
      for (const w of watches) {
        if (seenUrl.has(w.sitemapUrl)) continue; seenUrl.add(w.sitemapUrl);
        pending.push({ sitemap_url: w.sitemapUrl, domain: w.domain || d, parent_url: w.parentUrl || '',
          kind: w.kind, type: sitemaps.deriveType(w.sitemapUrl, w.domain || d), keyword: w.keyword || '',
          url_count: w.urlCount || 0, item_count: w.itemCount || 0, ratio: Number(w.ratio || 0), by_name: !!w.byName,
          industry, source: 'live' });
        if (w.kind === 'People') tally.people++; else tally.location++;
      }
      if (pending.length >= 300) await flushPending();
    }
    if (++done % 200 === 0) console.error(`  [${done}/${targets.length}] with-sitemap ${tally.withSitemap} · People ${tally.people} · Location ${tally.location} · upserted ${tally.upserted} · err ${tally.errors}`);
  });

  await flushPending();   // final
  console.error(`\nProbed ${tally.probed.toLocaleString()} | People/Location sitemaps found ${seenUrl.size.toLocaleString()} (People ${tally.people} · Location ${tally.location}) | errors ${tally.errors}`);
  if (dry) { console.error('--dry: no writes.'); process.exit(0); }
  console.error(`DONE: upserted ${tally.upserted.toLocaleString()}, monitored ${tally.monitored.toLocaleString()}.`);
  try { console.error('Library now:', JSON.stringify(await sitemaps.stats(smClient))); } catch (e) { /* */ }
  process.exit(0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
