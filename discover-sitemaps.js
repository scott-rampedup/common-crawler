/**
 * discover-sitemaps.js — build out the Sitemap Library (sitemaps.js OpenSearch index) by discovering
 * People + Location child sitemaps across target-industry company domains. For each domain: resolve
 * robots.txt -> `Sitemap:` (else /sitemap[_index].xml), walk the index with cc-engine.discoverSitemaps
 * (classifies each child People/Location via the two name lexicons + bio/location ratio), and upsert every
 * qualifying child into the Library (dedup by sitemap_url; domains taken highest company-contact_count first).
 *
 * Run ON the Fly app machine (needs OPENSEARCH_ENDPOINT + network; PROXY_FALLBACK_URL helps for
 * Cloudflare-fronted sitemaps):
 *   OPENSEARCH_ENDPOINT=… node discover-sitemaps.js --industries "real estate,insurance" --limit 2000
 *   node discover-sitemaps.js --limit 200 --dry-run          # print work-list + classifications, no writes
 * Flags: --industries "a,b" (default = location-rich verticals), --limit N (domains, default cap 50000),
 *   --concurrency N (12), --domain-timeout SECS (45), --min-count N (3), --min-ratio F (0.30),
 *   --offset N, --no-resume, --dry-run.
 */
const path = require('path');
const fs = require('fs');
const ccEngine = require('./cc-engine');
const ex = require('./extractor');
const sitemaps = require('./sitemaps');
const companies = require('./companies');

// Location- + people-rich verticals (companies.industry values, PDL taxonomy — lowercase exact match).
const TARGET_INDUSTRIES = [
  'real estate', 'insurance',
  'financial services', 'investment banking', 'investment management', 'banking', 'capital markets', 'venture capital & private equity',
  'legal services', 'law practice',
  'hospital & health care', 'medical practice', 'health, wellness and fitness',
  'retail', 'restaurants', 'automotive',
];

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);
const INDUSTRIES = arg('industries', '') ? arg('industries', '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean) : TARGET_INDUSTRIES;
const LIMIT = Number(arg('limit', '0')) || 0;
const CAP = LIMIT || 50000;                                    // safety cap on the in-memory work-list
const CONC = Number(arg('concurrency', '12')) || 12;
const DOMAIN_TIMEOUT = (Number(arg('domain-timeout', '45')) || 45) * 1000;
const MIN_COUNT = Number(arg('min-count', '3')) || 3;
const MIN_RATIO = Number(arg('min-ratio', '0.30')) || 0.30;
const OFFSET = Number(arg('offset', '0')) || 0;
const RESUME = !has('no-resume');
const DRY = has('dry-run');

// lexicons + rules — same loaders the app uses (ui-server BIO_SITEMAP_NAMES pattern + extractor loaders)
function loadNames(file) {
  try { return new Set(fs.readFileSync(path.join(__dirname, file), 'utf8').split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name'))); }
  catch (e) { return new Set(); }
}
const bioSitemapNames = loadNames('Sitemap extensions.csv');
const locationSitemapNames = loadNames('Sitemap extensions - locations.csv');
const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
let directoryRules = {}; try { directoryRules = ex.loadDirectoryRules(path.join(__dirname, 'data', 'directory-rules.json')); } catch (e) { /* built-ins apply */ }

const normHost = (s) => String(s || '').toLowerCase().trim().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];
const withTimeout = (p, ms) => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);

// Resolve a domain's sitemap URLs: robots.txt `Sitemap:` lines, else the common fallbacks.
// (Same logic as sitemap-monitor.resolveSitemaps, using cc-engine.fetchDoc which has a proxy fallback.)
async function resolveSitemaps(domain) {
  const host = normHost(domain); if (!host) return [];
  const found = new Set();
  for (const base of [`https://${host}`, `https://www.${host}`]) {
    const robots = await ccEngine.fetchDoc(`${base}/robots.txt`);
    if (robots) { const re = /^\s*sitemap:\s*(\S+)/gim; let m; while ((m = re.exec(robots))) { const u = m[1].trim(); if (u) found.add(u); } if (found.size) break; }
  }
  if (!found.size) for (const p of ['/sitemap_index.xml', '/sitemap.xml']) found.add(`https://${host}${p}`);
  return [...found];
}

// Stream target-industry company domains, highest contact_count first, deduped by registrable domain.
async function* streamDomains(client) {
  let after = null; const seen = new Set();
  const query = { bool: { filter: [{ terms: { industry: INDUSTRIES } }, { exists: { field: 'domain' } }] } };
  for (;;) {
    const body = { size: 2000, _source: ['domain', 'industry', 'id'], query, sort: [{ contact_count: 'desc' }, { id: 'asc' }] };
    if (after) body.search_after = after;
    const res = await client.search({ index: companies.INDEX, body });
    const hits = (res.body || res).hits.hits; if (!hits.length) break;
    for (const h of hits) {
      const d = normHost(h._source.domain); if (!d || seen.has(d)) continue; seen.add(d);
      yield { domain: d, industry: h._source.industry || '', company_id: h._source.id || h._id };
    }
    after = hits[hits.length - 1].sort;
  }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  if (!DRY) await sitemaps.ensureIndex(client);
  console.error(`Sitemap Library discovery | industries=[${INDUSTRIES.join(', ')}] | limit=${LIMIT || CAP + ' (cap)'} | conc=${CONC} | resume=${RESUME} | ${DRY ? 'DRY RUN' : 'WRITE'}`);
  console.error(`lexicons: ${bioSitemapNames.size} people names, ${locationSitemapNames.size} location names`);

  // work-list (bounded by OFFSET + CAP)
  const work = []; let skimmed = 0;
  for await (const rec of streamDomains(client)) {
    skimmed++; if (skimmed <= OFFSET) continue;
    work.push(rec); if (work.length >= CAP) break;
  }
  if (!LIMIT && work.length >= CAP) console.error(`NOTE: work-list capped at ${CAP.toLocaleString()} — raise with --limit or page with --offset.`);
  console.error(`domain work-list: ${work.length.toLocaleString()}`);

  // resume: drop domains already in the Library
  let todo = work;
  if (RESUME && !DRY && work.length) {
    const have = await sitemaps.existingDomains(client, work.map((w) => w.domain));
    todo = work.filter((w) => !have.has(w.domain));
    console.error(`resume: ${have.size.toLocaleString()} already in Library, ${todo.length.toLocaleString()} to process`);
  }

  let i = 0, processed = 0, foundPeople = 0, foundLoc = 0, domainsWith = 0, errs = 0, upserts = 0; const t0 = Date.now();
  const nowIso = new Date().toISOString();

  async function worker() {
    for (;;) {
      const k = i++; if (k >= todo.length) return;
      const { domain, industry, company_id } = todo[k];
      try {
        const urls = await withTimeout(resolveSitemaps(domain), DOMAIN_TIMEOUT);
        if (urls.length) {
          const { watches } = await withTimeout(
            ccEngine.discoverSitemaps({ urls, directoryRules, genderMap, bioSitemapNames, locationSitemapNames, minCount: MIN_COUNT, minRatio: MIN_RATIO }),
            DOMAIN_TIMEOUT);
          const keep = watches.filter((w) => w.byName || w.itemCount >= MIN_COUNT);
          if (keep.length) {
            domainsWith++;
            for (const w of keep) { if (w.kind === 'People') foundPeople++; else if (w.kind === 'Location') foundLoc++; }
            if (DRY) { for (const w of keep.slice(0, 3)) console.error(`  [${domain}] ${w.kind} ${w.sitemapUrl} (${w.itemCount}/${w.urlCount}${w.byName ? ', by-name' : ''})`); }
            else { const r = await sitemaps.bulkUpsert(client, keep.map((w) => sitemaps.docFromWatch(w, { industry, company_id, source: 'discovered' })), nowIso); upserts += r.upserted; errs += r.errors; }
          }
        }
      } catch (e) { errs++; }
      processed++;
      if (processed % 200 === 0) { const s = (Date.now() - t0) / 1000; console.error(`  processed ${processed.toLocaleString()}/${todo.length.toLocaleString()} | domains-with ${domainsWith.toLocaleString()} | People ${foundPeople.toLocaleString()} Location ${foundLoc.toLocaleString()} | ${errs} err | ${(processed / s).toFixed(1)}/s`); }
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(CONC, todo.length)) }, worker));

  console.error(`DONE: processed ${processed.toLocaleString()} | domains-with-sitemaps ${domainsWith.toLocaleString()} | People ${foundPeople.toLocaleString()} | Location ${foundLoc.toLocaleString()} | ${DRY ? '(dry run)' : 'upserted ' + upserts.toLocaleString()} | ${errs} err | ${Math.round((Date.now() - t0) / 1000)}s`);
  if (!DRY) { try { await client.indices.refresh({ index: sitemaps.INDEX }); console.error('Library now:', JSON.stringify(await sitemaps.stats(client))); } catch (e) { /* */ } }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
