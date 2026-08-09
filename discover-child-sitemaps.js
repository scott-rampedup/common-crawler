/**
 * discover-child-sitemaps.js — Phase 1 of the People/Location sitemap discovery tool (Common-Crawl-only).
 *
 * 1. Pull target domains from the Company Crawler (industry + English-speaking country).
 * 2. ONE Athena "%sitemap%.xml" sweep over the CC columnar index, restricted to those domains, returning
 *    each sitemap URL + its freshest WARC pointer.
 * 3. Fetch each captured sitemap's CONTENT straight from Common Crawl's WARCs (no live hits) and expand it:
 *      - a sitemap INDEX  -> classify each child sitemap by filename (People / Location)
 *      - a flat URLSET    -> classify the sitemap itself by bio-ratio (cc-engine.discoverSitemaps)
 *    CC usually captures the parent index but not the children, so expanding the index is where the People
 *    child sitemaps actually come from.
 * 4. Upsert the People/Location sitemaps into the Library as monitored=true (source='cc-index'); the Library
 *    monitor then populates the Contact Crawler.
 *
 *   OPENSEARCH_ENDPOINT=… node discover-child-sitemaps.js [--industry "real estate"] [--country "…"]
 *     [--crawl CC-MAIN-2026-30] [--limit-domains N] [--max-sitemaps N] [--conc 16] [--dry]
 */
const fs = require('fs');
const path = require('path');
const companies = require('./companies');
const sitemaps = require('./sitemaps');
const miner = require('./cc-athena-miner');
const ccEngine = require('./cc-engine');
const { makeCcS3 } = require('./cc-s3');
const { loadGenderMap } = require('./extractor');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const PEOPLE_TOKENS = new Set(['agent', 'agents', 'attorney', 'attorneys', 'lawyer', 'lawyers', 'advisor', 'advisors', 'adviser', 'advisers',
  'team', 'teams', 'staff', 'people', 'person', 'provider', 'providers', 'physician', 'physicians', 'doctor', 'doctors',
  'realtor', 'realtors', 'broker', 'brokers', 'professional', 'professionals', 'financialprofessionals', 'member', 'members',
  'author', 'authors', 'user', 'users', 'faculty', 'associate', 'associates', 'consultant', 'consultants', 'officer', 'officers',
  'banker', 'bankers', 'dentist', 'dentists', 'veterinarian', 'vet', 'vets', 'specialist', 'specialists', 'representative', 'representatives',
  'rep', 'reps', 'employee', 'employees', 'bio', 'bios', 'profile', 'profiles', 'leadership', 'roster', 'pathologist', 'pathologists',
  'nurse', 'nurses', 'stylist', 'stylists', 'loanofficer', 'loanofficers', 'clinician', 'clinicians', 'principal', 'principals']);
const LOC_TOKENS = new Set(['location', 'locations', 'store', 'stores', 'branch', 'branches', 'office', 'offices', 'dealer', 'dealers',
  'dealership', 'dealerships', 'agency', 'agencies', 'showroom', 'showrooms', 'clinic', 'clinics', 'restaurant', 'restaurants',
  'hotel', 'hotels', 'city', 'cities', 'territory', 'territories', 'salon', 'salons', 'shop', 'shops', 'outlet', 'outlets']);
const INDEX_RE = /^(sitemap|sitemap[-_]?index|sitemapindex|wp-sitemap|sitemap_index)(-\d+)?\.xml(\.gz)?$/i;
const SKIP_TOKENS = new Set(['post', 'posts', 'page', 'pages', 'product', 'products', 'category', 'categories', 'tag', 'tags',
  'blog', 'news', 'article', 'articles', 'image', 'images', 'video', 'videos', 'media', 'event', 'events', 'taxonomy',
  'attachment', 'attachments', 'archive', 'archives', 'faq', 'faqs', 'review', 'reviews', 'listing', 'listings', 'property', 'properties']);

function fileOf(url) { try { return new URL(url).pathname.split('/').filter(Boolean).pop() || ''; } catch (e) { return String(url).split('/').filter(Boolean).pop() || ''; } }
function tokensOf(name) { return String(name || '').toLowerCase().replace(/\.(xml|gz)$/g, '').split(/[^a-z0-9]+/).filter(Boolean); }
function loadNameSet(file) { try { return new Set(fs.readFileSync(path.join(__dirname, file), 'utf8').split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name'))); } catch (e) { return new Set(); } }

// Classify a sitemap URL by its filename. Returns { kind:'People'|'Location'|'', why, keyword }.
function classify(url, bioNames, locNames) {
  const fn = fileOf(url).toLowerCase(); if (!fn) return { kind: '', why: 'no-filename' };
  if (bioNames.has(fn)) return { kind: 'People', why: 'lexicon', keyword: fn };
  if (locNames.has(fn)) return { kind: 'Location', why: 'lexicon', keyword: fn };
  const toks = tokensOf(fn);
  const pt = toks.find((t) => PEOPLE_TOKENS.has(t)); if (pt) return { kind: 'People', why: 'token', keyword: pt };
  const lt = toks.find((t) => LOC_TOKENS.has(t)); if (lt) return { kind: 'Location', why: 'token', keyword: lt };
  if (INDEX_RE.test(fn)) return { kind: '', why: 'index', keyword: fn };
  if (toks.some((t) => SKIP_TOKENS.has(t))) return { kind: '', why: 'skip' };
  return { kind: '', why: 'unknown', keyword: fn };
}

async function pool(items, n, fn) { let i = 0; const workers = Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const idx = i++; try { await fn(items[idx]); } catch (e) { /* per-item */ } } }); await Promise.all(workers); }

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const industry = arg('industry', 'real estate');
  const country = arg('country', 'united states,united kingdom,canada,australia,new zealand,ireland');
  const limitDomains = Number(arg('limit-domains', '0')) || 0;
  const maxSitemaps = Number(arg('max-sitemaps', '0')) || 0;
  const conc = Number(arg('conc', '16')) || 16;
  const dry = has('dry');
  const tag = (arg('tag', '') || String(process.pid) + '_' + industry.replace(/[^a-z0-9]/gi, '')).replace(/[^a-z0-9_]/gi, '').slice(0, 40).toLowerCase();

  const coClient = companies.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const smClient = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(smClient);
  const bioNames = loadNameSet('Sitemap extensions.csv');
  const locNames = loadNameSet('Sitemap extensions - locations.csv');
  const genderMap = loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const fetchWarc = makeCcS3();

  // 1) Target domains.
  console.error(`Selecting domains: industry="${industry}", country="${country}"…`);
  const domains = new Set();
  await companies.each(coClient, { industry, country }, (row) => {
    const d = String(row.domain || '').toLowerCase().trim().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, '');
    if (d && d.includes('.') && !d.includes(' ')) domains.add(d);
  }, limitDomains || 5000000);
  console.error(`  ${domains.size.toLocaleString()} distinct domain(s).`);
  if (!domains.size) { console.error('No domains — nothing to do.'); process.exit(0); }

  // 2) AWS + domains table.
  const A = miner.aws();
  const acct = (await A.sts.send(new A.GetCallerIdentityCommand({}))).Account;
  const bucket = process.env.ATHENA_RESULTS_BUCKET || `aws-athena-query-results-${acct}-${miner.REGION}`;
  const output = `s3://${bucket}/`;
  const crawl = arg('crawl', '') || await miner.latestCrawl();
  console.error(`Crawl: ${crawl}`);
  await miner.ensureBucket(A, bucket);
  await miner.ensureTable(A, crawl, output);
  const domainsTable = 'cs_domains_' + tag;
  const prefix = `child-sitemaps/${tag}/`;
  await A.s3.send(new A.PutObjectCommand({ Bucket: bucket, Key: prefix + 'domains.txt', Body: Buffer.from([...domains].join('\n') + '\n', 'utf8') }));
  await miner.runAthena(A, `DROP TABLE IF EXISTS ${miner.DB}.${domainsTable}`, output, 'drop domains tbl');
  await miner.runAthena(A, miner.buildKeysTableSql({ keysTable: domainsTable, bucket, prefix }), output, 'create domains tbl');

  // 3) %sitemap%.xml sweep restricted to the domains — freshest WARC pointer per URL.
  const sql = `SELECT url, domain, filename, "offset", length FROM (
  SELECT i.url AS url, i.url_host_registered_domain AS domain, i.warc_filename AS filename,
         i.warc_record_offset AS "offset", i.warc_record_length AS length,
         row_number() OVER (PARTITION BY i.url ORDER BY i.fetch_time DESC) AS rn
  FROM ${miner.DB}.ccindex i
  JOIN ${miner.DB}.${domainsTable} d ON d.k = i.url_host_registered_domain
  WHERE i.crawl='${crawl}' AND i.subset='warc' AND i.fetch_status=200
    AND regexp_like(lower(i.url_path), 'sitemap')
    AND (lower(i.url_path) LIKE '%.xml' OR lower(i.url_path) LIKE '%.xml.gz')
) WHERE rn = 1`;
  const { id } = await miner.runAthena(A, sql, output, 'sitemap sweep');
  const loc = (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation;
  const captured = [];
  let first = true;
  await miner.s3StreamRows(A, loc, (r) => { if (first) { first = false; if (r[0] === 'url') return; } if (r[0]) captured.push({ url: r[0], domain: r[1], filename: r[2], offset: r[3], length: r[4] }); });
  console.error(`Captured sitemaps in CC: ${captured.length.toLocaleString()} — fetching + expanding from CC WARCs (conc ${conc})…`);

  // 4) Fetch each sitemap's content from CC and expand → People/Location child sitemaps.
  const docByUrl = new Map();      // sitemap_url -> Library doc (dedup)
  const tally = { fetched: 0, empty: 0, indexes: 0, urlsets: 0, people: 0, location: 0, unknownChildren: 0 };
  const unknownTokens = new Map();
  const addDoc = (url, domain, parent, kind, keyword, counts) => {
    if (docByUrl.has(url)) return;
    docByUrl.set(url, { sitemap_url: url, domain: domain || (function () { try { return new URL(url).hostname.replace(/^www\./, ''); } catch (e) { return ''; } })(),
      parent_url: parent || '', kind, type: sitemaps.deriveType(url, domain), keyword: keyword || '',
      url_count: (counts && counts.url_count) || 0, item_count: (counts && counts.item_count) || 0,
      ratio: (counts && counts.ratio) || 0, by_name: !!(counts && counts.by_name), industry, source: 'cc-index' });
    if (kind === 'People') tally.people++; else if (kind === 'Location') tally.location++;
  };

  await pool(captured, conc, async (row) => {
    const xml = await fetchWarc({ url: row.url, filename: row.filename, offset: row.offset, length: row.length });
    tally.fetched++;
    if (!xml) { tally.empty++; return; }
    const { isIndex, entries } = ccEngine.extractSitemapLocs(xml);
    if (isIndex) {
      tally.indexes++;
      for (const e of (entries || [])) {
        const c = classify(e.loc, bioNames, locNames);
        if (c.kind) addDoc(e.loc, row.domain, row.url, c.kind, c.keyword);
        else if (c.why === 'unknown' && c.keyword) { tally.unknownChildren++; unknownTokens.set(c.keyword, (unknownTokens.get(c.keyword) || 0) + 1); }
      }
    } else {
      // flat urlset — classify the sitemap itself by bio/location ratio (reuses the engine's classifier)
      tally.urlsets++;
      try {
        const { watches } = await ccEngine.discoverSitemaps({ content: xml, sourceUrl: row.url, directoryRules: {}, genderMap, bioSitemapNames: bioNames, locationSitemapNames: locNames });
        const w = (watches || [])[0];
        if (w && (w.kind === 'People' || w.kind === 'Location')) addDoc(row.url, row.domain, '', w.kind, w.keyword || fileOf(row.url), { url_count: w.urlCount, item_count: w.itemCount, ratio: w.ratio, by_name: w.byName });
      } catch (e) { /* */ }
    }
  });

  const topUnknown = [...unknownTokens.entries()].sort((a, b) => b[1] - a[1]).slice(0, 25);
  const docs = [...docByUrl.values()];
  const domSeen = new Set(docs.map((d) => d.domain));
  console.error(`\nFetched ${tally.fetched} (empty ${tally.empty}) | indexes ${tally.indexes} · urlsets ${tally.urlsets}`);
  console.error(`People/Location child sitemaps: ${docs.length.toLocaleString()} (People ${tally.people} · Location ${tally.location}) across ${domSeen.size.toLocaleString()} domain(s).`);
  console.error(`Top unknown child names (taxonomy candidates): ${topUnknown.map(([k, n]) => k + ':' + n).join('  ')}`);
  for (const d of docs.slice(0, 10)) console.error(`  [${d.kind}] ${d.sitemap_url}`);

  if (dry) { console.error('\n--dry: no writes.'); process.exit(0); }
  if (!docs.length) { console.error('Nothing to upsert.'); process.exit(0); }

  // 5) Upsert + monitor.
  const toUpsert = maxSitemaps ? docs.slice(0, maxSitemaps) : docs;
  const now = new Date().toISOString();
  let upserted = 0, errors = 0; const ids = [];
  for (let i = 0; i < toUpsert.length; i += 500) { const b = toUpsert.slice(i, i + 500); const r = await sitemaps.bulkUpsert(smClient, b, now); upserted += r.upserted; errors += r.errors; for (const d of b) ids.push(d.sitemap_url); }
  let monitored = 0;
  for (let i = 0; i < ids.length; i += 500) { const r = await sitemaps.bulkUpdate(smClient, ids.slice(i, i + 500), { monitored: 'true' }); monitored += r.updated; }
  console.error(`\nDONE: upserted ${upserted.toLocaleString()} (errors ${errors}), monitored ${monitored.toLocaleString()}.`);
  try { console.error('Library now:', JSON.stringify(await sitemaps.stats(smClient))); } catch (e) { /* */ }
  console.error('Next: the Library monitor extracts these into the Contact Crawler (or POST /api/sitemaps/monitor/run).');
  process.exit(0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
