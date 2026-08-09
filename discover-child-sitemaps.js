/**
 * discover-child-sitemaps.js — Phase 1 of the People/Location sitemap discovery tool.
 *
 * Common-Crawl-only: pull target domains from the Company Crawler (filtered by industry + English-speaking
 * country), run ONE Athena "%sitemap%.xml" sweep over the CC columnar index restricted to those domains,
 * classify each returned sitemap filename as People / Location (or an index/unknown candidate), and upsert
 * the People/Location ones into the Sitemap Library as monitored=true (source='cc-index'). The existing
 * Library monitor then extracts their bio pages into the Contact Crawler (CC-first, modelled+validated
 * emails). No live requests to the target sites in this phase — discovery is entirely from Common Crawl.
 *
 *   OPENSEARCH_ENDPOINT=… node discover-child-sitemaps.js \
 *     [--industry "real estate"] [--country "united states,united kingdom,canada,australia,new zealand,ireland"]
 *     [--crawl CC-MAIN-2026-25] [--limit-domains N] [--max-sitemaps N] [--dry]
 *
 * AWS creds (cc-athena principal: Athena+Glue+S3) come from env/~/.aws. Reuses cc-athena-miner plumbing.
 */
const fs = require('fs');
const path = require('path');
const companies = require('./companies');
const sitemaps = require('./sitemaps');
const miner = require('./cc-athena-miner');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

// Profession / place tokens (exact token match after splitting the filename on non-alphanumerics, so
// "steamboat" never matches "team"). Plurals + singulars both listed. Extend from the learned taxonomy later.
const PEOPLE_TOKENS = new Set(['agent', 'agents', 'attorney', 'attorneys', 'lawyer', 'lawyers', 'advisor', 'advisors', 'adviser', 'advisers',
  'team', 'teams', 'staff', 'people', 'person', 'provider', 'providers', 'physician', 'physicians', 'doctor', 'doctors',
  'realtor', 'realtors', 'broker', 'brokers', 'professional', 'professionals', 'financialprofessionals', 'member', 'members',
  'author', 'authors', 'faculty', 'associate', 'associates', 'consultant', 'consultants', 'officer', 'officers', 'banker', 'bankers',
  'dentist', 'dentists', 'veterinarian', 'vet', 'vets', 'specialist', 'specialists', 'representative', 'representatives', 'rep', 'reps',
  'employee', 'employees', 'bio', 'bios', 'profile', 'profiles', 'leadership', 'roster', 'pathologist', 'pathologists', 'nurse', 'nurses',
  'stylist', 'stylists', 'loanofficer', 'loanofficers', 'clinician', 'clinicians', 'principal', 'principals']);
const LOC_TOKENS = new Set(['location', 'locations', 'store', 'stores', 'branch', 'branches', 'office', 'offices', 'dealer', 'dealers',
  'dealership', 'dealerships', 'agency', 'agencies', 'showroom', 'showrooms', 'clinic', 'clinics', 'restaurant', 'restaurants',
  'hotel', 'hotels', 'city', 'cities', 'territory', 'territories', 'salon', 'salons', 'shop', 'shops', 'outlet', 'outlets']);
// Filenames that are the site's ROOT/index sitemap (a Parent) — captured as candidates for later recursion.
const INDEX_RE = /^(sitemap|sitemap[-_]?index|sitemapindex|wp-sitemap|sitemap_index)(-\d+)?\.xml(\.gz)?$/i;
// Obvious non-people/location content sitemaps — skipped outright.
const SKIP_TOKENS = new Set(['post', 'posts', 'page', 'pages', 'product', 'products', 'category', 'categories', 'tag', 'tags',
  'blog', 'news', 'article', 'articles', 'image', 'images', 'video', 'videos', 'media', 'event', 'events', 'taxonomy',
  'attachment', 'attachments', 'category', 'archive', 'archives', 'faq', 'faqs', 'review', 'reviews', 'listing', 'listings']);

function fileOf(url) { try { return new URL(url).pathname.split('/').filter(Boolean).pop() || ''; } catch (e) { return String(url).split('/').filter(Boolean).pop() || ''; } }
function tokensOf(name) { return String(name || '').toLowerCase().replace(/\.(xml|gz)$/g, '').split(/[^a-z0-9]+/).filter(Boolean); }
// Load exact-filename people/location lexicons (highest confidence) from the shipped CSVs.
function loadNameSet(file) { try { return new Set(fs.readFileSync(path.join(__dirname, file), 'utf8').split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name'))); } catch (e) { return new Set(); } }

function classify(url, bioNames, locNames) {
  const fn = fileOf(url).toLowerCase();
  if (!fn) return { kind: '', why: 'no-filename' };
  if (bioNames.has(fn)) return { kind: 'People', why: 'lexicon', keyword: fn };
  if (locNames.has(fn)) return { kind: 'Location', why: 'lexicon', keyword: fn };
  const toks = tokensOf(fn);
  if (toks.some((t) => PEOPLE_TOKENS.has(t))) return { kind: 'People', why: 'token', keyword: toks.find((t) => PEOPLE_TOKENS.has(t)) };
  if (toks.some((t) => LOC_TOKENS.has(t))) return { kind: 'Location', why: 'token', keyword: toks.find((t) => LOC_TOKENS.has(t)) };
  if (INDEX_RE.test(fn)) return { kind: '', why: 'index', keyword: fn };          // parent index — candidate for later recursion
  if (toks.some((t) => SKIP_TOKENS.has(t))) return { kind: '', why: 'skip' };
  return { kind: '', why: 'unknown', keyword: fn };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const industry = arg('industry', 'real estate');
  const country = arg('country', 'united states,united kingdom,canada,australia,new zealand,ireland');
  const limitDomains = Number(arg('limit-domains', '0')) || 0;
  const maxSitemaps = Number(arg('max-sitemaps', '0')) || 0;
  const dry = has('dry');
  const tag = (arg('tag', '') || String(process.pid) + '_' + industry.replace(/[^a-z0-9]/gi, '')).replace(/[^a-z0-9_]/gi, '').slice(0, 40).toLowerCase();

  const coClient = companies.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const smClient = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(smClient);
  const bioNames = loadNameSet('Sitemap extensions.csv');
  const locNames = loadNameSet('Sitemap extensions - locations.csv');

  // 1) Target domains from the Company Crawler (industry + country), normalized to the registrable domain.
  console.error(`Selecting domains: industry="${industry}", country="${country}"…`);
  const domains = new Set();
  await companies.each(coClient, { industry, country }, (row) => {
    let d = String(row.domain || '').toLowerCase().trim().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, '');
    if (d && d.includes('.') && !d.includes(' ')) domains.add(d);
  }, limitDomains || 5000000);
  console.error(`  ${domains.size.toLocaleString()} distinct domain(s).`);
  if (!domains.size) { console.error('No domains — nothing to do.'); process.exit(0); }

  // 2) AWS: results bucket + ccindex table/partition, upload the domains table.
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

  // 3) The %sitemap%.xml sweep, restricted to the target domains (freshest capture per URL isn't needed —
  //    we only want the distinct sitemap URLs).
  const sql = `SELECT DISTINCT i.url_host_registered_domain AS domain, i.url AS url
FROM ${miner.DB}.ccindex i
JOIN ${miner.DB}.${domainsTable} d ON d.k = i.url_host_registered_domain
WHERE i.crawl='${crawl}' AND i.subset='warc'
  AND i.fetch_status=200
  AND regexp_like(lower(i.url_path), 'sitemap')
  AND (lower(i.url_path) LIKE '%.xml' OR lower(i.url_path) LIKE '%.xml.gz')`;
  const { id } = await miner.runAthena(A, sql, output, 'sitemap sweep');
  const loc = (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation;

  // 4) Stream results → classify.
  const tally = { rows: 0, people: 0, location: 0, index: 0, skip: 0, unknown: 0 };
  const domSeen = new Set();
  const docs = [];      // People/Location docs to upsert
  const unknownTokens = new Map();
  let first = true;
  await miner.s3StreamRows(A, loc, (r) => {
    if (first && r[0] === 'domain') { first = false; return; }
    first = false;
    const [domain, url] = r;
    if (!url) return;
    tally.rows++;
    const c = classify(url, bioNames, locNames);
    if (c.kind === 'People' || c.kind === 'Location') {
      domSeen.add(domain);
      if (c.kind === 'People') tally.people++; else tally.location++;
      docs.push({ sitemap_url: url, domain, parent_url: '', kind: c.kind, type: sitemaps.deriveType(url, domain),
        keyword: c.keyword || '', url_count: 0, item_count: 0, ratio: 0, by_name: true, industry, source: 'cc-index' });
    } else if (c.why === 'index') tally.index++;
    else if (c.why === 'skip') tally.skip++;
    else { tally.unknown++; if (c.keyword) unknownTokens.set(c.keyword, (unknownTokens.get(c.keyword) || 0) + 1); }
  });

  const topUnknown = [...unknownTokens.entries()].sort((a, b) => b[1] - a[1]).slice(0, 25);
  console.error(`\nSitemap URLs: ${tally.rows.toLocaleString()} | People ${tally.people.toLocaleString()} · Location ${tally.location.toLocaleString()} · index ${tally.index.toLocaleString()} · skip ${tally.skip.toLocaleString()} · unknown ${tally.unknown.toLocaleString()}`);
  console.error(`People/Location across ${domSeen.size.toLocaleString()} domain(s).`);
  console.error(`Top unknown sitemap names (taxonomy candidates): ${topUnknown.map(([k, n]) => k + ':' + n).join('  ')}`);
  if (docs.length) { console.error('Samples:'); for (const d of docs.slice(0, 8)) console.error(`  [${d.kind}] ${d.sitemap_url}`); }

  if (dry) { console.error('\n--dry: no writes.'); process.exit(0); }
  if (!docs.length) { console.error('Nothing to upsert.'); process.exit(0); }

  // 5) Upsert to the Library, then flip monitored=true so the Library monitor extracts them.
  const toUpsert = maxSitemaps ? docs.slice(0, maxSitemaps) : docs;
  const now = new Date().toISOString();
  let upserted = 0, errors = 0;
  const ids = [];
  for (let i = 0; i < toUpsert.length; i += 500) {
    const batch = toUpsert.slice(i, i + 500);
    const r = await sitemaps.bulkUpsert(smClient, batch, now);
    upserted += r.upserted; errors += r.errors;
    for (const d of batch) ids.push(d.sitemap_url);
  }
  let monitored = 0;
  for (let i = 0; i < ids.length; i += 500) {
    const r = await sitemaps.bulkUpdate(smClient, ids.slice(i, i + 500), { monitored: 'true' });
    monitored += r.updated;
  }
  console.error(`\nDONE: upserted ${upserted.toLocaleString()} (errors ${errors}), monitored ${monitored.toLocaleString()}.`);
  try { console.error('Library now:', JSON.stringify(await sitemaps.stats(smClient))); } catch (e) { /* */ }
  console.error('Next: the Sitemap Library monitor will extract these into the Contact Crawler (or POST /api/sitemaps/monitor/run).');
  process.exit(0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
