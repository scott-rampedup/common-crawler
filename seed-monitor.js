/**
 * seed-monitor.js — bulk LIVE-harvest of bio-dedicated sitemaps, seeding the new-hire monitor.
 * --------------------------------------------------------------------------------------------
 * Common Crawl indexes almost no sitemaps (the columnar finder confirmed ~113 across the default
 * TLDs), so CC is a dead end for sitemap discovery. The real source is our own catalog of known
 * bio-domains (the distinct domains in the columnar harvest, ~437k). This script drives the SAME
 * discovery the /monitor "discover" button does — `monitor.discoverWatches({ domains:[d] })`, which
 * resolves each domain's robots.txt -> `Sitemap:` entries (else /sitemap[_index].xml), finds the
 * children that are DEDICATED to people pages (cc-engine.discoverBioSitemaps, name-matched against
 * "Sitemap extensions.csv" or bio-ratio), and SEEDS each as a watch (baseline only, no alerts) — but
 * fanned out over a big domain list with a concurrency pool, per-domain timeout, resume, and progress.
 *
 * Each domain is a DISTINCT host, so a wide pool is polite (no single site is hammered). It writes
 * straight into the monitor's SQLite store (watched_sitemaps / bio_urls), so run it ON the app
 * machine (the one with the data volume), pointed at the same DATA_DIR:
 *
 * DOMAIN mode (resolve each domain's sitemaps, then watch the bio-dedicated children):
 *   node seed-monitor.js --domains-file bio-domains.txt          # a plain domain-per-line / CSV list
 *   node seed-monitor.js --from-warc cc-warc-full.jsonl          # distinct domains from a harvest JSONL
 *   node seed-monitor.js --from-warc cc-warc-full.jsonl --domains-out bio-domains.txt   # just emit the list
 *   node seed-monitor.js --domains-file d.txt --dry-run          # count work after resume-skip, no network
 *
 * SITEMAP mode (watch EXACT, pre-reviewed sitemap URLs — higher quality than auto-discovery):
 *   node seed-monitor.js --from-master-list "Sitemap Master List.csv"   # the curated catalog (SiteMap-URL col)
 *   node seed-monitor.js --sitemaps-file sitemaps.txt                   # a plain sitemap-URL-per-line list
 *
 *   node seed-monitor.js --selftest                              # offline test
 *
 * Resume: domains already carrying a watch are skipped (re-run continues where it left off);
 * upsertWatch + seed sync are idempotent regardless. Flags: --concurrency N (default 12),
 * --domain-timeout SECS (45), --log-every N (500), --offset N, --no-resume. Env: DATA_DIR.
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');

// --- domain parsing -------------------------------------------------------------------------
// Normalize any domain-ish token to a bare registrable host (scheme/path/www/query stripped).
// Returns '' for blanks, header rows, and anything that isn't a plausible host.
function normalizeDomain(s) {
  let t = String(s == null ? '' : s).trim().toLowerCase();
  if (!t || t === 'domain' || t === 'name' || t === 'host') return '';
  t = t.replace(/^https?:\/\//, '').replace(/^www\./, '');
  t = t.split('/')[0].split('?')[0].split('#')[0].split('@').pop();   // drop path/query/userinfo
  t = t.split(':')[0];                                                // drop :port
  if (!/^[a-z0-9.-]+\.[a-z]{2,}$/.test(t)) return '';
  return t;
}

// Host of a URL (for deriving a domain from a harvest pointer's `url`).
function domainOfUrl(u) {
  const t = String(u == null ? '' : u).trim();
  if (!t) return '';
  try { return normalizeDomain(new URL(t).hostname); }
  catch { return normalizeDomain(t); }
}

// Parse a domain-list file (one per line, or a single-column CSV). Returns a de-duped array.
function loadDomainListText(text) {
  const set = new Set();
  for (const line of String(text || '').split(/\r?\n/)) {
    const d = normalizeDomain(line.split(',')[0]);   // take the first CSV column if present
    if (d) set.add(d);
  }
  return [...set];
}

// Normalize a sitemap URL: add scheme if bare, keep only http(s), drop blanks/headers. '' if invalid.
function normalizeSitemapUrl(s) {
  let t = String(s == null ? '' : s).trim();
  if (!t || t.toLowerCase() === 'sitemap' || t.toLowerCase() === 'url') return '';
  const scheme = t.match(/^([a-z][a-z0-9+.-]*):\/\//i);
  if (scheme) { if (!/^https?$/i.test(scheme[1])) return ''; }   // reject ftp:// etc., don't coerce them
  else { t = 'https://' + t; }                                   // bare host -> assume https
  try { const u = new URL(t); return (u.protocol === 'http:' || u.protocol === 'https:') ? u.toString() : ''; }
  catch { return ''; }
}

// Parse a plain sitemap-URL list (one per line, or first CSV column). De-duped.
function loadSitemapListText(text) {
  const set = new Set();
  for (const line of String(text || '').split(/\r?\n/)) { const u = normalizeSitemapUrl(line.split(',')[0]); if (u) set.add(u); }
  return [...set];
}

// Parse the curated "Sitemap Master List.csv" — many columns (Industry, Website, SiteMap, …); pull the
// SiteMap-URL column (located by header, falling back to index 2), de-dupe + validate. Columns before
// SiteMap (Industry, Website) are comma-free, so a plain split is safe for that column.
function loadMasterListSitemaps(text) {
  const lines = String(text || '').split(/\r?\n/);
  if (!lines.length) return [];
  const header = lines[0].split(',').map((h) => h.trim().toLowerCase());
  let idx = header.indexOf('sitemap');
  if (idx < 0) idx = 2;
  const set = new Set();
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    const u = normalizeSitemapUrl(cols[idx]);
    if (u) set.add(u);
  }
  return [...set];
}

// Stream a harvest JSONL ({url,...} per line) and collect its DISTINCT bio-domains. Streamed because
// the full harvest is ~1 GB; only the (small) distinct-domain Set is held in memory.
async function distinctDomainsFromWarc(filePath, { onProgress = () => {} } = {}) {
  const set = new Set();
  const rl = readline.createInterface({ input: fs.createReadStream(filePath), crlfDelay: Infinity });
  let lines = 0;
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    lines++;
    let o; try { o = JSON.parse(t); } catch { continue; }
    const d = o && (o.domain ? normalizeDomain(o.domain) : domainOfUrl(o.url));
    if (d) set.add(d);
    if (lines % 100000 === 0) onProgress({ lines, distinct: set.size });
  }
  return [...set];
}

// --- core driver (pure: inject discoverOne, no network/db) ----------------------------------
// Fan `discoverOne(domain) -> {added, bioCount, error?}` over `domains` with a fixed-size pool.
// `alreadyWatched` (Set of domains) is skipped for resume. Aggregates a stats summary.
async function seedDomains({ domains, alreadyWatched = new Set(), concurrency = 12,
                             discoverOne, onProgress = () => {} }) {
  if (typeof discoverOne !== 'function') throw new Error('seedDomains: discoverOne required');
  const total = domains.length;
  const stats = { total, processed: 0, skipped: 0, withWatch: 0, watchesAdded: 0,
                  biosBaselined: 0, noBioSitemap: 0, failed: 0 };
  let idx = 0;
  async function worker() {
    for (;;) {
      const i = idx++;
      if (i >= total) return;
      const d = domains[i];
      if (alreadyWatched.has(d)) { stats.skipped++; stats.processed++; onProgress(stats, d, { skipped: true }); continue; }
      let r;
      try { r = await discoverOne(d); }
      catch (e) { r = { added: 0, bioCount: 0, error: e && e.message ? e.message : String(e) }; }
      if (r.error) stats.failed++;
      else if (r.added > 0) { stats.withWatch++; stats.watchesAdded += r.added; stats.biosBaselined += (r.bioCount || 0); }
      else stats.noBioSitemap++;
      stats.processed++;
      onProgress(stats, d, r);
    }
  }
  const n = Math.max(1, Math.min(concurrency, total || 1));
  await Promise.all(Array.from({ length: n }, worker));
  return stats;
}

// Resolve `promise` but reject after `ms` so one slow site can't stall a pool slot forever.
function withTimeout(promise, ms, label = '') {
  if (!ms) return promise;
  let t;
  const timeout = new Promise((_, rej) => { t = setTimeout(() => rej(new Error(`timeout ${ms}ms ${label}`)), ms); });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

module.exports = { normalizeDomain, domainOfUrl, loadDomainListText, distinctDomainsFromWarc,
  normalizeSitemapUrl, loadSitemapListText, loadMasterListSitemaps, seedDomains, withTimeout };

// --- CLI ------------------------------------------------------------------------------------
if (require.main === module && !process.argv.includes('--selftest')) {
  (async () => {
    const argv = process.argv.slice(2);
    const flag = (n) => argv.includes(`--${n}`);
    const arg = (n, dflt) => { const i = argv.indexOf(`--${n}`); return i >= 0 && argv[i + 1] ? argv[i + 1] : dflt; };

    const domainsFile = arg('domains-file', '');
    const warcFile = arg('from-warc', '');
    const masterList = arg('from-master-list', '');     // curated "Sitemap Master List.csv" (SiteMap-URL column)
    const sitemapsFile = arg('sitemaps-file', '');      // plain sitemap-URL list (one per line)
    const listOut = arg('domains-out', '');
    const concurrency = Math.max(1, Number(arg('concurrency', 12)) || 12);
    const domainTimeout = Math.max(0, Number(arg('domain-timeout', 45)) || 0) * 1000;
    const logEvery = Math.max(1, Number(arg('log-every', 500)) || 500);
    const offset = Math.max(0, Number(arg('offset', 0)) || 0);
    const limit = Math.max(0, Number(arg('limit', 0)) || 0);
    const resume = !flag('no-resume');
    const dryRun = flag('dry-run');

    // SITEMAP mode seeds exact sitemap URLs (discoverWatches({sitemaps})); DOMAIN mode resolves each
    // domain's sitemaps first (discoverWatches({domains})). Resume keys on sitemap_url vs domain to match.
    const sitemapMode = !!(masterList || sitemapsFile);
    const noun = sitemapMode ? 'sitemap' : 'domain';

    if (!domainsFile && !warcFile && !masterList && !sitemapsFile) {
      console.error('Provide --domains-file/--from-warc (domain mode) or --from-master-list/--sitemaps-file (sitemap mode). (--selftest for the offline test.)');
      process.exit(2);
    }

    // 1) load + de-dupe the work list
    let items;
    if (masterList) {
      items = loadMasterListSitemaps(fs.readFileSync(masterList, 'utf8'));
    } else if (sitemapsFile) {
      items = loadSitemapListText(fs.readFileSync(sitemapsFile, 'utf8'));
    } else if (warcFile) {
      console.error(`Streaming distinct bio-domains from ${warcFile} ...`);
      items = await distinctDomainsFromWarc(warcFile, {
        onProgress: ({ lines, distinct }) => console.error(`  ${lines.toLocaleString()} lines -> ${distinct.toLocaleString()} distinct domains`) });
    } else {
      items = loadDomainListText(fs.readFileSync(domainsFile, 'utf8'));
    }
    console.error(`${items.length.toLocaleString()} distinct ${noun}(s) loaded.`);

    // Emit-and-exit: write the distinct list to a small file (to ship to the app machine, then run there).
    if (listOut) {
      fs.writeFileSync(listOut, items.join('\n') + '\n');
      console.error(`Wrote ${items.length.toLocaleString()} ${noun}(s) -> ${listOut}. (No seeding.)`);
      process.exit(0);
    }

    if (offset || limit) {
      items = items.slice(offset, limit ? offset + limit : undefined);
      console.error(`Slice [${offset}, ${offset + items.length}) -> ${items.length.toLocaleString()} ${noun}(s).`);
    }

    // 2) deps — the SAME monitor store + engine + discovery config the prod /monitor uses
    const DATA_DIR = process.env.DATA_DIR || __dirname;
    const engine = require('./cc-engine');
    const { makeDb } = require('./db');
    const { makeMonitor } = require('./sitemap-monitor');
    let genderMap = {};
    try { genderMap = require('./extractor').loadGenderMap(path.join(__dirname, 'names-genders.csv')); }
    catch { /* Gender is optional for URL classification */ }
    let bioSitemapNames = null;
    try {
      const csv = fs.readFileSync(path.join(__dirname, 'Sitemap extensions.csv'), 'utf8');
      bioSitemapNames = new Set(csv.split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name')));
    } catch { /* optional */ }
    console.error(`DATA_DIR=${DATA_DIR}; ${bioSitemapNames ? bioSitemapNames.size : 0} bio-sitemap name(s); ${Object.keys(genderMap).length.toLocaleString()} gender entries.`);

    const db = makeDb(DATA_DIR);
    const monitor = makeMonitor({ db, engine, fetchDoc: engine.fetchDoc, genderMap, bioSitemapNames });

    // 3) resume: skip items already represented in the watchlist (by sitemap_url or domain per mode)
    let alreadyWatched = new Set();
    if (resume) {
      alreadyWatched = sitemapMode
        ? new Set(db.listWatches().map((w) => String(w.sitemap_url || '')).filter(Boolean))
        : new Set(db.listWatches().map((w) => String(w.domain || '').toLowerCase()).filter(Boolean));
      console.error(`Resume: ${alreadyWatched.size.toLocaleString()} ${noun}(s) already watched will be skipped.`);
    }

    if (dryRun) {
      const todo = items.filter((d) => !alreadyWatched.has(d)).length;
      console.error(`DRY RUN: ${todo.toLocaleString()} ${noun}(s) would be probed (${(items.length - todo).toLocaleString()} skipped). No network.`);
      process.exit(0);
    }

    // 4) drive discovery over the list
    const t0 = Date.now();
    const discoverOne = sitemapMode
      ? (u) => withTimeout(monitor.discoverWatches({ sitemaps: [u] }), domainTimeout, u)
          .then((r) => ({ added: r.added, bioCount: r.watches.reduce((s, w) => s + (w.bioCount || 0), 0) }))
      : (d) => withTimeout(monitor.discoverWatches({ domains: [d] }), domainTimeout, d)
          .then((r) => ({ added: r.added, bioCount: r.watches.reduce((s, w) => s + (w.bioCount || 0), 0) }));

    const fmt = (s) => {
      const el = (Date.now() - t0) / 1000;
      const rate = s.processed / Math.max(1, el);
      const remain = (s.total - s.processed) / Math.max(0.01, rate);
      return `${s.processed.toLocaleString()}/${s.total.toLocaleString()} | watches +${s.watchesAdded.toLocaleString()} on ${s.withWatch.toLocaleString()} ${noun}s | bios ${s.biosBaselined.toLocaleString()} | none ${s.noBioSitemap.toLocaleString()} | fail ${s.failed.toLocaleString()} | skip ${s.skipped.toLocaleString()} | ${rate.toFixed(1)}/s ETA ${(remain / 60).toFixed(0)}m`;
    };
    console.error(`Seeding ${items.length.toLocaleString()} ${noun}(s), concurrency ${concurrency}, timeout ${domainTimeout / 1000}s ...`);
    const stats = await seedDomains({
      domains: items, alreadyWatched, concurrency, discoverOne,
      onProgress: (s) => { if (s.processed % logEvery === 0 || s.processed === s.total) console.error('  ' + fmt(s)); },
    });
    console.error(`\nDone in ${((Date.now() - t0) / 60000).toFixed(1)}m. ` + fmt(stats));
    process.exit(0);
  })().catch((e) => { console.error('seed-monitor failed:', e && e.stack ? e.stack : e); process.exit(1); });
}

// --- offline selftest -----------------------------------------------------------------------
if (require.main === module && process.argv.includes('--selftest')) {
  (async () => {
    let pass = 0, fail = 0;
    const ok = (name, cond) => { if (cond) { pass++; console.log('  ✓', name); } else { fail++; console.log('  ✗', name); } };

    // 1) domain normalization
    ok('normalizeDomain strips scheme/www/path/port', normalizeDomain('https://www.Acme.com:443/foo?x=1') === 'acme.com');
    ok('normalizeDomain rejects header rows + junk', normalizeDomain('name') === '' && normalizeDomain('not a domain') === '' && normalizeDomain('') === '');
    ok('domainOfUrl derives host from a url', domainOfUrl('https://www.foo.co.uk/agents/jane/') === 'foo.co.uk');
    ok('loadDomainListText de-dupes + takes first CSV col', JSON.stringify(loadDomainListText('domain\nacme.com\nwww.acme.com\nbeta.org,extra\n')) === JSON.stringify(['acme.com', 'beta.org']));

    // 1b) sitemap-URL normalization + master-list parsing
    ok('normalizeSitemapUrl adds scheme, keeps http(s), rejects header/junk',
      normalizeSitemapUrl('www.x.com/sitemap.xml') === 'https://www.x.com/sitemap.xml'
      && normalizeSitemapUrl('https://x.com/a.xml') === 'https://x.com/a.xml'
      && normalizeSitemapUrl('SiteMap') === '' && normalizeSitemapUrl('ftp://x/a.xml') === '');
    {
      const csv = 'Industry,Website,SiteMap,Key Word,Count,CompanyEmployeeSizeRange\n'
        + 'Real Estate,compass.com,https://www.compass.com/sitemaps/agent-pages/sitemap_1.xml,agents,30197,"10,001+ employees"\n'   // quoted comma AFTER the SiteMap col
        + 'Insurance,statefarm.com,https://www.statefarm.com/sitemap-agents.xml,agent,19306,"10,001+ employees"\n'
        + 'Dup,compass.com,https://www.compass.com/sitemaps/agent-pages/sitemap_1.xml,agents,30197,\n';                              // duplicate URL
      const urls = loadMasterListSitemaps(csv);
      ok('loadMasterListSitemaps finds the SiteMap col by header, de-dupes (quoted commas after it are harmless)',
        urls.length === 2 && urls[0] === 'https://www.compass.com/sitemaps/agent-pages/sitemap_1.xml'
        && urls[1] === 'https://www.statefarm.com/sitemap-agents.xml');
    }

    // 2) pure driver: pool covers every item exactly once, aggregates the right buckets
    const seen = [];
    const fakeDiscover = async (d) => {
      seen.push(d);
      if (d === 'fail.com') throw new Error('boom');
      if (d === 'none.com') return { added: 0, bioCount: 0 };
      return { added: 2, bioCount: 7 };                       // a "good" domain -> 2 watches, 7 bios
    };
    const list = ['a.com', 'b.com', 'none.com', 'fail.com', 'c.com'];
    const s1 = await seedDomains({ domains: list, concurrency: 3, discoverOne: fakeDiscover });
    ok('every domain processed exactly once', seen.length === 5 && new Set(seen).size === 5);
    ok('good domains tallied (3 with watches, +6 watches, 21 bios)', s1.withWatch === 3 && s1.watchesAdded === 6 && s1.biosBaselined === 21);
    ok('no-bio + failure bucketed separately', s1.noBioSitemap === 1 && s1.failed === 1 && s1.processed === 5);

    // 3) resume skips already-watched domains (no discover call for them)
    const seen2 = [];
    const s2 = await seedDomains({ domains: list, alreadyWatched: new Set(['a.com', 'b.com']),
      concurrency: 2, discoverOne: async (d) => { seen2.push(d); return { added: 1, bioCount: 1 }; } });
    ok('resume skips watched, discovers the rest', s2.skipped === 2 && seen2.length === 3 && !seen2.includes('a.com'));

    // 4) end-to-end against the REAL engine + a temp SQLite store + a fake site (no network)
    const os = require('os');
    const engine = require('./cc-engine');
    const { makeDb } = require('./db');
    const { makeMonitor } = require('./sitemap-monitor');
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'seed-'));
    const _log = console.log; console.log = () => {};                  // silence makeDb boot logs
    const db = makeDb(dir);
    console.log = _log;
    const H = 'agency.com';
    const docs = {
      [`https://${H}/robots.txt`]: `User-agent: *\nSitemap: https://${H}/sitemap_index.xml\n`,
      [`https://${H}/sitemap_index.xml`]: `<sitemapindex><sitemap><loc>https://${H}/agents.xml</loc><lastmod>2026-06-20</lastmod></sitemap>` +
        `<sitemap><loc>https://${H}/blog.xml</loc></sitemap></sitemapindex>`,
      [`https://${H}/agents.xml`]: `<urlset>${['jane-doe', 'john-roe', 'amy-poe'].map((s) =>
        `<url><loc>https://${H}/agents/${s}/</loc></url>`).join('')}</urlset>`,
      [`https://${H}/blog.xml`]: `<urlset><url><loc>https://${H}/blog/a/</loc></url><url><loc>https://${H}/news/b/</loc></url></urlset>`,
    };
    const fetchDoc = async (u) => docs[u] || '';
    const monitor = makeMonitor({ db, engine, fetchDoc, log: () => {} });
    const discoverOne = (d) => monitor.discoverWatches({ domains: [d] }).then((r) => ({ added: r.added, bioCount: r.watches.reduce((a, w) => a + w.bioCount, 0) }));

    const e1 = await seedDomains({ domains: [H], concurrency: 1, discoverOne });
    ok('e2e: bio-dedicated child seeded as a watch (blog.xml ignored)', e1.withWatch === 1 && e1.watchesAdded === 1 && db.listWatches().length === 1);
    ok('e2e: baseline seeded (3 bios) with no observations', db.listWatches()[0].present_count === 3 && db.recentObservations().length === 0);

    // re-run with resume derived from the store -> the domain is now skipped
    const watchedNow = new Set(db.listWatches().map((w) => w.domain));
    const e2 = await seedDomains({ domains: [H], alreadyWatched: watchedNow, concurrency: 1, discoverOne });
    ok('e2e: resume skips the now-watched domain', e2.skipped === 1 && e2.withWatch === 0);

    // 5) SITEMAP mode e2e: seed an EXACT child sitemap URL (no robots/index resolution), fresh store
    const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'seed-sm-'));
    const _l2 = console.log; console.log = () => {};
    const db2 = makeDb(dir2);
    console.log = _l2;
    const RH = 'realtors.example';
    const SM = `https://${RH}/sitemap-agents.xml`;
    const smDocs = { [SM]: `<urlset>${['jane-doe', 'john-roe', 'amy-poe', 'zoe-kim'].map((s) =>
      `<url><loc>https://${RH}/agents/${s}/</loc></url>`).join('')}</urlset>` };
    const mon2 = makeMonitor({ db: db2, engine, fetchDoc: async (u) => smDocs[u] || '', log: () => {} });
    const discoverSm = (u) => mon2.discoverWatches({ sitemaps: [u] }).then((r) => ({ added: r.added, bioCount: r.watches.reduce((a, w) => a + w.bioCount, 0) }));
    const sm1 = await seedDomains({ domains: [SM], concurrency: 1, discoverOne: discoverSm });
    ok('e2e(sitemap): exact child URL seeded as a watch (4 bios)', sm1.withWatch === 1 && sm1.biosBaselined === 4 && db2.listWatches()[0].sitemap_url === SM);
    const smWatched = new Set(db2.listWatches().map((w) => w.sitemap_url));   // resume keys on sitemap_url
    const sm2 = await seedDomains({ domains: [SM], alreadyWatched: smWatched, concurrency: 1, discoverOne: discoverSm });
    ok('e2e(sitemap): resume skips the now-watched sitemap URL', sm2.skipped === 1 && sm2.withWatch === 0);

    try { fs.rmSync(dir, { recursive: true, force: true }); fs.rmSync(dir2, { recursive: true, force: true }); } catch { /* temp */ }
    console.log(`\nseed-monitor self-test: ${pass} passed, ${fail} failed`);
    process.exit(fail ? 1 : 0);
  })().catch((e) => { console.error(e); process.exit(1); });
}
