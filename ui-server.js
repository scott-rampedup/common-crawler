const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'ui');
const RESULTS_CSV = path.join(__dirname, 'cc-results.csv');
const { runDomains, COLUMNS, extractBioUrlsFromSitemaps, extractBioUrlGroups, isBioOrContactUrl, discoverBioUrlsFromCC, resolveLatestCrawl } = require('./cc-engine');
const { loadGenderMap, loadEmailBlocklist, analyzePhones, geocodeRecords, geocodePhone, classifyEmail, cleanEmail, findPosition } = require('./extractor');
const { modelEmail } = require('./email-pattern');
const emailModel = require('./email-model');   // shared email-modelling (also used by the worker fleet)
const { importSheet } = require('./sheet-import');
const { siteSearch, bioRowsToRecords } = require('./serper');
const vcard = require('./vcard');
const mailer = require('./mailer');
const DEMO_MODE = process.env.DEMO_MODE === 'true';

// Email blocklist (addresses to drop). Loaded once; edit email-blocklist.txt to update.
let blocklist = new Set();
try {
  blocklist = loadEmailBlocklist(path.join(__dirname, 'email-blocklist.txt'));
  console.log(`Email blocklist: ${blocklist.size} address(es).`);
} catch (e) { /* none */ }

// First-name -> gender lookup, loaded once at startup (committed CSV ships in the image).
let GENDER_MAP = {};
try {
  GENDER_MAP = loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  console.log(`Loaded ${Object.keys(GENDER_MAP).length.toLocaleString()} name->gender entries.`);
} catch (e) {
  console.warn('names-genders.csv not loaded (Gender will be blank):', e.message);
}

// Where job data lives. On a host, point DATA_DIR at a persistent disk so jobs
// survive restarts and redeploys; locally it defaults to this folder.
const DATA_DIR = process.env.DATA_DIR || __dirname;

// ---------------------------------------------------------------- access control
// Hosted publicly, this tool exposes scraping + personal contact data, so it must
// sit behind a password. Configure either:
//   APP_PASSWORD=secret              -> any username, this shared password
//   AUTH_USERS=alice:pw1,bob:pw2     -> specific user:password pairs
// If NEITHER is set, the server runs open (fine for localhost, NOT for hosting).
const APP_PASSWORD = process.env.APP_PASSWORD || '';
function parseAuthUsers(raw) {
  const map = new Map();
  for (const pair of String(raw || '').split(',')) {
    const i = pair.indexOf(':');
    if (i <= 0) continue;
    const user = pair.slice(0, i).trim();
    const pass = pair.slice(i + 1);
    if (user && pass) map.set(user, pass);
  }
  return map;
}
const AUTH_USERS = parseAuthUsers(process.env.AUTH_USERS || '');
const AUTH_ENABLED = !!(APP_PASSWORD || AUTH_USERS.size);

function safeEqual(a, b) {
  const ab = Buffer.from(String(a || ''));
  const bb = Buffer.from(String(b || ''));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function checkAuth(req, res) {
  if (!AUTH_ENABLED) return true;
  const header = req.headers['authorization'] || '';
  const m = header.match(/^Basic\s+(.+)$/i);
  if (m) {
    const decoded = Buffer.from(m[1], 'base64').toString('utf8');
    const i = decoded.indexOf(':');
    const user = i >= 0 ? decoded.slice(0, i) : decoded;
    const pass = i >= 0 ? decoded.slice(i + 1) : '';
    if (AUTH_USERS.size && AUTH_USERS.has(user) && safeEqual(AUTH_USERS.get(user), pass)) return true;
    if (APP_PASSWORD && safeEqual(APP_PASSWORD, pass)) return true;   // any username, shared password
  }
  res.writeHead(401, {
    'WWW-Authenticate': 'Basic realm="RampedUp Contact Finder", charset="UTF-8"',
    'Content-Type': 'text/plain; charset=utf-8',
  });
  res.end('Authentication required.');
  return false;
}

// ---------------------------------------------------------------- background jobs
// A search runs as a job that lives on the server, not in the browser tab. You can
// start one, close the tab, and come back: jobs are persisted to disk per-domain so
// progress survives a restart, and an interrupted job can be resumed.
const JOBS_DIR = path.join(DATA_DIR, 'jobs');
try { fs.mkdirSync(JOBS_DIR, { recursive: true }); } catch (e) { /* ignore */ }
// Sweep throwaway per-job engine CSVs left over from past runs — they accumulate and can fill the
// data volume (records live in the central DB). Safe at startup: no job is running yet.
try { let n = 0; for (const f of fs.readdirSync(JOBS_DIR)) if (f.endsWith('.engine.csv')) { try { fs.unlinkSync(path.join(JOBS_DIR, f)); n++; } catch (e) {} } if (n) console.log(`Swept ${n} leftover engine.csv file(s).`); } catch (e) {}

// central, de-duplicated contacts database (every finished job merges into it).
// Two backends, same method names: SQLite (db.js, synchronous) or Postgres (db-pg.js, async — the
// shared store the worker fleet writes to). CONTACTS_PG=1 selects Postgres; every db call is awaited
// so either backend works (await on a sync return is a no-op). The SQLite instance is ALWAYS created
// because the sitemap monitor's tables live in it (monitorDb); only the CONTACTS store is swapped.
const { makeDb } = require('./db');
const { makeDb: makeContactsPg } = require('./db-pg');
const CONTACTS_PG = /^(1|true|yes|on)$/i.test(process.env.CONTACTS_PG || '');
const sqliteDb = makeDb(DATA_DIR);
let db = sqliteDb;                 // contacts backend (writes/edits) — reassigned to Postgres in startup() if flagged

// Two-plane read path: the Master DB tab (search / filter / export / facets) can be served from the
// OpenSearch production store instead of the processing DB. `reader` is what those read endpoints use;
// it points at OpenSearch when SEARCH_BACKEND=opensearch (+ OPENSEARCH_ENDPOINT), else at `db`.
const openSearch = require('./opensearch');
let reader = db;                   // read backend for search/export/facets/stats — set in startup()
function makeOsReader(endpoint) {
  const client = openSearch.makeClient(endpoint);
  return {
    _os: true,
    query: (o) => openSearch.search(client, o),
    each: (o, cb) => openSearch.each(client, o, cb),
    facets: () => openSearch.facets(client),
    stats: () => openSearch.stats(client),
    put: (docs) => openSearch.indexDocs(client, docs),           // authoritative edit write-through
    del: (emails) => openSearch.bulkDelete(client, emails),      // delete write-through
  };
}
let osSync = null;   // background delta syncer handle (fleet-ingested contacts -> OpenSearch)
const monitorDb = sqliteDb;        // sitemap-monitor tables always live in SQLite
const aiEnrich = require('./ai-enrich');

// ---- Sitemap monitor (new-employee detection via bio-dedicated child sitemaps) ----
// Re-checks watched child sitemaps on a schedule, diffs the URL set vs the stored baseline, and
// extracts the DELTA (new bios = candidate new hires). MONITOR_ENABLED turns the nightly tick on.
const ccEngine = require('./cc-engine');
const { makeMonitor } = require('./sitemap-monitor');
const MONITOR_ENABLED = /^(1|true|yes|on)$/i.test(process.env.MONITOR_ENABLED || '');
const MONITOR_INTERVAL_HOURS = Math.max(1, Number(process.env.MONITOR_INTERVAL_HOURS) || 24);
// Known people/bio sitemap filenames (agents-sitemap.xml, attorneys-sitemap.xml, loan-officer-sitemap.xml, …):
// a child sitemap whose name matches is treated as bio-dedicated by monitor discovery, and ALL its URLs
// are captured. Curated in "Sitemap extensions.csv" (ships in the image).
let BIO_SITEMAP_NAMES = new Set();
try {
  const csv = fs.readFileSync(path.join(__dirname, 'Sitemap extensions.csv'), 'utf8');
  BIO_SITEMAP_NAMES = new Set(csv.split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name')));
  console.log(`Loaded ${BIO_SITEMAP_NAMES.size} bio-sitemap filename pattern(s).`);
} catch (e) { /* optional */ }
const monitor = makeMonitor({
  db: monitorDb,                   // monitor tables are in SQLite; its deltas extract into `db` (contacts) via startJob
  engine: ccEngine,
  fetchDoc: ccEngine.fetchDoc,
  genderMap: GENDER_MAP,
  bioSitemapNames: BIO_SITEMAP_NAMES,
  // delta extraction reuses the normal CC-first webpage pipeline (-> Master DB upsert)
  extract: (urls, label) => { startJob(urls, '', false, 'webpage', 'Monitor', label || 'Monitor: new bios', null); },
  log: (m) => console.log(`[monitor] ${m}`),
});
let monitorRunning = false;
// Single guard shared by the scheduled tick AND the manual /api/monitor/run endpoint, so two passes
// never overlap.
async function runMonitorPassGuarded(opts = {}) {
  if (monitorRunning) return { skipped: true, reason: 'a monitor pass is already running' };
  monitorRunning = true;
  try { return await monitor.runMonitorPass(opts); }
  finally { monitorRunning = false; }
}

// Filter facets are DISTINCT scans over the whole contacts table — tens of seconds on a 500k+ row
// Postgres under fleet write-load. They change slowly, so cache them and refresh in the background;
// /api/db/facets then always serves instantly and never blocks a /search page load.
let facetsCache = { directory: [], emailType: [], phoneType: [], type: [] };
let facetsAt = 0, facetsRefreshing = false;
const FACETS_TTL_MS = 10 * 60 * 1000;
async function refreshFacets() {
  if (facetsRefreshing) return;
  facetsRefreshing = true;
  try { facetsCache = await reader.facets(); facetsAt = Date.now(); }
  catch (e) { console.error('facets refresh failed:', e.message); }
  finally { facetsRefreshing = false; }
}

// ---- Google Sheet -> Master DB scheduled sync (one-way, import-only) ----
// SHEET_SYNC_URL = the sheet to import; SHEET_SYNC_HOURS = interval (default 24).
const SHEET_SYNC_URL = process.env.SHEET_SYNC_URL || '';
const SHEET_SYNC_HOURS = Math.max(1, Number(process.env.SHEET_SYNC_HOURS) || 24);
let sheetSync = { running: false, lastRun: null, lastResult: null, lastError: null, url: SHEET_SYNC_URL };

// Mirror the Google Sheet sync as a single persistent 'Google Sheet' job so it shows in the panel
// with the right Type. Uses override counters (its contacts live in the Master DB, not the job).
const SHEET_JOB_ID = 'job_google_sheet';
function upsertSheetJob(res) {
  let sj = jobs.get(SHEET_JOB_ID);
  if (!sj) {
    sj = { id: SHEET_JOB_ID, createdAt: new Date().toISOString(), name: '', type: 'Google Sheet',
      domains: [], doneDomains: [], coverage: { found: 0, live: 0, empty: 0, errored: 0 },
      directoryFilter: '', liveOnly: false, mode: 'webpage', error: null, recordsByEmail: new Map(), lastProgress: null };
    jobs.set(SHEET_JOB_ID, sj);
  }
  sj.type = 'Google Sheet';
  sj.status = 'completed';
  sj.finishedAt = new Date().toISOString();
  sj.totalOverride = res.unique;       // unique bio URLs in the sheet
  sj.doneOverride = res.imported;      // rows imported this run
  sj.recordCount = res.imported;       // contacts (live in the Master DB)
  try { persistJob(sj); } catch (e) { /* best-effort */ }
}

async function runSheetSync(url) {
  const target = (url || SHEET_SYNC_URL || '').trim();
  if (!target) { sheetSync.lastError = 'No sheet URL configured (set SHEET_SYNC_URL).'; return sheetSync; }
  if (sheetSync.running) return sheetSync;
  sheetSync.running = true; sheetSync.lastError = null;
  const t0 = Date.now();
  try {
    console.log(`Sheet sync: importing ${target} ...`);
    const res = await importSheet(db, target, { genderMap: GENDER_MAP });
    res.elapsedMs = Date.now() - t0;
    sheetSync.lastResult = res; sheetSync.url = target;
    upsertSheetJob(res);                       // mirror the sync as a 'Google Sheet' job in the panel
    console.log(`Sheet sync: ${res.imported} imported (+${res.added} new) from ${res.unique} unique URL(s); DB total ${res.dbTotal} (${res.elapsedMs}ms).`);
  } catch (e) {
    sheetSync.lastError = e.message;
    console.error('Sheet sync failed:', e.message);
  } finally {
    sheetSync.running = false; sheetSync.lastRun = new Date().toISOString();
  }
  return sheetSync;
}

// ---- Site Search (serper.dev): discover bio URLs on a site -> webpage job -> Master DB ----
// One search at a time. SERPER_API_KEY required; SERPER_MAX_PAGES caps pagination (1 credit/page).
const SERPER_API_KEY = process.env.SERPER_API_KEY || '';
const SERPER_MAX_PAGES = Math.max(1, Number(process.env.SERPER_MAX_PAGES) || 20);
let siteSearchState = { running: false, status: 'idle', input: '', target: '', startedAt: null,
  finishedAt: null, pages: 0, totalResults: 0, bioCount: 0, credits: 0, results: [], jobId: null, error: null };
const SS_EMAIL_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const SS_PHONE_RE = /\+?\d[\d\s().-]{7,}\d/g;

async function runSiteSearch(input) {
  if (siteSearchState.running) return siteSearchState;
  siteSearchState = { running: true, status: 'searching', input, target: '', startedAt: new Date().toISOString(),
    finishedAt: null, pages: 0, totalResults: 0, bioCount: 0, credits: 0, results: [], jobId: null, error: null,
    snippetUpserted: 0, snippetAdded: 0, modelled: 0 };
  try {
    if (!SERPER_API_KEY) throw new Error('SERPER_API_KEY is not set.');
    const sr = await siteSearch(input, { apiKey: SERPER_API_KEY, maxPages: SERPER_MAX_PAGES,
      onPage: ({ page, total }) => { siteSearchState.pages = page; siteSearchState.totalResults = total; } });
    siteSearchState.target = sr.target; siteSearchState.credits = sr.credits; siteSearchState.totalResults = sr.results.length;
    if (sr.error && !sr.results.length) throw new Error('serper: ' + sr.error);

    const bio = [];
    for (const r of sr.results) {
      // Site Search intentionally has NO bio-URL filter — the user already scoped the query with
      // `site:<target>`, so every returned result is a candidate to crawl/extract (unlike sitemap /
      // Common Crawl discovery, which still gate on isBioOrContactUrl). Keeps keyword-less people
      // pages (/profiles/12345, /p/jdoe, unrecognized sections) that the classifier would drop.
      const text = `${r.title}\n${r.snippet}`;
      const emails = [...new Set((text.match(SS_EMAIL_RE) || []).map((e) => cleanEmail(e)).filter(Boolean))];
      const phones = [...new Set((text.match(SS_PHONE_RE) || []).map((p) => p.trim()).filter((p) => p.replace(/\D/g, '').length >= 10))].slice(0, 2);
      bio.push({ url: r.link, title: r.title, description: r.snippet, emails, phones, position: findPosition(r.title, r.snippet) || '' });
    }
    siteSearchState.results = bio; siteSearchState.bioCount = bio.length;

    if (bio.length) {
      // 1) push what the search itself found (position / email / phone + URL-derived name) into the
      //    Master DB, modelling an email for the email-less ones from the company's known pattern.
      const today = new Date().toISOString().slice(0, 10);
      const serperRecords = bioRowsToRecords(bio, GENDER_MAP, today);
      const modelled = await modelMissingEmailsForRecords(serperRecords);
      const merged = await db.upsertMany(serperRecords);
      siteSearchState.snippetAdded = merged.added; siteSearchState.snippetUpserted = merged.processed; siteSearchState.modelled = modelled;
      // reflect each row's final email (found-in-snippet or modelled) + name/position back to the UI rows
      const recByUrl = new Map(serperRecords.map((r) => [r['Web Source URL'], r]));
      for (const b of bio) {
        const rec = recByUrl.get(String(b.url).split('?')[0].split('#')[0]);
        if (!rec) continue;
        b.name = [rec['First'], rec['Last']].filter(Boolean).join(' ');
        if (rec['Email Address']) { b.email = rec['Email Address']; b.emailType = rec['Email Type']; }
        if (!b.position && rec['Position']) b.position = rec['Position'];
      }
      console.log(`Site Search: search-result records -> ${merged.processed} upserted (+${merged.added} new), ${modelled} modelled email(s).`);

      // 2) also crawl the bio URLs to enrich further (merges into the same DB by email)
      const job = startJob(bio.map((b) => b.url), '', false, 'webpage', 'Site Search Results', `Site Search: ${siteSearchState.target || input}`);
      siteSearchState.jobId = job.id;
      siteSearchState.status = 'processing';
      console.log(`Site Search: ${bio.length} bio URL(s) from ${sr.results.length} results (${sr.credits} credits) -> job ${job.id}.`);
    } else {
      siteSearchState.status = 'done';
      console.log(`Site Search: 0 bio URLs from ${sr.results.length} results for site:${sr.target}.`);
    }
  } catch (e) {
    siteSearchState.status = 'failed'; siteSearchState.error = e.message;
    console.error('Site Search failed:', e.message);
  } finally {
    siteSearchState.running = false; siteSearchState.finishedAt = new Date().toISOString();
  }
  return siteSearchState;
}

// Status incl. the processing job's progress so the UI shows the full picture.
function siteSearchStatus() {
  const job = siteSearchState.jobId ? jobs.get(siteSearchState.jobId) : null;
  let status = siteSearchState.status;
  if (status === 'processing' && job && job.status !== 'running') status = 'done';
  return { ...siteSearchState, configured: !!SERPER_API_KEY, maxPages: SERPER_MAX_PAGES, status,
    job: job ? { id: job.id, status: job.status, total: job.domains.length, done: job.doneDomains.length, recordCount: job.recordsByEmail.size } : null };
}

// ---- user accounts / roles / sessions ----
const { makeUsers } = require('./users');
const users = makeUsers(DATA_DIR);
{
  const seeded = users.seedDefaultAdmin();
  if (seeded && seeded.generated) {
    console.log('\n========================================================================');
    console.log(`  Default admin created -> username: "${seeded.username}"   password: "${seeded.password}"`);
    console.log('  Sign in and change this password right away.');
    console.log('========================================================================\n');
  } else if (seeded) {
    console.log(`Seeded admin "${seeded.username}" from ADMIN_USERNAME/ADMIN_PASSWORD secrets.`);
  }
}

// One-time (idempotent) purge: remove any already-stored contacts whose email is on the
// blocklist. New crawls already drop blocklisted emails at ingestion; this catches ones
// stored before they were added to the list.
try {
  const before = sqliteDb.count();
  for (const e of blocklist) sqliteDb.deleteByEmail(e);
  const removed = before - sqliteDb.count();
  if (removed) console.log(`Email blocklist: removed ${removed} existing contact(s).`);
} catch (e) { /* ignore */ }

// One-time background backfill: fill missing Phone Location / Phone 2 Location for existing
// records (toll-free numbers, non-E.164 phones, and the newer Phone 2 Location field). Runs
// async so it never blocks server startup.
(async () => {
  try {
    const need = [];
    sqliteDb.each({}, (rec) => {
      if (!rec['Phone Location'] || (rec['Phone 2'] && !rec['Phone 2 Location'])) need.push(rec);
    });
    if (!need.length) return;
    await geocodeRecords(need);
    const items = need
      .map((r) => ({ email: r['Email Address'], loc1: r['Phone Location'], loc2: r['Phone 2 Location'] }))
      .filter((x) => x.loc1 || x.loc2);
    const filled = sqliteDb.backfillLocations(items);
    if (filled) console.log(`Phone geocode backfill: filled ${filled} location(s) across ${need.length} record(s).`);
  } catch (e) { console.warn('Phone geocode backfill failed:', e.message); }
})();

const jobs = new Map();   // id -> { ...meta, recordsByEmail: Map }

function newJobId() {
  return `j_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function recordsToCsv(records) {
  const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [COLUMNS.join(',')];
  for (const r of records) lines.push(COLUMNS.map((c) => esc(r[c])).join(','));
  return lines.join('\n');
}

function jobRawRecords(job) {
  return [...job.recordsByEmail.values()];
}
// served records get the dataset-wide phone analysis (dedupe Phone 2, Direct->Office)
function jobRecords(job) {
  return analyzePhones(jobRawRecords(job));
}

function jobSummary(job) {
  return {
    id: job.id,
    createdAt: job.createdAt,
    finishedAt: job.finishedAt || null,
    status: job.status,                       // running | completed | failed | interrupted
    name: job.name || '',
    type: job.type || jobTypeFromMode(job.mode),
    total: job.totalOverride != null ? job.totalOverride : job.domains.length,
    done: job.doneOverride != null ? job.doneOverride : job.doneDomains.length,
    recordCount: job.recordCount != null ? job.recordCount : job.recordsByEmail.size,
    coverage: job.coverage,
    directoryFilter: job.directoryFilter || '',
    liveOnly: !!job.liveOnly,
    mode: job.mode || 'domain',
    error: job.error || null,
    lastProgress: job.lastProgress || null,
  };
}

// Persisting rewrites the whole job file (all records), so doing it every domain is O(n^2) on a
// big crawl. Throttle the per-domain saves; the final save (after the run) is always unthrottled.
function persistJobThrottled(job, ms = 5000) {
  const now = Date.now();
  if (job._lastPersistMs && now - job._lastPersistMs < ms) return;
  job._lastPersistMs = now;
  persistJob(job);
}

function persistJob(job) {
  if (job.deleted) return;                 // don't resurrect a job that was deleted
  const out = {
    id: job.id,
    createdAt: job.createdAt,
    finishedAt: job.finishedAt || null,
    status: job.status,
    name: job.name || '',
    type: job.type || jobTypeFromMode(job.mode),
    domains: job.domains,
    doneDomains: job.doneDomains,
    coverage: job.coverage,
    directoryFilter: job.directoryFilter || '',
    liveOnly: !!job.liveOnly,
    mode: job.mode || 'domain',
    error: job.error || null,
    totalOverride: job.totalOverride, doneOverride: job.doneOverride, recordCount: job.recordCount,   // for the Google Sheet job
    records: jobRawRecords(job),
  };
  try { fs.writeFileSync(path.join(JOBS_DIR, `${job.id}.json`), JSON.stringify(out)); }
  catch (e) { console.error(`Failed to persist job ${job.id}:`, e.message); }
}

function loadJobs() {
  let files = [];
  try { files = fs.readdirSync(JOBS_DIR).filter((f) => f.endsWith('.json')); } catch (e) { return; }
  for (const f of files) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(JOBS_DIR, f), 'utf8'));
      const recordsByEmail = new Map();
      for (const r of (j.records || [])) {
        const k = String(r['Email Address'] || '').toLowerCase() || `_${recordsByEmail.size}`;
        recordsByEmail.set(k, r);
      }
      // a job still marked "running" means the server died mid-run — it can be resumed
      const status = j.status === 'running' ? 'interrupted' : j.status;
      jobs.set(j.id, {
        id: j.id, createdAt: j.createdAt, finishedAt: j.finishedAt || null, status,
        name: j.name || '', type: j.type || (j.mode === 'webpage' ? 'Webpages' : 'Domains'),
        domains: j.domains || [], doneDomains: j.doneDomains || [],
        coverage: j.coverage || { found: 0, live: 0, empty: 0, errored: 0 },
        directoryFilter: j.directoryFilter || '', liveOnly: !!j.liveOnly, mode: j.mode || 'domain',
        totalOverride: j.totalOverride, doneOverride: j.doneOverride, recordCount: j.recordCount,
        error: j.error || null, recordsByEmail, lastProgress: null,
      });
    } catch (e) { console.error(`Failed to load job file ${f}:`, e.message); }
  }
  console.log(`Loaded ${jobs.size} saved job(s) from ${JOBS_DIR}`);
}

// Each job file embeds ALL its records (a big crawl/load can be tens of MB), and they accumulate
// forever — a backlog once filled the data volume (ENOSPC -> jobs "failed" mid-run). The records
// already live in the central DB, so old per-job files are redundant beyond dashboard history.
// At startup (after loadJobs, before resume), keep the most recent JOBS_RETAIN terminal jobs and
// delete the rest; never touch active (running/interrupted/queued/starting) jobs or the persistent
// Google Sheet job. Bounds accumulation so a big harvest of chunked loader jobs can't refill the disk.
function pruneOldJobs() {
  const RETAIN = Math.max(0, Number(process.env.JOBS_RETAIN) || 100);
  const ACTIVE = new Set(['running', 'interrupted', 'queued', 'starting']);
  const terminal = [...jobs.values()]
    .filter((j) => j.id !== SHEET_JOB_ID && !ACTIVE.has(j.status))
    .sort((a, b) => String(b.finishedAt || b.createdAt || '').localeCompare(String(a.finishedAt || a.createdAt || '')));
  const drop = terminal.slice(RETAIN);
  if (!drop.length) return;
  let n = 0, bytes = 0;
  for (const j of drop) {
    const fp = path.join(JOBS_DIR, `${j.id}.json`);
    try { bytes += fs.statSync(fp).size; } catch (e) { /* may already be gone */ }
    try { fs.unlinkSync(fp); n++; } catch (e) { /* ignore */ }
    jobs.delete(j.id);
  }
  console.log(`Pruned ${n} old job file(s) (${(bytes / 1048576).toFixed(1)} MB freed), kept ${RETAIN} most recent.`);
}

// Fill in a best-guess email for bio records that have a name + Gender but no published
// address (sitemap/webpage mode). The format is LEARNED from that company's own
// Professional emails — this job's finds plus the central DB — and the result is labelled
// Email Type "Modelled" so a guess is never mistaken for a verified address. If the company
// publishes no parseable emails at all (e.g. rsmuk.com), nothing is modelled and the people
// are still kept with their name + phone + LinkedIn. Mutates job records in place.
// Core: model an email for each email-less record that has a name + Gender, learning the format
// from that company's Professional emails (the given records + the central DB). Result is
// labelled Email Type "Modelled". Mutates records in place; returns how many were modelled.
// Model emails for email-less bios from each company's known Professional-email pattern. Delegates to
// the shared email-model module (the worker fleet uses the same logic); the central-DB sample lookup
// goes through whichever contacts backend is active.
async function modelMissingEmailsForRecords(records) {
  return emailModel.modelMissingEmails(records, {
    dbQuery: (domain) => db.query({ domain, emailType: 'Professional', pageSize: 500 }).then((r) => r.rows || []),
  });
}

async function modelMissingEmails(job) {
  if ((job.mode || 'domain') !== 'webpage') return 0;          // sitemap/webpage only
  const n = await modelMissingEmailsForRecords([...job.recordsByEmail.values()]);
  if (n) console.log(`Job ${job.id}: modelled ${n} email(s) from company pattern(s).`);
  return n;
}

// run a set of domains for a job, accumulating records + coverage, persisting per domain
async function runJobDomains(job, domainsToRun) {
  job.status = 'running';
  job.error = null;
  job.stopRequested = false;
  persistJob(job);
  try {
    await runDomains(domainsToRun, {
      demoMode: DEMO_MODE,
      directoryFilter: job.directoryFilter,
      genderMap: GENDER_MAP,                                   // fill Gender via first-name lookup
      liveOnly: !!job.liveOnly,                                // skip Common Crawl when requested
      mode: job.mode || 'domain',                              // 'webpage' = only the exact URLs
      _warcByUrl: job._warcByUrl || null,                      // pre-resolved WARC pointers -> skip per-URL index lookups
      shouldStop: () => job.stopRequested,                     // honor a STOP request
      outPath: path.join(JOBS_DIR, `${job.id}.engine.csv`),   // throwaway; we keep our own records
      onRecord: (row) => {
        const k = String(row['Email Address'] || '').toLowerCase() || `_${job.recordsByEmail.size}`;
        job.recordsByEmail.set(k, row);
      },
      onProgress: (p) => {
        job.lastProgress = p;
        if (p.domain && (p.status === 'domain-done' || p.status === 'no-candidates')) {
          if (!job.doneDomains.includes(p.domain)) job.doneDomains.push(p.domain);
          // tally coverage ourselves so it stays correct across resumes
          // (Common Crawl -> found; Live Crawl / Webpage -> live)
          if (p.status === 'domain-done' && p.source === 'Common Crawl') job.coverage.found += 1;
          else if (p.status === 'domain-done') job.coverage.live += 1;
          else job.coverage.empty += 1;
          persistJobThrottled(job);        // throttled: final unthrottled save happens after the run
        }
      },
    });
    job.status = job.stopRequested ? 'stopped' : 'completed';
  } catch (e) {
    job.status = 'failed';
    job.error = e.message;
  }
  job.stopRequested = false;
  // read any linked .vcf vCards and merge their fields (name/email/title + cell => Mobile phone)
  // BEFORE email modelling, so a real vCard email wins over a modelled one.
  try {
    const v = await vcard.enrichRecords([...job.recordsByEmail.values()], { genderMap: GENDER_MAP });
    if (v.fetched) console.log(`Job ${job.id}: read ${v.fetched} vCard(s), enriched ${v.applied} record(s).`);
  } catch (e) { console.error('vCard enrichment failed:', e.message); }
  try { await modelMissingEmails(job); }                            // model emails for email-less bios (sitemap/webpage)
  catch (e) { console.error('email modelling failed:', e.message); }
  try { await geocodeRecords([...job.recordsByEmail.values()]); }   // City, Region, Country
  catch (e) { console.error('geocode failed:', e.message); }
  job.finishedAt = new Date().toISOString();
  persistJob(job);
  // merge this job's fully-processed records into the central database
  try {
    const merged = await db.upsertMany(jobRecords(job));
    console.log(`Central DB: merged ${merged.processed} record(s), +${merged.added} new (total ${merged.total}).`);
  } catch (e) { console.error('Central DB merge failed:', e.message); }
  // the per-job engine CSV is throwaway (records are in our DB now) — delete it so it can't fill /data
  try { fs.unlinkSync(path.join(JOBS_DIR, `${job.id}.engine.csv`)); } catch (e) { /* may not exist */ }
  console.log(`Job ${job.id} ${job.status} — ${job.recordsByEmail.size} record(s)`);
}

const JOB_TYPES = ['Domains', 'Webpages', 'Sitemaps', 'Site Search Results', 'Google Sheet', 'CC Discovery', 'Monitor'];
const jobTypeFromMode = (mode) => (mode === 'webpage' ? 'Webpages' : 'Domains');
const normalizeJobType = (t, mode) => (JOB_TYPES.includes(t) ? t : jobTypeFromMode(mode));

function startJob(domains, directoryFilter, liveOnly, mode, type, name, warcByUrl) {
  const m = mode === 'webpage' ? 'webpage' : 'domain';
  const job = {
    id: newJobId(),
    createdAt: new Date().toISOString(),
    finishedAt: null,
    status: 'running',
    name: typeof name === 'string' ? name.trim().slice(0, 120) : '',
    type: normalizeJobType(type, m),
    domains,
    doneDomains: [],
    coverage: { found: 0, live: 0, empty: 0, errored: 0 },
    directoryFilter: directoryFilter || '',
    liveOnly: !!liveOnly,
    mode: m,
    stopRequested: false,
    error: null,
    recordsByEmail: new Map(),
    lastProgress: null,
  };
  job._warcByUrl = warcByUrl || null;     // transient WARC fast-path map (not persisted; resume falls back to index lookups)
  jobs.set(job.id, job);
  runJobDomains(job, domains);            // fire and forget; survives this request
  return job;
}

function resumeJob(id) {
  const job = jobs.get(id);
  if (!job) return null;
  if (job.status === 'running') return job;
  const remaining = job.domains.filter((d) => !job.doneDomains.includes(d));
  if (remaining.length === 0) { job.status = 'completed'; persistJob(job); return job; }
  runJobDomains(job, remaining);          // fire and forget
  return job;
}

function deleteJob(id) {
  const job = jobs.get(id);
  if (!job) return false;
  job.deleted = true;                     // suppress any further persistence
  job.stopRequested = true;               // wind down if it's still running
  jobs.delete(id);
  for (const f of [`${id}.json`, `${id}.engine.csv`]) {
    try { fs.unlinkSync(path.join(JOBS_DIR, f)); } catch (e) { /* may not exist */ }
  }
  console.log(`Deleted job ${id}`);
  return true;
}

function contentType(filePath) {
  if (filePath.endsWith('.html')) return 'text/html; charset=utf-8';
  if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
  if (filePath.endsWith('.js')) return 'application/javascript; charset=utf-8';
  if (filePath.endsWith('.json')) return 'application/json; charset=utf-8';
  if (filePath.endsWith('.svg')) return 'image/svg+xml';
  if (filePath.endsWith('.png')) return 'image/png';
  if (filePath.endsWith('.jpg') || filePath.endsWith('.jpeg')) return 'image/jpeg';
  if (filePath.endsWith('.webp')) return 'image/webp';
  if (filePath.endsWith('.ico')) return 'image/x-icon';
  return 'application/octet-stream';
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      values.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  values.push(current);
  return values;
}

function parseCsv(csvText) {
  const lines = csvText.split(/\r?\n/);
  const headerLine = lines.shift();
  if (!headerLine) return [];

  const headers = parseCsvLine(headerLine);
  const rows = [];

  for (const line of lines) {
    if (!line.trim()) continue;
    const values = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || '';
    });
    rows.push(row);
  }

  return rows;
}

function serveStaticFile(res, filePath) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }

    res.writeHead(200, { 'Content-Type': contentType(filePath) });
    res.end(data);
  });
}

// "acme.com, foo.org\nbar.io" -> ['acme.com','foo.org','bar.io'] (root domains, www-stripped)
function parseDomainsParam(raw) {
  return String(raw || '')
    .split(/[\s,]+/)
    .map((s) => s.trim().toLowerCase().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, ''))
    .filter(Boolean);
}

function sendJson(res, data) {
  const payload = JSON.stringify(data);
  res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(payload);
}

// ---- session / auth helpers ----
const jsonHdr = { 'Content-Type': 'application/json; charset=utf-8' };
const SESSION_COOKIE = 'sid';
const RANK_ANALYST = users.roleRank('analyst');
const RANK_ADMIN = users.roleRank('admin');

function parseCookies(req) {
  const out = {};
  for (const part of String(req.headers['cookie'] || '').split(';')) {
    const i = part.indexOf('=');
    if (i > 0) out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
  }
  return out;
}
function currentUser(req) { return users.sessionUser(parseCookies(req)[SESSION_COOKIE]); }
function setSessionCookie(res, token) {
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${token}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=${60 * 60 * 24 * 14}`);
}
function clearSessionCookie(res) {
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0`);
}
function readJsonBody(req, cb) {
  let body = '';
  req.on('data', (c) => { body += c; if (body.length > 2e6) req.destroy(); });
  req.on('end', () => { try { cb(JSON.parse(body || '{}')); } catch (e) { cb(null); } });
}
function jsonErr(res, code, msg) { res.writeHead(code, jsonHdr); res.end(JSON.stringify({ error: msg })); }

// Read a raw sitemap upload (sitemap XML, a gzipped sitemap, or a newline/comma list of sitemap
// URLs) and hand back { content, urls } (or { error }). Shared by /api/sitemap/extract and
// /api/sitemap/run. Reads bytes so .xml.gz uploads work; caps the upload at 64MB.
function readSitemapInput(req, cb) {
  const chunks = []; let size = 0; let aborted = false;
  req.on('data', (c) => {
    size += c.length;
    if (size > 64 * 1024 * 1024) { aborted = true; req.destroy(); return; }   // 64MB cap
    chunks.push(c);
  });
  req.on('end', () => {
    if (aborted) return cb({ error: 'Sitemap upload too large (64MB max).' });
    try {
      let buf = Buffer.concat(chunks);
      // gunzip an uploaded .gz (magic 1f 8b); a fetched .gz is handled inside the engine's fetchDoc
      if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) { try { buf = zlib.gunzipSync(buf); } catch { /* keep raw */ } }
      const text = buf.toString('utf8').trim();
      if (!text) return cb({ error: 'Empty sitemap input.' });
      const looksXml = /<\?xml|<urlset[\s>]|<sitemapindex[\s>]/i.test(text);
      if (looksXml) return cb({ content: text, urls: [] });
      const urls = text.split(/[\r\n,]+/).map((s) => s.trim()).filter((s) => /^https?:\/\//i.test(s));
      if (!urls.length) return cb({ error: 'No sitemap XML or sitemap URL(s) found in the input.' });
      cb({ content: '', urls });
    } catch (e) {
      cb({ error: e.message || 'Failed to parse sitemap.' });
    }
  });
}

// A readable job name for the per-sitemap jobs started by /api/sitemap/run.
function sitemapJobName(source) {
  const s = String(source || '').trim();
  if (!s || s === '(pasted sitemap)') return 'Sitemap';
  try { const u = new URL(s); return ('Sitemap: ' + u.hostname.replace(/^www\./, '') + u.pathname).slice(0, 120); }
  catch { return ('Sitemap: ' + s).slice(0, 120); }
}

// Admin-only API (the request handler already verified the admin role before calling this).
function handleAdmin(req, res, p) {
  let m = p.match(/^\/api\/admin\/pages\/(privacy|terms)$/);
  if (m) {
    const key = m[1];
    if (req.method === 'GET') { sendJson(res, { key, content: users.getSetting(key) || '' }); return; }
    if (req.method === 'POST') {
      readJsonBody(req, (b) => {
        if (!b || typeof b.content !== 'string') return jsonErr(res, 400, 'Bad request');
        users.setSetting(key, b.content); sendJson(res, { ok: true });
      });
      return;
    }
  }
  if (p === '/api/admin/email-status' && req.method === 'GET') {
    sendJson(res, { enabled: mailer.mailEnabled(), adminEmail: mailer.adminEmail() });
    return;
  }
  if (p === '/api/admin/test-email' && req.method === 'POST') {
    readJsonBody(req, async (b) => {
      const to = (b && typeof b.to === 'string' && b.to.trim()) ? b.to.trim() : mailer.adminEmail();
      if (!mailer.mailEnabled()) return jsonErr(res, 400, 'SMTP is not configured yet (set the SMTP_* secrets).');
      if (!to) return jsonErr(res, 400, 'No recipient — enter an address or set ADMIN_EMAIL.');
      const r = await mailer.sendMail({
        to, subject: 'Common Crawler SMTP test',
        text: 'Success — Common Crawler can send email via SMTP.',
        html: '<p>Success — Common Crawler can send email via SMTP.</p>',
      });
      if (r.ok) sendJson(res, { ok: true, to });
      else jsonErr(res, 502, r.error || 'Send failed (no error detail).');
    });
    return;
  }
  if (p === '/api/admin/users' && req.method === 'GET') { sendJson(res, users.listUsers()); return; }
  if (p === '/api/admin/users' && req.method === 'POST') {
    readJsonBody(req, (b) => {
      if (!b) return jsonErr(res, 400, 'Bad request');
      const r = users.createUser({
        username: b.username, password: b.password, role: b.role, active: b.active !== false,
        first: b.first, last: b.last, company: b.company, title: b.title, email: b.email, phone: b.phone,
      });
      if (!r.ok) return jsonErr(res, 400, r.error);
      sendJson(res, r.user);
    });
    return;
  }
  m = p.match(/^\/api\/admin\/users\/(\d+)\/(activate|deactivate|promote|demote|delete|reset-password)$/);
  if (m && req.method === 'POST') {
    const id = Number(m[1]); const action = m[2];
    const t = users.getById(id);
    if (!t) return jsonErr(res, 404, 'User not found');
    const lastAdmin = t.role === 'admin' && t.active && users.activeAdminCount() <= 1;
    if (action === 'activate') {
      users.setActive(id, true);
      if (t.email) mailer.sendMail({ to: t.email, ...mailer.templates.accountActivated(t) });   // best-effort
    }
    else if (action === 'deactivate') { if (lastAdmin) return jsonErr(res, 400, 'Cannot deactivate the last active admin.'); users.setActive(id, false); users.destroyUserSessions(id); }
    else if (action === 'promote') users.setRole(id, t.role === 'user' ? 'analyst' : 'admin');
    else if (action === 'demote') { if (lastAdmin) return jsonErr(res, 400, 'Cannot demote the last active admin.'); users.setRole(id, t.role === 'admin' ? 'analyst' : 'user'); }
    else if (action === 'delete') { if (lastAdmin) return jsonErr(res, 400, 'Cannot delete the last active admin.'); users.deleteUser(id); }
    else if (action === 'reset-password') { sendJson(res, { ok: true, tempPassword: users.resetPassword(id) }); return; }
    sendJson(res, { ok: true });
    return;
  }
  jsonErr(res, 404, 'Not found');
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  console.log(`[${new Date().toISOString()}] ${req.method} ${url.pathname}`);

  // ---------------- authentication + role gate ----------------
  const me = currentUser(req);
  const p = url.pathname;
  // Machine loader auth: a valid LOADER_TOKEN header grants ANALYST-level API access (start/read jobs)
  // without a personal session — for unattended batch loads. Never grants admin. (See load-bio-urls --token.)
  const LOADER_TOKEN = process.env.LOADER_TOKEN || '';
  const hasLoaderToken = !!LOADER_TOKEN && safeEqual(LOADER_TOKEN, String(req.headers['x-loader-token'] || ''));
  const PUBLIC_PATH = (
    p.startsWith('/ui/') || p === '/favicon.ico' ||
    p === '/home' ||
    p === '/login' || p === '/signup' || p === '/forgot' || p === '/privacy' || p === '/terms' ||
    p === '/api/auth/login' || p === '/api/auth/signup' || p === '/api/auth/logout' || p === '/api/auth/me' ||
    p === '/api/auth/forgot' ||
    (p.startsWith('/api/pages/') && req.method === 'GET')
  );
  if (!PUBLIC_PATH && !me && !hasLoaderToken) {
    if (p.startsWith('/api/')) jsonErr(res, 401, 'Authentication required');
    else if (p === '/' || p === '/index.html') { serveStaticFile(res, path.join(PUBLIC_DIR, 'home.html')); return; }  // anon root -> public landing
    else { res.writeHead(302, { Location: '/login' }); res.end(); }
    return;
  }
  const rank = me ? users.roleRank(me.role) : -1;
  const isAnalyst = rank >= RANK_ANALYST || hasLoaderToken;   // token = analyst-level (jobs); not admin
  const isAdmin = rank >= RANK_ADMIN;

  // ---- public auth + legal pages ----
  if (p === '/login') { serveStaticFile(res, path.join(PUBLIC_DIR, 'login.html')); return; }
  if (p === '/signup') { serveStaticFile(res, path.join(PUBLIC_DIR, 'signup.html')); return; }
  if (p === '/forgot') { serveStaticFile(res, path.join(PUBLIC_DIR, 'forgot.html')); return; }
  if (p === '/privacy' || p === '/terms') { serveStaticFile(res, path.join(PUBLIC_DIR, 'legal.html')); return; }
  if (p === '/home') { serveStaticFile(res, path.join(PUBLIC_DIR, 'home.html')); return; }  // public landing (always reachable)

  // ---- public page content (Privacy Policy / Terms of Use text) ----
  if (p.startsWith('/api/pages/') && req.method === 'GET') {
    const key = p.slice('/api/pages/'.length);
    if (key === 'privacy' || key === 'terms') {
      sendJson(res, { key, title: key === 'privacy' ? 'Privacy Policy' : 'Terms of Use', content: users.getSetting(key) || '' });
    } else { jsonErr(res, 404, 'Not found'); }
    return;
  }

  // ---- auth API ----
  if (p === '/api/auth/me') { if (me) sendJson(res, users.pub(me)); else jsonErr(res, 401, 'Not signed in'); return; }
  if (p === '/api/auth/login' && req.method === 'POST') {
    readJsonBody(req, (b) => {
      if (!b) return jsonErr(res, 400, 'Bad request');
      const row = users.verify(b.username, b.password);
      if (!row) return jsonErr(res, 401, 'Invalid username or password.');
      if (!row.active) return jsonErr(res, 403, 'Your account is pending administrator activation.');
      setSessionCookie(res, users.createSession(row.id));
      sendJson(res, users.pub(row));
    });
    return;
  }
  if (p === '/api/auth/logout' && req.method === 'POST') {
    users.destroySession(parseCookies(req)[SESSION_COOKIE]); clearSessionCookie(res); sendJson(res, { ok: true });
    return;
  }
  if (p === '/api/auth/signup' && req.method === 'POST') {
    readJsonBody(req, (b) => {
      if (!b) return jsonErr(res, 400, 'Bad request');
      if (!b.agree) return jsonErr(res, 400, 'You must accept the Privacy Policy and Terms of Use to sign up.');
      const r = users.createUser({
        username: b.username, password: b.password, role: 'user', active: 0,
        first: b.first, last: b.last, company: b.company, title: b.title, email: b.email, phone: b.phone,
      });
      if (!r.ok) return jsonErr(res, 400, r.error);
      console.log(`New signup pending activation: "${r.user.username}" (${r.user.email || 'no email'})`);
      // notify admins + confirm to the signer-up (best-effort; never blocks the response)
      const adminRecipients = [mailer.adminEmail(), ...users.listUsers().filter((u) => u.role === 'admin' && u.active && u.email).map((u) => u.email)]
        .filter(Boolean).filter((e, i, a) => a.indexOf(e) === i).join(', ');
      mailer.sendMail({ to: adminRecipients, ...mailer.templates.signupAdminAlert(r.user) });
      if (r.user.email) mailer.sendMail({ to: r.user.email, ...mailer.templates.signupConfirm(r.user) });
      sendJson(res, { ok: true });
    });
    return;
  }

  // POST /api/auth/forgot  { email }  -> email a temp password if the address matches an active
  // account. Always responds with the same generic message (no account enumeration).
  if (p === '/api/auth/forgot' && req.method === 'POST') {
    readJsonBody(req, (b) => {
      const generic = { ok: true, message: 'If that email matches an account, we’ve sent password reset instructions.' };
      const email = b && typeof b.email === 'string' ? b.email.trim() : '';
      if (!email) return jsonErr(res, 400, 'Email is required.');
      const row = users.getByEmail(email);
      if (row && row.active && row.email) {
        const tempPw = users.resetPassword(row.id);   // also invalidates existing sessions
        console.log(`Password reset requested for "${row.username}" -> emailing ${row.email}`);
        mailer.sendMail({ to: row.email, ...mailer.templates.passwordReset(row, tempPw) });
      } else {
        console.log(`Password reset requested for ${email} -> no active account (no email sent)`);
      }
      sendJson(res, generic);   // identical response whether or not the account exists
    });
    return;
  }

  // ---- admin page + API (admin only) ----
  if (p === '/admin') {
    if (!isAdmin) { res.writeHead(302, { Location: '/search' }); res.end(); return; }
    serveStaticFile(res, path.join(PUBLIC_DIR, 'admin.html')); return;
  }
  if (p.startsWith('/api/admin/')) {
    if (!isAdmin) return jsonErr(res, 403, 'Admin access required');
    handleAdmin(req, res, p); return;
  }

  if (url.pathname === '/' || url.pathname === '/index.html') {
    if (!isAnalyst) { res.writeHead(302, { Location: '/search' }); res.end(); return; }   // 'user' role -> Search only
    serveStaticFile(res, path.join(PUBLIC_DIR, 'index.html'));
    return;
  }

  if (url.pathname === '/search' || url.pathname === '/search.html') {
    serveStaticFile(res, path.join(PUBLIC_DIR, 'search.html'));
    return;
  }

  if (url.pathname === '/site-search' || url.pathname === '/site-search.html') {
    if (!isAnalyst) { res.writeHead(302, { Location: '/search' }); res.end(); return; }   // analyst+ (runs crawls + serper credits)
    serveStaticFile(res, path.join(PUBLIC_DIR, 'site-search.html'));
    return;
  }

  if (url.pathname === '/monitor' || url.pathname === '/monitor.html') {
    if (!isAnalyst) { res.writeHead(302, { Location: '/search' }); res.end(); return; }   // analyst+ (manages watches + runs passes)
    serveStaticFile(res, path.join(PUBLIC_DIR, 'monitor.html'));
    return;
  }

  if (url.pathname.startsWith('/ui/')) {
    const filePath = path.join(PUBLIC_DIR, url.pathname.replace(/^\/ui\//, ''));
    serveStaticFile(res, filePath);
    return;
  }

  if (url.pathname === '/api/results') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    fs.readFile(RESULTS_CSV, 'utf8', (err, csvText) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: 'Could not read cc-results.csv' }));
        return;
      }

      const rows = parseCsv(csvText);
      sendJson(res, rows);
    });
    return;
  }

  if (url.pathname === '/api/config') {
    sendJson(res, { demoMode: DEMO_MODE, source: DEMO_MODE ? 'Demo' : 'Common Crawl' });
    return;
  }

  if (url.pathname === '/api/search' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body || '{}');
        const domains = Array.isArray(payload.domains) ? payload.domains : [];
        const directoryFilter = typeof payload.directoryFilter === 'string' ? payload.directoryFilter.trim() : '';
        if (domains.length === 0) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No domains provided' }));
          return;
        }

        console.log(`Running Common Crawl search for ${domains.length} domain(s)...`);
        res.writeHead(200, {
          'Content-Type': 'application/x-ndjson; charset=utf-8',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive'
        });

        const results = await runDomains(domains, {
          outPath: RESULTS_CSV,
          directoryFilter,
          demoMode: DEMO_MODE,
          onRecord: (row) => res.write(JSON.stringify({ type: 'record', row }) + '\n'),
          onProgress: (progress) => res.write(JSON.stringify({ type: 'progress', progress }) + '\n'),
        });

        res.write(JSON.stringify({ type: 'done', resultsCount: results.length }) + '\n');
        res.end();
      } catch (err) {
        console.error(err);
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: err.message || 'Search failed' }));
        } else {
          res.write(JSON.stringify({ type: 'error', error: err.message || 'Search failed' }) + '\n');
          res.end();
        }
      }
    });
    return;
  }

  // ---- background jobs API ----
  // GET /api/jobs  -> list of job summaries (newest first)
  if (url.pathname === '/api/jobs' && req.method === 'GET') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    const list = [...jobs.values()].map(jobSummary)
      .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
    sendJson(res, list);
    return;
  }

  // POST /api/jobs  { domains: [...], directoryFilter? }  -> start a job
  if (url.pathname === '/api/jobs' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = JSON.parse(body || '{}');
        const domains = Array.isArray(payload.domains) ? payload.domains.filter(Boolean) : [];
        const directoryFilter = typeof payload.directoryFilter === 'string' ? payload.directoryFilter.trim() : '';
        const liveOnly = payload.liveOnly === true;
        const mode = payload.mode === 'webpage' ? 'webpage' : 'domain';
        const type = typeof payload.type === 'string' ? payload.type : '';
        const name = typeof payload.name === 'string' ? payload.name : '';
        // Optional WARC fast path (webpage mode): pre-resolved pointers from cc-domain-miner --warc-out
        // let extraction fetch each archived page directly, skipping the per-URL CC index lookup.
        const warcArr = Array.isArray(payload.warc) ? payload.warc : [];
        const warcByUrl = warcArr.length
          ? new Map(warcArr.filter((w) => w && w.url && w.filename)
              .map((w) => [String(w.url), { url: String(w.url), filename: w.filename, offset: w.offset, length: w.length, timestamp: w.timestamp }]))
          : null;
        if (domains.length === 0) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No domains provided' }));
          return;
        }
        const job = startJob(domains, directoryFilter, liveOnly, mode, type, name, warcByUrl);
        console.log(`Started job ${job.id} for ${domains.length} domain(s)`);
        sendJson(res, jobSummary(job));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: e.message || 'Bad request' }));
      }
    });
    return;
  }

  // ---- Sitemap monitor (new-employee detection) ----
  // GET /api/monitor -> status + headline stats.
  if (url.pathname === '/api/monitor' && req.method === 'GET') {
    sendJson(res, { stats: monitorDb.monitorStats(), enabled: MONITOR_ENABLED,
      intervalHours: MONITOR_INTERVAL_HOURS, running: monitorRunning });
    return;
  }
  // GET /api/monitor/watches -> every watched child sitemap (+ present/departed counts).
  if (url.pathname === '/api/monitor/watches' && req.method === 'GET') {
    sendJson(res, { watches: monitorDb.listWatches() });
    return;
  }
  // GET /api/monitor/changes?event=&domain=&limit= -> the change feed.
  if (url.pathname === '/api/monitor/changes' && req.method === 'GET') {
    sendJson(res, { changes: monitorDb.recentObservations({
      event: url.searchParams.get('event') || '',
      domain: url.searchParams.get('domain') || '',
      limit: Number(url.searchParams.get('limit')) || 200,
    }) });
    return;
  }
  // POST /api/monitor/watch  { domains:[], sitemaps:[] }  -> discover bio-dedicated child sitemaps,
  // register each as a watch, seed its baseline (no observations). Analyst+.
  if (url.pathname === '/api/monitor/watch' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, async (payload) => {
      if (!payload) return jsonErr(res, 400, 'Bad JSON');
      const domains = Array.isArray(payload.domains) ? payload.domains.filter(Boolean) : [];
      const sitemaps = Array.isArray(payload.sitemaps) ? payload.sitemaps.filter(Boolean) : [];
      if (!domains.length && !sitemaps.length) return jsonErr(res, 400, 'Provide domains or sitemaps');
      try {
        const out = await monitor.discoverWatches({ domains, sitemaps });
        console.log(`[monitor] registered ${out.added} watch(es)`);
        sendJson(res, { ok: true, ...out });
      } catch (e) { jsonErr(res, 500, e.message || 'discover failed'); }
    });
    return;
  }
  // POST /api/monitor/run  { force? }  -> run one monitoring pass now. Analyst+.
  if (url.pathname === '/api/monitor/run' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, async (payload) => {
      try {
        const summary = await runMonitorPassGuarded({ force: !!(payload && payload.force) });
        sendJson(res, { ok: true, summary });
      } catch (e) { jsonErr(res, 500, e.message || 'monitor pass failed'); }
    });
    return;
  }
  // POST /api/monitor/toggle  { sitemapUrl, status:'active'|'paused' }  -> pause/resume a watch.
  if (url.pathname === '/api/monitor/toggle' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, (payload) => {
      const sm = payload && String(payload.sitemapUrl || '').trim();
      const status = payload && String(payload.status || '').trim();
      if (!sm || (status !== 'active' && status !== 'paused')) return jsonErr(res, 400, 'sitemapUrl + status required');
      sendJson(res, { ok: monitorDb.setWatchStatus(sm, status) });
    });
    return;
  }
  // POST /api/monitor/unwatch  { sitemapUrl }  -> stop watching + drop its baseline. Analyst+.
  if (url.pathname === '/api/monitor/unwatch' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, (payload) => {
      const sm = payload && String(payload.sitemapUrl || '').trim();
      if (!sm) return jsonErr(res, 400, 'sitemapUrl required');
      sendJson(res, { ok: monitorDb.removeWatch(sm) });
    });
    return;
  }

  // POST /api/sitemap/extract  (raw body: sitemap XML, a gzipped sitemap, or a newline/comma
  // list of sitemap URLs) -> { ok, bioUrls, totalUrls, sitemapsFetched }. The client then runs
  // bioUrls as a normal 'webpage' job. Reads the body as bytes so .xml.gz uploads work.
  if (url.pathname === '/api/sitemap/extract' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readSitemapInput(req, async (inp) => {
      if (inp.error) return jsonErr(res, 400, inp.error);
      try {
        // directoryRules stays {} on the server by design (see project notes); genderMap aids bio detection
        const out = await extractBioUrlsFromSitemaps({ content: inp.content, urls: inp.urls, genderMap: GENDER_MAP });
        console.log(`Sitemap extract: ${out.bioUrls.length} bio URL(s) from ${out.totalUrls} URL(s) across ${out.sitemapsFetched} fetched sitemap(s)`);
        sendJson(res, { ok: true, ...out });
      } catch (e) {
        jsonErr(res, 500, e.message || 'Failed to parse sitemap.');
      }
    });
    return;
  }

  // POST /api/sitemap/run?directoryFilter=&liveOnly=  (raw body: same as /api/sitemap/extract)
  // Processes the sitemaps ONE AT A TIME: each sitemap's bio URLs start their OWN 'webpage' job,
  // instead of merging every sitemap into a single job (which overflows the /api/jobs body cap and
  // memory when several large sitemaps are uploaded). Jobs are created in-process via startJob, so
  // the bio URLs never round-trip through an HTTP body. Returns the jobs that were started.
  if (url.pathname === '/api/sitemap/run' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    const directoryFilter = (url.searchParams.get('directoryFilter') || '').trim();
    const liveOnly = /^(1|true|yes)$/i.test(url.searchParams.get('liveOnly') || '');
    const keepForMonitor = !/^(0|false|no)$/i.test(url.searchParams.get('monitor') || '1');   // default ON
    readSitemapInput(req, async (inp) => {
      if (inp.error) return jsonErr(res, 400, inp.error);
      try {
        const startedJobs = [];
        const out = await extractBioUrlGroups({
          content: inp.content, urls: inp.urls, genderMap: GENDER_MAP,
          onGroup: (g) => {                                  // fires once per sitemap, in order
            const job = startJob(g.bioUrls, directoryFilter, liveOnly, 'webpage', 'Sitemaps', sitemapJobName(g.source));
            startedJobs.push({ id: job.id, name: job.name, source: g.source, bioUrls: g.bioUrls.length });
          },
        });
        console.log(`Sitemap run: started ${startedJobs.length} job(s) across ${out.totalGroups} sitemap(s), ${out.totalBioUrls} bio URL(s) from ${out.sitemapsFetched} fetched sitemap(s)`);
        // Keep the submitted sitemaps' bio-DEDICATED child sitemaps for new-hire monitoring. Runs in the
        // background (re-walks the sitemaps + applies the dedicated bio-ratio filter) so it never blocks
        // the run or risks the request timeout; the watches show up in the Monitor tab. Idempotent.
        const willMonitor = keepForMonitor && Array.isArray(inp.urls) && inp.urls.length > 0;
        if (willMonitor) {
          monitor.discoverWatches({ sitemaps: inp.urls })
            .then((r) => console.log(`Sitemap run: kept ${r.added} bio-dedicated child sitemap(s) for monitoring`))
            .catch((e) => console.error('Sitemap run: monitor registration failed:', e.message));
        }
        sendJson(res, {
          ok: true, jobs: startedJobs, sitemaps: out.totalGroups, totalBioUrls: out.totalBioUrls,
          totalUrls: out.totalUrls, sitemapsFetched: out.sitemapsFetched, sitemapsOk: out.sitemapsOk,
          monitoring: willMonitor,
        });
      } catch (e) {
        jsonErr(res, 500, e.message || 'Failed to process sitemaps.');
      }
    });
    return;
  }

  // POST /api/cc-discover  { domains: [...] } -> { ok, bioUrls, totalUrls, domainsScanned, perDomain }.
  // Auto-discovers bio/contact pages for bare domains straight from the Common Crawl index (one
  // CDX query per domain, no sitemap needed). The client then runs bioUrls as a normal 'webpage'
  // job (type 'CC Discovery'), which reads each page from CC via the bulk index cache.
  if (url.pathname === '/api/cc-discover' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, async (b) => {
      try {
        if (!b) return jsonErr(res, 400, 'Bad JSON body.');
        const domains = Array.isArray(b.domains)
          ? b.domains.map((d) => String(d || '').trim()).filter(Boolean)
          : String(b.domains || '').split(/[\r\n,]+/).map((s) => s.trim()).filter(Boolean);
        if (!domains.length) return jsonErr(res, 400, 'No domains provided.');
        if (domains.length > 500) return jsonErr(res, 400, 'Too many domains (max 500 per discovery).');
        // directoryRules stays {} on the server by design (see project notes); genderMap aids bio detection
        const out = await discoverBioUrlsFromCC({ domains, genderMap: GENDER_MAP });
        console.log(`CC discover: ${out.bioUrls.length} bio URL(s) from ${out.totalUrls} archived URL(s) across ${out.domainsScanned} domain(s)`);
        sendJson(res, { ok: true, ...out });
      } catch (e) {
        jsonErr(res, 500, e.message || 'Failed to discover from Common Crawl.');
      }
    });
    return;
  }

  // ---- Google Sheet -> Master DB sync (one-way import) ----
  if (url.pathname === '/api/sheet/status' && req.method === 'GET') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    sendJson(res, { ...sheetSync, configuredUrl: SHEET_SYNC_URL, intervalHours: SHEET_SYNC_HOURS });
    return;
  }
  if (url.pathname === '/api/sheet/import' && req.method === 'POST') {
    if (!isAdmin) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, (b) => {
      const u = (b && typeof b.url === 'string' && b.url.trim()) || SHEET_SYNC_URL;
      if (!u) return jsonErr(res, 400, 'No sheet URL — provide {url} or set SHEET_SYNC_URL.');
      if (sheetSync.running) return jsonErr(res, 409, 'A sheet import is already running.');
      runSheetSync(u);                                  // fire-and-forget; poll /api/sheet/status
      sendJson(res, { ok: true, started: true, url: u });
    });
    return;
  }

  // ---- Site Search (serper.dev -> bio URLs -> webpage job -> Master DB) ----
  if (url.pathname === '/api/site-search/status' && req.method === 'GET') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    sendJson(res, siteSearchStatus());
    return;
  }
  if (url.pathname === '/api/site-search/start' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    readJsonBody(req, (b) => {
      const input = (b && typeof b.url === 'string' && b.url.trim()) || '';
      if (!input) return jsonErr(res, 400, 'Enter a webpage or domain to search.');
      if (!SERPER_API_KEY) return jsonErr(res, 400, 'SERPER_API_KEY is not configured on the server.');
      if (siteSearchState.running) return jsonErr(res, 409, 'A site search is already running — only one at a time.');
      runSiteSearch(input);                             // fire-and-forget; poll /api/site-search/status
      sendJson(res, { ok: true, started: true, input });
    });
    return;
  }

  // ---- central database (SQLite, server-side paginated) ----
  if (url.pathname === '/api/db/stats' && req.method === 'GET') { sendJson(res, await reader.stats()); return; }
  if (url.pathname === '/api/db/facets' && req.method === 'GET') {
    if (Date.now() - facetsAt > FACETS_TTL_MS) refreshFacets();   // stale -> refresh in the background (don't block)
    sendJson(res, facetsCache);
    return;
  }
  if (url.pathname === '/api/db/query' && req.method === 'GET') {
    const q = url.searchParams;
    sendJson(res, await reader.query({
      page: q.get('page'), pageSize: q.get('pageSize'),
      search: q.get('search') || '', directory: q.get('directory') || '',
      emailType: q.get('emailType') || '', phoneType: q.get('phoneType') || '',
      gender: q.get('gender') || 'na', domain: q.get('domain') || '',
      domains: parseDomainsParam(q.get('domains')), position: q.get('position') || '', location: q.get('location') || '',
      type: q.get('type') || '',
      linkedin: q.get('linkedin') === '1', sort: q.get('sort') || '', dir: q.get('dir'),
    }));
    return;
  }
  if (url.pathname === '/api/db/export.csv' && req.method === 'GET') {
    const q = url.searchParams;
    const opts = {
      search: q.get('search') || '', directory: q.get('directory') || '',
      emailType: q.get('emailType') || '', phoneType: q.get('phoneType') || '',
      gender: q.get('gender') || 'na', domain: q.get('domain') || '',
      domains: parseDomainsParam(q.get('domains')), position: q.get('position') || '', location: q.get('location') || '',
      type: q.get('type') || '',
      linkedin: q.get('linkedin') === '1',
    };
    res.writeHead(200, {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="contacts-database.csv"`,
    });
    res.write(COLUMNS.join(',') + '\n');
    const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
    await reader.each(opts, (rec) => { res.write(COLUMNS.map((c) => esc(rec[c])).join(',') + '\n'); });
    res.end();
    return;
  }

  // POST /api/db/update  { edits: [{ email, updates }] }  -> apply manual edits to records
  if (url.pathname === '/api/db/update' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body || '{}');
        const edits = Array.isArray(payload.edits) ? payload.edits : [];
        const results = await Promise.all(edits.map(async (e) => {
          try { return { email: e.email, ...(await db.updateRecord(e.email, e.updates || {})) }; }
          catch (err) { return { email: e.email, ok: false, error: err.message || 'update failed' }; }
        }));
        // Write-through edits to OpenSearch so they reflect immediately (authoritative, bypasses the
        // score gate). On an email change, also delete the stale old-email doc.
        if (reader._os) {
          const docs = [], dels = [], nowIso = new Date().toISOString();
          for (const r of results) {
            if (!r.ok || !r.record) continue;
            docs.push(openSearch.recordToDoc(r.record, nowIso));
            const newEmail = String(r.record['Email Address'] || '').trim().toLowerCase();
            if (r.email && String(r.email).trim().toLowerCase() !== newEmail) dels.push(r.email);
          }
          try {
            if (docs.length) await reader.put(docs);
            if (dels.length) await reader.del(dels);
          } catch (e) { console.error('OpenSearch edit write-through failed:', e.message); }
        }
        sendJson(res, { results });
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: e.message || 'Bad request' }));
      }
    });
    return;
  }

  // POST /api/db/delete  { emails: [...] }  -> permanently delete records from the DB
  if (url.pathname === '/api/db/delete' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body || '{}');
        const emails = (Array.isArray(payload.emails) ? payload.emails : [])
          .map((e) => String(e || '').trim().toLowerCase()).filter(Boolean);
        if (!emails.length) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No records selected' }));
          return;
        }
        const before = await db.count();
        for (const e of emails) await db.deleteByEmail(e);
        // Write-through the deletes to OpenSearch (a delta scan can't see a removed row).
        if (reader._os) { try { await reader.del(emails); } catch (e) { console.error('OpenSearch delete write-through failed:', e.message); } }
        sendJson(res, { deleted: before - await db.count() });
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: e.message || 'Bad request' }));
      }
    });
    return;
  }

  // POST /api/db/ai-enrich  { emails: [...] }  -> Claude cleans/infers fields, auto-applies
  if (url.pathname === '/api/db/ai-enrich' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', async () => {
      try {
        if (!aiEnrich.isConfigured()) {
          res.writeHead(503, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'AI Search is not configured (ANTHROPIC_API_KEY is not set on the server).' }));
          return;
        }
        const payload = JSON.parse(body || '{}');
        const emails = (Array.isArray(payload.emails) ? payload.emails : [])
          .map((e) => String(e || '').trim().toLowerCase()).filter(Boolean);
        if (!emails.length) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No records selected' }));
          return;
        }
        // Load fresh records from the DB (don't trust client-sent field values).
        const records = (await Promise.all(emails.map((e) => db.getByEmail(e)))).filter(Boolean);
        const enriched = await aiEnrich.enrichMany(records, { concurrency: 4 });
        const results = [];
        for (let k = 0; k < records.length; k++) {
          const rec = records[k];
          const email = String(rec['Email Address'] || '').trim().toLowerCase();
          const en = enriched[k] || { ok: false, error: 'no result' };
          if (!en.ok) { results.push({ email, ok: false, error: en.error }); continue; }
          const fields = Object.keys(en.updates || {});
          if (!fields.length) { results.push({ email, ok: true, changed: 0, changes: {} }); continue; }
          try {
            const upd = await db.updateRecord(email, en.updates);   // auto-apply
            results.push({
              email, ok: upd.ok, changed: fields.length, changes: en.changes,
              newEmail: en.updates['Email Address'] || undefined,
            });
          } catch (err) {
            results.push({ email, ok: false, error: err.message || 'apply failed' });
          }
        }
        sendJson(res, { results });
      } catch (e) {
        console.error('ai-enrich error:', e);
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: e.message || 'AI Search failed' }));
        }
      }
    });
    return;
  }

  // routes that target a single job: /api/jobs/:id , /api/jobs/:id/records , /api/jobs/:id/results.csv , /api/jobs/:id/resume
  const jobMatch = url.pathname.match(/^\/api\/jobs\/([^/]+)(\/records|\/results\.csv|\/resume|\/stop|\/name)?$/);
  if (jobMatch) {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    const id = jobMatch[1];
    const sub = jobMatch[2] || '';
    const job = jobs.get(id);
    if (!job) {
      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Job not found' }));
      return;
    }

    if (sub === '' && req.method === 'GET') { sendJson(res, jobSummary(job)); return; }

    if (sub === '' && req.method === 'DELETE') { deleteJob(id); sendJson(res, { deleted: true, id }); return; }

    if (sub === '/records' && req.method === 'GET') { sendJson(res, jobRecords(job)); return; }

    if (sub === '/results.csv' && req.method === 'GET') {
      res.writeHead(200, {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': `attachment; filename="${id}.csv"`,
      });
      res.end(recordsToCsv(jobRecords(job)));
      return;
    }

    if (sub === '/resume' && req.method === 'POST') {
      const resumed = resumeJob(id);
      console.log(`Resume requested for job ${id} -> ${resumed.status}`);
      sendJson(res, jobSummary(resumed));
      return;
    }

    if (sub === '/stop' && req.method === 'POST') {
      if (job.status === 'running') { job.stopRequested = true; console.log(`Stop requested for job ${id}`); }
      sendJson(res, jobSummary(job));
      return;
    }

    if (sub === '/name' && req.method === 'POST') {
      readJsonBody(req, (b) => {
        if (!b) return jsonErr(res, 400, 'Bad request');
        job.name = String(b.name || '').trim().slice(0, 120);
        persistJob(job);
        sendJson(res, jobSummary(job));
      });
      return;
    }
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

loadJobs();
pruneOldJobs();
// Bring up the contacts backend (Postgres if CONTACTS_PG) BEFORE accepting traffic, so the first
// request reads the right store. Falls back to SQLite if the PG connection fails (never hard-down).
(async () => {
  if (CONTACTS_PG) {
    try {
      db = await makeContactsPg({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL });
      console.log('Contacts store: Postgres (CONTACTS_PG=1).');
    } catch (e) {
      console.error('Postgres contacts init FAILED — falling back to SQLite:', e.message);
      db = sqliteDb;
    }
  } else {
    console.log('Contacts store: SQLite.');
  }
  // Read backend: OpenSearch production store if flagged, else the same store we write to.
  reader = db;
  if (String(process.env.SEARCH_BACKEND || '').toLowerCase() === 'opensearch' && process.env.OPENSEARCH_ENDPOINT) {
    try {
      reader = makeOsReader(process.env.OPENSEARCH_ENDPOINT);
      console.log('Search backend: OpenSearch (SEARCH_BACKEND=opensearch).');
      // Keep OpenSearch current with the processing DB: stream fleet-ingested/edited contacts across.
      // Only meaningful when the source of truth is Postgres (DATABASE_URL); SQLite fallback has no fleet.
      if (process.env.DATABASE_URL && process.env.OS_SYNC !== '0') {
        osSync = require('./opensearch-sync').startSync({
          endpoint: process.env.OPENSEARCH_ENDPOINT,
          connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL,
        });
        console.log('OpenSearch delta sync: ON.');
      }
    } catch (e) {
      console.error('OpenSearch reader init FAILED — search falls back to the DB:', e.message);
      reader = db;
    }
  } else {
    console.log('Search backend: DB (contacts store).');
  }
  refreshFacets();                                      // warm the filter-facets cache in the background
  setInterval(() => refreshFacets(), FACETS_TTL_MS);    // keep it fresh
server.listen(PORT, async () => {
  console.log(`UI server running at http://localhost:${PORT}`);
  if(!DEMO_MODE) { try { await resolveLatestCrawl(); } catch (e) { console.error('Latest-crawl resolve failed:', e.message); } }  // always read the freshest CC corpus
  if(DEMO_MODE) {
    console.log('⚠️  DEMO MODE ENABLED - Using mock data. Set DEMO_MODE=false to connect to real Common Crawl.');
  } else {
    console.log('Using real Common Crawl API.');
  }
  if (AUTH_ENABLED) {
    const who = AUTH_USERS.size ? `${AUTH_USERS.size} user login(s)` : 'shared password';
    console.log(`🔒 Access control: ON (${who}).`);
  } else {
    console.log('⚠️  Access control: OFF — no APP_PASSWORD/AUTH_USERS set. Fine for localhost, NOT for hosting.');
  }
  console.log(`Data dir: ${DATA_DIR}`);
  if (SHEET_SYNC_URL) {
    console.log(`Sheet sync: ON for ${SHEET_SYNC_URL} every ${SHEET_SYNC_HOURS}h.`);
    setTimeout(() => runSheetSync(), 30000);                              // initial import shortly after startup
    setInterval(() => runSheetSync(), SHEET_SYNC_HOURS * 3600 * 1000);   // then on the interval
  }
  if (MONITOR_ENABLED) {
    console.log(`Sitemap monitor: ON, every ${MONITOR_INTERVAL_HOURS}h.`);
    setInterval(() => { runMonitorPassGuarded().catch((e) => console.error('[monitor] pass crashed:', e.message)); },
      MONITOR_INTERVAL_HOURS * 3600 * 1000);
  } else {
    console.log('Sitemap monitor: OFF (set MONITOR_ENABLED=1 to run the nightly new-hire pass).');
  }
  // One-off bulk relabel: set Position AND Title = POSITION_FIX_VALUE for records matching any of
  // POSITION_FIX_DOMAIN (exact source domain), POSITION_FIX_EMAIL_DOMAIN (email @domain), and/or
  // POSITION_FIX_PREFIX (Position starts-with). Idempotent. Unset the secrets after use.
  if (process.env.POSITION_FIX_VALUE && (process.env.POSITION_FIX_DOMAIN || process.env.POSITION_FIX_EMAIL_DOMAIN || process.env.POSITION_FIX_PREFIX)) {
    try {
      const match = { domain: process.env.POSITION_FIX_DOMAIN || '', emailDomain: process.env.POSITION_FIX_EMAIL_DOMAIN || '', prefix: process.env.POSITION_FIX_PREFIX || '' };
      const r = sqliteDb.bulkSetPosition(match, process.env.POSITION_FIX_VALUE);
      console.log(`Position/Title fix: ${r.matched} record(s) matching ${JSON.stringify(match)} -> Position+Title "${process.env.POSITION_FIX_VALUE}".`);
      console.log(`  source domains: ${JSON.stringify(r.domains)}`);
      console.log(`  was: ${JSON.stringify(r.samples)}`);
    } catch (e) { console.error('Position/Title fix failed:', e.message); }
  }

  // Maintenance chain: (1) one-off Angola->UK correction + re-geocode of the corrected numbers
  // (gated by ANGOLA_MAINT=1), then (2) resume any interrupted jobs (always — survives restarts).
  (async () => {
    if (process.env.ANGOLA_MAINT === '1') {
      try {
        const affected = sqliteDb.fixAngola();
        console.log(`Angola fix: ${affected.length} record(s) -> +44 / United Kingdom.`);
        const keep = (l) => (l && !/angola/i.test(l)) ? l : '';        // never re-write an Angola value
        const items = [];
        for (const a of affected) {
          const loc1 = keep(a.phone ? await geocodePhone(a.phone) : '');
          const loc2 = keep(a.phone_2 ? await geocodePhone(a.phone_2) : '');
          if (loc1 || loc2) items.push({ email: a.email, loc1, loc2 });
        }
        const wrote = sqliteDb.backfillLocations(items);
        console.log(`Angola fix: re-geocoded ${affected.length} corrected number set(s); refined ${wrote} location field(s).`);
      } catch (e) { console.error('Angola maint failed:', e.message); }
    }

    // Idempotent deletion of ALL century21.com records, gated by DELETE_C21_ONCE=<token>. A marker
    // file stores the last-applied token, so the deletion runs once PER token value — bump the token
    // (e.g. '1' -> '2') to re-run after a fresh crawl. The removed rows are backed up to DATA_DIR
    // first, and the field coverage (how many had email/phone) is logged so the crawl quality shows.
    const c21Token = String(process.env.DELETE_C21_ONCE || '').trim();
    if (c21Token) {
      try {
        const marker = path.join(DATA_DIR, '.century21-deleted.token');
        const applied = fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8').trim() : '';
        if (applied === c21Token) {
          console.log(`Century21 deletion: token "${c21Token}" already applied — skipping.`);
        } else {
          const out = sqliteDb.deleteByDomain('century21.com');
          const withEmail = out.rows.filter((r) => String(r['Email Address'] || '').trim()).length;
          const withPhone = out.rows.filter((r) => String(r['Phone'] || '').trim()).length;
          if (out.rows.length) {
            const bak = path.join(DATA_DIR, `century21-deleted-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
            fs.writeFileSync(bak, JSON.stringify(out.rows));
            console.log(`Century21 deletion: backed up ${out.rows.length} row(s) -> ${bak}`);
          }
          fs.writeFileSync(marker, c21Token);
          console.log(`Century21 deletion (token "${c21Token}"): removed ${out.deleted}; deleted set had email=${withEmail}, phone=${withPhone}; central DB total now ${sqliteDb.count()}.`);
        }
      } catch (e) { console.error('Century21 deletion failed:', e.message); }
    }

    // Idempotent junk-clean: delete century21 records that did NOT come from the Site API (the early
    // "via webpage" scrape junk with no email/phone), keeping the clean adapter records. Token-gated.
    const c21JunkToken = String(process.env.CLEAN_C21_JUNK || '').trim();
    if (c21JunkToken) {
      try {
        const marker = path.join(DATA_DIR, '.century21-junk.token');
        const applied = fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8').trim() : '';
        if (applied === c21JunkToken) {
          console.log(`Century21 junk-clean: token "${c21JunkToken}" already applied — skipping.`);
        } else {
          const out = sqliteDb.deleteByDomain('century21.com', { exceptSource: 'Site API' });
          fs.writeFileSync(marker, c21JunkToken);
          console.log(`Century21 junk-clean (token "${c21JunkToken}"): removed ${out.deleted} non-Site-API record(s); central DB total now ${sqliteDb.count()}.`);
          const cs = sqliteDb.domainStats('century21.com');
          const rs = sqliteDb.domainStats('remax.com');
          console.log(`Coverage — century21: ${cs.total} records (${cs.withEmail} email, ${cs.withPhone} phone); remax: ${rs.total} records (${rs.withEmail} email, ${rs.withPhone} phone).`);
        }
      } catch (e) { console.error('Century21 junk-clean failed:', e.message); }
    }

    // Idempotent one-off: start the remax agent crawl (fetch the sitemap server-side, extract all bio
    // URLs, run them through the remax adapter). Token-gated so it starts once per token value.
    const remaxToken = String(process.env.PROCESS_REMAX || '').trim();
    if (remaxToken) {
      try {
        const marker = path.join(DATA_DIR, '.remax-started.token');
        const applied = fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8').trim() : '';
        if (applied === remaxToken) {
          console.log(`Remax: token "${remaxToken}" already started — skipping.`);
        } else {
          const sitemap = process.env.REMAX_SITEMAP || 'https://www.remax.com/AgentSitemaps/agents-2026_Q2-1.xml';
          const started = [];
          const out = await extractBioUrlGroups({
            urls: [sitemap], genderMap: GENDER_MAP, candidateCap: 100000, maxUrls: 200000,
            onGroup: (g) => { const job = startJob(g.bioUrls, '', false, 'webpage', 'Sitemaps', sitemapJobName(g.source)); started.push(job.id); },
          });
          fs.writeFileSync(marker, remaxToken);
          console.log(`Remax: started ${started.length} job(s), ${out.totalBioUrls} bio URL(s) from ${out.sitemapsFetched} fetched sitemap(s).`);
        }
      } catch (e) { console.error('Remax start failed:', e.message); }
    }

    // Idempotent one-off: run a set of sitemap crawls. `skipExisting` makes it a GAP-FILL (only
    // pages not already captured for that domain) — used to recover remax agents that failed the
    // first pass without re-fetching the ones we have. century21 runs the full agent-detail index.
    const runCrawlsToken = String(process.env.RUN_CRAWLS || '').trim();
    if (runCrawlsToken) {
      try {
        const marker = path.join(DATA_DIR, '.run-crawls.token');
        const applied = fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8').trim() : '';
        if (applied === runCrawlsToken) {
          console.log(`Crawls: token "${runCrawlsToken}" already started — skipping.`);
        } else {
          const targets = [
            { name: 'remax-gapfill', sitemap: 'https://www.remax.com/AgentSitemaps/agents-2026_Q2-1.xml', domain: 'remax.com', skipExisting: true, cap: 100000, max: 200000 },
            { name: 'century21-full', sitemap: 'https://www.century21.com/xml-sitemaps/sitemapindex-agents-detail.xml', domain: 'century21.com', skipExisting: false, cap: 300000, max: 600000 },
          ];
          for (const t of targets) {
            const existing = t.skipExisting ? sqliteDb.existingUrls(t.domain) : null;
            let started = 0, kept = 0, skipped = 0;
            const out = await extractBioUrlGroups({
              urls: [t.sitemap], genderMap: GENDER_MAP, candidateCap: t.cap, maxUrls: t.max,
              onGroup: (g) => {
                let urls = g.bioUrls;
                if (existing) { const before = urls.length; urls = urls.filter((u) => !existing.has(String(u).split('?')[0])); skipped += before - urls.length; }
                if (urls.length) { startJob(urls, '', false, 'webpage', 'Sitemaps', sitemapJobName(g.source)); started++; kept += urls.length; }
              },
            });
            console.log(`Crawls[${t.name}]: started ${started} job(s), ${kept} URL(s)${existing ? ` (skipped ${skipped} already-captured)` : ''}, ${out.sitemapsFetched} sitemap(s) fetched.`);
          }
          fs.writeFileSync(marker, runCrawlsToken);
        }
      } catch (e) { console.error('Crawls start failed:', e.message); }
    }

    // One-time fix: correct remax records whose Location is RE/MAX HQ ("Denver, CO") by re-deriving
    // the agent's City/State from the bio-URL slug. Token-gated + marker so it runs once per value.
    const remaxLocToken = String(process.env.FIX_REMAX_LOC || '').trim();
    if (remaxLocToken) {
      try {
        const marker = path.join(DATA_DIR, '.remax-locfix.token');
        const applied = fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8').trim() : '';
        if (applied === remaxLocToken) {
          console.log(`Remax loc-fix: token "${remaxLocToken}" already applied — skipping.`);
        } else {
          const { remaxLocationFromUrl } = require('./site-apis');
          const r = sqliteDb.fixRemaxLocations(remaxLocationFromUrl);
          fs.writeFileSync(marker, remaxLocToken);
          console.log(`Remax loc-fix (token "${remaxLocToken}"): scanned ${r.scanned} 'Denver, CO' record(s), corrected ${r.fixed}, unparsed ${r.unparsed}.`);
          if (r.samples && r.samples.length) for (const s of r.samples) console.log(`  loc-fix unparsed: ${s}`);
          // also persist the report so it survives the crawl log flood
          try { fs.writeFileSync(path.join(DATA_DIR, 'remax-locfix-report.json'), JSON.stringify({ token: remaxLocToken, ...r }, null, 2)); } catch (e) { /* ignore */ }
        }
      } catch (e) { console.error('Remax loc-fix failed:', e.message); }
    }

    // resume interrupted jobs (a job that was running when the server restarted) so long crawls finish
    try {
      let resumed = 0;
      for (const job of jobs.values()) {
        if (job.status === 'interrupted') {
          const remaining = job.domains.filter((d) => !job.doneDomains.includes(d)).length;
          resumeJob(job.id);
          resumed++;
          console.log(`Resumed interrupted job ${job.id}: ${remaining}/${job.domains.length} domain(s) remaining.`);
        }
      }
      if (resumed) console.log(`Auto-resumed ${resumed} interrupted job(s).`);
    } catch (e) { console.error('Resume interrupted jobs failed:', e.message); }
  })();
});
})();   // end contacts-backend bootstrap

