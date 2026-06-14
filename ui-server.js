const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'ui');
const RESULTS_CSV = path.join(__dirname, 'cc-results.csv');
const { runDomains, COLUMNS } = require('./cc-engine');
const { loadGenderMap, loadEmailBlocklist, analyzePhones, geocodeRecords } = require('./extractor');
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

// central, de-duplicated contacts database (every finished job merges into it)
const { makeDb } = require('./db');
const db = makeDb(DATA_DIR);
const aiEnrich = require('./ai-enrich');

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
  const before = db.count();
  for (const e of blocklist) db.deleteByEmail(e);
  const removed = before - db.count();
  if (removed) console.log(`Email blocklist: removed ${removed} existing contact(s).`);
} catch (e) { /* ignore */ }

// One-time background backfill: fill missing Phone Location / Phone 2 Location for existing
// records (toll-free numbers, non-E.164 phones, and the newer Phone 2 Location field). Runs
// async so it never blocks server startup.
(async () => {
  try {
    const need = [];
    db.each({}, (rec) => {
      if (!rec['Phone Location'] || (rec['Phone 2'] && !rec['Phone 2 Location'])) need.push(rec);
    });
    if (!need.length) return;
    await geocodeRecords(need);
    const items = need
      .map((r) => ({ email: r['Email Address'], loc1: r['Phone Location'], loc2: r['Phone 2 Location'] }))
      .filter((x) => x.loc1 || x.loc2);
    const filled = db.backfillLocations(items);
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
    total: job.domains.length,
    done: job.doneDomains.length,
    recordCount: job.recordsByEmail.size,
    coverage: job.coverage,
    directoryFilter: job.directoryFilter || '',
    liveOnly: !!job.liveOnly,
    mode: job.mode || 'domain',
    error: job.error || null,
    lastProgress: job.lastProgress || null,
  };
}

function persistJob(job) {
  if (job.deleted) return;                 // don't resurrect a job that was deleted
  const out = {
    id: job.id,
    createdAt: job.createdAt,
    finishedAt: job.finishedAt || null,
    status: job.status,
    domains: job.domains,
    doneDomains: job.doneDomains,
    coverage: job.coverage,
    directoryFilter: job.directoryFilter || '',
    liveOnly: !!job.liveOnly,
    mode: job.mode || 'domain',
    error: job.error || null,
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
        domains: j.domains || [], doneDomains: j.doneDomains || [],
        coverage: j.coverage || { found: 0, live: 0, empty: 0, errored: 0 },
        directoryFilter: j.directoryFilter || '', liveOnly: !!j.liveOnly, mode: j.mode || 'domain',
        error: j.error || null, recordsByEmail, lastProgress: null,
      });
    } catch (e) { console.error(`Failed to load job file ${f}:`, e.message); }
  }
  console.log(`Loaded ${jobs.size} saved job(s) from ${JOBS_DIR}`);
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
          persistJob(job);
        }
      },
    });
    job.status = job.stopRequested ? 'stopped' : 'completed';
  } catch (e) {
    job.status = 'failed';
    job.error = e.message;
  }
  job.stopRequested = false;
  try { await geocodeRecords([...job.recordsByEmail.values()]); }   // City, Region, Country
  catch (e) { console.error('geocode failed:', e.message); }
  job.finishedAt = new Date().toISOString();
  persistJob(job);
  // merge this job's fully-processed records into the central database
  try {
    const merged = db.upsertMany(jobRecords(job));
    console.log(`Central DB: merged ${merged.processed} record(s), +${merged.added} new (total ${merged.total}).`);
  } catch (e) { console.error('Central DB merge failed:', e.message); }
  console.log(`Job ${job.id} ${job.status} — ${job.recordsByEmail.size} record(s)`);
}

function startJob(domains, directoryFilter, liveOnly, mode) {
  const job = {
    id: newJobId(),
    createdAt: new Date().toISOString(),
    finishedAt: null,
    status: 'running',
    domains,
    doneDomains: [],
    coverage: { found: 0, live: 0, empty: 0, errored: 0 },
    directoryFilter: directoryFilter || '',
    liveOnly: !!liveOnly,
    mode: mode === 'webpage' ? 'webpage' : 'domain',
    stopRequested: false,
    error: null,
    recordsByEmail: new Map(),
    lastProgress: null,
  };
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
    if (action === 'activate') users.setActive(id, true);
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

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  console.log(`[${new Date().toISOString()}] ${req.method} ${url.pathname}`);

  // ---------------- authentication + role gate ----------------
  const me = currentUser(req);
  const p = url.pathname;
  const PUBLIC_PATH = (
    p.startsWith('/ui/') || p === '/favicon.ico' ||
    p === '/home' ||
    p === '/login' || p === '/signup' || p === '/forgot' || p === '/privacy' || p === '/terms' ||
    p === '/api/auth/login' || p === '/api/auth/signup' || p === '/api/auth/logout' || p === '/api/auth/me' ||
    (p.startsWith('/api/pages/') && req.method === 'GET')
  );
  if (!PUBLIC_PATH && !me) {
    if (p.startsWith('/api/')) jsonErr(res, 401, 'Authentication required');
    else if (p === '/' || p === '/index.html') { serveStaticFile(res, path.join(PUBLIC_DIR, 'home.html')); return; }  // anon root -> public landing
    else { res.writeHead(302, { Location: '/login' }); res.end(); }
    return;
  }
  const rank = me ? users.roleRank(me.role) : -1;
  const isAnalyst = rank >= RANK_ANALYST;
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
      sendJson(res, { ok: true });
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
        if (domains.length === 0) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No domains provided' }));
          return;
        }
        const job = startJob(domains, directoryFilter, liveOnly, mode);
        console.log(`Started job ${job.id} for ${domains.length} domain(s)`);
        sendJson(res, jobSummary(job));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: e.message || 'Bad request' }));
      }
    });
    return;
  }

  // ---- central database (SQLite, server-side paginated) ----
  if (url.pathname === '/api/db/stats' && req.method === 'GET') { sendJson(res, db.stats()); return; }
  if (url.pathname === '/api/db/facets' && req.method === 'GET') { sendJson(res, db.facets()); return; }
  if (url.pathname === '/api/db/query' && req.method === 'GET') {
    const q = url.searchParams;
    sendJson(res, db.query({
      page: q.get('page'), pageSize: q.get('pageSize'),
      search: q.get('search') || '', directory: q.get('directory') || '',
      emailType: q.get('emailType') || '', phoneType: q.get('phoneType') || '',
      gender: q.get('gender') || 'na', domain: q.get('domain') || '',
      domains: parseDomainsParam(q.get('domains')), position: q.get('position') || '',
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
      domains: parseDomainsParam(q.get('domains')), position: q.get('position') || '',
      type: q.get('type') || '',
      linkedin: q.get('linkedin') === '1',
    };
    res.writeHead(200, {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="contacts-database.csv"`,
    });
    res.write(COLUMNS.join(',') + '\n');
    const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
    db.each(opts, (rec) => { res.write(COLUMNS.map((c) => esc(rec[c])).join(',') + '\n'); });
    res.end();
    return;
  }

  // POST /api/db/update  { edits: [{ email, updates }] }  -> apply manual edits to records
  if (url.pathname === '/api/db/update' && req.method === 'POST') {
    if (!isAnalyst) { jsonErr(res, 403, 'Forbidden'); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      try {
        const payload = JSON.parse(body || '{}');
        const edits = Array.isArray(payload.edits) ? payload.edits : [];
        const results = edits.map((e) => {
          try { return { email: e.email, ...db.updateRecord(e.email, e.updates || {}) }; }
          catch (err) { return { email: e.email, ok: false, error: err.message || 'update failed' }; }
        });
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
    req.on('end', () => {
      try {
        const payload = JSON.parse(body || '{}');
        const emails = (Array.isArray(payload.emails) ? payload.emails : [])
          .map((e) => String(e || '').trim().toLowerCase()).filter(Boolean);
        if (!emails.length) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No records selected' }));
          return;
        }
        const before = db.count();
        for (const e of emails) db.deleteByEmail(e);
        sendJson(res, { deleted: before - db.count() });
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
        const records = emails.map((e) => db.getByEmail(e)).filter(Boolean);
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
            const upd = db.updateRecord(email, en.updates);   // auto-apply
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
  const jobMatch = url.pathname.match(/^\/api\/jobs\/([^/]+)(\/records|\/results\.csv|\/resume|\/stop)?$/);
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
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

loadJobs();
server.listen(PORT, () => {
  console.log(`UI server running at http://localhost:${PORT}`);
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
});

