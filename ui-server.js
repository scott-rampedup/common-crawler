const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'ui');
const RESULTS_CSV = path.join(__dirname, 'cc-results.csv');
const { runDomains, COLUMNS } = require('./cc-engine');
const DEMO_MODE = process.env.DEMO_MODE === 'true';

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

function jobRecords(job) {
  return [...job.recordsByEmail.values()];
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
    error: job.error || null,
    lastProgress: job.lastProgress || null,
  };
}

function persistJob(job) {
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
    error: job.error || null,
    records: jobRecords(job),
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
        directoryFilter: j.directoryFilter || '', liveOnly: !!j.liveOnly, error: j.error || null,
        recordsByEmail, lastProgress: null,
      });
    } catch (e) { console.error(`Failed to load job file ${f}:`, e.message); }
  }
  console.log(`Loaded ${jobs.size} saved job(s) from ${JOBS_DIR}`);
}

// run a set of domains for a job, accumulating records + coverage, persisting per domain
async function runJobDomains(job, domainsToRun) {
  job.status = 'running';
  job.error = null;
  persistJob(job);
  try {
    await runDomains(domainsToRun, {
      demoMode: DEMO_MODE,
      directoryFilter: job.directoryFilter,
      liveOnly: !!job.liveOnly,                                // skip Common Crawl when requested
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
          if (p.status === 'domain-done' && p.source === 'Live Crawl') job.coverage.live += 1;
          else if (p.status === 'domain-done') job.coverage.found += 1;
          else job.coverage.empty += 1;
          persistJob(job);
        }
      },
    });
    job.status = 'completed';
  } catch (e) {
    job.status = 'failed';
    job.error = e.message;
  }
  job.finishedAt = new Date().toISOString();
  persistJob(job);
  console.log(`Job ${job.id} ${job.status} — ${job.recordsByEmail.size} record(s)`);
}

function startJob(domains, directoryFilter, liveOnly) {
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

function contentType(filePath) {
  if (filePath.endsWith('.html')) return 'text/html; charset=utf-8';
  if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
  if (filePath.endsWith('.js')) return 'application/javascript; charset=utf-8';
  if (filePath.endsWith('.json')) return 'application/json; charset=utf-8';
  if (filePath.endsWith('.svg')) return 'image/svg+xml';
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

function sendJson(res, data) {
  const payload = JSON.stringify(data);
  res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(payload);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  console.log(`[${new Date().toISOString()}] ${req.method} ${url.pathname}`);

  if (!checkAuth(req, res)) return;   // gate everything behind the password when configured

  if (url.pathname === '/' || url.pathname === '/index.html') {
    serveStaticFile(res, path.join(PUBLIC_DIR, 'index.html'));
    return;
  }

  if (url.pathname.startsWith('/ui/')) {
    const filePath = path.join(PUBLIC_DIR, url.pathname.replace(/^\/ui\//, ''));
    serveStaticFile(res, filePath);
    return;
  }

  if (url.pathname === '/api/results') {
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
    const list = [...jobs.values()].map(jobSummary)
      .sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
    sendJson(res, list);
    return;
  }

  // POST /api/jobs  { domains: [...], directoryFilter? }  -> start a job
  if (url.pathname === '/api/jobs' && req.method === 'POST') {
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = JSON.parse(body || '{}');
        const domains = Array.isArray(payload.domains) ? payload.domains.filter(Boolean) : [];
        const directoryFilter = typeof payload.directoryFilter === 'string' ? payload.directoryFilter.trim() : '';
        const liveOnly = payload.liveOnly === true;
        if (domains.length === 0) {
          res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'No domains provided' }));
          return;
        }
        const job = startJob(domains, directoryFilter, liveOnly);
        console.log(`Started job ${job.id} for ${domains.length} domain(s)`);
        sendJson(res, jobSummary(job));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: e.message || 'Bad request' }));
      }
    });
    return;
  }

  // routes that target a single job: /api/jobs/:id , /api/jobs/:id/records , /api/jobs/:id/results.csv , /api/jobs/:id/resume
  const jobMatch = url.pathname.match(/^\/api\/jobs\/([^/]+)(\/records|\/results\.csv|\/resume)?$/);
  if (jobMatch) {
    const id = jobMatch[1];
    const sub = jobMatch[2] || '';
    const job = jobs.get(id);
    if (!job) {
      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Job not found' }));
      return;
    }

    if (sub === '' && req.method === 'GET') { sendJson(res, jobSummary(job)); return; }

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

