/**
 * universe-scheduler.js — Phase 3: fire the whole-universe CC refresh once per new Common Crawl release.
 * ---------------------------------------------------------------------------------------------------
 * Run this on a cron (e.g. daily). It checks the latest CC crawl against a stored watermark; when a NEW
 * crawl has dropped it runs the full Lambda-scale two-hop routine and advances the watermark:
 *
 *   HOP 1  dump 23.5M domains -> waterfall-resolve homes across the newest N crawls -> cc-enrich Lambda
 *          fleet (enrich-drive) -> load-enriched (companies index + bio URLs)
 *   HOP 2  waterfall-resolve the bio URLs -> cc-extract Lambda fleet (lambda-drive) -> load-extracted
 *          (contacts) + live-crawl fallback for the bio pages not in the archive
 *
 * Env: OPENSEARCH_ENDPOINT, OUT_BUCKET, plus AWS creds. Options:
 *   --force        run even if the latest crawl == the watermark
 *   --dry-run      report what it would do, don't run
 *   --depth N      waterfall crawl-stack depth (default 6)
 *   --cap N        max companies to dump (default 25,000,000 = whole universe)
 *   --no-live      skip the Hop-2 live-crawl fallback
 *   --state FILE   watermark file (default ./.universe-state.json)
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const { spawnSync } = require('child_process');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes(f);

const STATE_FILE = arg('--state', path.join(__dirname, '.universe-state.json'));
const DEPTH = Number(arg('--depth', '6'));
const CAP = arg('--cap', '25000000');
const FORCE = has('--force');
const DRY = has('--dry-run');
const LIVE = !has('--no-live');
const SCRATCH = process.env.UNIVERSE_SCRATCH || path.join(__dirname, '_universe');
const OUT_BUCKET = process.env.OUT_BUCKET || 'aws-athena-query-results-475987770186-us-east-1';

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
fs.mkdirSync(SCRATCH, { recursive: true });

function getJSON(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { timeout: 20000 }, (res) => { let b = ''; res.on('data', (d) => b += d); res.on('end', () => { try { resolve(JSON.parse(b)); } catch (e) { reject(e); } }); })
      .on('error', reject).on('timeout', function () { this.destroy(new Error('timeout')); });
  });
}
const readState = () => { try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch { return { lastCrawl: '', history: [] }; } };
const writeState = (s) => fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2));

function step(name, args, env) {
  console.error(`\n══════ ${name} ${new Date().toISOString().slice(11, 19)} ══════`);
  const r = spawnSync(process.execPath, args, { stdio: 'inherit', cwd: __dirname, env: { ...process.env, ...(env || {}) } });
  if (r.status !== 0) throw new Error(`${name} exited ${r.status}`);
}

(async () => {
  const crawls = await getJSON('https://index.commoncrawl.org/collinfo.json');   // newest first
  const ids = (crawls || []).map((c) => c.id).filter((id) => /^CC-MAIN-\d{4}-\d+$/.test(id));
  if (!ids.length) throw new Error('could not read CC crawl list');
  const latest = ids[0];
  const stack = ids.slice(0, DEPTH).join(',');
  const state = readState();

  console.error(`latest CC crawl: ${latest} | watermark: ${state.lastCrawl || '(none)'} | waterfall depth ${DEPTH}`);
  if (latest === state.lastCrawl && !FORCE) { console.error('Up to date — no new crawl. Nothing to do.'); return; }
  if (DRY) { console.error(`[dry-run] would refresh the universe against stack: ${stack}`); return; }

  const RUN = latest.replace(/[^A-Za-z0-9]/g, '').slice(-8);   // e.g. 202625
  const OS = process.env.OPENSEARCH_ENDPOINT;
  const F = (s) => path.join(SCRATCH, `u-${s}`);
  const started = new Date().toISOString(); const t0 = Date.now();

  // ---- HOP 1: dump -> waterfall-resolve homes -> Lambda enrich -> load ----
  step('dump universe domains', ['dump-company-urls.js', '{}', CAP, F('urls.txt'), F('targets.ndjson')], { OPENSEARCH_ENDPOINT: OS });
  step('resolve homes (waterfall)', ['cc-athena-miner.js', '--resolve-urls', F('urls.txt'), '--warc-out', F('homeptr.jsonl'), '--crawls', stack, '--resolve-tag', `u${RUN}h`]);
  step('enrich fleet (cc-enrich)', ['enrich-drive.js', F('homeptr.jsonl'), F('targets.ndjson')], { RUN, CRAWL: latest, OUT_BUCKET });
  step('load enriched -> companies', ['load-enriched.js', `cc-enriched/${RUN}/`], { OPENSEARCH_ENDPOINT: OS, OUT_BUCKET, BIO_OUT: F('bio-urls.txt') });

  // ---- HOP 2: waterfall-resolve bio URLs -> Lambda extract -> load (+ live fallback) ----
  if (fs.existsSync(F('bio-urls.txt')) && fs.statSync(F('bio-urls.txt')).size > 2) {
    step('resolve bio URLs (waterfall)', ['cc-athena-miner.js', '--resolve-urls', F('bio-urls.txt'), '--warc-out', F('bioptr.jsonl'), '--crawls', stack, '--resolve-tag', `u${RUN}b`]);
    step('extract fleet (cc-extract)', ['lambda-drive.js', F('bioptr.jsonl')], { RUN: `${RUN}b`, OUT_BUCKET });
    step('load contacts -> Master DB', ['load-extracted.js', `cc-extracted/${RUN}b/`], { OPENSEARCH_ENDPOINT: OS, OUT_BUCKET });
    if (LIVE) step('live-crawl bio misses', ['extract-from-pointers.js', '--ptr', F('bioptr.jsonl'), '--live', F('bio-urls.txt'), '--tag', `u${RUN}`], { OPENSEARCH_ENDPOINT: OS });
  }

  const secs = Math.round((Date.now() - t0) / 1000);
  state.lastCrawl = latest;
  state.history = (state.history || []).concat([{ crawl: latest, started, secs, stack }]).slice(-24);
  writeState(state);
  console.error(`\n══════ UNIVERSE REFRESH COMPLETE — ${latest} in ${secs}s. Watermark advanced. ══════`);
})().catch((e) => { console.error('scheduler error:', e && e.stack || e); process.exit(1); });
