/**
 * bio-pump.js — keep the bio-URL backlog moving, always. No tick, no button, no interval to wait for.
 * ----------------------------------------------------------------------------------------------------
 * The pipeline kept reintroducing the same shape: a timer that decides when work is allowed to start.
 * First a 6-hour drain, then a 15-minute feed and an hourly live crawl. Each was an improvement in
 * latency and none of them removed the actual problem -- a URL that arrived one second after a tick sat
 * there doing nothing until the next one, for no reason except that a clock said so.
 *
 * This replaces the clock with a condition: work moves whenever work exists and whatever moves it is
 * free. The loop never exits.
 *
 *   while (true):
 *     feed any newly queued URLs            -> resolve -> crawl_queue  (the workers claim continuously)
 *     if misses exist and no fleet is busy  -> launch the live crawl for them
 *     if anything happened, go straight round again; only idle when there is genuinely nothing to do
 *
 * The only pause is when the queue is empty, and even that is seconds, not hours. Nothing waits on a
 * schedule; a batch starts the moment the previous one frees the thing it needed.
 *
 *   node bio-pump.js            # run forever (this is how it is meant to run)
 *   node bio-pump.js --once     # a single pass, for testing
 *
 * Env: OPENSEARCH_ENDPOINT, DATABASE_URL, FLY_API_TOKEN, AWS creds.
 *      BIO_PUMP_IDLE_MS   (default 20000) pause only when there was nothing to do
 *      BIO_PUMP_MIN_LIVE  (default 1)     misses needed before a fleet is worth launching
 *
 * Each pass runs in a CHILD PROCESS. A pass that dies -- OOM on a huge resolve, an Athena fault, a bad
 * S3 response -- takes the child with it and the pump keeps going. The alternative, running the work
 * in-process, means one bad batch ends the thing whose entire job is not to stop.
 */
const path = require('path');
const { spawn } = require('child_process');

const has = (f) => process.argv.includes('--' + f);
const ONCE = has('once');
const IDLE_MS = Number(process.env.BIO_PUMP_IDLE_MS) || 20000;
const MIN_LIVE = Number(process.env.BIO_PUMP_MIN_LIVE) || 1;

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('bio-pump: OPENSEARCH_ENDPOINT required'); process.exit(1); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ts = () => new Date().toISOString().slice(11, 19);

// Run a script to completion, capturing stderr so a pass can report what it did without the pump
// having to parse a stream. Resolves even on failure -- a failed pass must not end the loop.
function runPass(script, args) {
  return new Promise((resolve) => {
    const out = [];
    const p = spawn(process.execPath, [path.join(__dirname, script), ...args], {
      cwd: __dirname, env: process.env, stdio: ['ignore', 'pipe', 'pipe'],
    });
    p.stdout.on('data', (d) => out.push(String(d)));
    p.stderr.on('data', (d) => out.push(String(d)));
    p.on('error', (e) => resolve({ code: -1, text: 'spawn failed: ' + e.message }));
    p.on('close', (code) => resolve({ code, text: out.join('') }));
  });
}

// Did this pass actually move anything? Used to decide whether to go straight round again.
function feedDidWork(text) {
  const m = /([\d,]+)\s+queued for the worker fleet/.exec(text);
  if (m) return Number(m[1].replace(/,/g, '')) > 0;
  return !/nothing new/i.test(text) && /crawl_queue:/.test(text);
}
function liveDidWork(text) { return /shard\(s\) crawling/.test(text); }

// Is a live fleet still working? The fleet is the one genuinely scarce resource here, so the next batch
// of misses waits for it to finish -- not for a clock.
async function fleetBusy() {
  if (!process.env.FLY_API_TOKEN) return false;
  try {
    const { fleetStatus } = require('./fleet-launch');
    const st = await fleetStatus({ namePrefix: 'live-fleet' });
    return !!(st && st.ok && st.running > 0);
  } catch (e) { return false; }           // can't tell -> don't block the pipeline on a status call
}

let stopping = false;
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => { console.log(`bio-pump: ${sig} — finishing the current pass then stopping`); stopping = true; });
}

(async () => {
  console.log(`bio-pump: running. idle pause ${Math.round(IDLE_MS / 1000)}s, live threshold ${MIN_LIVE}`);
  let passes = 0, fedTotal = 0, liveTotal = 0;
  for (;;) {
    let didSomething = false;
    passes++;

    // ---- 1. feed: newly queued URLs -> resolved -> crawl_queue
    const feed = await runPass('bio-feed.js', []);
    if (feed.code !== 0) {
      console.error(`${ts()} feed pass FAILED (exit ${feed.code}) — retrying next pass`);
      console.error(feed.text.split('\n').filter(Boolean).slice(-3).map((l) => '    ' + l).join('\n'));
    } else if (feedDidWork(feed.text)) {
      didSomething = true; fedTotal++;
      for (const l of feed.text.split('\n')) if (/queued for the worker fleet|not in the archive|new row/.test(l)) console.log(`${ts()} feed  ${l.trim()}`);
    }

    // ---- 1b. recover error rows. A worker killed mid-batch, or a transient Postgres deadlock, parks
    // rows in 'error' and nothing ever looked at them again -- that is precisely a zombie. Hand them
    // back every pass; rows that have genuinely been tried to exhaustion are deleted, not left sitting.
    try {
      const { Pool } = require('pg');
      if (!global.__pumpPool) {
        global.__pumpPool = new Pool({ connectionString: process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/'), max: 2 });
        global.__pumpPool.on('error', (e) => console.error('[pump pg] idle client error:', e.message));
      }
      const { makeQueue } = require('./crawl-queue');
      const qh = await makeQueue(global.__pumpPool);
      const r = await qh.requeueErrors({});
      if (r.requeued || r.deleted) {
        didSomething = didSomething || r.requeued > 0;
        console.log(`${ts()} recov ${r.requeued.toLocaleString()} error row(s) back to pending`
          + (r.deleted ? `, ${r.deleted.toLocaleString()} deleted (tried to exhaustion)` : ''));
      }
    } catch (e) { console.error(`${ts()} recov failed: ${e.message}`); }

    // ---- 2. live crawl: whatever the archive lacked, as soon as the fleet is free
    if (!(await fleetBusy())) {
      const live = await runPass('bio-live.js', ['--min', String(MIN_LIVE)]);
      if (live.code !== 0) {
        console.error(`${ts()} live pass FAILED (exit ${live.code})`);
      } else if (liveDidWork(live.text)) {
        didSomething = true; liveTotal++;
        for (const l of live.text.split('\n')) if (/shard\(s\) crawling/.test(l)) console.log(`${ts()} live  ${l.trim()}`);
      }
    }

    if (ONCE || stopping) { console.log(`bio-pump: stopping after ${passes} pass(es)`); break; }
    // Straight round again when work is flowing; pause only when there is nothing to do at all.
    if (!didSomething) {
      if (passes % 30 === 0) console.log(`${ts()} idle — ${passes} passes, ${fedTotal} feed / ${liveTotal} live batches so far`);
      await sleep(IDLE_MS);
    }
  }
  process.exit(0);
})().catch((e) => { console.error('bio-pump fatal', e && e.stack || e); process.exit(1); });
