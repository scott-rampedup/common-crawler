/**
 * worker.js — extraction worker for the bio-URL fleet.
 * ----------------------------------------------------
 * Pulls a batch of URLs from the Postgres crawl_queue, fetches each archived page directly via its
 * WARC pointer (the free CC fast path — no index lookup, no proxy), runs the SAME extractRecord the
 * web app uses, phone-analyzes + geocodes the batch, and upserts to the shared Postgres contacts
 * store (db-pg, score-gated). Many of these run in parallel across machines; the queue's
 * FOR UPDATE SKIP LOCKED guarantees disjoint work. Built to chew through the 3.73M columnar harvest.
 *
 *   node worker.js                # run continuously (Fly worker process)
 *   node worker.js --drain        # exit when the queue is empty (one-shot)
 *   node worker.js --stats        # print queue stats and exit
 *   node worker.js --selftest     # offline orchestration test
 *
 * Env: DATABASE_URL, WORKER_CONCURRENCY (default 8), WORKER_BATCH (default 200),
 *      WARC_RPS (default 10 — polite per-worker fetch rate; CC 403s a hammering IP),
 *      WARC_RETRIES (default 4), WORKER_MODEL_EMAILS (default on — model email-less bios from the
 *      company pattern so they aren't dropped), WORKER_ID (default hostname), PGSSL=1 to enable TLS.
 */
const path = require('path');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Concurrency-limited map: run `fn` over items, at most `limit` in flight. Preserves input order.
async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const lim = Math.max(1, limit | 0);
  async function run() { while (i < items.length) { const idx = i++; out[idx] = await fn(items[idx], idx); } }
  await Promise.all(Array.from({ length: Math.min(lim, items.length) }, run));
  return out;
}

// Token-paced gate: spaces fetch STARTS to <= rps per second (across all concurrent fetches). The
// Common Crawl WARC store 403s a single IP that hammers it, so politeness is required — concurrency
// alone isn't enough; the request RATE is what gets throttled.
function makeRateLimiter(rps) {
  const interval = rps > 0 ? 1000 / rps : 0;
  let next = 0;
  return async function take() {
    if (!interval) return;
    const at = Math.max(Date.now(), next);
    next = at + interval;
    const wait = at - Date.now();
    if (wait > 0) await sleep(wait);
  };
}

// Errors worth retrying: CC throttle/availability (403/429/5xx) + transient network faults.
const TRANSIENT = /\b(403|408|425|429|500|502|503|504)\b|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN|socket hang up|network|timeout/i;

// Fetch one WARC record politely: pace via `rate`, and on a transient error back off (exponential +
// jitter) and retry. A persistent failure rethrows so the URL is requeued for a calmer pass later.
async function politeFetch(ptr, { fetchWarc, rate, retries, baseDelay }) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (rate) await rate();
    try { return await fetchWarc({ url: ptr.url, filename: ptr.filename, offset: ptr.offset, length: ptr.length, timestamp: ptr.timestamp }); }
    catch (e) {
      lastErr = e;
      if (attempt < retries && TRANSIENT.test(String(e && e.message || ''))) {
        await sleep(baseDelay * Math.pow(2, attempt) + Math.floor(Math.random() * 250));
        continue;
      }
      throw e;
    }
  }
  throw lastErr;
}

// Build a worker from injected deps (so the orchestration is offline-testable):
//   queue        - makeQueue(pool)
//   dbpg         - makeDb(...) (uses .upsertMany)
//   fetchWarc    - async (pointer) => html string ('' when the archive has no usable HTML)
//   extractRecord- (html, url, deps) => record | null
//   analyzePhones- (records) => records
//   geocodeRecords- async (records) => void
//   wireless, genderMap - extractor inputs
function makeWorker(deps = {}) {
  const {
    queue, dbpg, fetchWarc, extractRecord, analyzePhones, geocodeRecords,
    wireless, genderMap = {}, log = () => {},
    workerId = 'worker', concurrency = 8, batchSize = 200,
    warcRps = 10, fetchRetries = 4, baseDelay = 1000,
    modelMissingEmails = null, modelEmails = false,
  } = deps;
  if (!queue || !dbpg || !fetchWarc || !extractRecord) throw new Error('makeWorker: queue, dbpg, fetchWarc, extractRecord required');
  const rate = makeRateLimiter(warcRps);

  // Fetch one archived page via its WARC pointer and extract a record. A thrown error (network/
  // transient, after retries) -> {ok:false} so the queue requeues it; empty HTML (page gone from the
  // archive) -> a clean {ok:true, rec:null} so it's marked done, not retried forever.
  async function extractOne(ptr) {
    try {
      const html = await politeFetch(ptr, { fetchWarc, rate, retries: fetchRetries, baseDelay });
      if (!html) return { url: ptr.url, ok: true, rec: null };
      const ts = String(ptr.timestamp || '').slice(0, 8).replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
      const rec = extractRecord(html, ptr.url, { wireless, genderMap, directoryRules: {}, source: 'Common Crawl', timestamp: ts, allowNoEmail: true });
      return { url: ptr.url, ok: true, rec: rec || null };
    } catch (e) {
      return { url: ptr.url, ok: false, error: e.message };
    }
  }

  // Process one claimed batch end-to-end. Returns counts.
  async function runBatch(batch) {
    const results = await mapLimit(batch, concurrency, extractOne);
    let recs = results.filter((r) => r.ok && r.rec).map((r) => r.rec);
    if (recs.length) {
      // Model emails for email-less bios from the company's pattern (same-batch + central DB), so they
      // aren't dropped at the email-keyed upsert. Best-effort; the DB lookup is light + indexed.
      if (modelEmails && modelMissingEmails) {
        try { await modelMissingEmails(recs, { dbQuery: (domain) => dbpg.sampleProfessionalEmails(domain) }); }
        catch (e) { /* best-effort modelling */ }
      }
      if (analyzePhones) recs = analyzePhones(recs) || recs;   // dedupe Phone 2, relabel Direct->Office
      if (geocodeRecords) await geocodeRecords(recs);          // fill Phone Location (City, Region, Country)
    }
    let added = 0;
    if (recs.length) { const u = await dbpg.upsertMany(recs); added = u.added; }
    // mark every successfully-fetched URL done (records=1 if it yielded a contact, else 0);
    // retry the ones that threw.
    const done = results.filter((r) => r.ok).map((r) => ({ url: r.url, records: r.rec ? 1 : 0 }));
    await queue.markDoneMany(done);
    const failed = results.filter((r) => !r.ok);
    for (const r of failed) await queue.markError(r.url, r.error);
    return { claimed: batch.length, extracted: recs.length, added, errors: failed.length };
  }

  // Claim → process loop. `drain` exits when the queue empties; otherwise idles with backoff.
  async function runLoop({ drain = false, signal = null } = {}) {
    const totals = { batches: 0, claimed: 0, extracted: 0, added: 0, errors: 0 };
    let emptyPolls = 0;
    const stopped = () => signal && signal.stopped;
    while (!stopped()) {
      const batch = await queue.claimBatch(batchSize, workerId);
      if (!batch.length) {
        if (drain) { log('queue drained'); break; }
        emptyPolls++;
        await sleep(Math.min(15000, 1000 * emptyPolls));
        continue;
      }
      emptyPolls = 0;
      const r = await runBatch(batch);
      totals.batches++; totals.claimed += r.claimed; totals.extracted += r.extracted;
      totals.added += r.added; totals.errors += r.errors;
      log(`batch #${totals.batches}: claimed ${r.claimed}, extracted ${r.extracted}, +${r.added} new, ${r.errors} err  (cum +${totals.added})`);
      // Periodically reclaim work orphaned by a dead/removed worker (claims only auto-recover at boot
      // otherwise) so the run always finishes — idempotent; 15-min threshold never touches live batches.
      if (queue.requeueStale && totals.batches % 50 === 0) {
        const stale = await queue.requeueStale(15);
        if (stale) log(`requeued ${stale} stale claim(s)`);
      }
    }
    return totals;
  }

  return { extractOne, runBatch, runLoop };
}

module.exports = { makeWorker, mapLimit };

// ---------------------------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------------------------
if (require.main === module) {
  if (process.argv.includes('--selftest')) { runSelftest(); }
  else { runMain().catch((e) => { console.error('worker fatal:', e); process.exit(1); }); }
}

async function runMain() {
  const os = require('os');
  const { makeDb } = require('./db-pg');
  const { makeQueue } = require('./crawl-queue');
  const engine = require('./cc-engine');
  const extractor = require('./extractor');
  const { loadWirelessBlocks } = require('./wireless-block-classifier');

  const dbpg = await makeDb({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL });
  const queue = await makeQueue(dbpg._pool);

  if (process.argv.includes('--stats')) {
    console.log(JSON.stringify(await queue.stats(), null, 1));
    await dbpg.close();
    return;
  }

  const wireless = loadWirelessBlocks(path.join(__dirname, 'phone-blocks.csv'));
  const genderMap = extractor.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const workerId = process.env.WORKER_ID || os.hostname();
  const worker = makeWorker({
    queue, dbpg,
    fetchWarc: engine.fetchWarc, extractRecord: extractor.extractRecord,
    analyzePhones: extractor.analyzePhones, geocodeRecords: extractor.geocodeRecords,
    wireless, genderMap, workerId,
    concurrency: Number(process.env.WORKER_CONCURRENCY) || 8,
    batchSize: Number(process.env.WORKER_BATCH) || 200,
    warcRps: Number(process.env.WARC_RPS) || 10,         // polite per-worker fetch rate (CC 403s a hammering IP)
    fetchRetries: Number(process.env.WARC_RETRIES) || 4,
    modelMissingEmails: require('./email-model').modelMissingEmails,
    modelEmails: !/^(0|false|no|off)$/i.test(process.env.WORKER_MODEL_EMAILS || '1'),   // model email-less bios (default on)
    log: (m) => console.log(`[${workerId}] ${m}`),
  });

  const reclaimed = await queue.requeueStale(30);   // recover work from any worker that died
  if (reclaimed) console.log(`[${workerId}] requeued ${reclaimed} stale claim(s)`);

  const signal = { stopped: false };
  const onSig = () => { if (!signal.stopped) { console.log(`[${workerId}] shutdown — finishing current batch`); signal.stopped = true; } };
  process.on('SIGTERM', onSig); process.on('SIGINT', onSig);

  const totals = await worker.runLoop({ drain: process.argv.includes('--drain'), signal });
  console.log(`[${workerId}] done:`, JSON.stringify(totals));
  await dbpg.close();
}

// Offline orchestration self-test with fully faked deps (the extractor + queue SQL have their own tests).
async function runSelftest() {
  let pass = 0, fail = 0;
  const ok = (n, c) => { if (c) { pass++; console.log('  ✓', n); } else { fail++; console.log('  ✗', n); } };

  // fake queue backed by an in-memory list
  const pending = [
    { url: 'https://a.com/agents/jane', filename: 'f', offset: '0', length: '1', timestamp: '20260601000000' }, // has email
    { url: 'https://a.com/agents/john', filename: 'f', offset: '0', length: '1', timestamp: '20260601000000' }, // archive empty -> 0 recs
    { url: 'https://a.com/agents/boom', filename: 'f', offset: '0', length: '1', timestamp: '20260601000000' }, // throws -> retry
  ];
  const marks = { done: [], error: [] };
  let claimAttempts = 0;
  const queue = {
    async claimBatch(n) { claimAttempts++; return pending.splice(0, n); },
    async markDoneMany(items) { marks.done.push(...items); return items.length; },
    async markError(url, msg) { marks.error.push({ url, msg }); },
  };
  const fetched = [];
  const fetchWarc = async (p) => {
    fetched.push(p.url);
    if (p.url.endsWith('/boom')) throw new Error('socket hang up');
    if (p.url.endsWith('/john')) return '';                 // archived but no usable HTML
    return '<h1>Jane Roe</h1>';
  };
  const upserts = [];
  const dbpg = { async upsertMany(recs) { upserts.push(...recs); return { processed: recs.length, added: recs.length, total: recs.length }; } };
  const extractRecord = (html, url) => html ? { 'Email Address': 'jane.roe@a.com', 'Web Source URL': url } : null;
  let geocoded = 0, modelledCalled = 0;
  const worker = makeWorker({
    queue, dbpg, fetchWarc, extractRecord,
    analyzePhones: (recs) => recs, geocodeRecords: async (recs) => { geocoded += recs.length; },
    modelEmails: true, modelMissingEmails: async (recs) => { modelledCalled += recs.length; },
    concurrency: 2, batchSize: 10, warcRps: 0, fetchRetries: 0, log: () => {},   // no pacing/retry in the test
  });

  const totals = await worker.runLoop({ drain: true });

  ok('claims and processes every pending URL', fetched.length === 3);
  ok('extracts the page that has HTML, skips the empty one', upserts.length === 1 && upserts[0]['Email Address'] === 'jane.roe@a.com');
  ok('geocodes the extracted batch', geocoded === 1);
  ok('runs email modelling on the batch when enabled', modelledCalled === 1);
  ok('marks fetched URLs done (records=1 for the hit, 0 for the empty)',
    marks.done.length === 2 &&
    marks.done.find((d) => d.url.endsWith('/jane')).records === 1 &&
    marks.done.find((d) => d.url.endsWith('/john')).records === 0);
  ok('marks the thrown URL as error (for retry)', marks.error.length === 1 && marks.error[0].url.endsWith('/boom'));
  ok('drains and reports totals', totals.claimed === 3 && totals.extracted === 1 && totals.added === 1 && totals.errors === 1);
  ok('stops claiming once the queue is empty', claimAttempts === 2);   // one full batch + one empty

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
}
