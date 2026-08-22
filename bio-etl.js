/**
 * bio-etl.js — ONE command for the two standing bio-URL pipelines, both running at Lambda speed.
 *
 *   MODE 1 — sweep Common Crawl's index for bio pages and extract them:
 *     OPENSEARCH_ENDPOINT=… node bio-etl.js --mode discover --crawl CC-MAIN-2026-30 [--per-domain 3] [--limit N]
 *
 *   MODE 2 — take a URL list (the Sitemap Monitor's nightly output, or any list) and extract it:
 *     OPENSEARCH_ENDPOINT=… node bio-etl.js --mode urls --in s3://bucket/monitor-queue/pending/ [--live] [--drain]
 *
 * WHY THIS EXISTS: every stage was already built and proven — cc-athena-miner discovers/resolves,
 * lambda-drive fans out across cc-extract, load-extracted indexes. They were just never chained, so the
 * work kept being run one machine at a time. Measured on this box: a single Fly machine extracts at
 * 86 pages/s; the same work through Lambda runs at 4,386/s.
 *
 * The shape of each mode reflects what the source actually is:
 *
 *   discover — the whole crawl is the input, so there is nothing to resolve. 18.1M bio pages exist in one
 *              crawl across the 11 English-speaking TLDs (measured, 468-term directory list).
 *   urls     — resolve first, Lambda the hits for free, spend proxy budget only on the remainder. How big
 *              that remainder is depends entirely on the SOURCE, and the difference is not small:
 *              Google Maps URLs resolved 54.2% in Common Crawl, but Sitemap-Monitor URLs resolved only
 *              12.2-17.4%, because monitor URLs are new pages — which is the whole point of monitoring.
 *              So on monitor output the live remainder is ~83% of the work, not ~46%.
 *
 * Both modes end at the same place: extracted JSONL in S3 -> load-extracted -> the Master DB.
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectsCommand } = require('@aws-sdk/client-s3');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const MODE = arg('mode', '') || process.env.MODE || '';
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const RUN = arg('run', '') || process.env.RUN || `bio-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '')}`;
const SCRATCH = arg('scratch', '') || process.env.SCRATCH || '/tmp/_bio-etl';
const CRAWL = arg('crawl', '') || process.env.CRAWL || '';
const CRAWLS = arg('crawls', '') || process.env.CRAWLS || 'CC-MAIN-2026-30,CC-MAIN-2026-25,CC-MAIN-2026-21,CC-MAIN-2026-17';
const PER_DOMAIN = arg('per-domain', '') || process.env.PER_DOMAIN || '3';
const LIMIT = arg('limit', '') || process.env.LIMIT || '0';
const IN = arg('in', '') || process.env.IN || '';
const LIVE = has('live') || /^(1|true|yes|on)$/i.test(process.env.LIVE || '');
const DRAIN = has('drain') || /^(1|true|yes|on)$/i.test(process.env.DRAIN || '');
const SKIP_KNOWN = !/^(0|false|no|off)$/i.test(process.env.SKIP_KNOWN || '1');
const BIO_TERMS = arg('bio-terms', '') || process.env.BIO_TERMS || path.join(__dirname, 'data', 'bio-path-terms.txt');

function step(label, script, args, extraEnv) {
  console.error(`\n══════ ${label} · ${new Date().toISOString().slice(11, 19)} ══════`);
  const r = spawnSync(process.execPath, [path.join(__dirname, script), ...args], {
    stdio: 'inherit', cwd: __dirname, env: { ...process.env, ...(extraEnv || {}) },
  });
  if (r.status !== 0) { console.error(`✗ ${script} exited ${r.status}`); process.exit(r.status || 1); }
}

const s3 = new S3Client({ region: REGION });
async function listKeys(prefix) {
  const keys = []; let token = null;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: prefix, ContinuationToken: token }));
    for (const o of (r.Contents || [])) if (o.Size > 0) keys.push(o.Key);
    token = r.IsTruncated ? r.NextContinuationToken : null;
  } while (token);
  return keys;
}
async function getText(key) {
  const o = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  return o.Body.transformToString();
}

// ---- consumed-queue-object ledger -------------------------------------------------------------------
// The IAM user cannot s3:DeleteObject on the queue prefix, and DeleteObjects reports that per-object
// rather than throwing — so every drain "succeeded" while the queue only grew, and each run reprocessed
// the whole history. This records which objects a run has already consumed so the next run skips them,
// keeping the pipeline correct on read-only credentials. It does not replace the IAM fix: the objects
// still accumulate, still cost storage, and still slow every LIST.
const CONSUMED_INDEX = process.env.QUEUE_CONSUMED_INDEX || 'queue_consumed';
let _osClient = null;
function osClient() {
  if (!_osClient) _osClient = require('./opensearch').makeClient(process.env.OPENSEARCH_ENDPOINT);
  return _osClient;
}
async function ensureConsumedIndex() {
  const c = osClient();
  try { const ex = await c.indices.exists({ index: CONSUMED_INDEX }); if (ex.body === true || ex === true) return; }
  catch (e) { /* fall through */ }
  try { await c.indices.create({ index: CONSUMED_INDEX, body: { settings: { number_of_shards: 1, number_of_replicas: 0 },
    mappings: { properties: { key: { type: 'keyword' }, consumed_at: { type: 'date' }, run: { type: 'keyword' } } } } }); }
  catch (e) { if (!/resource_already_exists/i.test(String(e && e.message))) throw e; }
}
async function markConsumed(keys) {
  if (!keys || !keys.length) return 0;
  await ensureConsumedIndex();
  const c = osClient();
  const at = new Date().toISOString();
  let n = 0;
  for (let i = 0; i < keys.length; i += 2000) {
    const body = [];
    for (const k of keys.slice(i, i + 2000)) {
      body.push({ index: { _index: CONSUMED_INDEX, _id: k } });
      body.push({ key: k, consumed_at: at, run: RUN });
    }
    const r = await c.bulk({ body, refresh: false });
    const b = r.body || r;
    n += ((b.items || []).length);
    if (b && b.items) for (const it of b.items) if (it.index && it.index.error) n--;
  }
  try { await c.indices.refresh({ index: CONSUMED_INDEX }); } catch (e) { /* best-effort */ }
  return n;
}
async function alreadyConsumed(keys) {
  const done = new Set();
  if (!keys.length) return done;
  try {
    await ensureConsumedIndex();
    const c = osClient();
    for (let i = 0; i < keys.length; i += 1000) {
      const chunk = keys.slice(i, i + 1000);
      const r = await c.mget({ index: CONSUMED_INDEX, body: { ids: chunk }, _source: false });
      for (const d of (((r.body || r).docs) || [])) if (d && d.found) done.add(d._id);
    }
  } catch (e) { console.error('  (consumed-key ledger unavailable: ' + e.message + ')'); }
  return done;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (MODE !== 'discover' && MODE !== 'urls') { console.error('need --mode discover|urls'); process.exit(1); }
  fs.mkdirSync(SCRATCH, { recursive: true });
  const F = {
    urls: path.join(SCRATCH, `${RUN}-urls.txt`),
    ptr: path.join(SCRATCH, `${RUN}-ptr.jsonl`),
    miss: path.join(SCRATCH, `${RUN}-miss.txt`),
  };
  const t0 = Date.now();
  let consumed = [];
  const summary = { mode: MODE, run: RUN, urls: 0, resolved: 0, miss: 0, extracted: 0, indexed: 0 };

  // ---------------------------------------------------------------- MODE 1: sweep the CC index
  if (MODE === 'discover') {
    const crawlArgs = ['--bio-terms-file', BIO_TERMS, '--warc-out', F.ptr, '--per-domain', String(PER_DOMAIN)];
    if (CRAWL) crawlArgs.push('--crawl', CRAWL);
    if (Number(LIMIT) > 0) crawlArgs.push('--limit', String(LIMIT));
    step('discover bio pages in Common Crawl', 'cc-athena-miner.js', crawlArgs);
  }

  // ---------------------------------------------------------------- MODE 2: a URL list
  if (MODE === 'urls') {
    if (!IN) { console.error('--mode urls needs --in <file | s3://bucket/prefix | prefix/>'); process.exit(1); }
    console.error(`\n══════ collect URLs from ${IN} ══════`);
    const seen = new Set();
    const out = fs.createWriteStream(F.urls);
    // Domain gate, applied HERE rather than at fetch time. This is the earliest point a URL can be
    // dropped, so a blocked domain costs nothing downstream: no Athena resolve, no Lambda invocation, no
    // miss-list entry, no fleet time. Measured 2026-08-14, the sub-2%-yield tail was 61.1% of all fetching.
    let blockedSet = new Set();
    try { blockedSet = await require('./domain-gate').loadSet(osClient()); }
    catch (e) { console.error('  (domain gate unavailable: ' + e.message + ' — crawling everything)'); }
    if (blockedSet.size) console.error(`  domain gate: ${blockedSet.size} domain(s) blocked`);
    let blockedN = 0;
    const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return ''; } };
    const addLine = (line) => {
      const u = String(line || '').trim();
      if (!u || /^(vcards?|url|link|href)$/i.test(u)) return;
      if (!/^https?:\/\//i.test(u)) return;                       // a bio URL list is always absolute
      if (seen.has(u)) return;
      if (blockedSet.size && blockedSet.has(hostOf(u))) { blockedN++; return; }
      seen.add(u); out.write(u + '\n');
    };
    if (/^s3:\/\//i.test(IN) || !fs.existsSync(IN)) {
      const prefix = IN.replace(/^s3:\/\/[^/]+\//i, '');
      let keys = await listKeys(prefix);
      if (!keys.length) { console.error(`nothing under s3://${BUCKET}/${prefix} — nothing to do.`); process.exit(0); }
      console.error(`  ${keys.length} object(s) under ${prefix}`);
      // Skip objects a previous run VERIFIED as fully converted.
      //
      // The ledger used to be written when a run merely READ an object -- before its misses had been
      // crawled -- so a fleet shard that died took its slice with it and the object was skipped forever
      // after. Measured cost: 7,966 objects / 32,312,633 URLs marked consumed, of which a 2,000-URL sample
      // spread across the whole history found 9 with contacts. 0.4%.
      //
      // The ledger is now only a cache of "every URL in this object is already known", and membership has
      // to be earned by checking. Anything not in it is read and have-checked -- cheap, since these are
      // text files -- which means a dead shard's work returns automatically on the next run.
      const done = await alreadyConsumed(keys);
      if (done.size) {
        keys = keys.filter((k) => !done.has(k));
        console.error(`  ${done.size} verified-complete by an earlier run -- ${keys.length} to check`);
        if (!keys.length) { console.error('  everything under this prefix is converted; nothing to do.'); process.exit(0); }
      }
      const { knownSet } = require('./skip-known');
      const nowComplete = [];
      let objUrls = 0, objKnown = 0;
      for (const k of keys) {
        const urls = (await getText(k)).split('\n').map((x) => x.trim()).filter(Boolean);
        objUrls += urls.length;
        let have;
        try { have = await knownSet(urls); }
        catch (e) { have = new Set(); console.error(`  have-check failed for ${k} (${e.message}) -- treating all as unprocessed`); }
        objKnown += have.size;
        const todo = urls.filter((u) => !have.has(u));
        for (const line of todo) addLine(line);
        if (urls.length && !todo.length) nowComplete.push(k);
      }
      console.error(`  ${objUrls.toLocaleString()} URL(s) across ${keys.length} object(s); ${objKnown.toLocaleString()} already done`);
      if (nowComplete.length) {
        try { const n = await markConsumed(nowComplete); console.error(`  ${n.toLocaleString()} object(s) fully converted -> ledger`); }
        catch (e) { console.error('  could not record completed objects:', e.message); }
      }
      // Deliberately empty: `consumed` drove the delete-and-mark-on-read path further down, which is the
      // behaviour that lost 32M URLs. Objects are retired only by the verified check above.
      consumed = [];
    } else {
      const rl = readline.createInterface({ input: fs.createReadStream(IN), crlfDelay: Infinity });
      for await (const line of rl) addLine(line);
    }
    await new Promise((r) => out.end(r));
    if (blockedN) console.error(`  domain gate dropped ${blockedN.toLocaleString()} URL(s) before any fetching`);
    summary.urls = seen.size;
    console.error(`  ${seen.size.toLocaleString()} unique URL(s) -> ${F.urls}`);
    if (!seen.size) { console.error('nothing to do.'); process.exit(0); }

    step('resolve them in Common Crawl', 'cc-athena-miner.js',
      ['--resolve-urls', F.urls, '--warc-out', F.ptr, '--crawls', CRAWLS, '--resolve-tag', RUN.replace(/[^a-z0-9]/gi, '').slice(0, 24)]);

    // Which ones the archive did NOT have — the only URLs that justify live proxy budget.
    const resolved = new Set();
    const keyOf = (u) => String(u || '').trim().replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/$/, '').toLowerCase();
    if (fs.existsSync(F.ptr)) {
      const rl = readline.createInterface({ input: fs.createReadStream(F.ptr), crlfDelay: Infinity });
      for await (const l of rl) { if (l.trim()) { try { resolved.add(keyOf(JSON.parse(l).url)); } catch (e) { /* */ } } }
    }
    const mo = fs.createWriteStream(F.miss);
    for (const u of seen) if (!resolved.has(keyOf(u))) { mo.write(u + '\n'); summary.miss++; }
    await new Promise((r) => mo.end(r));
    summary.resolved = resolved.size;
    console.error(`  in Common Crawl: ${resolved.size.toLocaleString()} | not in CC: ${summary.miss.toLocaleString()}`
      + `${LIVE ? ' (live fallback ON)' : ' (skipped — pass --live to crawl them)'}`);
  }

  // ---------------------------------------------------------------- shared: drop pages we already have
  // STREAMED, never slurped. A full crawl is ~18.1M pointers ≈ 3.6GB of JSONL: readFileSync would exceed
  // V8's string limit and OOM the box before a single page was extracted. This reads a window at a time,
  // asks OpenSearch which of that window it already has, and writes survivors straight back out.
  if (SKIP_KNOWN && fs.existsSync(F.ptr)) {
    const os_ = require('./opensearch');
    const client = os_.makeClient(process.env.OPENSEARCH_ENDPOINT);
    const tmp = F.ptr + '.filtered';
    const out = fs.createWriteStream(tmp);
    const rl = readline.createInterface({ input: fs.createReadStream(F.ptr), crlfDelay: Infinity });
    let seenN = 0, knownN = 0, keptN = 0;
    let win = [];                                          // [{line,url}] awaiting a lookup
    const WINDOW = 1024;                                   // one terms query per window
    const flushWindow = async () => {
      if (!win.length) return;
      const urls = [...new Set(win.map((w) => w.url).filter(Boolean))];
      const have = new Set();
      if (urls.length) {
        try {
          const r = await client.search({ index: os_.INDEX, body: { size: 0, query: { terms: { web_source_url: urls } },
            aggs: { u: { terms: { field: 'web_source_url', size: urls.length } } } } });
          for (const b of (((r.body || r).aggregations.u.buckets) || [])) have.add(b.key);
        } catch (e) { /* an unfiltered page costs a fetch, not correctness */ }
      }
      for (const w of win) {
        if (w.url && have.has(w.url)) { knownN++; continue; }
        keptN++;
        if (!out.write(w.line + '\n')) await new Promise((res) => out.once('drain', res));
      }
      win = [];
    };
    console.error(`\n══════ skip pages already in the Master DB (streamed) ══════`);
    for await (const line of rl) {
      if (!line.trim()) continue;
      seenN++;
      let url = ''; try { url = JSON.parse(line).url || ''; } catch (e) { /* keep unparseable, extract will drop it */ }
      win.push({ line, url });
      if (win.length >= WINDOW) {
        await flushWindow();
        if (seenN % 250000 < WINDOW) console.error(`  checked ${seenN.toLocaleString()} | already have ${knownN.toLocaleString()} | keeping ${keptN.toLocaleString()}`);
      }
    }
    await flushWindow();
    await new Promise((r) => out.end(r));
    fs.renameSync(tmp, F.ptr);
    console.error(`  already have ${knownN.toLocaleString()} | NEW to extract ${keptN.toLocaleString()}`);
  }

  // ---------------------------------------------------------------- Lambda fan-out
  // Counted by streaming for the same reason.
  let nPtr = 0;
  if (fs.existsSync(F.ptr)) {
    const rl = readline.createInterface({ input: fs.createReadStream(F.ptr), crlfDelay: Infinity });
    for await (const l of rl) if (l.trim()) nPtr++;
  }
  summary.extracted = nPtr;
  if (nPtr) {
    step(`extract ${nPtr.toLocaleString()} page(s) across the Lambda fleet`, 'lambda-drive.js', [F.ptr], { RUN });
    step('index the extracted JSONL', 'load-extracted.js', [`cc-extracted/${RUN}/`]);
  } else {
    console.error('\nno pointers to extract (everything already in the Master DB).');
  }

  // ---------------------------------------------------------------- live remainder (opt-in)
  if (MODE === 'urls' && LIVE && summary.miss) {
    step(`live-crawl the ${summary.miss.toLocaleString()} page(s) Common Crawl lacks`, 'extract-from-pointers.js',
      ['--live', F.miss, '--tag', RUN]);
  }

  // ---------------------------------------------------------------- filter the remainder too
  // The same skip-known filter was applied to the CC pointer list only, which is backwards: a pointer
  // costs an S3 range read inside a Lambda, while a miss costs a live proxied fetch — the most expensive
  // operation here — and the miss list is the larger side (82.6% of the 2026-08-14 queue). Unfiltered, a
  // re-run re-crawls everything already done: 2,563,533 pages and ~13 fleet-hours for nothing.
  if (SKIP_KNOWN && MODE === 'urls' && summary.miss && fs.existsSync(F.miss)) {
    console.error(`\n══════ skip remainder URLs already in the Master DB ══════`);
    try {
      const { filterList } = require('./skip-known');
      const filtered = F.miss + '.filtered';
      const r = await filterList(F.miss, filtered);
      fs.renameSync(filtered, F.miss);
      summary.miss = r.kept;
    } catch (e) { console.error('  remainder filter failed (keeping the full list):', e.message); }
  }

  // ---------------------------------------------------------------- preserve the un-crawled remainder
  // The miss list is the URLs Common Crawl does not have — on monitor output that is ~88% of the queue,
  // and it exists ONLY on this machine's /tmp. Deleting the queue objects while it sits there unprocessed
  // destroys them permanently: the queue was the only durable copy. That is a live hazard, because --drain
  // deletes whether or not --live ran, and the live step can also die (the first fleet lost four of eight
  // shards to heap exhaustion). So the remainder is uploaded BEFORE anything is deleted, and a failure to
  // upload cancels the drain rather than proceeding.
  let missSaved = true;
  const missKey = `bio-resolve/${RUN}/miss.txt`;
  if (MODE === 'urls' && summary.miss && fs.existsSync(F.miss)) {
    try {
      const st = fs.statSync(F.miss);
      const { PutObjectCommand } = require('@aws-sdk/client-s3');
      await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: missKey,
        Body: fs.createReadStream(F.miss), ContentLength: st.size, ContentType: 'text/plain' }));
      console.error(`\nremainder saved: ${summary.miss.toLocaleString()} URL(s) -> s3://${BUCKET}/${missKey}`);
      if (!LIVE) {
        console.error('  (--live was not set, so these are NOT yet crawled — run the fleet over that key:)');
        console.error(`  node live-fleet-shard.js --in s3://${BUCKET}/${missKey} --shard i/N --tag ${RUN}`);
      }
    } catch (e) {
      missSaved = false;
      console.error(`\nFAILED to save the remainder (${e.message}) — NOT draining the queue, so nothing is lost.`);
    }
  }

  // ---------------------------------------------------------------- drain the queue (opt-in)
  //
  // DeleteObjects does NOT throw when S3 refuses an individual key — it returns a per-object Errors array
  // alongside Deleted. This code used to ignore that array and print "drained N queue object(s)"
  // unconditionally. The IAM user (cc-athena) has no s3:DeleteObject, so EVERY drain since the queue was
  // created reported success while deleting nothing: the queue only ever grew, and each run re-resolved
  // and re-processed the entire history from 2026-08-13 onwards.
  //
  // So: read the response, and when the delete is refused, fall back to a consumed-key ledger so the next
  // run skips those objects anyway. The ledger keeps the pipeline correct on read-only credentials; it is
  // not a substitute for the IAM fix, because the objects still accumulate and still cost storage.
  if (DRAIN && consumed.length && missSaved) {
    let deleted = 0; const failed = [];
    for (let i = 0; i < consumed.length; i += 1000) {
      const batch = consumed.slice(i, i + 1000).map((Key) => ({ Key }));
      try {
        const r = await s3.send(new DeleteObjectsCommand({ Bucket: BUCKET, Delete: { Objects: batch } }));
        const res = r || {};
        deleted += (res.Deleted || []).length;
        for (const e of (res.Errors || [])) failed.push({ key: e.Key, code: e.Code, message: e.Message });
      } catch (e) {
        for (const b of batch) failed.push({ key: b.Key, code: 'Exception', message: e.message });
      }
    }
    console.error(`\nqueue: ${deleted.toLocaleString()} object(s) deleted, ${failed.length.toLocaleString()} refused`);
    if (failed.length) {
      const f = failed[0];
      console.error(`  first refusal: ${f.code} — ${String(f.message).slice(0, 200)}`);
      if (/AccessDenied/i.test(f.code || '')) {
        console.error('  the IAM user cannot delete from this bucket. Grant s3:DeleteObject on');
        console.error(`  arn:aws:s3:::${BUCKET}/monitor-queue/pending/* — until then the objects stay and only the ledger prevents reprocessing.`);
      }
      try {
        const n = await markConsumed(failed.map((x) => x.key));
        console.error(`  recorded ${n.toLocaleString()} key(s) as consumed so the next run skips them.`);
      } catch (e) { console.error('  AND the consumed-key ledger failed:', e.message, '— the next run WILL reprocess these.'); }
    }
  }

  // ---------------------------------------------------------------- hand the remainder to a fleet
  // The scheduled drain resolves and Lambdas the ~17% Common Crawl already holds, in minutes. The other
  // ~83% needs live fetching, which is hours of work and must not run inline on a web-server machine (one
  // of ours has 2GB). Until now that half waited for someone to type eight `flyctl machine run` commands,
  // which is the last reason this pipeline was not actually unattended.
  //
  // Requires FLY_API_TOKEN. Without it the run still succeeds and prints the command, so a missing token
  // degrades to the manual path rather than silently dropping the work.
  const FLEET_SHARDS = Number(process.env.FLEET_SHARDS || 0);
  if (FLEET_SHARDS > 0 && missSaved && summary.miss && process.env.FLY_API_TOKEN) {
    try {
      const { launchFleet, reapFleet } = require('./fleet-launch');
      await reapFleet({ log: (m) => console.error(m) });          // free the names from the previous fleet
      const started = await launchFleet({
        in: `s3://${BUCKET}/${missKey}`, shards: FLEET_SHARDS, tag: RUN, log: (m) => console.error(m),
      });
      console.error(`\nfleet: ${started.length} shard(s) crawling ${summary.miss.toLocaleString()} URL(s)`);
      console.error(`  watch: node fleet-health.js --prefix live-fleet --watch 300`);
    } catch (e) {
      console.error(`\nfleet launch FAILED (${e.message}) — the remainder is safe at s3://${BUCKET}/${missKey}`);
    }
  } else if (FLEET_SHARDS > 0 && summary.miss && !process.env.FLY_API_TOKEN) {
    console.error('\nFLEET_SHARDS is set but FLY_API_TOKEN is not — not launching; the remainder is saved above.');
  }

  console.error(`\n══════ BIO ETL DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  console.error(`  mode ${summary.mode} · run ${summary.run}`);
  if (MODE === 'urls') console.error(`  ${summary.urls.toLocaleString()} URL(s) -> ${summary.resolved.toLocaleString()} in CC, ${summary.miss.toLocaleString()} not`);
  console.error(`  ${summary.extracted.toLocaleString()} page(s) extracted via Lambda -> indexed (see the load line above)`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
