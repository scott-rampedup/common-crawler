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
 *   urls     — a list is mostly ALREADY in the archive: 54.2% of freshly-discovered bio URLs resolved in
 *              Common Crawl (measured on 28,410 from the Google Maps run). So resolve first, Lambda the
 *              hits for free, and spend proxy budget only on the remainder — and only with --live.
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
    const addLine = (line) => {
      const u = String(line || '').trim();
      if (!u || /^(vcards?|url|link|href)$/i.test(u)) return;
      if (!/^https?:\/\//i.test(u)) return;                       // a bio URL list is always absolute
      if (seen.has(u)) return;
      seen.add(u); out.write(u + '\n');
    };
    if (/^s3:\/\//i.test(IN) || !fs.existsSync(IN)) {
      const prefix = IN.replace(/^s3:\/\/[^/]+\//i, '');
      const keys = await listKeys(prefix);
      if (!keys.length) { console.error(`nothing under s3://${BUCKET}/${prefix} — nothing to do.`); process.exit(0); }
      console.error(`  ${keys.length} object(s) under ${prefix}`);
      for (const k of keys) { for (const line of (await getText(k)).split('\n')) addLine(line); }
      consumed = keys;
    } else {
      const rl = readline.createInterface({ input: fs.createReadStream(IN), crlfDelay: Infinity });
      for await (const line of rl) addLine(line);
    }
    await new Promise((r) => out.end(r));
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
  if (DRAIN && consumed.length && missSaved) {
    for (let i = 0; i < consumed.length; i += 1000) {
      const batch = consumed.slice(i, i + 1000).map((Key) => ({ Key }));
      try { await s3.send(new DeleteObjectsCommand({ Bucket: BUCKET, Delete: { Objects: batch } })); }
      catch (e) { console.error('  queue drain failed:', e.message); }
    }
    console.error(`\ndrained ${consumed.length} queue object(s).`);
  }

  console.error(`\n══════ BIO ETL DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  console.error(`  mode ${summary.mode} · run ${summary.run}`);
  if (MODE === 'urls') console.error(`  ${summary.urls.toLocaleString()} URL(s) -> ${summary.resolved.toLocaleString()} in CC, ${summary.miss.toLocaleString()} not`);
  console.error(`  ${summary.extracted.toLocaleString()} page(s) extracted via Lambda -> indexed (see the load line above)`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
