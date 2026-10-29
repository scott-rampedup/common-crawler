/**
 * bio-feed.js — keep the bio-URL queue flowing CONTINUOUSLY instead of in 6-hour batches.
 * ---------------------------------------------------------------------------------------
 * The old shape: bio-etl.js woke on a timer, re-read ALL 13,281 S3 queue objects, have-checked ~6.8M
 * URLs to work out what was new (~1 hour), then resolved + extracted + live-crawled inline. Because
 * every run paid that hour regardless of how much had arrived, it could not run often -- the 6-hour
 * interval existed purely to amortise a fixed scan cost. A URL queued at 12:43 waited for the next
 * firing and then sat through the scan.
 *
 * The new shape: this reads ONLY the objects written since the last run (S3 objects are immutable, so
 * a LastModified cursor is sound), resolves just those URLs against Common Crawl, and writes the WARC
 * pointers into the Postgres crawl_queue. From there the existing worker fleet -- the same one that
 * drained 6,051,835 URLs at ~514/s -- picks them up continuously. No cadence to tune, no scan to
 * amortise, and the work is claimable the moment it lands.
 *
 *   node bio-feed.js                 # one incremental pass
 *   node bio-feed.js --dry           # report what it would do
 *   node bio-feed.js --reset-cursor  # re-read the whole queue once (recovery)
 *   node bio-feed.js --since 2026-10-15T00:00:00Z
 *
 * Env: OPENSEARCH_ENDPOINT, DATABASE_URL, AWS creds. CRAWLS / CRAWLS_N pin the corpora.
 *      BIO_FEED_MAX_URLS (default 250000) bounds one pass so a backlog is worked in slices.
 *
 * Misses (URLs Common Crawl has no record of) are written to a miss list in S3 for the live-crawl
 * fleet; the workers are WARC-only and cannot fetch a row with no pointer.
 *
 * PRIORITY. Queue objects whose key carries -p{N} (written by the app for a Chrome-extension push) are
 * read FIRST, their URLs go into crawl_queue at that priority, their misses go to their own bio-miss
 * file, and any of them already pending in crawl_queue is MOVED UP rather than silently deduplicated
 * into the back of the line. Everything without the marker is priority 0 and behaves exactly as before.
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');
const { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);

const DRY = has('dry');
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const CFG = process.env.CC_CONFIG_INDEX || 'cc_config';
const CURSOR_ID = 'bio_feed_cursor';
const MAX_URLS = Number(process.env.BIO_FEED_MAX_URLS) || 250000;
const RUN = 'feed-' + new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('bio-feed: OPENSEARCH_ENDPOINT required'); process.exit(1); }
if (!process.env.DATABASE_URL) { console.error('bio-feed: DATABASE_URL required'); process.exit(1); }

const s3 = new S3Client({ region: REGION });
const readBody = async (b) => { const c = []; for await (const x of b) c.push(x); return Buffer.concat(c).toString('utf8'); };

// Priority travels in the object key: {stamp}-p{N}-{count}.txt, written by queueBioUrls. Reading it
// from the LISTING means a pass can order itself before fetching anything, and an object with no
// marker is ordinary bulk work at 0 — which is every object written before this existed.
const prioOfKey = (k) => {
  const m = /-p([0-9])-\d+\.txt$/i.exec(String(k || ''));
  return m ? Number(m[1]) : 0;
};

async function getCursor(osc) {
  if (has('reset-cursor')) { console.error('  --reset-cursor: reading the WHOLE queue this pass'); return ''; }
  const since = arg('since', '');
  if (since) return since;
  try {
    const g = await osc.get({ index: CFG, id: CURSOR_ID });
    return ((g.body || g)._source || {}).cursor || '';
  } catch (e) {
    const code = e && (e.statusCode || (e.meta && e.meta.statusCode));
    if (code === 404 || /404|not_found|index_not_found/i.test(String(e && e.message))) return '';
    throw e;                                  // a real read failure must not read as "start from zero"
  }
}

(async () => {
  const t0 = Date.now();
  const openSearch = require('./opensearch');
  const osc = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const cursor = await getCursor(osc);
  console.error(`bio-feed ${RUN} — cursor ${cursor || '(none: full pass)'}`);

  // 1) Only the objects written SINCE the cursor. S3 objects here are write-once, so LastModified is a
  //    sound high-water mark; nothing already read can change underneath us.
  let tok = null; const fresh = []; let listed = 0, newest = cursor;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
    for (const o of (r.Contents || [])) {
      listed++;
      const iso = new Date(o.LastModified).toISOString();
      if (!cursor || iso > cursor) fresh.push({ key: o.Key, iso, prio: prioOfKey(o.Key) });
      if (!newest || iso > newest) newest = iso;
    }
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);
  // HIGHEST PRIORITY FIRST, then oldest first within a priority. The order matters for two reasons:
  // MAX_URLS truncates a pass, so whatever is read first is what moves this pass; and the priority a
  // URL carries into crawl_queue is decided here.
  fresh.sort((a, b) => (b.prio - a.prio) || a.iso.localeCompare(b.iso));
  const prioObjs = fresh.filter((f) => f.prio > 0).length;
  console.error(`  ${listed.toLocaleString()} object(s) in the queue; ${fresh.length.toLocaleString()} new since the cursor`
    + (prioObjs ? ` (${prioObjs} PRIORITY — read first)` : ''));
  if (!fresh.length) { console.error('  nothing new — done.'); process.exit(0); }

  // 2) Read just those, collect distinct URLs with the priority of the object they came from (the
  //    highest, if the same URL is in two). `consumed` records what was actually read, because the
  //    cursor may no longer advance to `newest` — see step 7.
  const urls = new Set();
  const prioOf = new Map();
  const consumed = [];
  for (const f of fresh) {
    if (urls.size >= MAX_URLS) {
      console.error(`  hit BIO_FEED_MAX_URLS (${MAX_URLS.toLocaleString()}) — ${(fresh.length - consumed.length).toLocaleString()} object(s) wait for the next pass`);
      break;
    }
    try {
      const txt = await readBody((await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: f.key }))).Body);
      for (const line of txt.split('\n')) {
        const u = line.trim();
        if (/^https?:\/\/[^\s/]+\.[^\s]/i.test(u)) {
          urls.add(u);
          if (f.prio > (prioOf.get(u) || 0)) prioOf.set(u, f.prio);
        }
      }
      consumed.push(f);
    } catch (e) { console.error(`  read failed ${f.key}: ${e.message}`); consumed.push(f); }
  }
  // A loop, not Math.max(...values): this map can hold 250,000 entries and a spread that wide
  // overflows the argument stack.
  let topPrio = 0;
  for (const v of prioOf.values()) if (v > topPrio) topPrio = v;

  // How far the cursor may advance. NOT simply `newest`: a pass stops at MAX_URLS, and it now reads in
  // priority order rather than strictly oldest-first, so the newest object LISTED can easily be one
  // this pass never opened. The cursor is the only record of what has been consumed, and nothing ever
  // looks below it again — so advancing past an unopened object drops its URLs silently and for good.
  // (That was already true for a truncated pass before priority existed: 250,001 URLs in one tick lost
  // everything past the cut.) Advance only to the newest consumed object older than everything left
  // behind; a priority object read out of order is simply re-read next pass, which costs one S3 GET and
  // no Athena, because its URLs are in crawl_queue by then.
  const consumedKeys = new Set(consumed.map((f) => f.key));
  let minSkipped = '';
  for (const f of fresh) if (!consumedKeys.has(f.key) && (!minSkipped || f.iso < minSkipped)) minSkipped = f.iso;
  let safeCursor = newest;
  if (minSkipped) {
    safeCursor = cursor;
    for (const f of consumed) if (f.iso < minSkipped && f.iso > safeCursor) safeCursor = f.iso;
    console.error(`  cursor held at ${safeCursor || '(unchanged)'} — ${(fresh.length - consumed.length).toLocaleString()} object(s) not read this pass`);
  }
  console.error(`  ${urls.size.toLocaleString()} distinct URL(s) in the new objects`
    + (topPrio ? ` — ${prioOf.size.toLocaleString()} at priority ${topPrio}` : ''));

  // 3) Drop what the queue table already holds and what we already have a contact for. Both are cheap
  //    and both save Athena scan, which is the only part of this that costs money per URL.
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/'), max: 4 });
  pool.on('error', (e) => console.error('  [pg] idle client error:', e.message));
  const { makeQueue } = require('./crawl-queue');
  const queue = await makeQueue(pool);
  let list = [...urls];
  const already = new Set();
  for (let i = 0; i < list.length; i += 5000) {
    const chunk = list.slice(i, i + 5000);
    const r = await pool.query('SELECT url FROM crawl_queue WHERE url = ANY($1::text[])', [chunk]);
    for (const row of r.rows) already.add(row.url);
  }
  list = list.filter((u) => !already.has(u));
  console.error(`  already in crawl_queue: ${already.size.toLocaleString()} — ${list.length.toLocaleString()} left`);

  // A PRIORITY URL that is already in the table is not nothing to do. The row exists, so it is dropped
  // here before the resolve — correctly, there is no pointer to buy twice — but it is sitting wherever
  // it was first queued, which during a Maps backlog is behind several hundred thousand others. Move
  // it. Without this, the most common extension case (a page already discovered by a sitemap sweep)
  // would be acknowledged as queued and then wait days.
  if (topPrio > 0) {
    const byPrio = new Map();
    for (const u of already) {
      const p = prioOf.get(u) || 0;
      if (p > 0) { if (!byPrio.has(p)) byPrio.set(p, []); byPrio.get(p).push(u); }
    }
    let raised = 0;
    for (const [p, us] of byPrio) raised += await queue.raisePriority(us, p);
    if (byPrio.size) console.error(`  moved ${raised.toLocaleString()} already-queued URL(s) to the front of the queue`);
  }

  const { knownSet } = require('./skip-known');
  let knownN = 0;
  const todo = [];
  for (let i = 0; i < list.length; i += 1000) {
    const chunk = list.slice(i, i + 1000);
    let have; try { have = await knownSet(chunk); } catch (e) { have = new Set(); }
    knownN += have.size;
    for (const u of chunk) if (!have.has(u)) todo.push(u);
  }
  console.error(`  already known (contact or fresh ledger entry): ${knownN.toLocaleString()} — ${todo.length.toLocaleString()} to resolve`);

  if (DRY) { console.error('\n--dry: stopping before Athena. Cursor NOT advanced.'); await pool.end(); process.exit(0); }
  if (!todo.length) {
    await osc.index({ index: CFG, id: CURSOR_ID, refresh: true, body: { cursor: safeCursor, at: new Date().toISOString(), note: 'nothing new to resolve' } });
    console.error(`  nothing to resolve; cursor -> ${safeCursor}`);
    await pool.end(); process.exit(0);
  }

  // 4) Resolve ONLY these against Common Crawl.
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'biofeed-'));
  const fUrls = path.join(dir, 'urls.txt'), fPtr = path.join(dir, 'ptr.jsonl');
  fs.writeFileSync(fUrls, todo.join('\n') + '\n');
  let CRAWLS = arg('crawls', '') || process.env.CRAWLS || '';
  if (!CRAWLS) {
    const n = Number(process.env.CRAWLS_N) || 4;
    CRAWLS = (await require('./cc-athena-miner').latestCrawls(n, ['CC-MAIN-2026-39'])).join(',');
  }
  console.error(`  resolving ${todo.length.toLocaleString()} URL(s) against ${CRAWLS}`);
  const r = spawnSync(process.execPath, [path.join(__dirname, 'cc-athena-miner.js'),
    '--resolve-urls', fUrls, '--warc-out', fPtr, '--crawls', CRAWLS, '--resolve-tag', RUN.replace(/[^a-z0-9]/gi, '').slice(0, 24)],
    { stdio: 'inherit', cwd: __dirname, env: process.env });
  if (r.status !== 0) { console.error(`  resolve FAILED (exit ${r.status}) — cursor NOT advanced, the next pass retries`); await pool.end(); process.exit(1); }

  // 5) Pointers -> crawl_queue, each row carrying the priority of the object its URL arrived in. The
  //    resolve can hand back a URL in a different shape than it went in (scheme, www, trailing slash),
  //    so the lookup is by the same normalised key the miss list uses below.
  const keyOf = (u) => String(u || '').trim().replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/$/, '').toLowerCase();
  const prioByKey = new Map();
  for (const [u, p] of prioOf) prioByKey.set(keyOf(u), p);
  const { loadFile } = require('./load-queue');
  let loaded = { inserted: 0, parsed: 0 };
  if (fs.existsSync(fPtr)) {
    loaded = await loadFile(fPtr, { queue, chunkSize: 5000, priorityOf: (u) => prioByKey.get(keyOf(u)) || 0 });
    console.error(`  crawl_queue: ${loaded.inserted.toLocaleString()} new row(s) from ${loaded.parsed.toLocaleString()} pointer(s)`);
  }

  // 6) Misses -> a list the live fleet can crawl. Workers are WARC-only; a pointerless row would sit
  //    in the table forever, so these do NOT go in as rows.
  const resolved = new Set();
  if (fs.existsSync(fPtr)) {
    for (const line of fs.readFileSync(fPtr, 'utf8').split('\n')) {
      if (!line.trim()) continue;
      try { resolved.add(keyOf(JSON.parse(line).url)); } catch (e) { /* skip bad line */ }
    }
  }
  const miss = todo.filter((u) => !resolved.has(keyOf(u)));
  // Split the miss list by priority. This is the half of the pipeline that is easy to forget: for a
  // page the extension has just found, Common Crawl usually does NOT hold it — that is often exactly
  // why someone is pushing it by hand — so the live fleet, not the WARC workers, is the path that
  // matters, and it has its own queue (bio-miss/) with its own ordering and its own launch gate.
  // Priority rides in the key here too, and bio-live reads it from the listing.
  if (miss.length) {
    const groups = new Map();                 // priority -> urls
    for (const u of miss) {
      const p = prioByKey.get(keyOf(u)) || 0;
      if (!groups.has(p)) groups.set(p, []);
      groups.get(p).push(u);
    }
    for (const [p, us] of [...groups.entries()].sort((a, b) => b[0] - a[0])) {
      const key = `bio-miss/${RUN}${p ? `-p${p}` : ''}-${us.length}.txt`;
      await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: us.join('\n') + '\n', ContentType: 'text/plain' }));
      console.error(`  ${us.length.toLocaleString()} not in the archive${p ? ` (PRIORITY ${p})` : ''} -> s3://${BUCKET}/${key} (live fleet)`);
    }
  }

  // 7) Advance the cursor only after the work is durably recorded.
  await osc.index({ index: CFG, id: CURSOR_ID, refresh: true,
    body: { cursor: safeCursor, at: new Date().toISOString(),
            result: { objects: consumed.length, listed: fresh.length, urls: urls.size,
                      priorityUrls: prioOf.size, topPriority: topPrio,
                      resolved: loaded.inserted, miss: miss.length } } });
  console.error(`\nbio-feed done in ${Math.round((Date.now() - t0) / 1000)}s — cursor -> ${safeCursor}`);
  console.error(`  ${loaded.inserted.toLocaleString()} queued for the worker fleet | ${miss.length.toLocaleString()} for live crawl`);
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) { /* tmp */ }
  await pool.end();
  process.exit(0);
})().catch((e) => { console.error('bio-feed ERR', e && e.stack || e); process.exit(1); });
