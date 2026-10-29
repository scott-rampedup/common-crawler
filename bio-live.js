/**
 * bio-live.js — crawl the bio URLs Common Crawl has no record of.
 * ----------------------------------------------------------------
 * The other half of bio-feed. Roughly half of every batch resolves to a WARC pointer and goes into
 * crawl_queue, where the continuous workers take it. The rest -- pages the archive simply does not hold
 * -- can only be fetched live, and worker.js cannot do it: it fetches BY POINTER, so a row with no
 * pointer would sit in the queue forever. bio-feed therefore writes those to bio-miss/ and stops.
 *
 * Under the old batch drain, bio-etl handed that remainder to a live fleet at the end of each run. When
 * the pipeline moved to the continuous feed, the hand-off did not come with it, and the misses just
 * accumulated -- 7,982 URLs across 10 files before anyone looked. This closes that loop: gather what has
 * piled up, hand it to the fleet in one list, and advance a cursor so the same URLs are not re-crawled.
 *
 *   node bio-live.js                 # gather + launch if the backlog justifies it
 *   node bio-live.js --dry           # report what it would do
 *   node bio-live.js --min 1         # launch no matter how small the backlog
 *
 * Env: OPENSEARCH_ENDPOINT, FLY_API_TOKEN, AWS creds.
 *      BIO_LIVE_MIN_URLS   (default 2000) don't wake a fleet for a handful of URLs
 *      BIO_LIVE_SHARDS     (default 12)   shards to launch; sharding is BY DOMAIN, so more shards
 *                                         never means more pressure on one site
 *      BIO_LIVE_MAX_URLS   (default 400000) cap on one run
 *
 * The cursor advances only AFTER the fleet is launched, so a failed launch retries next tick.
 *
 * PRIORITY. Miss files whose key carries -p{N} are read first AND ignore BIO_LIVE_MIN_URLS: a push
 * somebody is waiting on must not sit here waiting for 2,000 other misses to accumulate. A sub-minimum
 * priority batch launches on a fleet sized to it (BIO_LIVE_PRIO_PER_SHARD, default 250 URLs/shard)
 * rather than the full BIO_LIVE_SHARDS.
 */
const { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);

const DRY = has('dry');
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const MISS_PREFIX = 'bio-miss/';
const CFG = process.env.CC_CONFIG_INDEX || 'cc_config';
const CURSOR_ID = 'bio_live_cursor';
const MIN_URLS = Number(arg('min', '')) || Number(process.env.BIO_LIVE_MIN_URLS) || 2000;
const SHARDS = Number(process.env.BIO_LIVE_SHARDS) || 12;
const MAX_URLS = Number(process.env.BIO_LIVE_MAX_URLS) || 400000;
const RUN = 'biolive-' + new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('bio-live: OPENSEARCH_ENDPOINT required'); process.exit(1); }

const s3 = new S3Client({ region: REGION });
const readBody = async (b) => { const c = []; for await (const x of b) c.push(x); return Buffer.concat(c).toString('utf8'); };

// bio-feed writes a priority miss list as bio-miss/{run}-p{N}-{count}.txt. Same convention as the
// monitor queue, read from the listing, and no marker means ordinary bulk work.
const prioOfKey = (k) => {
  const m = /-p([0-9])-\d+\.txt$/i.exec(String(k || ''));
  return m ? Number(m[1]) : 0;
};

(async () => {
  const openSearch = require('./opensearch');
  const osc = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);

  let cursor = '';
  try { cursor = ((await osc.get({ index: CFG, id: CURSOR_ID })).body || {})._source.cursor || ''; }
  catch (e) {
    const code = e && (e.statusCode || (e.meta && e.meta.statusCode));
    if (!(code === 404 || /404|not_found|index_not_found/i.test(String(e && e.message)))) throw e;
  }
  console.error(`bio-live ${RUN} — cursor ${cursor || '(none)'}`);

  let tok = null; const files = []; let newest = cursor;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: MISS_PREFIX, ContinuationToken: tok }));
    for (const o of (r.Contents || [])) {
      const iso = new Date(o.LastModified).toISOString();
      if (!cursor || iso > cursor) files.push({ key: o.Key, iso, prio: prioOfKey(o.Key) });
      if (!newest || iso > newest) newest = iso;
    }
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);
  files.sort((a, b) => (b.prio - a.prio) || a.iso.localeCompare(b.iso));
  const prioFiles = files.filter((f) => f.prio > 0).length;
  console.error(`  ${files.length} new miss file(s) since the cursor` + (prioFiles ? ` (${prioFiles} PRIORITY — read first)` : ''));
  if (!files.length) { console.error('  nothing to crawl — done.'); process.exit(0); }

  const urls = new Set();
  const prioUrls = new Set();
  const consumed = [];
  for (const f of files) {
    if (urls.size >= MAX_URLS) break;
    try {
      const txt = await readBody((await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: f.key }))).Body);
      for (const line of txt.split('\n')) {
        const u = line.trim();
        if (/^https?:\/\//i.test(u)) { urls.add(u); if (f.prio > 0) prioUrls.add(u); }
      }
    } catch (e) { console.error(`  read failed ${f.key}: ${e.message}`); }
    consumed.push(f);
  }
  console.error(`  ${urls.size.toLocaleString()} distinct URL(s)` + (prioUrls.size ? `, ${prioUrls.size.toLocaleString()} at priority` : ''));

  // The cursor may only advance over files this pass actually read. It stops at MAX_URLS and now reads
  // priority-first, so the newest file listed can be one that was never opened — and since nothing ever
  // looks below the cursor again, advancing past it would drop those URLs for good.
  const consumedKeys = new Set(consumed.map((f) => f.key));
  let minSkipped = '';
  for (const f of files) if (!consumedKeys.has(f.key) && (!minSkipped || f.iso < minSkipped)) minSkipped = f.iso;
  let safeCursor = newest;
  if (minSkipped) {
    safeCursor = cursor;
    for (const f of consumed) if (f.iso < minSkipped && f.iso > safeCursor) safeCursor = f.iso;
    console.error(`  ${files.length - consumed.length} file(s) not read this pass — cursor will hold at ${safeCursor || '(unchanged)'}`);
  }

  // Don't wake a fleet for a handful. They keep accumulating and the next tick reconsiders, so nothing
  // is dropped -- this only decides whether it is worth the machines yet.
  //
  // A PRIORITY batch is the exception, and it is the whole point: the archive usually does not hold a
  // page the extension has just found, so the miss list IS the extension's path, and this gate is what
  // would make it slow. 15 URLs would sit here waiting for 1,985 more to accumulate -- which, at the
  // rate misses actually arrive, can be days. Those launch immediately, on a fleet sized to the batch:
  // sharding is by domain, so 12 shards for 15 URLs is 11 machines that boot, hash nothing to
  // themselves and exit.
  // Size the fleet to the WORK, not to whether the gate was cleared. The sizing branch below hung off
  // `urls.size >= MIN_URLS`, and bio-pump invokes this with `--min 1`, so on the path that actually
  // runs continuously the condition was always true and the full SHARDS always launched: 12 machines
  // booted for 23 URLs of a single domain, 11 of which hashed nothing to themselves and exited.
  // Sharding is by domain, so the distinct-domain count is a hard ceiling on useful shards.
  const PER_SHARD = Number(process.env.BIO_LIVE_PRIO_PER_SHARD) || 250;
  const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./i, '').toLowerCase(); } catch (e) { return String(u); } };
  const domainCount = new Set([...urls].map(hostOf)).size;
  const shards = Math.max(1, Math.min(SHARDS, Math.ceil(urls.size / PER_SHARD), domainCount));
  if (!prioUrls.size && urls.size < MIN_URLS) {
    console.error(`  below BIO_LIVE_MIN_URLS (${MIN_URLS.toLocaleString()}) — leaving them to accumulate. Cursor NOT advanced.`);
    process.exit(0);
  }
  if (prioUrls.size && urls.size < MIN_URLS) {
    console.error(`  below BIO_LIVE_MIN_URLS (${MIN_URLS.toLocaleString()}) but ${prioUrls.size.toLocaleString()} URL(s) are priority — launching ${shards} shard(s) now`);
  }
  if (DRY) { console.error(`\n--dry: would launch ${shards} shard(s) over ${urls.size.toLocaleString()} URL(s). Cursor NOT advanced.`); process.exit(0); }
  if (!process.env.FLY_API_TOKEN) { console.error('  FLY_API_TOKEN not set — cannot launch. Cursor NOT advanced.'); process.exit(1); }

  // One combined list: live-fleet-shard downloads the whole file and keeps its own domain-hashed slice,
  // so the fleet size can change without re-splitting anything.
  const listKey = `bio-live-runs/${RUN}-${urls.size}.txt`;
  await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: listKey, Body: [...urls].join('\n') + '\n', ContentType: 'text/plain' }));
  console.error(`  wrote s3://${BUCKET}/${listKey}`);

  const { launchFleet, reapFleet } = require('./fleet-launch');
  await reapFleet({ namePrefix: 'live-fleet', log: () => {} });
  const started = await launchFleet({
    in: `s3://${BUCKET}/${listKey}`, shards, tag: RUN, namePrefix: 'live-fleet',
    log: (m) => console.error(m),
  });
  if (!started.length) { console.error('  no shard started — cursor NOT advanced, the next tick retries.'); process.exit(1); }
  console.error(`  ${started.length} shard(s) crawling ${urls.size.toLocaleString()} URL(s)`);

  await osc.index({ index: CFG, id: CURSOR_ID, refresh: true,
    body: { cursor: safeCursor, at: new Date().toISOString(),
            result: { files: consumed.length, listed: files.length, urls: urls.size,
                      priorityUrls: prioUrls.size, shards: started.length, list: listKey } } });
  console.error(`  cursor -> ${safeCursor}`);
  process.exit(0);
})().catch((e) => { console.error('bio-live ERR', e && e.stack || e); process.exit(1); });
