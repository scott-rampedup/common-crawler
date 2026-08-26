/**
 * bio-backlog-count.js — compute the TRUE bio-URL backlog and cache it for the UI.
 *
 *   OPENSEARCH_ENDPOINT=… node bio-backlog-count.js [--conc 24] [--quiet]
 *
 * WHY: the Data Importer's backlog figure counts every URL in every queue object that is not marked
 * verified-complete. That reads 13,495,935 when the genuine remaining work is about 4,000,000, because
 * most of those URLs are already known individually -- they have a contact, or a crawl-ledger entry saying
 * the page was fetched and had no person on it -- while the OBJECT they live in is not yet 100% done and
 * so is still counted whole.
 *
 * A number that overstates the work by 3x is not a progress indicator, it is a source of alarm. This has
 * already gone wrong once here: the counter reported 10,398,933 URLs when 97,504 were unprocessed.
 *
 * The honest number needs a per-URL check, which is minutes of work, not milliseconds -- so it cannot run
 * on a page load. This computes it out of band and writes it to cc_config; the API serves the cached value
 * with the time it was measured, so the UI shows a real number and says how fresh it is.
 */
const os = require('./opensearch');
const { knownSet } = require('./skip-known');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const CONC = Math.max(1, Number(arg('--conc', '')) || 24);
const QUIET = process.argv.includes('--quiet');
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const CFG = process.env.CC_CONFIG_INDEX || 'cc_config';
const DOC_ID = 'bio_backlog_count';

async function computeBacklog({ client, log = () => {} } = {}) {
  const c = client || os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const s3 = new S3Client({ region: REGION });
  const t0 = Date.now();

  let tok = null; const keys = []; const sizes = new Map();
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
    for (const o of (r.Contents || [])) { keys.push(o.Key); sizes.set(o.Key, o.Size || 0); }
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);

  // Objects already retired by the verified-complete ledger need no per-URL work.
  const consumed = new Set();
  for (let i = 0; i < keys.length; i += 1000) {
    try {
      const r = await c.mget({ index: process.env.QUEUE_CONSUMED_INDEX || 'queue_consumed',
        body: { ids: keys.slice(i, i + 1000) }, _source: false });
      for (const d of (((r.body || r).docs) || [])) if (d && d.found) consumed.add(d._id);
    } catch (e) { /* ledger absent -> nothing retired */ }
  }
  const pendingKeys = keys.filter((k) => !consumed.has(k));
  const rawUrls = pendingKeys.reduce((n, k) => n + Number((/-(\d+)\.txt$/.exec(k) || [0, 0])[1]), 0);
  log(`  ${keys.length.toLocaleString()} objects, ${pendingKeys.length.toLocaleString()} not retired => ${rawUrls.toLocaleString()} URLs counted today`);

  let seen = 0, known = 0, unique = 0, readErrors = 0, done = 0;
  const dedupe = new Set();          // 53-bit hashes: the same URL is queued on many nights
  const hash = (str) => { let h1 = 0x811c9dc5, h2 = 0x01000193;
    for (let i = 0; i < str.length; i++) { const ch = str.charCodeAt(i); h1 ^= ch; h1 = Math.imul(h1, 0x01000193); h2 = Math.imul(h2 ^ ch, 0x85ebca6b); }
    return (h1 >>> 0) * 4194304 + (h2 >>> 11); };

  const one = async (key) => {
    let urls;
    try {
      const r = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
      const ch = []; for await (const x of r.Body) ch.push(x);
      urls = Buffer.concat(ch).toString('utf8').split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    } catch (e) { readErrors++; return; }
    seen += urls.length;
    const fresh = urls.filter((u) => { const h = hash(u); if (dedupe.has(h)) return false; dedupe.add(h); return true; });
    if (!fresh.length) return;
    unique += fresh.length;
    try { const have = await knownSet(fresh, { client: c }); known += have.size; }
    catch (e) { /* unknown -> counted as outstanding, which overstates rather than hides */ }
  };

  for (let i = 0; i < pendingKeys.length; i += CONC) {
    await Promise.all(pendingKeys.slice(i, i + CONC).map(one));
    done = Math.min(i + CONC, pendingKeys.length);
    if (done % 2000 < CONC) {
      const rate = done / Math.max(1, (Date.now() - t0) / 1000);
      log(`  ${done.toLocaleString()}/${pendingKeys.length.toLocaleString()} objects | ${unique.toLocaleString()} unique | ${known.toLocaleString()} already known | ETA ${Math.round((pendingKeys.length - done) / Math.max(0.1, rate) / 60)}m`);
    }
  }

  const outstanding = Math.max(0, unique - known);
  const result = {
    computed_at: new Date().toISOString(),
    seconds: Math.round((Date.now() - t0) / 1000),
    objects_total: keys.length,
    objects_pending: pendingKeys.length,
    urls_counted: rawUrls,          // what the old headline showed
    urls_seen: seen,
    urls_unique: unique,            // after de-duplicating across nights
    urls_known: known,              // already a contact, or already crawled
    urls_outstanding: outstanding,  // THE number: genuine remaining work
    read_errors: readErrors,
  };
  // Keep a HISTORY, not just the latest reading.
  //
  // A single number answers none of the questions a queue counter exists for. "2,168,551" looks identical
  // whether the queue is draining, growing or frozen, which is exactly how a seven-hour drain outage went
  // unnoticed. With a series we can report the direction and the rate, which is what actually tells you
  // whether the pipeline is working.
  try {
    let history = [];
    try { const g = await c.get({ index: CFG, id: DOC_ID }); history = ((g.body || g)._source || {}).history || []; }
    catch (e) { /* first run */ }
    history.unshift({ at: result.computed_at, unique: result.urls_unique, known: result.urls_known, outstanding: result.urls_outstanding });
    result.history = history.slice(0, 180);            // ~a week of hourly readings
    await c.index({ index: CFG, id: DOC_ID, body: result, refresh: true });
  } catch (e) { log(`  could not cache the count: ${e.message}`); }
  return result;
}

module.exports = { computeBacklog, DOC_ID };

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const log = QUIET ? () => {} : (m) => console.error(m);
  console.error('══════ BIO BACKLOG (true count) ══════');
  const r = await computeBacklog({ log });
  console.error(`\n  counted by the old headline : ${r.urls_counted.toLocaleString()}`);
  console.error(`  unique across all objects   : ${r.urls_unique.toLocaleString()}`);
  console.error(`  already known               : ${r.urls_known.toLocaleString()}`);
  console.error(`  OUTSTANDING                 : ${r.urls_outstanding.toLocaleString()}`);
  console.error(`  computed in ${r.seconds}s, cached to ${CFG}/${DOC_ID}`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
