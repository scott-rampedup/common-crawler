/**
 * rebuild-bio-worklist.js — rebuild the REAL bio-URL work list from S3, using the contacts index as the
 * only authority on what is actually done.
 *
 *   OPENSEARCH_ENDPOINT=… node rebuild-bio-worklist.js [--out s3://bucket/key.txt] [--conc 16] [--dry-run]
 *
 * WHY THIS EXISTS
 * The queue_consumed ledger marks a queue object consumed when a drain READS it, not when its URLs have
 * been through CC resolution and the live crawl. Combined with BIO_ETL_LIVE=0 (misses were never crawled)
 * and fleet shards that die silently (Fly reports a dead shard as `stopped`, same as success), that marked
 * 7,966 objects / 32,312,633 URLs as done. Sampling 2,000 of those URLs across the whole history found 9
 * with contacts — 0.4%. The backlog was never processed; it was marked processed.
 *
 * So the ledger does not get a vote here. An URL is done when a contact exists for it, which is a fact we
 * can check and the ledger is a claim we cannot verify. This reads every queue object, drops URLs that
 * already have a contact, dedupes, and writes what is genuinely left.
 *
 * Safe to re-run: it derives everything from current state and writes a new list. It never deletes.
 */
const fs = require('fs');
const os = require('./opensearch');
const { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const num = (f, d) => Number(arg(f, '')) || d;
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const OUT = arg('--out', `s3://${BUCKET}/bio-worklist/worklist-${new Date().toISOString().slice(0, 10)}.txt`);
const CONC = num('--conc', 16);
const LIMIT_OBJ = num('--limit-objects', 0);
const DRY = process.argv.includes('--dry-run');
const TMP = '/tmp/bio-worklist.txt';

const s3 = new S3Client({ region: REGION });
const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);

// 32M URLs will not fit in a Set of strings on any sane heap, so dedupe on a 53-bit hash instead. Two
// 32-bit FNV variants combined: at 32M keys the expected collisions are ~0.06, i.e. effectively none, and
// a collision costs one skipped URL rather than anything structural.
function hash53(str) {
  let h1 = 0x811c9dc5, h2 = 0x01000193;
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    h1 ^= c; h1 = Math.imul(h1, 0x01000193);
    h2 = Math.imul(h2 ^ c, 0x85ebca6b);
  }
  return (h1 >>> 0) * 4194304 + (h2 >>> 11);
}

async function listObjects() {
  let tok = null; const keys = [];
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
    for (const o of (r.Contents || [])) keys.push(o.Key);
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);
  return keys.sort();
}

async function readObject(Key) {
  const r = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key }));
  const ch = []; for await (const x of r.Body) ch.push(x);
  return Buffer.concat(ch).toString('utf8');
}

// Which of these URLs already have a contact? This is the authority.
async function haveSet(urls) {
  const have = new Set();
  for (let i = 0; i < urls.length; i += 1024) {
    const chunk = urls.slice(i, i + 1024);
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const r = await client.search({ index: os.INDEX, body: { size: 0,
          query: { terms: { web_source_url: chunk } },
          aggs: { u: { terms: { field: 'web_source_url', size: chunk.length } } } } }, { requestTimeout: 120000 });
        for (const b of (((r.body || r).aggregations.u.buckets) || [])) have.add(b.key);
        break;
      } catch (e) {
        // Never swallow this into "no contacts found" -- that would put converted URLs back on the work
        // list. Retry, and on final failure throw so the run fails loudly instead of doubling the work.
        if (attempt === 2) throw new Error(`have-check failed after 3 tries: ${e.message}`);
        await new Promise((r2) => setTimeout(r2, 500 * (attempt + 1)));
      }
    }
  }
  return have;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const t0 = Date.now();
  console.error('══════ REBUILD BIO WORK LIST ══════');
  let keys = await listObjects();
  if (LIMIT_OBJ) keys = keys.slice(0, LIMIT_OBJ);
  console.error(`queue objects        : ${keys.length.toLocaleString()}`);

  const seen = new Set();
  let totalUrls = 0, dupes = 0, alreadyHave = 0, kept = 0, readErrors = 0, done = 0;
  const out = fs.createWriteStream(TMP);
  const write = (line) => new Promise((res) => { if (!out.write(line)) out.once('drain', res); else res(); });

  const handle = async (key) => {
    let text;
    try { text = await readObject(key); }
    catch (e) { readErrors++; console.error(`  read failed ${key}: ${e.message}`); return; }
    const urls = text.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    totalUrls += urls.length;
    const fresh = [];
    for (const u of urls) { const h = hash53(u); if (seen.has(h)) { dupes++; continue; } seen.add(h); fresh.push(u); }
    if (!fresh.length) return;
    const have = await haveSet(fresh);
    alreadyHave += have.size;
    const need = fresh.filter((u) => !have.has(u));
    kept += need.length;
    if (need.length) await write(need.join('\n') + '\n');
  };

  for (let i = 0; i < keys.length; i += CONC) {
    await Promise.all(keys.slice(i, i + CONC).map(handle));
    done = Math.min(i + CONC, keys.length);
    if (done % 500 < CONC) {
      const rate = done / Math.max(1, (Date.now() - t0) / 1000);
      console.error(`  ${done.toLocaleString()}/${keys.length.toLocaleString()} objects | ${totalUrls.toLocaleString()} URLs seen | ${kept.toLocaleString()} still to do | ETA ${Math.round((keys.length - done) / rate / 60)}m`);
    }
  }
  await new Promise((res) => out.end(res));

  const size = fs.statSync(TMP).size;
  console.error(`\n══════ DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  console.error(`  objects read       : ${(keys.length - readErrors).toLocaleString()}  (${readErrors} failed)`);
  console.error(`  URLs seen          : ${totalUrls.toLocaleString()}`);
  console.error(`  duplicates dropped : ${dupes.toLocaleString()}`);
  console.error(`  already have contact: ${alreadyHave.toLocaleString()}`);
  console.error(`  REAL WORK LIST     : ${kept.toLocaleString()} URL(s), ${(size / 1e6).toFixed(0)}MB`);

  if (DRY) { console.error('\ndry-run: not uploading.'); return; }
  const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(OUT);
  if (!m) { console.error(`\nwritten locally: ${TMP}`); return; }
  await s3.send(new PutObjectCommand({ Bucket: m[1], Key: m[2], Body: fs.createReadStream(TMP), ContentLength: size, ContentType: 'text/plain' }));
  console.error(`\nuploaded -> ${OUT}`);
  console.error(`next: node bio-etl.js --mode urls --in ${OUT} --live`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
