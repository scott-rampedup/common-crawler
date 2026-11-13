#!/usr/bin/env node
//
// Queue the bio URLs that pp-bio-import.js collected, for the nightly bio-ETL drain.
//
// WHY NOT /api/bio-urls. That endpoint is the INTERACTIVE door -- it caps at 20,000 per request and
// mints a Data Importer receipt per call, which is right for a person standing on a page waiting for
// a contact to appear, and wrong for 1.6M URLs nobody is watching. The bulk producers (the sitemap
// monitor, cc-bio-discover) write objects under the monitor-queue prefix instead and let
// `bio-etl --mode urls` drain them. This does the same thing, in the same key format, so backlog.js
// can still recover the URL count from the key with /-(\d+)\.txt$/.
//
// PRIORITY 0, DELIBERATELY. An extension push goes to the front because someone is waiting. This is
// bulk: hundreds of thousands of URLs that must stay strictly FIFO, or one run of this puts every
// later interactive push behind it -- which is exactly what a Maps export did once, stranding every
// subsequent push behind 863,965 URLs.
//
// Re-running is safe. The drain keeps a per-URL crawl_log and skips what it has already fetched, so
// a second pass over a file that has grown costs an S3 put and nothing else.
//
// Usage:
//   node pp-queue-urls.js --file /data/pp-bio-urls.txt            # dry run
//   node pp-queue-urls.js --file /data/pp-bio-urls.txt --write
const fs = require('fs');
const readline = require('readline');

const argv = process.argv.slice(2);
const flag = (n, d) => {
  const i = argv.indexOf('--' + n);
  return i >= 0 ? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true) : d;
};
const FILE = String(flag('file', '/data/pp-bio-urls.txt'));
const WRITE = !!flag('write', false);
const CHUNK = Math.max(1000, Number(flag('chunk', 50000)));
const LABEL = String(flag('label', 'Public Prospects: domain-aligned bio URLs'));
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';

(async () => {
  if (!fs.existsSync(FILE)) { console.error('no such file: ' + FILE); process.exit(1); }

  const region = process.env.AWS_REGION || 'us-east-1';
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
  let s3 = null, PutObjectCommand = null;
  if (WRITE) {
    const { S3Client, PutObjectCommand: P } = require('@aws-sdk/client-s3');
    s3 = new S3Client({ region });
    PutObjectCommand = P;
  }

  const seen = new Set();
  let batch = [];
  let total = 0, dupes = 0, bad = 0, objects = 0, queued = 0;

  async function flush() {
    if (!batch.length) return;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    // Key shape mirrors queueBioUrls: prefix + stamp + optional -p<priority> + -<count>.txt.
    // The count rides in the key so backlog.js never has to GET the object just to size the queue.
    const key = `${PREFIX}${stamp}-pp-${batch.length}.txt`;
    if (WRITE) {
      await s3.send(new PutObjectCommand({
        Bucket: bucket, Key: key, Body: batch.join('\n') + '\n', ContentType: 'text/plain' }));
    }
    objects++; queued += batch.length;
    console.log(`  ${WRITE ? 'put' : 'would put'} ${String(batch.length).padStart(7)} URL(s) -> s3://${bucket}/${key}`);
    batch = [];
  }

  const rl = readline.createInterface({ input: fs.createReadStream(FILE), crlfDelay: Infinity });
  for await (const line of rl) {
    const u = line.trim();
    if (!u) continue;
    total++;
    if (!/^https?:\/\/[^\s/]+\.[^\s]/i.test(u)) { bad++; continue; }
    if (seen.has(u)) { dupes++; continue; }
    seen.add(u);
    batch.push(u);
    if (batch.length >= CHUNK) await flush();
  }
  await flush();

  console.log('');
  console.log(`file                 ${FILE}`);
  console.log(`lines read           ${total}`);
  console.log(`  duplicates dropped ${dupes}`);
  console.log(`  not http(s)        ${bad}`);
  console.log(`${WRITE ? 'QUEUED' : 'would queue'}             ${queued} URL(s) in ${objects} object(s) at priority 0`);
  console.log(`label                ${LABEL}`);
  if (!WRITE) console.log('\n(dry run -- pass --write to actually queue)');
})().catch((e) => { console.error('FAILED:', e.message); process.exit(1); });
