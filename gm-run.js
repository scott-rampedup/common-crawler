/**
 * gm-run.js — run the whole Google-Maps ETL on one machine, from an S3-staged export.
 *
 *   OPENSEARCH_ENDPOINT=… node gm-run.js --s3 s3://bucket/google-maps/export.csv [--limit N]
 *
 * The exports are ~480MB and .dockerignore keeps CSVs out of the image, so the file is staged in S3 and
 * pulled to local disk here. gm-load needs a real path — it streams the file with a state-machine parser
 * and scans a directory for *.csv — so this downloads into an otherwise empty scratch dir rather than
 * trying to hand it a stream.
 *
 * Chains the phases that already exist rather than reimplementing them:
 *   gm-load   -> gm-locations.ndjson + gm-bio-urls.txt
 *   gm-build  -> gm-hq.ndjson / gm-loc.ndjson / gm-contacts.ndjson
 *   gm-upsert -> applies all three to OpenSearch
 *
 * --backfill-hq is deliberately NOT passed. It tags EVERY existing company company_type=HQ via
 * update_by_query, which would undo the HQ/Affiliate distinction: HQ is per-webpage, not per-domain
 * (adobe.com holds 13), and 281,691 records are already slated to demote to Affiliate. Re-tagging
 * everything HQ would erase that and is not recoverable from the records themselves.
 */
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const S3 = arg('--s3', '');
const DIR = arg('--dir', '/tmp/gm');
const LIMIT = arg('--limit', '');
const ONLY = arg('--only', '');            // 'load' = Phase 1 only (regenerate + queue the bio URLs)
const NO_QUEUE = process.argv.includes('--no-queue');
const QUEUE_PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';

// Queue the bio URLs the parse found, instead of leaving them on a machine that is about to be destroyed.
//
// gm-load writes gm-bio-urls.txt and nothing ever read it. Two runs produced bio URLs -- 14,084 from the
// 17-08 export and ~14,000 more from 24-08 -- and both sets died with their machine's /tmp. They are the
// most valuable output of the parse: pages naming a person at a business we now hold a record for.
async function queueBioUrls(file) {
  if (NO_QUEUE || !fs.existsSync(file)) return;
  const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  if (!lines.length) { console.error('  no bio URLs to queue'); return; }
  const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
  const region = process.env.AWS_REGION || 'us-east-1';
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
  const s3 = new S3Client({ region });
  let objects = 0, failed = 0;
  for (let i = 0; i < lines.length; i += 3000) {
    const chunk = lines.slice(i, i + 3000);
    const key = `${QUEUE_PREFIX}${new Date().toISOString().replace(/[:.]/g, '-')}-gm-${chunk.length}.txt`;
    try {
      await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: chunk.join('\n') + '\n', ContentType: 'text/plain' }));
      objects++;
    } catch (e) { failed++; console.error(`  queue write FAILED: ${e.message}`); }
    await new Promise((r) => setTimeout(r, 5));      // distinct timestamps in the key
  }
  console.error(`  queued ${lines.length.toLocaleString()} bio URL(s) -> ${objects} object(s) under ${QUEUE_PREFIX}${failed ? ` (${failed} FAILED)` : ''}`);
}

function step(label, script, args) {
  console.error(`\n══════ ${label} · ${new Date().toISOString().slice(11, 19)} ══════`);
  const r = spawnSync(process.execPath, [path.join(__dirname, script), ...args], {
    stdio: 'inherit', cwd: __dirname, env: process.env,
  });
  if (r.status !== 0) { console.error(`✗ ${script} exited ${r.status}`); process.exit(r.status || 1); }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  fs.mkdirSync(DIR, { recursive: true });

  if (S3) {
    const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(S3);
    if (!m) { console.error('bad --s3 uri'); process.exit(1); }
    const dest = path.join(DIR, path.basename(m[2]));
    if (fs.existsSync(dest) && fs.statSync(dest).size > 0) {
      console.error(`already downloaded: ${dest} (${(fs.statSync(dest).size / 1e6).toFixed(0)}MB)`);
    } else {
      const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
      console.error(`downloading ${S3} …`);
      const r = await new S3Client({ region: process.env.AWS_REGION || 'us-east-1' })
        .send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }));
      await new Promise((res, rej) => { const w = fs.createWriteStream(dest); r.Body.pipe(w); w.on('finish', res); w.on('error', rej); r.Body.on('error', rej); });
      console.error(`  ${(fs.statSync(dest).size / 1e6).toFixed(0)}MB -> ${dest}`);
    }
  }

  const t0 = Date.now();
  step('Phase 1 — parse the export into Location records', 'gm-load.js',
    ['--src', DIR, '--out', DIR, ...(LIMIT ? ['--limit', LIMIT] : [])]);
  // Queue immediately after the parse, so the URLs are durable before anything else can fail.
  await queueBioUrls(path.join(DIR, 'gm-bio-urls.txt'));
  if (ONLY === 'load') { console.error('\n--only load: stopping after Phase 1.'); return; }

  step('Phase 2/3/5 — group by domain, resolve HQ, build contacts', 'gm-build.js',
    ['--in', path.join(DIR, 'gm-locations.ndjson'), '--out', DIR]);
  step('Phase 4 — apply to OpenSearch', 'gm-upsert.js', ['--in', DIR]);

  console.error(`\n══════ GM ETL DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  for (const f of ['gm-locations.ndjson', 'gm-bio-urls.txt', 'gm-hq.ndjson', 'gm-loc.ndjson', 'gm-contacts.ndjson']) {
    const p = path.join(DIR, f);
    if (fs.existsSync(p)) console.error(`  ${f.padEnd(24)} ${(fs.statSync(p).size / 1e6).toFixed(1)}MB`);
  }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
