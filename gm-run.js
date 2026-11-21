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

// Queueing the bio URLs lives in queue-bio-urls.js now, because overture-run.js needed the identical
// hand-off and grew the identical bug without it: a producer that writes a URL file, prints where it
// is, and passes it to nobody. The reasoning that was here -- app push for a named receipt in the Data
// Importer, direct S3 write as the fallback so a batch never depends on the app being up, and both the
// byte cap and the 20,000-URL cap honoured -- moved with it.
const { queueBioUrls } = require('./queue-bio-urls');

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
  await queueBioUrls(path.join(DIR, 'gm-bio-urls.txt'), { source: 'Google Maps', tag: 'gm', noQueue: NO_QUEUE });
  if (ONLY === 'load') { console.error('\n--only load: stopping after Phase 1.'); return; }

  step('Phase 2/3/5 — group by domain, resolve HQ, build contacts', 'gm-build.js',
    ['--in', path.join(DIR, 'gm-locations.ndjson'), '--out', DIR]);
  step('Phase 4 — apply to OpenSearch', 'gm-upsert.js', ['--in', DIR]);

  // Phase 5 — the standing procedure for every Maps export: one BIO URL list per file, pushed under
  // that file's name, and the pages labelled BIO URL in the Company Crawler. It runs AFTER the upsert
  // because the labelling step needs the Location records to exist before it can label them.
  //
  // This finds more than the Phase-1 queue does: that one takes the team-profile column, while this
  // also takes a row's OWN page when that page is a person page — 40,803 against 19,049 on the 26-10
  // export. Both run: Phase 1 queues early so the URLs are durable before anything else can fail, and
  // the queue de-duplicates by URL anyway.
  step('Phase 5 — per-file BIO URL list, queued and labelled', 'gm-bio-scan.js',
    ['--src', DIR, '--out', path.join(DIR, 'bio-lists'), '--push', '--label']);

  console.error(`\n══════ GM ETL DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  for (const f of ['gm-locations.ndjson', 'gm-bio-urls.txt', 'gm-hq.ndjson', 'gm-loc.ndjson', 'gm-contacts.ndjson']) {
    const p = path.join(DIR, f);
    if (fs.existsSync(p)) console.error(`  ${f.padEnd(24)} ${(fs.statSync(p).size / 1e6).toFixed(1)}MB`);
  }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
