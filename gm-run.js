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
  step('Phase 2/3/5 — group by domain, resolve HQ, build contacts', 'gm-build.js',
    ['--in', path.join(DIR, 'gm-locations.ndjson'), '--out', DIR]);
  step('Phase 4 — apply to OpenSearch', 'gm-upsert.js', ['--in', DIR]);

  console.error(`\n══════ GM ETL DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  for (const f of ['gm-locations.ndjson', 'gm-bio-urls.txt', 'gm-hq.ndjson', 'gm-loc.ndjson', 'gm-contacts.ndjson']) {
    const p = path.join(DIR, f);
    if (fs.existsSync(p)) console.error(`  ${f.padEnd(24)} ${(fs.statSync(p).size / 1e6).toFixed(1)}MB`);
  }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
