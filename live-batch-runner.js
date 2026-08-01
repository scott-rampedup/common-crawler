/**
 * live-batch-runner.js — dedicated live-crawl runner for a batch of not-in-CC bio URLs.
 * ------------------------------------------------------------------------------------
 * Meant to run on an ISOLATED Fly machine (fly machine run), separate from the web app, so a
 * multi-hour/day live-crawl doesn't compete with production traffic. Uses the app's env: the Evomi
 * proxy (PROXY_URL / PROXY_FALLBACK_URL), OPENSEARCH_ENDPOINT, and AWS creds (for S3).
 *
 * Downloads a URL list from S3, records a clean OpenSearch before/after count (so we can measure
 * net-new), then hands the list to extract-from-pointers.js --live (Evomi residential -> Master DB,
 * Source "Live Crawl"). Idempotent: extract-from-pointers upserts by email.
 *
 *   S3_KEY=live-candidates/batch-1m.txt TAG=live-batch1 LIVE_CONC=150 node live-batch-runner.js
 */
const fs = require('fs');
const { execFileSync } = require('child_process');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const os = require('./opensearch');

(async () => {
  const region = process.env.AWS_REGION || 'us-east-1';
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
  const key = process.env.S3_KEY;
  const tag = process.env.TAG || 'live-batch';
  if (!key) { console.error('need S3_KEY'); process.exit(1); }
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }

  // 1) fetch the URL list from S3
  const s3 = new S3Client({ region });
  const o = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bodyStr = await o.Body.transformToString();
  const file = '/tmp/live-urls.txt';
  fs.writeFileSync(file, bodyStr);
  const n = bodyStr.split('\n').filter(Boolean).length;
  console.log(`[runner] downloaded ${n.toLocaleString()} URLs from s3://${bucket}/${key} (LIVE_CONC=${process.env.LIVE_CONC || 4})`);

  // 2) clean before-count
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const before = (await client.count({ index: 'contacts' })).body.count;
  const t0 = Date.now();
  console.log(`[runner] OpenSearch BEFORE: ${before.toLocaleString()}  @ ${new Date().toISOString()}`);

  // 3) live-crawl (blocks until done; progress streams from extract-from-pointers)
  try {
    execFileSync('node', ['extract-from-pointers.js', '--live', file, '--tag', tag],
      { stdio: 'inherit', env: process.env });
  } catch (e) { console.error('[runner] extract-from-pointers exited non-zero:', e.status || e.message); }

  // 4) after-count -> net-new
  await client.indices.refresh({ index: 'contacts' }).catch(() => {});
  const after = (await client.count({ index: 'contacts' })).body.count;
  const mins = ((Date.now() - t0) / 60000).toFixed(1);
  console.log(`[runner] OpenSearch AFTER:  ${after.toLocaleString()}  (NET-NEW +${(after - before).toLocaleString()}) in ${mins} min`);
})().catch((e) => { console.error('[runner] error:', e.message); process.exit(1); });
