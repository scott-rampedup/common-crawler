/**
 * load-extracted.js — bulk-load Lambda-extracted contact JSONL from S3 into OpenSearch.
 * -------------------------------------------------------------------------------------------------
 * The cc-extract Lambdas write extracted contact records (display-field JSONL) to S3; this reads them
 * and score-gated-upserts them into the OpenSearch production store (dedup by email, best record wins).
 * The S3 JSONL is the durable source of the Lambda-extracted data (it never passes through Postgres),
 * so OpenSearch = union of fleet contacts (via the PG delta-sync) + Lambda contacts (via this loader).
 *
 *   OPENSEARCH_ENDPOINT=… OUT_BUCKET=… node load-extracted.js [s3-prefix]
 *
 * Idempotent — safe to re-run. Pass a per-run prefix (e.g. cc-extracted/2026-21/) to load one run.
 */
const https = require('https');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const os = require('./opensearch');

(async () => {
  const prefix = process.argv[2] || 'cc-extracted/';
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${process.env.AWS_REGION || 'us-east-1'}`;
  const region = process.env.AWS_REGION || 'us-east-1';
  // Keep-alive so the many S3 GETs across a crawl reuse a small socket pool instead of churning
  // short-lived TCP connections (which exhausted Windows ephemeral ports and EACCES'd the next crawl).
  const s3 = new S3Client({ region, requestHandler: new NodeHttpHandler({ httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 64 }) }) });
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const CONC = Number(process.env.LOAD_CONC) || 8;   // OpenSearch write-throttles above ~8 concurrent bulk streams
  console.error(`loading s3://${bucket}/${prefix} -> OpenSearch (conc ${CONC})`);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  // bulkUpsert with exponential backoff: a 429 (write-queue full) throws, so retry rather than silently
  // dropping the file's records — the failure mode that lost ~97% of a 4,847-file load run at conc 32.
  async function upsertRetry(docs) {
    for (let attempt = 0; ; attempt++) {
      try { return (await os.bulkUpsert(client, docs)).errors || 0; }
      catch (e) { if (attempt >= 6) throw e; await sleep(Math.min(16000, 500 * 2 ** attempt)); }
    }
  }

  // List every object under the prefix first, then load them through a concurrency pool — the per-file
  // S3 GET + bulkUpsert is the pace-limiter, so parallelizing it turns a ~45-min sequential load into minutes.
  const keys = [];
  let token;
  do {
    const list = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token }));
    for (const obj of (list.Contents || [])) if (obj.Key.endsWith('.jsonl')) keys.push(obj.Key);
    token = list.IsTruncated ? list.NextContinuationToken : null;
  } while (token);
  console.error(`${keys.length.toLocaleString()} file(s) to load`);

  let idx = 0, files = 0, total = 0, errs = 0;
  const t0 = Date.now();
  async function worker() {
    for (;;) {
      const k = idx++; if (k >= keys.length) return;
      const key = keys[k];
      try {
        const r = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const body = await r.Body.transformToString();
        const now = new Date().toISOString();
        const docs = body.trim().split('\n').filter(Boolean)
          .map((l) => { try { return os.recordToDoc(JSON.parse(l), now); } catch (e) { return null; } })
          .filter((d) => d && d.email);
        for (let i = 0; i < docs.length; i += 2000) errs += await upsertRetry(docs.slice(i, i + 2000));
        total += docs.length;
      } catch (e) { errs++; }
      if (++files % 1000 === 0) console.error(`  ${files.toLocaleString()}/${keys.length.toLocaleString()} files, ${total.toLocaleString()} docs`);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(CONC, keys.length)) }, worker));
  console.error(`DONE: ${files.toLocaleString()} file(s), ${total.toLocaleString()} docs -> OpenSearch, ${errs} error(s), ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('load error:', e.message); process.exit(1); });
