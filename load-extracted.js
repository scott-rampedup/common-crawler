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
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const os = require('./opensearch');

(async () => {
  const prefix = process.argv[2] || 'cc-extracted/';
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${process.env.AWS_REGION || 'us-east-1'}`;
  const region = process.env.AWS_REGION || 'us-east-1';
  const s3 = new S3Client({ region });
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const CONC = Number(process.env.LOAD_CONC) || 24;
  console.error(`loading s3://${bucket}/${prefix} -> OpenSearch (conc ${CONC})`);

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
        for (let i = 0; i < docs.length; i += 2000) {
          const res = await os.bulkUpsert(client, docs.slice(i, i + 2000));
          errs += res.errors;
        }
        total += docs.length;
      } catch (e) { errs++; }
      if (++files % 1000 === 0) console.error(`  ${files.toLocaleString()}/${keys.length.toLocaleString()} files, ${total.toLocaleString()} docs`);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(CONC, keys.length)) }, worker));
  console.error(`DONE: ${files.toLocaleString()} file(s), ${total.toLocaleString()} docs -> OpenSearch, ${errs} error(s), ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('load error:', e.message); process.exit(1); });
