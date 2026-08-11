/**
 * cc-s3.js — fetch a Common Crawl WARC record DIRECTLY from s3://commoncrawl (us-east-1).
 * --------------------------------------------------------------------------------------
 * The AWS-native replacement for cc-engine.fetchWarc (which ranges over data.commoncrawl.org /
 * CloudFront and is rate-shaped to ~10 req/s per egress IP). CC's WARCs live in the PUBLIC
 * `commoncrawl` bucket in us-east-1 (AWS Open Data); an in-region S3 GetObject with a byte Range is
 * fast, un-CloudFronted, and scales per-prefix — so run extraction *inside* us-east-1 (Lambda/Fargate)
 * and read the archive directly.
 *
 * Identical output contract to cc-engine.fetchWarc: returns the HTML body string (or '' when the
 * record is missing/blank). The gunzip + WARC->HTML parse is reused from cc-engine (warcToHtml), so
 * the whole extractor pipeline downstream is unchanged.
 *
 *   const { makeCcS3 } = require('./cc-s3');
 *   const fetchWarc = makeCcS3();                 // fetchWarc(ptr) -> html
 *   const html = await fetchWarc({ url, filename, offset, length });
 */
const zlib = require('zlib');
const { warcToHtml } = require('./cc-engine');

const CC_BUCKET = process.env.CC_BUCKET || 'commoncrawl';

// Collect a readable stream (S3 GetObject Body) into one Buffer.
async function streamToBuffer(body) {
  if (body && typeof body.transformToByteArray === 'function') {
    return Buffer.from(await body.transformToByteArray());        // SDK v3 browser/node helper
  }
  const chunks = [];
  for await (const c of body) chunks.push(c);
  return Buffer.concat(chunks);
}

// Build a fetchWarc(ptr)->html backed by direct S3 range reads. Pass an S3Client (so a Lambda can
// reuse one warm client); otherwise one is created from the default chain in the given region.
function makeCcS3(opts = {}) {
  let s3 = opts.s3;
  const region = opts.region || process.env.AWS_REGION || 'us-east-1';
  const bucket = opts.bucket || CC_BUCKET;
  const requestPays = opts.requestPays || false;
  if (!s3) {
    const { S3Client } = require('@aws-sdk/client-s3');
    // The SDK's default socket pool is 50, which silently caps WARC fetch concurrency no matter what
    // CONC says. Size the pool above the intended concurrency so the fetch pump is the only limit.
    const maxSockets = Number(process.env.S3_MAX_SOCKETS) || 256;
    let requestHandler;
    try {
      const { NodeHttpHandler } = require('@smithy/node-http-handler');
      const https = require('https');
      requestHandler = new NodeHttpHandler({ httpsAgent: new https.Agent({ maxSockets, keepAlive: true }) });
    } catch (e) { /* older SDK layout — fall back to the default handler */ }
    s3 = new S3Client({ region, ...(requestHandler ? { requestHandler } : {}), maxAttempts: 5 });
  }
  const { GetObjectCommand } = require('@aws-sdk/client-s3');

  return async function fetchWarcS3(ptr) {
    const start = Number(ptr.offset);
    const end = start + Number(ptr.length) - 1;
    if (!ptr.filename || !Number.isFinite(start) || !Number.isFinite(end) || end < start) return '';
    const cmd = new GetObjectCommand({
      Bucket: bucket,
      Key: ptr.filename,
      Range: `bytes=${start}-${end}`,
      ...(requestPays ? { RequestPayer: 'requester' } : {}),
    });
    let res;
    try {
      res = await s3.send(cmd);
    } catch (e) {
      const code = (e && e.$metadata && e.$metadata.httpStatusCode) || 0;
      const name = (e && (e.name || e.Code)) || 'error';
      if (code === 404 || /NoSuchKey|InvalidRange/i.test(name)) return '';   // record gone -> caller marks done
      // surface the HTTP status so the worker's transient-retry (403/429/5xx) backs off + retries throttles
      throw new Error(`s3 ${code || name} for ${ptr.url}: ${e && e.message}`);
    }
    const gz = await streamToBuffer(res.Body);
    // WARC records are per-record gzip members; gunzipSync stops at the first member's end.
    let raw;
    try { raw = zlib.gunzipSync(gz); }
    catch { return ''; }                    // truncated/again — treat as no HTML (caller marks done)
    return warcToHtml(raw);
  };
}

module.exports = { makeCcS3, streamToBuffer };
