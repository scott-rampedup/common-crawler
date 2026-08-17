// Throughput-spike Lambda handler (us-east-1). Measures direct s3://commoncrawl WARC read rate.
// Uses the @aws-sdk/client-s3 bundled in the nodejs20.x runtime; no external deps → tiny zip.
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const zlib = require('zlib');
const s3 = new S3Client({});

async function streamToBuffer(body) {
  if (body && body.transformToByteArray) return Buffer.from(await body.transformToByteArray());
  const chunks = []; for await (const c of body) chunks.push(c); return Buffer.concat(chunks);
}

exports.handler = async (event) => {
  const pointers = event.pointers || [];
  const C = event.concurrency || 128;
  let ok = 0, empty = 0, bytes = 0, throttled = 0, othererr = 0;
  const errs = {};
  let idx = 0;
  const t0 = Date.now();
  async function worker() {
    for (;;) {
      const i = idx++; if (i >= pointers.length) return;
      const p = pointers[i];
      try {
        const start = Number(p.offset), end = start + Number(p.length) - 1;
        const res = await s3.send(new GetObjectCommand({ Bucket: 'commoncrawl', Key: p.filename, Range: `bytes=${start}-${end}` }));
        const gz = await streamToBuffer(res.Body);
        let raw; try { raw = zlib.gunzipSync(gz); } catch { empty++; continue; }
        bytes += raw.length; ok++;
      } catch (e) {
        const name = (e && (e.name || e.Code)) || 'err';
        const code = (e && e.$metadata && e.$metadata.httpStatusCode) || '';
        if (/SlowDown|throttl|503|429|ServiceUnavailable/i.test(name + code)) throttled++; else othererr++;
        errs[name + ':' + code] = (errs[name + ':' + code] || 0) + 1;
      }
    }
  }
  await Promise.all(Array.from({ length: C }, worker));
  const secs = (Date.now() - t0) / 1000;
  return { region: process.env.AWS_REGION, count: pointers.length, concurrency: C, secs,
    rate: +(pointers.length / secs).toFixed(1), ok, empty, throttled, othererr,
    mbps: +(bytes / 1048576 / secs).toFixed(1), errs };
};
