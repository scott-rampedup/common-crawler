const fs = require('fs');
const { S3Client, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand,
        AbortMultipartUploadCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const FILE = process.argv[2];
const KEY = 'google-maps/' + require('path').basename(FILE);
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || ('aws-athena-query-results-475987770186-' + REGION);
const PART = 32 * 1024 * 1024, CONC = 4;
(async () => {
  const s3 = new S3Client({ region: REGION });
  const size = fs.statSync(FILE).size;
  try { const h = await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: KEY }));
    if (h.ContentLength === size) { console.log('already staged — skipping'); console.log('S3URI s3://' + BUCKET + '/' + KEY); return; }
  } catch (e) {}
  const nParts = Math.ceil(size / PART);
  console.log('uploading ' + (size/1e6).toFixed(0) + 'MB as ' + nParts + ' parts');
  const cmu = await s3.send(new CreateMultipartUploadCommand({ Bucket: BUCKET, Key: KEY, ContentType: 'text/csv' }));
  const uid = cmu.UploadId; const done = []; let next = 0, fin = 0;
  const fd = fs.openSync(FILE, 'r');
  try {
    const worker = async () => { for (;;) {
      const i = next++; if (i >= nParts) return;
      const off = i * PART, len = Math.min(PART, size - off);
      const buf = Buffer.allocUnsafe(len); fs.readSync(fd, buf, 0, len, off);
      for (let a = 0; ; a++) { try {
        const r = await s3.send(new UploadPartCommand({ Bucket: BUCKET, Key: KEY, UploadId: uid, PartNumber: i+1, Body: buf }));
        done[i] = { ETag: r.ETag, PartNumber: i+1 }; fin++;
        if (fin % 4 === 0 || fin === nParts) console.log('  ' + fin + '/' + nParts + ' parts');
        break;
      } catch (e) { if (a >= 4) throw e; await new Promise(r=>setTimeout(r, 1000 * 2 ** a)); } }
    } };
    await Promise.all(Array.from({ length: Math.min(CONC, nParts) }, worker));
    await s3.send(new CompleteMultipartUploadCommand({ Bucket: BUCKET, Key: KEY, UploadId: uid,
      MultipartUpload: { Parts: done.sort((a,b)=>a.PartNumber-b.PartNumber) } }));
    const h = await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: KEY }));
    console.log('COMPLETE — S3 ' + h.ContentLength + (h.ContentLength === size ? ' (matches local)' : ' MISMATCH vs ' + size));
    console.log('S3URI s3://' + BUCKET + '/' + KEY);
  } catch (e) { await s3.send(new AbortMultipartUploadCommand({ Bucket: BUCKET, Key: KEY, UploadId: uid })).catch(()=>{}); throw e; }
  finally { fs.closeSync(fd); }
})().catch(e => { console.log('ERR ' + e.name + ': ' + e.message); process.exit(1); });
