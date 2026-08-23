/**
 * split-worklist.js — split a large S3 URL list into chunk objects under a prefix.
 *
 * bio-etl reads an S3 key with getText(), which buffers the whole object into ONE string. Node caps a
 * string at ~512MB (0x1fffffe8 chars), so the 593MB rebuilt work list crashed it outright:
 *   ERR Cannot create a string longer than 0x1fffffe8 characters
 * Chunking also lets bio-etl treat the result as a normal prefix, the same shape as the monitor queue.
 */
const readline = require('readline');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i+1] && !process.argv[i+1].startsWith('--') ? process.argv[i+1] : d; };
const IN = arg('--in', ''), OUT = arg('--out', ''), PER = Number(arg('--per', '400000'));
const region = process.env.AWS_REGION || 'us-east-1';
const s3 = new S3Client({ region });
const parse = (u) => { const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(u); if (!m) throw new Error('bad s3 uri: ' + u); return { Bucket: m[1], Key: m[2] }; };
(async () => {
  const src = parse(IN), dst = parse(OUT.replace(/\/?$/, '/'));
  const r = await s3.send(new GetObjectCommand(src));
  const rl = readline.createInterface({ input: r.Body, crlfDelay: Infinity });
  let buf = [], n = 0, chunk = 0, total = 0;
  const flush = async () => {
    if (!buf.length) return;
    const key = `${dst.Key}part-${String(chunk).padStart(4, '0')}-${buf.length}.txt`;
    await s3.send(new PutObjectCommand({ Bucket: dst.Bucket, Key: key, Body: buf.join('\n') + '\n', ContentType: 'text/plain' }));
    console.error(`  ${key} (${buf.length.toLocaleString()} URLs)`);
    chunk++; buf = [];
  };
  for await (const line of rl) {
    const t = line.trim(); if (!t) continue;
    buf.push(t); n++; total++;
    if (n >= PER) { await flush(); n = 0; }
  }
  await flush();
  console.error(`\nDONE: ${total.toLocaleString()} URLs -> ${chunk} chunk(s) under s3://${dst.Bucket}/${dst.Key}`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
