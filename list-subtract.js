/**
 * list-subtract.js — remove every URL that appears in one or more ALREADY-ATTEMPTED lists.
 *
 *   node list-subtract.js --in s3://…/miss-new.txt --out s3://…/miss-todo.txt \
 *                         --minus s3://…/drain20260813/miss.txt[,s3://…/other.txt]
 *
 * skip-known can only recognise a URL that PRODUCED a contact, because the only record of a fetch is
 * web_source_url on the contact it created. Measured on the 2026-08-14 drain, that catches 304,831 of
 * 3,730,274 — 8.2% — while the previous night's fleet had actually attempted 2,563,533 of them. The
 * ~2.1M that were fetched and yielded nothing look brand new to the index, so the fleet would crawl them
 * a second time to learn the same thing: about 12 fleet-hours.
 *
 * The miss lists ARE the attempted lists, and they are already in S3 per run. Subtracting them is exact
 * and needs no new infrastructure. The durable fix is a crawl ledger that records every attempt with its
 * outcome — this is the version that works today, against runs that have already happened.
 *
 * A subtracted URL is never retried, so this is deliberately blunt: pass only lists whose crawl actually
 * COMPLETED. A shard that died mid-list did not attempt its tail, and subtracting it would silently
 * abandon those URLs.
 *
 * The minus set is held in memory as a Set of strings — ~250MB per 2.5M URLs. Everything else streams.
 */
const fs = require('fs');
const readline = require('readline');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('in', ''), OUT = arg('out', ''), MINUS = arg('minus', '');
const REGION = process.env.AWS_REGION || 'us-east-1';

const s3 = () => new S3Client({ region: REGION });
const parseS3 = (u) => { const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(u); return m ? { Bucket: m[1], Key: m[2] } : null; };
async function openRead(src) {
  const p = parseS3(src);
  if (p) return (await s3().send(new GetObjectCommand(p))).Body;
  return fs.createReadStream(src);
}
const urlOf = (t) => { if (!t.startsWith('{')) return t; try { return JSON.parse(t).url || ''; } catch (e) { return ''; } };

(async () => {
  if (!IN || !OUT || !MINUS) { console.error('need --in <list> --out <list> --minus <list[,list…]>'); process.exit(1); }

  const seen = new Set();
  for (const src of MINUS.split(',').map((s) => s.trim()).filter(Boolean)) {
    let n = 0;
    const rl = readline.createInterface({ input: await openRead(src), crlfDelay: Infinity });
    for await (const line of rl) { const u = urlOf(line.trim()); if (u) { seen.add(u); n++; } }
    console.error(`  minus ${src}: ${n.toLocaleString()} URL(s) (set now ${seen.size.toLocaleString()})`);
  }

  const outIsS3 = !!parseS3(OUT);
  const localOut = outIsS3 ? `/tmp/_subtract-${process.pid}.txt` : OUT;
  const out = fs.createWriteStream(localOut);
  let total = 0, dropped = 0, kept = 0;
  const rl = readline.createInterface({ input: await openRead(IN), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim(); if (!t) continue;
    total++;
    const u = urlOf(t);
    if (u && seen.has(u)) { dropped++; continue; }
    kept++;
    if (!out.write(t + '\n')) await new Promise((r) => out.once('drain', r));
    if (total % 500000 === 0) console.error(`  scanned ${total.toLocaleString()} | dropped ${dropped.toLocaleString()} | keeping ${kept.toLocaleString()}`);
  }
  await new Promise((r) => out.end(r));

  if (outIsS3) {
    const st = fs.statSync(localOut);
    await s3().send(new PutObjectCommand({ ...parseS3(OUT), Body: fs.createReadStream(localOut), ContentLength: st.size, ContentType: 'text/plain' }));
    fs.unlinkSync(localOut);
  }
  console.error(`\n${kept.toLocaleString()} of ${total.toLocaleString()} URL(s) remain (${dropped.toLocaleString()} already attempted) -> ${OUT}`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
