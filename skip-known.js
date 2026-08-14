/**
 * skip-known.js — drop URLs the Master DB already has. Module + CLI.
 *
 *   node skip-known.js --in s3://bucket/…/miss.txt --out s3://bucket/…/miss-new.txt
 *   node skip-known.js --in /tmp/miss.txt --out /tmp/miss-new.txt
 *
 * bio-etl applied this filter to the Common Crawl pointer list ONLY, never to the miss list — which is
 * backwards. A CC pointer costs an S3 range read inside a Lambda; a miss costs a live proxied fetch, the
 * single most expensive operation in the pipeline. On monitor output the miss list is also the larger
 * side by far (82.6% of the 2026-08-14 queue). Re-running a drain therefore re-crawled every URL already
 * done: 2,563,533 pages, ~13 fleet-hours, for nothing.
 *
 * The filter is `web_source_url`, which extract-from-pointers sets on every contact it writes. That means
 * it only recognises URLs that PRODUCED a contact — a page fetched last night that yielded nothing is not
 * in the index and will be fetched again. Closing that gap needs an attempted-URL ledger, which is a
 * different (and much larger) object than this; what this does is remove the provably-redundant work.
 *
 * Streamed throughout: the lists run to millions of lines and must never be read into memory at once.
 */
const fs = require('fs');
const readline = require('readline');
const os = require('./opensearch');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const REGION = process.env.AWS_REGION || 'us-east-1';
const WINDOW = Number(process.env.SKIP_WINDOW) || 1024;   // one terms query per window

const s3 = () => new S3Client({ region: REGION });
const parseS3 = (u) => { const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(u); return m ? { Bucket: m[1], Key: m[2] } : null; };

async function openRead(src) {
  const p = parseS3(src);
  if (p) return (await s3().send(new GetObjectCommand(p))).Body;
  return fs.createReadStream(src);
}

/**
 * Filter a URL list against the contacts index.
 * @param {string} inPath  s3:// or local path — one URL per line, or JSONL with a .url field
 * @param {string} outPath s3:// or local path for the survivors
 * @returns {{seen:number, known:number, kept:number}}
 */
async function filterList(inPath, outPath, opts = {}) {
  const client = opts.client || os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const log = opts.log || ((m) => console.error(m));
  const outIsS3 = !!parseS3(outPath);
  const localOut = outIsS3 ? `/tmp/_skip-known-${Date.now()}.txt` : outPath;
  const out = fs.createWriteStream(localOut);

  let seen = 0, known = 0, kept = 0, win = [];
  const flushWindow = async () => {
    if (!win.length) return;
    const urls = [...new Set(win.map((w) => w.url).filter(Boolean))];
    const have = new Set();
    if (urls.length) {
      try {
        const r = await client.search({ index: os.INDEX, body: { size: 0, query: { terms: { web_source_url: urls } },
          aggs: { u: { terms: { field: 'web_source_url', size: urls.length } } } } });
        for (const b of (((r.body || r).aggregations.u.buckets) || [])) have.add(b.key);
      } catch (e) { /* an unfiltered URL costs a fetch, not correctness — never drop on error */ }
    }
    for (const w of win) {
      if (w.url && have.has(w.url)) { known++; continue; }
      kept++;
      if (!out.write(w.line + '\n')) await new Promise((res) => out.once('drain', res));
    }
    win = [];
  };

  const rl = readline.createInterface({ input: await openRead(inPath), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    seen++;
    let url = t;
    if (t.startsWith('{')) { try { url = JSON.parse(t).url || ''; } catch (e) { url = ''; } }
    win.push({ line: t, url });
    if (win.length >= WINDOW) {
      await flushWindow();
      if (seen % 250000 < WINDOW) log(`  checked ${seen.toLocaleString()} | already have ${known.toLocaleString()} | keeping ${kept.toLocaleString()}`);
    }
  }
  await flushWindow();
  await new Promise((r) => out.end(r));

  if (outIsS3) {
    const p = parseS3(outPath);
    const st = fs.statSync(localOut);
    await s3().send(new PutObjectCommand({ ...p, Body: fs.createReadStream(localOut), ContentLength: st.size, ContentType: 'text/plain' }));
    fs.unlinkSync(localOut);
  }
  log(`  already have ${known.toLocaleString()} | NEW ${kept.toLocaleString()} of ${seen.toLocaleString()}`);
  return { seen, known, kept };
}

module.exports = { filterList };

if (require.main === module) {
  const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
  const IN = arg('in', ''), OUT = arg('out', '');
  if (!IN || !OUT) { console.error('need --in <path|s3://…> --out <path|s3://…>'); process.exit(1); }
  const t0 = Date.now();
  filterList(IN, OUT).then((r) => {
    console.error(`\n${r.kept.toLocaleString()} URL(s) -> ${OUT}  (${r.known.toLocaleString()} already known, ${Math.round((Date.now() - t0) / 1000)}s)`);
  }).catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
}
