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
 * It now filters against TWO sources, because one of them cannot see most repeat work:
 *
 *   contacts.web_source_url — the page PRODUCED a contact. Catches only the productive minority: on the
 *                             2026-08-14 list that was 304,831 of 3,730,274 (8.2%).
 *   crawl_log (crawl-ledger) — the page was ATTEMPTED, whatever came of it. This is the big silent
 *                             bucket: fetched fine, no person on it, invisible to the contacts index, and
 *                             therefore re-fetched every cycle forever.
 *
 * Either lookup failing yields an empty skip set rather than an exception: an unfiltered URL costs one
 * fetch, a wrongly-dropped one is lost data, and this filter must never be why a page goes missing.
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

  // Two independent sources of "we already did this":
  //   contacts.web_source_url — the page PRODUCED a contact.
  //   crawl_log               — the page was ATTEMPTED, whatever came of it. This is the one that catches
  //                             the big silent bucket: fetched fine, no person on it, invisible to the
  //                             contacts index, and therefore re-fetched forever without the ledger.
  const useLedger = !/^(0|false|no|off)$/i.test(process.env.CRAWL_LEDGER || '1');
  let ledger = null;
  if (useLedger) {
    try { ledger = require('./crawl-ledger'); await ledger.ensureIndex(client); }
    catch (e) { ledger = null; log(`  (crawl ledger unavailable: ${e.message} — filtering on contacts only)`); }
  }

  let seen = 0, known = 0, knownLedger = 0, kept = 0, win = [];
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
    let attempted = new Set();
    if (ledger && urls.length) {
      try { attempted = await ledger.skipSet(client, urls); }
      catch (e) { attempted = new Set(); }
    }
    for (const w of win) {
      if (w.url && have.has(w.url)) { known++; continue; }
      if (w.url && attempted.has(w.url)) { knownLedger++; continue; }
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
      if (seen % 250000 < WINDOW) log(`  checked ${seen.toLocaleString()} | have contact ${known.toLocaleString()} | already attempted ${knownLedger.toLocaleString()} | keeping ${kept.toLocaleString()}`);
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
  log(`  have contact ${known.toLocaleString()} | already attempted ${knownLedger.toLocaleString()} | NEW ${kept.toLocaleString()} of ${seen.toLocaleString()}`);
  return { seen, known, knownLedger, kept };
}

/**
 * The same "we already did this" test filterList applies, but in memory and returning the set.
 *
 * Two independent sources, because the contacts index alone misses the big silent bucket: a page that was
 * fetched fine and simply had no person on it produces no contact, so a contacts-only check re-fetches it
 * forever. bio-etl needs this per queue object to decide whether an object is genuinely finished.
 */
// The ledger index only has to be ensured ONCE per process. Doing it inside knownSet meant an index
// round-trip on every call: the backlog count made 3,447 of them and took 2,299s where the actual lookups
// are a few minutes' work. Cached as a promise so concurrent callers share one check.
let _ledgerReady = null;
function getLedger(client) {
  if (/^(0|false|no|off)$/i.test(process.env.CRAWL_LEDGER || '1')) return Promise.resolve(null);
  if (!_ledgerReady) {
    _ledgerReady = (async () => {
      try { const l = require('./crawl-ledger'); await l.ensureIndex(client); return l; }
      catch (e) { return null; }
    })();
  }
  return _ledgerReady;
}

async function knownSet(urls, opts = {}) {
  const client = opts.client || os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const out = new Set();
  const list = [...new Set((urls || []).filter(Boolean))];
  if (!list.length) return out;
  const ledger = await getLedger(client);
  const CONC = Math.max(1, Number(process.env.KNOWN_CONC) || 6);

  // Chunks ran strictly in series, so a 4,000-URL object cost 8 sequential round-trips. They are
  // independent lookups; run a few at a time.
  const chunks = [];
  for (let i = 0; i < list.length; i += 1024) chunks.push(list.slice(i, i + 1024));
  const one = async (chunk) => {
    try {
      const r = await client.search({ index: os.INDEX, body: { size: 0, query: { terms: { web_source_url: chunk } },
        aggs: { u: { terms: { field: 'web_source_url', size: chunk.length } } } } }, { requestTimeout: 120000 });
      for (const b of (((r.body || r).aggregations.u.buckets) || [])) out.add(b.key);
    } catch (e) { /* unfiltered URL costs a fetch, not correctness */ }
    if (ledger) { try { for (const u of await ledger.skipSet(client, chunk)) out.add(u); } catch (e) { /* */ } }
  };
  for (let i = 0; i < chunks.length; i += CONC) await Promise.all(chunks.slice(i, i + CONC).map(one));
  return out;
}

module.exports = { filterList, knownSet };

if (require.main === module) {
  const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
  const IN = arg('in', ''), OUT = arg('out', '');
  if (!IN || !OUT) { console.error('need --in <path|s3://…> --out <path|s3://…>'); process.exit(1); }
  const t0 = Date.now();
  filterList(IN, OUT).then((r) => {
    console.error(`\n${r.kept.toLocaleString()} URL(s) -> ${OUT}  (${r.known.toLocaleString()} already known, ${Math.round((Date.now() - t0) / 1000)}s)`);
  }).catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
}
