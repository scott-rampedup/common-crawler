/**
 * prep-live-candidates.js — build the live-crawl candidate list: the sitemap "misses"
 * (bio URLs NOT found in Common Crawl) MINUS the ones that DID resolve in CC on the 2nd pass
 * (already extracted by the harvest). Then emit a diverse strided sample of --take N.
 *
 *   node prep-live-candidates.js --misses monitor-misses.txt --exclude cc-warc-mmiss-all.jsonl \
 *        --out batch-1m.txt --take 1000000
 */
const fs = require('fs');
const readline = require('readline');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 ? process.argv[i + 1] : d; };
const MISSES = arg('--misses', 'monitor-misses.txt');
const EXCLUDE = arg('--exclude', 'cc-warc-mmiss-all.jsonl');
const OUT = arg('--out', 'batch-1m.txt');
const TAKE = Number(arg('--take', '1000000'));

(async () => {
  // 1) build the exclusion set (URLs already resolved+extracted from CC)
  const excl = new Set();
  if (fs.existsSync(EXCLUDE)) {
    const rl = readline.createInterface({ input: fs.createReadStream(EXCLUDE), crlfDelay: Infinity });
    for await (const line of rl) { const t = line.trim(); if (!t) continue; try { const u = JSON.parse(t).url; if (u) excl.add(u); } catch (e) { /* skip */ } }
  }
  console.log(`exclusion set: ${excl.size.toLocaleString()} CC-resolved URLs`);

  // 2) count non-excluded to pick a stride that yields ~TAKE, diverse across the whole file
  let total = 0, kept = 0;
  const rl1 = readline.createInterface({ input: fs.createReadStream(MISSES), crlfDelay: Infinity });
  for await (const line of rl1) { const t = line.trim(); if (!t) continue; total++; if (!excl.has(t)) kept++; }
  const stride = Math.max(1, Math.floor(kept / TAKE));
  console.log(`misses: ${total.toLocaleString()} | live candidates (not in CC): ${kept.toLocaleString()} | stride ${stride} -> ~${Math.floor(kept / stride).toLocaleString()}`);

  // 3) write every `stride`-th non-excluded URL
  const ws = fs.createWriteStream(OUT);
  let idx = 0, written = 0;
  const rl2 = readline.createInterface({ input: fs.createReadStream(MISSES), crlfDelay: Infinity });
  for await (const line of rl2) {
    const t = line.trim(); if (!t || excl.has(t)) continue;
    if (idx % stride === 0) { ws.write(t + '\n'); written++; }
    idx++;
  }
  ws.end();
  console.log(`wrote ${written.toLocaleString()} URLs -> ${OUT}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
