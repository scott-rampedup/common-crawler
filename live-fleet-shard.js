/**
 * live-fleet-shard.js — run ONE shard of a live-crawl across a fleet of machines.
 *
 *   LIVE_CONC=192 node live-fleet-shard.js --in s3://bucket/…/miss.txt --shard 0/8 --tag drain-2026-08-13
 *
 * The live path is the pipeline's real constraint. Common Crawl resolves only 12.2% of URLs discovered by
 * the Sitemap Monitor (the 54.2% figure came from Google Maps URLs and does not transfer — monitor URLs are
 * new pages, which is the entire point of monitoring). So ~88% must be fetched live, and one machine at the
 * shipped LIVE_CONC=4 does 2 pages/s: 15 days for 2,563,533 URLs.
 *
 * Sharding is BY REGISTRABLE DOMAIN, not round-robin, and that is the important design choice. Round-robin
 * would spread one site's URLs across every machine in the fleet, so N machines would hit the same server
 * simultaneously with no shared rate limiting — turning a politeness problem into a blocking problem, and
 * the 403/429 responses would be indistinguishable from the site being down. Hashing the domain keeps every
 * URL for a site on one machine, where the existing per-host pacing still applies.
 *
 * Each shard downloads the full list and keeps only its own slice. That is deliberately simpler than
 * pre-splitting into N objects: the fleet size can change without re-splitting, and a shard that dies can
 * be relaunched by itself with the identical command.
 *
 * BALANCING: hashing the domain balances DOMAINS, not URLs, and URLs-per-domain is extremely skewed — the
 * first run of this split 2,563,533 URLs into shards of 943,397 and 78,853, a 12x spread, because a single
 * large site lands whole in one shard. Wall-clock is set by the slowest shard, so that is an 11-hour job
 * masquerading as a 1-hour one. So the default is a greedy longest-processing-time bin-pack: count URLs per
 * domain in a first pass, then assign domains largest-first to whichever shard is currently least loaded.
 * Domain affinity is preserved (a site still lands on exactly one machine) and every shard gets a similar
 * URL count. Every machine runs the same deterministic computation over the same input, so the assignment
 * agrees across the fleet with no coordination. --hash restores the old behaviour.
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('in', '') || process.env.IN || '';
const SHARD = arg('shard', '') || process.env.SHARD || '0/1';
const TAG = arg('tag', '') || process.env.TAG || 'live-fleet';
const REGION = process.env.AWS_REGION || 'us-east-1';

const [SI, SN] = SHARD.split('/').map((x) => Number(x));
if (!Number.isInteger(SI) || !Number.isInteger(SN) || SN < 1 || SI < 0 || SI >= SN) {
  console.error(`bad --shard "${SHARD}" — expected i/N with 0 <= i < N`); process.exit(1);
}

// FNV-1a, the same hash the other sharded jobs in this repo use, so shard membership is comparable.
function fnv1a(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0; }
  return h >>> 0;
}
const domainOf = (u) => {
  try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return u; }
};

const HASH_ONLY = process.argv.includes('--hash');
// --skip N drops the first N URLs of THIS shard. extract-from-pointers walks its input in order and logs
// "[live] N/total", so N is a precise resume point. This exists because shard throughput varies ~20x by
// site latency (16/s to 365/s at identical LIVE_CONC), and the only lever on a slow site is more
// concurrency — which means restarting the shard. Without a resume point, raising concurrency costs all
// the progress it was meant to accelerate.
const SKIP = Number(arg('skip', '0')) || 0;

async function openIn() {
  if (/^s3:\/\//i.test(IN)) {
    const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(IN);
    return (await new S3Client({ region: REGION }).send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }))).Body;
  }
  return fs.createReadStream(IN);
}
const urlOf = (t) => {
  if (!t.startsWith('{')) return t;
  try { return JSON.parse(t).url || ''; } catch (e) { return ''; }
};

(async () => {
  if (!IN) { console.error('need --in <s3://… or path>'); process.exit(1); }
  const mine = path.join('/tmp', `${TAG}-shard${SI}of${SN}.txt`);

  // ---- pass 1: URLs per domain (skipped for --hash, which needs no global view) ----
  let owner = null;
  if (!HASH_ONLY) {
    const counts = new Map();
    let n = 0;
    const rl0 = readline.createInterface({ input: await openIn(), crlfDelay: Infinity });
    for await (const line of rl0) {
      const t = line.trim(); if (!t) continue;
      const u = urlOf(t); if (!u) continue;
      const d = domainOf(u);
      counts.set(d, (counts.get(d) || 0) + 1);
      n++;
    }
    // Greedy LPT: largest domains first into the least-loaded shard. Sorting by count then by name keeps
    // the order total, so every machine in the fleet derives the identical assignment independently.
    const domains = [...counts.entries()].sort((a, b) => (b[1] - a[1]) || (a[0] < b[0] ? -1 : 1));
    const load = new Array(SN).fill(0);
    owner = new Map();
    for (const [d, cnt] of domains) {
      let best = 0;
      for (let i = 1; i < SN; i++) if (load[i] < load[best]) best = i;
      owner.set(d, best);
      load[best] += cnt;
    }
    const min = Math.min(...load), max = Math.max(...load);
    console.error(`balanced ${n.toLocaleString()} URL(s) over ${counts.size.toLocaleString()} domain(s) into ${SN} shard(s)`);
    console.error(`  per-shard: ${load.map((x) => x.toLocaleString()).join(' · ')}`);
    console.error(`  spread ${max ? (((max - min) / max) * 100).toFixed(1) : '0'}%  (this shard: ${load[SI].toLocaleString()})`);
  }

  // ---- pass 2: write only this shard's URLs ----
  const out = fs.createWriteStream(mine);
  let total = 0, kept = 0, written = 0;
  const rl = readline.createInterface({ input: await openIn(), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    total++;
    const u = urlOf(t);
    if (!u) continue;
    const d = domainOf(u);
    const shard = owner ? owner.get(d) : (fnv1a(d) % SN);
    if (shard !== SI) continue;
    kept++;
    if (kept <= SKIP) continue;                 // resume point — counted, not written
    written++;
    if (!out.write(u + '\n')) await new Promise((r) => out.once('drain', r));
  }
  await new Promise((r) => out.end(r));
  console.error(`shard ${SI}/${SN}: ${kept.toLocaleString()} of ${total.toLocaleString()} URL(s)`
    + (SKIP ? ` — skipping first ${SKIP.toLocaleString()}, ${written.toLocaleString()} to do` : '') + ` -> ${mine}`);
  if (!written) { console.error('nothing left in this shard after --skip — done.'); return; }

  // Hand off to the extractor that already knows how to live-fetch, model emails and write JSONL.
  const r = spawnSync(process.execPath, [path.join(__dirname, 'extract-from-pointers.js'), '--live', mine, '--tag', `${TAG}-s${SI}`],
    { stdio: 'inherit', cwd: __dirname, env: process.env });
  process.exit(r.status || 0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
