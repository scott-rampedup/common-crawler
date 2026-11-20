/**
 * ndjson-partition.js — split a Location ndjson into N parts by a hash of the grouping key, so a phase
 * that groups in memory can be run once per part instead of once over everything.
 *
 * WHY THIS EXISTS. gm-build.js groups Locations by root domain IN MEMORY: it holds every location of
 * every domain at once to resolve the HQ and roll up. That is fine for the 4,510,566 Google Maps rows it
 * carries today. The Overture ingest produces ~26.3M Locations across up to ~13.5M distinct domains
 * (measured on release 2026-11-18.0), which will not fit.
 *
 * Partitioning by a HASH OF THE KEY, not by line number, is what makes this safe: every location of a
 * given root domain lands in the same part, so each part can be grouped independently and the result is
 * identical to grouping the whole file. Splitting by size would scatter a domain's locations across
 * parts and produce one HQ per part for the same company.
 *
 * gm-build itself is unchanged -- it just runs N times.
 *
 *   node ndjson-partition.js --in overture-locations.ndjson --out <dir> [--parts 16] [--key root_domain]
 *   for i in $(seq 0 15); do node gm-build.js --in <dir>/part-$i.ndjson --out <dir>/b$i; done
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const crypto = require('crypto');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };

const IN = arg('in', '');
const OUT = arg('out', '');
const PARTS = Math.max(1, Number(arg('parts', '16')) || 16);
const KEY = arg('key', 'root_domain');

// md5 of the key, low 4 bytes, mod PARTS. Any stable hash does; md5 is here because it is in core and
// spreads far better than a string-length or first-character split, which would pile every .com domain
// into one part.
function partOf(value) {
  const h = crypto.createHash('md5').update(String(value || '')).digest();
  return h.readUInt32BE(0) % PARTS;
}

module.exports = { partOf };

if (require.main === module && process.argv.includes('--selftest')) {
  let pass = 0, fail = 0;
  const eq = (label, got, want) => { if (got === want) { pass++; return; } fail++; console.error('  FAIL ' + label + ': got ' + got + ' want ' + want); };

  // The property the whole design rests on: the same key always lands in the same part.
  eq('a key is stable', partOf('acme.com'), partOf('acme.com'));
  eq('a second key is stable', partOf('example.org'), partOf('example.org'));
  eq('part is in range', partOf('acme.com') < PARTS && partOf('acme.com') >= 0, true);

  // And the spread is even enough to be worth doing. A poor hash would make one part carry everything.
  const counts = new Array(PARTS).fill(0);
  for (let i = 0; i < 20000; i++) counts[partOf('domain-' + i + '.com')]++;
  const lo = Math.min(...counts), hi = Math.max(...counts), mean = 20000 / PARTS;
  eq('no part is under half the mean', lo > mean * 0.5, true);
  eq('no part is over 1.5x the mean', hi < mean * 1.5, true);

  console.log(`ndjson-partition selftest: ${pass} passed, ${fail} failed (spread ${lo}-${hi}, mean ${Math.round(mean)})`);
  process.exit(fail ? 1 : 0);
}

if (require.main === module && !process.argv.includes('--selftest')) (async () => {
  if (!IN || !OUT) { console.error('need --in <ndjson> --out <dir>'); process.exit(1); }
  fs.mkdirSync(OUT, { recursive: true });
  const streams = [], counts = new Array(PARTS).fill(0);
  for (let i = 0; i < PARTS; i++) streams.push(fs.createWriteStream(path.join(OUT, `part-${i}.ndjson`)));

  let n = 0, noKey = 0;
  const keys = new Set();
  const rl = readline.createInterface({ input: fs.createReadStream(IN), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    let r; try { r = JSON.parse(line); } catch (e) { continue; }
    const k = r && r[KEY];
    if (!k) { noKey++; continue; }
    n++;
    if (keys.size < 2000000) keys.add(k);         // capped: only used to report the domain count
    const p = partOf(k);
    counts[p]++;
    if (!streams[p].write(line + '\n')) await new Promise((res) => streams[p].once('drain', res));
    if (n % 1000000 === 0) console.error(`  ${n.toLocaleString()} lines`);
  }
  await Promise.all(streams.map((s) => new Promise((res) => s.end(res))));

  const N = (x) => Number(x).toLocaleString();
  console.error(`\nDONE: ${N(n)} lines -> ${PARTS} parts${noKey ? `, ${N(noKey)} skipped with no ${KEY}` : ''}`);
  console.error(`distinct ${KEY}: ${keys.size >= 2000000 ? '2,000,000+' : N(keys.size)}`);
  for (let i = 0; i < PARTS; i++) console.error(`  part-${String(i).padEnd(3)} ${N(counts[i])}`);
  const lo = Math.min(...counts), hi = Math.max(...counts);
  console.error(`spread ${N(lo)} - ${N(hi)}${lo && hi / lo > 1.5 ? '   <- uneven; a few very large domains?' : ''}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
