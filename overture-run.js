/**
 * overture-run.js — the whole Overture Maps ETL on one machine, partition by partition.
 *
 *   OPENSEARCH_ENDPOINT=… node overture-run.js --dry                # plan + counts, no AWS, no writes
 *   OPENSEARCH_ENDPOINT=… node overture-run.js --parts 16 --only 0  # one partition, end to end
 *   OPENSEARCH_ENDPOINT=… node overture-run.js --parts 16           # all of them
 *
 * Chains the phases that already exist rather than reimplementing them:
 *   overture-load  -> part-<i>.ndjson.gz (+ overture-bio-urls.txt)
 *   gm-build       -> gm-hq / gm-loc / gm-contacts  (per partition)
 *   gm-upsert      -> applies all three to OpenSearch (per partition)
 *
 * WHY PARTITION BY PARTITION, and why the extract is one pass. gm-build sorts externally, and measured on
 * a real shard its working set peaks at 4.36GB for a 1.48GB input -- about 2.9x. The release yields
 * ~23.7GB of Locations, so doing it whole would want ~69GB against 21GB free on the volume. Partitioned
 * by domain hash, each part is ~1.48GB and costs ~4.4GB transiently, which fits with room to spare.
 * Memory is not the constraint: the same run peaked at 2.20GB node / 1.61GB sort.
 *
 * Each partition is extracted once (gzipped, ~3.4GB for the set), then built and upserted one at a time
 * with the intermediates deleted after, so peak disk stays near one partition's worth rather than the
 * release's.
 *
 * SAFETY. --dry stops before any write. --only <i> runs a single partition so the first one can be read
 * before the rest follow. gm-upsert is idempotent (HQ update is partial, Location is index-by-cid,
 * contacts are score-gated), so a repeated partition costs time and changes nothing.
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { spawnSync } = require('child_process');
const { queueBioUrls } = require('./queue-bio-urls');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);

const DIR = arg('dir', path.join(process.env.DATA_DIR || '/tmp', 'overture'));
const PARTS = Math.max(1, Number(arg('parts', '16')) || 16);
const ONLY = arg('only', '') === '' ? null : Number(arg('only', '0'));
const RELEASE = arg('release', '2026-11-18.0');
const SHARDS = arg('shards', '');
const DRY = has('dry');
const SKIP_EXTRACT = has('skip-extract');
const N = (x) => Number(x).toLocaleString();

function step(label, script, args) {
  console.error(`\n═══ ${label} · ${new Date().toISOString().slice(11, 19)} ═══`);
  const r = spawnSync(process.execPath, [path.join(__dirname, script), ...args],
    { stdio: 'inherit', cwd: __dirname, env: process.env });
  if (r.status !== 0) { console.error(`✗ ${script} exited ${r.status}`); process.exit(r.status || 1); }
}

// gm-build wants a plain file; the partitions are stored gzipped because the uncompressed set does not
// fit. Expand one, use it, delete it.
// STREAMED, not gunzipSync. A partition decompresses to ~2.55GB and gunzipSync returns a single Buffer,
// so the first real run died here with:
//   The value of "length" is out of range. It must be >= 0 && <= 2147483647. Received 2552511497
// That is Node's hard maximum Buffer length (2GB-1); no partition count that keeps the gzipped set a
// sensible size would stay under it, so the fix is to never hold the whole thing at once.
function gunzipTo(src, dst) {
  const t0 = Date.now();
  return new Promise((resolve, reject) => {
    const r = fs.createReadStream(src);
    const g = zlib.createGunzip();
    const w = fs.createWriteStream(dst);
    r.on('error', reject); g.on('error', reject); w.on('error', reject);
    w.on('finish', () => resolve({ bytes: fs.statSync(dst).size, secs: Math.round((Date.now() - t0) / 1000) }));
    r.pipe(g).pipe(w);
  });
}

function rm(p) { try { fs.rmSync(p, { recursive: true, force: true }); } catch (e) { /* already gone */ } }

// Count newlines over a fixed buffer. Works at any file size; nothing is held beyond 1MB.
function countLines(file) {
  const fd = fs.openSync(file, 'r');
  try {
    const buf = Buffer.allocUnsafe(1 << 20);
    let n = 0, read = 0, lastByte = 0;
    while ((read = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
      for (let i = 0; i < read; i++) if (buf[i] === 10) n++;
      lastByte = buf[read - 1];
    }
    if (lastByte && lastByte !== 10) n++;        // a final line with no trailing newline still counts
    return n;
  } finally { fs.closeSync(fd); }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT && !DRY) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  fs.mkdirSync(DIR, { recursive: true });

  // ---- phase 1: extract every shard into gzipped partitions, one pass ----
  if (!SKIP_EXTRACT) {
    const a = ['--release', RELEASE, '--out', DIR, '--parts', String(PARTS)];
    if (SHARDS) a.push('--shards', SHARDS);
    if (DRY) { console.error(`[dry] would run: overture-load.js ${a.join(' ')}`); }
    else step('PHASE 1/3  extract -> gzipped partitions', 'overture-load.js', a);
  } else {
    console.error('skipping extract (--skip-extract)');
  }

  const parts = [];
  for (let i = 0; i < PARTS; i++) {
    const f = path.join(DIR, `part-${i}.ndjson.gz`);
    if (fs.existsSync(f)) parts.push({ i, f, gz: fs.statSync(f).size });
  }
  console.error(`\npartitions present: ${parts.length}/${PARTS}`
    + (parts.length ? `  (${(parts.reduce((s, p) => s + p.gz, 0) / 1e9).toFixed(2)} GB gzipped)` : ''));
  if (!parts.length && !DRY) { console.error('no partitions to process'); process.exit(1); }

  const todo = ONLY === null ? parts : parts.filter((p) => p.i === ONLY);
  if (ONLY !== null && !todo.length) { console.error(`partition ${ONLY} not found`); process.exit(1); }

  // ---- phases 2 and 3, one partition at a time ----
  const tally = { parts: 0, hq: 0, loc: 0, contacts: 0 };
  for (const p of todo) {
    const work = path.join(DIR, `work-${p.i}`);
    const raw = path.join(work, 'locations.ndjson');
    rm(work); fs.mkdirSync(work, { recursive: true });

    if (DRY) { console.error(`[dry] would build + upsert part-${p.i} (${(p.gz / 1e6).toFixed(0)} MB gz)`); continue; }

    const g = await gunzipTo(p.f, raw);
    console.error(`\n█ partition ${p.i}: ${(g.bytes / 1e9).toFixed(2)} GB expanded in ${g.secs}s`);

    step(`PHASE 2/3  gm-build part-${p.i}`, 'gm-build.js', ['--in', raw, '--out', work]);
    // The expanded copy and the sort's scratch are the bulk of the footprint; drop them before upserting.
    rm(raw); rm(path.join(work, '_gm-keyed.tsv')); rm(path.join(work, '_gm-sorted.tsv'));

    step(`PHASE 3/3  gm-upsert part-${p.i}`, 'gm-upsert.js', ['--in', work]);

    for (const [k, f] of [['hq', 'gm-hq.ndjson'], ['loc', 'gm-loc.ndjson'], ['contacts', 'gm-contacts.ndjson']]) {
      // Counted by scanning bytes, never by building a string. gm-loc.ndjson was 972MB for a 1.48GB
      // input and a partition is 2.55GB, so .toString() on it throws ERR_STRING_TOO_LONG -- V8 caps a
      // single string well below the file size. Same family of mistake as the gunzipSync above.
      try { tally[k] += countLines(path.join(work, f)); } catch (e) { /* counted as zero */ }
    }
    tally.parts++;
    rm(work);
    console.error(`█ partition ${p.i} done — running totals: ${N(tally.hq)} HQ, ${N(tally.loc)} Locations, ${N(tally.contacts)} contacts`);
  }

  console.error(`\n══════ OVERTURE RUN COMPLETE ══════`);
  console.error(`  partitions processed ${tally.parts}`);
  console.error(`  HQ records           ${N(tally.hq)}`);
  console.error(`  child Locations      ${N(tally.loc)}`);
  console.error(`  contacts             ${N(tally.contacts)}`);
  // HAND THE BIO URLs ON, do not just report where they are. This printed a count and a path and
  // queued nothing, so 168,154 Overture bio URLs sat on the volume until someone noticed -- the same
  // failure gm-run.js already documents for Google Maps, where two exports' worth died with a machine's
  // /tmp. Queueing them here is what makes them reachable: bio-pump picks the objects up continuously.
  const bio = path.join(DIR, 'overture-bio-urls.txt');
  if (fs.existsSync(bio)) {
    const n = fs.readFileSync(bio, 'utf8').split('\n').filter((x) => x.trim()).length;
    console.error(`  bio URLs for Hop-2   ${N(n)}  -> ${bio}`);
    await queueBioUrls(bio, { source: 'Overture', tag: 'ov', noQueue: process.argv.includes('--no-queue') });
  }
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
