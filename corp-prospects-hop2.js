/**
 * corp-prospects-hop2.js — merge the sharded sitemap-expand-urls output and run the SAME Hop 2 the
 * universe refresh runs: waterfall-resolve the page URLs in Common Crawl, then extract contacts from the
 * WARC records, live-crawling only what the archive doesn't have.
 *
 *   S3_PREFIX=corp-prospects/bio-urls.txt node corp-prospects-hop2.js [--live] [--crawls a,b,c] [--tag t]
 *
 * Steps:
 *   1. download every s3://$OUT_BUCKET/$S3_PREFIX* part the expand fleet wrote, merge + dedupe -> urls.txt
 *   2. cc-athena-miner --resolve-urls urls.txt --warc-out ptr.jsonl   (ONE index scan for all shards)
 *   3. extract-from-pointers --ptr ptr.jsonl [--live miss.txt]        (the standard ingest rules)
 *
 * Merging before step 2 is the point of the S3 hand-off: Athena's cost is the index scan, which is the
 * same whether you hand it 400k keys or 2.4M, so one merged resolve costs a fraction of six sharded ones.
 *
 * Config via env (Fly would parse bare flags as its own):
 *   S3_PREFIX   key prefix the expand fleet uploaded to (required)
 *   OUT_BUCKET  bucket (defaults to the athena results bucket, same as the rest of the pipeline)
 *   CRAWLS      comma-separated CC crawl ids, newest first
 *   TAG         names the scratch artifacts + the Athena resolve tag
 *   LIVE=1      live-crawl the pages Common Crawl doesn't have (otherwise they're skipped)
 *   SCRATCH     working dir (default /data/_corp-prospects, falls back to ./_corp-prospects)
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = arg('s3-prefix', '') || process.env.S3_PREFIX || '';
const CRAWLS = arg('crawls', '') || process.env.CRAWLS || 'CC-MAIN-2026-25,CC-MAIN-2026-21,CC-MAIN-2026-17,CC-MAIN-2026-12';
const TAG = arg('tag', '') || process.env.TAG || 'corp-prospects';
const LIVE = has('live') || /^(1|true|yes|on)$/i.test(process.env.LIVE || '');
let SCRATCH = arg('scratch', '') || process.env.SCRATCH || '/data/_corp-prospects';

function node(script, args) {
  const banner = `\n══════ ${script} ${new Date().toISOString().slice(11, 19)} ══════`;
  console.error(banner);
  const r = spawnSync(process.execPath, [path.join(__dirname, script), ...args], { stdio: 'inherit', env: process.env, cwd: __dirname });
  if (r.status !== 0) { console.error(`✗ ${script} exited ${r.status}`); process.exit(r.status || 1); }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!PREFIX) { console.error('need S3_PREFIX (the key prefix the expand fleet uploaded to)'); process.exit(1); }
  // /data only exists on the machine holding the volume; anywhere else fall back to the working dir.
  try { fs.mkdirSync(SCRATCH, { recursive: true }); } catch (e) { SCRATCH = path.join(__dirname, '_corp-prospects'); fs.mkdirSync(SCRATCH, { recursive: true }); }
  const F = { urls: path.join(SCRATCH, `${TAG}-urls.txt`), ptr: path.join(SCRATCH, `${TAG}-ptr.jsonl`), miss: path.join(SCRATCH, `${TAG}-miss.txt`) };

  // ---- 1) merge the shard parts ----
  console.error(`\n══════ merge s3://${BUCKET}/${PREFIX}* ══════`);
  const s3 = new S3Client({ region: REGION });
  const keys = [];
  let token = null;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: token }));
    for (const o of (r.Contents || [])) keys.push(o.Key);
    token = r.IsTruncated ? r.NextContinuationToken : null;
  } while (token);
  if (!keys.length) { console.error(`no parts found under s3://${BUCKET}/${PREFIX}`); process.exit(1); }
  console.error(`${keys.length} part(s): ${keys.join(', ')}`);

  const seen = new Set();
  const out = fs.createWriteStream(F.urls);
  let read = 0;
  for (const key of keys) {
    const o = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    const body = await o.Body.transformToString();
    for (const line of body.split('\n')) {
      const u = line.trim();
      if (!u) continue;
      read++;
      if (seen.has(u)) continue;
      seen.add(u);
      out.write(u + '\n');
    }
  }
  await new Promise((r) => out.end(r));
  console.error(`merged ${read.toLocaleString()} line(s) -> ${seen.size.toLocaleString()} unique URLs -> ${F.urls}`);
  if (!seen.size) { console.error('nothing to resolve'); process.exit(0); }

  // ---- 2) waterfall-resolve in Common Crawl ----
  node('cc-athena-miner.js', ['--resolve-urls', F.urls, '--warc-out', F.ptr, '--crawls', CRAWLS, '--resolve-tag', `${TAG}b`]);

  // ---- which URLs the archive did NOT have (the live fallback list) ----
  const resolved = new Set();
  const keyOf = (u) => String(u || '').trim().replace(/^https?:\/\//i, '').replace(/^www\./i, '').replace(/\/$/, '').toLowerCase();
  if (fs.existsSync(F.ptr)) {
    const rl = readline.createInterface({ input: fs.createReadStream(F.ptr), crlfDelay: Infinity });
    for await (const l of rl) { if (!l.trim()) continue; try { resolved.add(keyOf(JSON.parse(l).url)); } catch (e) { /* */ } }
  }
  let miss = 0;
  const mo = fs.createWriteStream(F.miss);
  for (const u of seen) if (!resolved.has(keyOf(u))) { mo.write(u + '\n'); miss++; }
  await new Promise((r) => mo.end(r));
  console.error(`\nin Common Crawl: ${resolved.size.toLocaleString()} | not in CC: ${miss.toLocaleString()}${LIVE ? ' (live fallback ON)' : ' (skipped — set LIVE=1 to crawl them)'}`);
  // Preserve the miss list off-machine. The first run's misses were lost when its machine was destroyed,
  // which meant a later "scan more crawls" pass had no cheap way to target only the pages still missing.
  if (miss) {
    try {
      const { PutObjectCommand: Put } = require('@aws-sdk/client-s3');
      const key = `${PREFIX.replace(/[^/]*$/, '')}${TAG}-miss.txt`;
      await s3.send(new Put({ Bucket: BUCKET, Key: key, Body: fs.createReadStream(F.miss), ContentLength: fs.statSync(F.miss).size, ContentType: 'text/plain' }));
      console.error(`  miss list saved -> s3://${BUCKET}/${key}`);
    } catch (e) { console.error('  (could not save the miss list to S3:', e.message + ')'); }
  }

  // ---- 2b) drop pointers for pages we have ALREADY ingested ----
  // A second pass over older crawls re-finds most of what the first pass already resolved. Fetching and
  // re-extracting those is ~1M wasted S3 reads for records the score gate will mostly reject anyway, so
  // filter by web_source_url before extracting. SKIP_KNOWN=0 disables (e.g. to deliberately refresh).
  if (!/^(0|false|no|off)$/i.test(process.env.SKIP_KNOWN || '') && fs.existsSync(F.ptr)) {
    const osx = require('./opensearch');
    const client = osx.makeClient(process.env.OPENSEARCH_ENDPOINT);
    const lines = [];
    const rl2 = readline.createInterface({ input: fs.createReadStream(F.ptr), crlfDelay: Infinity });
    for await (const l of rl2) { if (l.trim()) lines.push(l); }
    console.error(`\n══════ filtering ${lines.length.toLocaleString()} pointer(s) against contacts we already have ══════`);
    const known = new Set();
    const urls = lines.map((l) => { try { return JSON.parse(l).url; } catch (e) { return ''; } });
    const uniq = [...new Set(urls.filter(Boolean))];
    for (let i = 0; i < uniq.length; i += 1024) {
      const chunk = uniq.slice(i, i + 1024);
      try {
        const r = await client.search({ index: osx.INDEX, body: { size: 0, query: { terms: { web_source_url: chunk } },
          aggs: { u: { terms: { field: 'web_source_url', size: chunk.length } } } } });
        for (const b of (((r.body || r).aggregations.u.buckets) || [])) known.add(b.key);
      } catch (e) { /* best-effort: an unfiltered page costs a fetch, not correctness */ }
      if (i && i % 102400 === 0) console.error(`  checked ${i.toLocaleString()}/${uniq.length.toLocaleString()} | already have ${known.size.toLocaleString()}`);
    }
    const kept = [];
    for (let i = 0; i < lines.length; i++) if (urls[i] && !known.has(urls[i])) kept.push(lines[i]);
    console.error(`  already ingested: ${known.size.toLocaleString()} | NEW to fetch: ${kept.length.toLocaleString()}`);
    fs.writeFileSync(F.ptr, kept.join('\n') + (kept.length ? '\n' : ''));
    if (!kept.length && !(LIVE && miss)) { console.error('\nnothing new in this crawl stack — done.'); return; }
  }

  // ---- 3) ingest under the standard rules ----
  const exArgs = ['--ptr', F.ptr, '--tag', TAG];
  if (LIVE && miss) exArgs.push('--live', F.miss);
  node('extract-from-pointers.js', exArgs);

  console.error(`\n══════ HOP 2 DONE ══════\n  ${seen.size.toLocaleString()} page URLs -> ${resolved.size.toLocaleString()} via CC${LIVE ? ` + ${miss.toLocaleString()} via live` : ''} -> Master DB (see the DONE line above)`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
