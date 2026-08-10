/**
 * sitemap-expand-urls.js — Hop 1 for a catalog of sitemaps: fetch each Library sitemap and write out the
 * PAGE URLs it lists, so the existing Hop 2 chain can take over:
 *
 *   node sitemap-expand-urls.js --source corp-prospects --out bio-urls.txt
 *   node cc-athena-miner.js --resolve-urls bio-urls.txt --warc-out bioptr.jsonl --crawls <stack> --resolve-tag cp
 *   node extract-from-pointers.js --ptr bioptr.jsonl --tag corp-prospects [--live bio-miss.txt]
 *
 * universe-refresh gets its bio URLs by resolving company HOME pages in CC; a curated sitemap catalog
 * already names the directories, so this fetches the sitemap XML directly (one cheap request each) and
 * emits the URLs beneath it. Only the sitemap fetch is live — the page bodies come from Common Crawl.
 *
 * Config from flags OR env (env lets a dedicated `fly machine run` machine set them without command flags,
 * which Fly would otherwise parse as its own):
 *   SOURCE=corp-prospects  Library source tag to expand ('' = every source)
 *   KIND=People            People | Location  (matches the watch kind we keep)
 *   OUT=/data/bio-urls.txt output file (one URL per line)
 *   CONC=12                sitemaps fetched in parallel
 *   SHARD=i/N              horizontal split, same FNV-1a scheme as discover-child-sitemaps-live
 *   LIMIT=0                max sitemaps to expand (0 = all)
 *
 * Resumable: --resume skips sitemaps already marked expanded (expanded_at) in the Library, so a killed run
 * picks up where it stopped instead of re-fetching from the top.
 */
const fs = require('fs');
const path = require('path');
const sitemaps = require('./sitemaps');
const ccEngine = require('./cc-engine');
const { loadGenderMap } = require('./extractor');
// S3 upload is optional: a sharded fleet writes one part each, and corp-prospects-hop2 merges the parts
// into the single URL list the Athena resolve wants (one index scan instead of one per shard).
let S3 = null; try { S3 = require('@aws-sdk/client-s3'); } catch (e) { /* local-file mode only */ }

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const SOURCE = arg('source', '') || process.env.SOURCE || 'corp-prospects';
const KIND = arg('kind', '') || process.env.KIND || 'People';
const OUT = arg('out', '') || process.env.OUT || path.join(__dirname, 'expanded-urls.txt');
const CONC = Number(arg('conc', '') || process.env.CONC || 12) || 12;
const LIMIT = Number(arg('limit', '') || process.env.LIMIT || 0) || 0;
const RESUME = has('resume') || /^(1|true|yes|on)$/i.test(process.env.RESUME || '');
const DRY = has('dry') || /^(1|true|yes|on)$/i.test(process.env.DRY || '');
// S3_KEY (or --s3-key): upload OUT here when the pass finishes. A shard suffix is appended automatically
// so six machines writing the same key don't overwrite each other.
const S3_KEY = arg('s3-key', '') || process.env.S3_KEY || '';
const OUT_BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${process.env.AWS_REGION || 'us-east-1'}`;

// Same FNV-1a split the live discovery fleet uses: a sitemap always lands in the same shard.
const shardArg = arg('shard', '') || process.env.SHARD || '';
const sm = /^(\d+)\s*\/\s*(\d+)$/.exec(shardArg.trim());
if (shardArg && !sm) { console.error(`bad --shard "${shardArg}" (want i/N)`); process.exit(1); }
const SHARD_I = sm ? Number(sm[1]) : 0, SHARD_N = sm ? Math.max(1, Number(sm[2])) : 1;
if (SHARD_I >= SHARD_N) { console.error(`--shard index ${SHARD_I} out of range for N=${SHARD_N}`); process.exit(1); }
function hashStr(d) { let h = 0x811c9dc5; for (let i = 0; i < d.length; i++) { h ^= d.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h; }
const inShard = (s) => SHARD_N === 1 || (hashStr(s) % SHARD_N) === SHARD_I;

const loadNameSet = (file) => { try { return new Set(fs.readFileSync(path.join(__dirname, file), 'utf8').split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name'))); } catch (e) { return new Set(); } };

// Generic sitemap words carrying no profession signal (mirrors sitemap-lib-monitor's KW_STOP), used for the
// keyword second pass when the strict classification returns nothing.
const KW_STOP = new Set(['sitemap', 'sitemaps', 'sitemapindex', 'index', 'xml', 'gz', 'wp', 'post', 'posts', 'page', 'pages', 'main', 'all', 'www', 'http', 'https', 'html']);
function keywordTokens(keyword) {
  const raw = String(keyword || '').toLowerCase().replace(/\.[a-z0-9]+$/, '');
  const out = new Set();
  for (const t of raw.split(/[^a-z0-9]+/)) {
    if (t.length < 3 || KW_STOP.has(t)) continue;
    out.add(t);
    if (t.endsWith('s') && t.length > 3) out.add(t.slice(0, -1));
  }
  return out;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const genderMap = loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const bioNames = loadNameSet('Sitemap extensions.csv');
  const locNames = loadNameSet('Sitemap extensions - locations.csv');

  const filter = [{ term: { kind: KIND } }];
  if (SOURCE) filter.push({ term: { source: SOURCE } });
  const must_not = [{ term: { status: 'inactive' } }];
  if (RESUME) must_not.push({ exists: { field: 'expanded_at' } });
  const QUERY = { bool: { filter, must_not } };

  const total = (await client.count({ index: sitemaps.INDEX, body: { query: QUERY } })).body.count;
  console.error(`${KIND} sitemaps${SOURCE ? ` from source="${SOURCE}"` : ''}${RESUME ? ' not yet expanded' : ''}: ${total.toLocaleString()}`
    + `${SHARD_N > 1 ? ` | shard ${SHARD_I}/${SHARD_N}` : ''}${DRY ? '  [DRY — no fetches, no writes]' : ''}`);
  if (!total) { console.error('nothing to expand.'); process.exit(0); }

  const outStream = DRY ? null : fs.createWriteStream(OUT, { flags: 'a' });
  const seen = new Set();
  const t0 = Date.now();
  const tally = { sitemaps: 0, fetched: 0, withUrls: 0, empty: 0, errors: 0, urls: 0, dup: 0 };

  const write = (urls) => {
    let n = 0;
    for (const u of urls) {
      if (!u || seen.has(u)) { if (u) tally.dup++; continue; }
      seen.add(u); n++;
      if (outStream) outStream.write(u + '\n');
    }
    tally.urls += n;
    return n;
  };

  async function expandOne(d) {
    tally.fetched++;
    let pageUrls = [];
    try {
      const { watches } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules: {}, genderMap, bioSitemapNames: bioNames, locationSitemapNames: locNames });
      pageUrls = [...new Set((watches || []).filter((w) => w.kind === KIND).flatMap((w) => (w.urls || []).map((u) => u.url)))];
      // Strict pass found nothing -> retry with the stored keyword's profession tokens, exactly as the
      // nightly monitor does (a directory named /team/ often won't classify on filename alone).
      if (!pageUrls.length) {
        const hints = keywordTokens(d.keyword);
        if (hints.size) {
          const { watches: w2 } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules: {}, genderMap, bioSitemapNames: bioNames, locationSitemapNames: locNames, keywordHints: hints });
          pageUrls = [...new Set((w2 || []).filter((w) => w.kind === KIND).flatMap((w) => (w.urls || []).map((u) => u.url)))];
        }
      }
    } catch (e) { tally.errors++; return; }

    if (pageUrls.length) { tally.withUrls++; write(pageUrls); } else tally.empty++;
    // Mark it expanded so --resume can skip it, and record the page count for Have-vs-Pages.
    if (!DRY) {
      try { await sitemaps.setMonitorState(client, d.sitemap_url, { expanded_at: new Date().toISOString(), url_count: pageUrls.length || d.url_count || 0 }); }
      catch (e) { /* best-effort */ }
    }
  }

  // Page the Library by sitemap_url (stable under the writes this run makes behind the cursor).
  let after = null, queue = [];
  const drain = async () => { while (queue.length) { await Promise.all(queue.splice(0, CONC).map(expandOne)); } };
  for (;;) {
    const body = { size: 2000, query: QUERY, _source: ['sitemap_url', 'domain', 'keyword', 'url_count'], sort: [{ sitemap_url: 'asc' }] };
    if (after) body.search_after = after;
    const hits = (await client.search({ index: sitemaps.INDEX, body })).body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      const d = h._source;
      if (!inShard(d.sitemap_url)) continue;
      tally.sitemaps++;
      if (!DRY) queue.push(d);
      if (LIMIT && tally.sitemaps >= LIMIT) break;
    }
    await drain();
    console.error(`  ${tally.sitemaps.toLocaleString()} sitemap(s) | ${tally.withUrls.toLocaleString()} with URLs, ${tally.empty.toLocaleString()} empty, ${tally.errors.toLocaleString()} err | ${tally.urls.toLocaleString()} unique URLs`);
    after = hits[hits.length - 1].sort;
    if (LIMIT && tally.sitemaps >= LIMIT) break;
  }
  await drain();
  if (outStream) await new Promise((r) => outStream.end(r));

  // Hand the part off to S3 so the merge step can pull every shard's URLs into one resolve.
  if (S3_KEY && !DRY && S3) {
    const key = SHARD_N > 1 ? `${S3_KEY}.shard-${SHARD_I}-of-${SHARD_N}` : S3_KEY;
    try {
      const s3 = new S3.S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
      await s3.send(new S3.PutObjectCommand({ Bucket: OUT_BUCKET, Key: key, Body: fs.readFileSync(OUT), ContentType: 'text/plain' }));
      console.error(`uploaded ${tally.urls.toLocaleString()} URL(s) -> s3://${OUT_BUCKET}/${key}`);
    } catch (e) { console.error('S3 upload FAILED (the local file is still on the machine):', e.message); process.exitCode = 1; }
  }

  console.error(`\nDONE${DRY ? ' [DRY]' : ''}: ${tally.sitemaps.toLocaleString()} sitemap(s) in shard | fetched ${tally.fetched.toLocaleString()} `
    + `(${tally.withUrls.toLocaleString()} yielded URLs, ${tally.empty.toLocaleString()} empty, ${tally.errors.toLocaleString()} errors) `
    + `-> ${tally.urls.toLocaleString()} unique page URLs${tally.dup ? ` (${tally.dup.toLocaleString()} dups collapsed)` : ''}`
    + `${DRY ? '' : ` -> ${OUT}`} | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
