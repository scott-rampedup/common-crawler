/**
 * team-page-harvest.js — turn known TEAM PAGES into BIO URLs.
 *
 *   OPENSEARCH_ENDPOINT=… node team-page-harvest.js [--dry] [--limit N] [--conc 64]
 *                                                   [--out s3://bucket/key.txt] [--sample]
 *
 * Company Crawler holds ~2M companies with a real team page but only ~225k with a bio_url: the pages
 * naming those people have never been opened. This walks them and emits the individual profile URLs,
 * which then go through the EXISTING extractor -- `bio-etl.js --mode urls --in <out> --live`, which
 * already resolves Common Crawl first and spends proxy budget only on the misses. Nothing here crawls
 * for contacts; this stage only produces the URL list.
 *
 * Neither existing job mode fits, which is why this exists: a `webpage` job extracts from the exact URL
 * (right for a bio page, but a team page is a list, not a person), and a `domain` job re-discovers the
 * whole site from robots/sitemap/homepage, throwing away the fact that we already know the page.
 *
 * WHAT COUNTS AS A BIO LINK. classifyDirectory alone is too loose on these pages -- measured on a random
 * sample it returned /associate-benefits, /capabilities/supply-chain-management/, /board-of-trustees/
 * and /pharmacy-residencies, because a content path can contain a bio-directory word. So a candidate is
 * kept only when the last path segment actually reads like a PERSON:
 *   - the given name is in the 131k-name gender map, or
 *   - the slug is 2-3 alphabetic tokens sitting directly under a strong bio directory (/team/jane-smith)
 * The first rule is the same corroboration that cleaned up the myrealtor.nz names.
 *
 * WHY COMMON CRAWL FIRST. Measured on a random 624 team pages, only 14% still answer a live request --
 * the Maps data is stale and most of these businesses are gone. That is not the fetcher (example.com and
 * apache.org fetch fine through the same call), it is the source. The archive still holds those pages as
 * they were, so resolving against Common Crawl is the only way to reach the dead majority; the live walk
 * is opt-in (--live) and runs only over what CC could not supply.
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const co = require('./companies');
const cc = require('./cc-engine');
const { classifyDirectory, loadGenderMap } = require('./extractor');
const { nameFromPath, pathNameTokens } = require('./name-from-path');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const SAMPLE = process.argv.includes('--sample');
const LIMIT = Number(arg('--limit', '0')) || 0;
const CONC = Number(arg('--conc', '')) || 64;
const REGION = process.env.AWS_REGION || 'us-east-1';
const OUT = arg('--out', `s3://${process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`}/bio-worklist/team-harvest-${new Date().toISOString().slice(0, 10)}.txt`);
const TMP = '/tmp/team-harvest.txt';
// Same default corpus set bio-etl resolves against, so a page missing from one crawl can still be found
// in an older one -- which matters here, where most of these sites are already gone.
const CRAWLS = arg('--crawls', '') || process.env.CRAWLS || 'CC-MAIN-2026-30,CC-MAIN-2026-25,CC-MAIN-2026-21,CC-MAIN-2026-17';
const LIVE = process.argv.includes('--live');      // also walk the CC misses over the proxy
const NO_CC = process.argv.includes('--no-cc');    // skip the archive (live only)
const SCRATCH = process.env.SCRATCH || '/tmp/_team-harvest';
const F = { urls: path.join(SCRATCH, 'pages.txt'), ptr: path.join(SCRATCH, 'ptr.jsonl') };
try { fs.mkdirSync(SCRATCH, { recursive: true }); } catch (e) { /* */ }

// Directory segments strong enough that a 2-3 token slug beneath one is a person even when the gender
// map does not know the given name (international names are under-represented in it).
const STRONG_DIR = new Set([
  'team', 'teams', 'our-team', 'ourteam', 'meet-the-team', 'meettheteam', 'meet-our-team',
  'staff', 'our-staff', 'ourstaff', 'people', 'our-people', 'ourpeople', 'agents', 'our-agents',
  'attorneys', 'lawyers', 'physicians', 'doctors', 'providers', 'advisors', 'brokers', 'realtors',
  'associates', 'partners', 'consultants', 'faculty', 'profiles', 'bios', 'directory',
]);

let GENDER = {};

/** Does this URL's last path segment read like a person's name? */
function looksLikePerson(url) {
  let u;
  try { u = new URL(url); } catch (e) { return false; }
  const segs = u.pathname.split('/').filter(Boolean);
  if (segs.length < 1) return false;
  const slug = segs[segs.length - 1];
  const toks = pathNameTokens(slug);
  if (toks.length < 2 || toks.length > 3) return false;         // "jane-smith", "mary-jo-blake"
  const { first, last } = nameFromPath(slug);
  if (!first || !last) return false;
  if (GENDER[String(first).toLowerCase()]) return true;          // the map knows the given name
  const parent = (segs[segs.length - 2] || '').toLowerCase();
  return STRONG_DIR.has(parent);                                 // /team/<two-token-slug>
}

function bioLinksFrom(html, pageUrl, domain) {
  const out = new Set();
  let links = [];
  try { links = cc.extractSameDomainLinks(html, pageUrl, domain) || []; } catch (e) { return []; }
  for (const raw of links) {
    const u = String(raw).split('#')[0];
    if (!/^https?:\/\//i.test(u)) continue;
    if (u.split('?')[0] === pageUrl.split('?')[0]) continue;     // the page itself
    if (classifyDirectory(u, '', {}, GENDER) !== 'BIO URL') continue;
    if (!looksLikePerson(u)) continue;
    out.add(u.split('?')[0]);
  }
  return [...out];
}

// Read one archived page out of Common Crawl.
//
// NOT cc-engine.fetchWarc: that pulls a byte range over HTTPS from data.commoncrawl.org, which answers
// 403 to this box (every one of a 32-record test failed that way). The Lambda extractor never hit this
// because it reads the same bytes from S3, so do the same here -- the `commoncrawl` bucket serves ranged
// GETs with the machine's own credentials and returned the full HTML for every record tested.
const zlib = require('zlib');
let _s3 = null;
async function warcHtml(rec) {
  const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
  if (!_s3) _s3 = new S3Client({ region: REGION });
  const start = Number(rec.offset), end = start + Number(rec.length) - 1;
  const r = await _s3.send(new GetObjectCommand({ Bucket: 'commoncrawl', Key: rec.filename, Range: `bytes=${start}-${end}` }));
  const chunks = [];
  for await (const c of r.Body) chunks.push(c);
  return cc.warcToHtml(zlib.gunzipSync(Buffer.concat(chunks)));
}

module.exports = { looksLikePerson, bioLinksFrom, warcHtml, STRONG_DIR };

function step(label, script, args) {
  console.error(`\n====== ${label} · ${new Date().toISOString().slice(11, 19)} ======`);
  const r = spawnSync(process.execPath, [path.join(__dirname, script), ...args], { stdio: 'inherit', cwd: __dirname, env: process.env });
  if (r.status !== 0) throw new Error(`${script} exited ${r.status}`);
}

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  try { GENDER = loadGenderMap(path.join(__dirname, 'names-genders.csv')); }
  catch (e) { console.error('gender map unavailable:', e.message); }
  console.error(`gender map: ${Object.keys(GENDER).length.toLocaleString()} names`);

  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const t0 = Date.now();
  // _doc order clusters by insertion, which on this index means alphabetically-adjacent domains -- a
  // "sample" taken that way is 3,000 near-identical tiny sites and tells you nothing about the corpus.
  // --sample therefore randomises; the full run keeps the stable keyword sort for search_after.
  const base = { wildcard: { 'team_page.keyword': { value: '?*' } } };
  const query = SAMPLE ? { function_score: { query: base, random_score: {} } } : base;
  const sort = SAMPLE ? undefined : [{ 'team_page.keyword': 'asc' }];

  // ---- 1. collect the team pages we are going to open
  const pages = new Map();                       // url -> domain
  let after = null;
  while (!LIMIT || pages.size < LIMIT) {
    const body = { size: 2000, query, _source: ['domain', 'team_page'] };
    if (sort) body.sort = sort;
    if (after && sort) body.search_after = after;
    const hits = ((await client.search({ index: co.INDEX, body })).body || {}).hits.hits;
    if (!hits || !hits.length) break;
    for (const h of hits) {
      const u = String(h._source.team_page || '').trim();
      if (/^https?:\/\//i.test(u) && !pages.has(u)) pages.set(u, String(h._source.domain || '').replace(/^www\./, ''));
      if (LIMIT && pages.size >= LIMIT) break;
    }
    if (!sort) break;                       // random sample: one page only, no stable cursor
    after = hits[hits.length - 1].sort;
  }
  console.error(`team pages to open : ${pages.size.toLocaleString()}`);
  if (!pages.size) { console.error('nothing to do.'); return; }

  fs.writeFileSync(F.urls, [...pages.keys()].join('\n') + '\n');

  // ---- 2. Common Crawl first. 86% of these pages no longer answer a live request (measured on a random
  // 624: only 14% fetched), but the archive still holds the page as it was. This is the same CC-first
  // order bio-etl uses, and it is the only way to reach the dead majority.
  const seen = new Set();
  let ccPages = 0, ccBios = 0, livePages = 0, liveBios = 0, dead = 0, warcErr = 0;
  const examples = [];
  const out = DRY ? null : fs.createWriteStream(TMP);
  const emit = (urls, from) => {
    let n = 0;
    for (const u of urls) {
      if (seen.has(u)) continue;
      seen.add(u); n++;
      if (examples.length < 10) examples.push(u);
      if (out) out.write(u + '\n');
    }
    if (from === 'cc') ccBios += n; else liveBios += n;
    return n;
  };

  if (!NO_CC) {
    step('resolve the team pages in Common Crawl', 'cc-athena-miner.js',
      ['--resolve-urls', F.urls, '--warc-out', F.ptr, '--crawls', CRAWLS,
       '--resolve-tag', 'tph' + Date.now().toString(36)]);

    const ptrs = [];
    if (fs.existsSync(F.ptr)) {
      const rl = readline.createInterface({ input: fs.createReadStream(F.ptr), crlfDelay: Infinity });
      for await (const l of rl) { if (l.trim()) { try { ptrs.push(JSON.parse(l)); } catch (e) { /* */ } } }
    }
    console.error(`  resolved in CC : ${ptrs.length.toLocaleString()} of ${pages.size.toLocaleString()}`);

    const oneWarc = async (rec) => {
      let html = '';
      try { html = await warcHtml(rec); } catch (e) { warcErr++; return; }
      if (!html) { warcErr++; return; }
      ccPages++;
      const dom = pages.get(rec.url) || (() => { try { return new URL(rec.url).hostname.replace(/^www\./, ''); } catch (e) { return ''; } })();
      emit(bioLinksFrom(html, rec.url, dom), 'cc');
    };
    for (let i = 0; i < ptrs.length; i += CONC) {
      await Promise.all(ptrs.slice(i, i + CONC).map(oneWarc));
      if (i && i % 20000 < CONC) console.error(`  WARC ${i.toLocaleString()}/${ptrs.length.toLocaleString()} | ${ccBios.toLocaleString()} bio URL(s)`);
    }
    for (const r of ptrs) pages.delete(r.url);       // leave only the CC misses
    console.error(`  CC harvest     : ${ccPages.toLocaleString()} page(s) read, ${ccBios.toLocaleString()} bio URL(s), ${warcErr.toLocaleString()} WARC error(s)`);
  }

  // ---- 3. live-fetch only what Common Crawl did not have, and only when asked
  if (LIVE && pages.size) {
    console.error(`\n====== live-fetch the ${pages.size.toLocaleString()} CC miss(es) ======`);
    const entries = [...pages.entries()];
    const oneLive = async ([page, dom]) => {
      let html = '';
      try { html = await cc.fetchDoc(page, { timeout: 10000, fallbackStatus: [403, 429, 503], maxBytes: 8 * 1024 * 1024 }); }
      catch (e) { /* dead */ }
      if (!html) { dead++; return; }
      livePages++;
      emit(bioLinksFrom(html, page, dom), 'live');
    };
    for (let i = 0; i < entries.length; i += CONC) {
      await Promise.all(entries.slice(i, i + CONC).map(oneLive));
      if (i && i % 20000 < CONC) console.error(`  live ${i.toLocaleString()}/${entries.length.toLocaleString()} | ${liveBios.toLocaleString()} bio URL(s)`);
    }
  } else if (pages.size) {
    console.error(`\n(${pages.size.toLocaleString()} CC miss(es) left alone; pass --live to walk them)`);
  }

  if (out) await new Promise((r) => out.end(r));
  const total = seen.size;
  console.error(`\n====== ${DRY ? 'DRY RUN' : 'DONE'} · ${Math.round((Date.now() - t0) / 1000)}s ======`);
  console.error(`  from Common Crawl : ${ccPages.toLocaleString()} page(s) -> ${ccBios.toLocaleString()} bio URL(s)`);
  console.error(`  from live crawl   : ${livePages.toLocaleString()} page(s) -> ${liveBios.toLocaleString()} bio URL(s)   (${dead.toLocaleString()} dead)`);
  console.error(`  BIO URLs distinct : ${total.toLocaleString()}`);
  console.error('  examples:');
  for (const e of examples) console.error('    ' + e);
  if (DRY || !total) { console.error('\n(dry run or nothing found: no upload)'); return; }

  const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(OUT);
  if (!m) { console.error(`\nwritten locally: ${TMP}`); return; }
  const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
  const size = fs.statSync(TMP).size;
  await new S3Client({ region: REGION }).send(new PutObjectCommand({
    Bucket: m[1], Key: m[2], Body: fs.createReadStream(TMP), ContentLength: size, ContentType: 'text/plain',
  }));
  console.error(`\nuploaded -> ${OUT}  (${(size / 1e6).toFixed(1)}MB)`);
  console.error(`next: node bio-etl.js --mode urls --in ${OUT} --live`);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
