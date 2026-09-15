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
 * EXPECT A LOW FETCH RATE. On a random 80, only 30% of team pages loaded at all -- the Maps data is
 * stale and many of these businesses are gone. That is not a bug in the fetcher (example.com and
 * apache.org fetch fine through the same call); it is the shape of the source. Run the output through
 * bio-etl's CC-first path rather than assuming a live crawl will reach them.
 */
const fs = require('fs');
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

module.exports = { looksLikePerson, bioLinksFrom, STRONG_DIR };

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  try { GENDER = loadGenderMap(require('path').join(__dirname, 'names-genders.csv')); }
  catch (e) { console.error('gender map unavailable:', e.message); }
  console.error(`gender map: ${Object.keys(GENDER).length.toLocaleString()} names`);

  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const t0 = Date.now();
  const query = { wildcard: { 'team_page.keyword': { value: '?*' } } };
  const sort = SAMPLE
    ? [{ _doc: 'asc' }]
    : [{ 'team_page.keyword': 'asc' }];

  const out = DRY ? null : fs.createWriteStream(TMP);
  const seen = new Set();
  let scanned = 0, fetched = 0, dead = 0, productive = 0, bios = 0, dupes = 0;
  const examples = [];

  const one = async (row) => {
    const page = String(row.team_page || '');
    if (!page) return;
    let html = '';
    try {
      html = await cc.fetchDoc(page, { timeout: 10000, fallbackStatus: [403, 429, 503], maxBytes: 8 * 1024 * 1024 });
    } catch (e) { /* counted as dead below */ }
    if (!html) { dead++; return; }
    fetched++;
    const dom = String(row.domain || '').replace(/^www\./, '');
    const found = bioLinksFrom(html, page, dom);
    if (!found.length) return;
    productive++;
    for (const u of found) {
      if (seen.has(u)) { dupes++; continue; }
      seen.add(u); bios++;
      if (examples.length < 10) examples.push(u);
      if (out) out.write(u + '\n');
    }
  };

  let after = null;
  outer:
  for (;;) {
    const body = { size: 1000, query, _source: ['domain', 'team_page'], sort };
    if (after) body.search_after = after;
    const hits = ((await client.search({ index: co.INDEX, body })).body || {}).hits.hits;
    if (!hits || !hits.length) break;
    for (let i = 0; i < hits.length; i += CONC) {
      await Promise.all(hits.slice(i, i + CONC).map((h) => { scanned++; return one(h._source); }));
      if (LIMIT && scanned >= LIMIT) { after = hits[hits.length - 1].sort; break outer; }
    }
    after = hits[hits.length - 1].sort;
    const s = Math.max(1, (Date.now() - t0) / 1000);
    console.error(`  ${scanned.toLocaleString()} pages | ${fetched.toLocaleString()} fetched | ${bios.toLocaleString()} bio URL(s) | ${Math.round(scanned / s)}/s`);
  }
  if (out) await new Promise((r) => out.end(r));

  console.error(`\n====== ${DRY ? 'DRY RUN' : 'DONE'} · ${Math.round((Date.now() - t0) / 1000)}s ======`);
  console.error(`  team pages read     : ${scanned.toLocaleString()}`);
  console.error(`  fetched OK          : ${fetched.toLocaleString()} (${(100 * fetched / Math.max(1, scanned)).toFixed(0)}%)   dead/unreachable ${dead.toLocaleString()}`);
  console.error(`  pages yielding bios : ${productive.toLocaleString()} (${(100 * productive / Math.max(1, fetched)).toFixed(0)}% of fetched)`);
  console.error(`  BIO URLs (distinct) : ${bios.toLocaleString()}   (${dupes.toLocaleString()} duplicate(s) dropped)`);
  console.error('  examples:');
  for (const e of examples) console.error('    ' + e);
  if (DRY || !bios) { console.error('\n(dry run or nothing found: no upload)'); return; }

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
