/**
 * team-page-clean.js — clear `team_page` on company records where the URL is not actually a team page.
 *
 *   OPENSEARCH_ENDPOINT=… node team-page-clean.js [--dry] [--limit N] [--sample]
 *
 * The Google-Maps scraper fills team_page with whatever it decided was the team page, and for a large
 * minority that is just the site's homepage ("http://www.texascleanteam.com/"). Those are worthless as
 * bio-URL input and they inflate every count built on the field.
 *
 * WHAT COUNTS AS A TEAM PAGE: the URL PATH must contain a segment naming a group of people. The test is
 * on the path only, never the host -- "texascleanteam.com/" scores on its hostname otherwise, which is
 * how a homepage came to look like a team page in the first place. Segments are split on - and _ so
 * "meet-our-team" and "our_staff" both resolve to their tokens.
 *
 * Recruiting pages are explicitly NOT team pages. "/join-our-team" and "/careers" contain the token
 * "team" but list vacancies, not the people who work there, so they yield no bios.
 *
 * Clearing sets the field to "" rather than removing it, matching how empty values are already stored.
 */
const co = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const SAMPLE = process.argv.includes('--sample');     // random order, for an unbiased estimate
const LIMIT = Number(arg('--limit', '0')) || 0;
const PAGE = 2000;

// Words that only name people when they ARE the whole segment. As loose tokens they match a subject
// rather than a group: "management" kept /blog/weight-management and /warehousing-inventory-management,
// and "leadership" would keep /thought-leadership. "management-team" and "leadership-team" still pass
// below, on the "team" token.
const TEAM_SEGMENTS = new Set([
  'management', 'leadership', 'senior-leadership', 'executive-leadership', 'our-leadership',
  'our-management', 'the-management', 'seniorleadership', 'ourleadership', 'ourmanagement',
  // Staff directories and "who we are" pages. Whole-segment only for the same reason: "directory" as a
  // loose token would take /directory/products and /business-directory, and "employee" would take
  // /employee-benefits. As complete segments these are rosters -- 2,400 of them arrived in the first
  // supplied list and were being rejected.
  'directory', 'staff-directory', 'staffdirectory', 'employee-directory', 'employeedirectory',
  'team-directory', 'teamdirectory', 'agent-directory', 'physician-directory', 'provider-directory',
  'who-we-are', 'whoweare', 'our-vets', 'ourvets',
]);

// A path segment naming a group of people at the company.
const TEAM_TOKENS = new Set([
  'team', 'teams', 'staff', 'people', 'personnel', 'leaders',
  'executives', 'directors', 'board', 'trustees', 'officers', 'principals', 'partners',
  'associates', 'employees', 'faculty', 'crew', 'roster', 'members',
  'agents', 'attorneys', 'lawyers', 'solicitors', 'barristers', 'advisors', 'advisers',
  'brokers', 'realtors', 'consultants', 'specialists', 'professionals', 'practitioners',
  'providers', 'physicians', 'doctors', 'surgeons', 'dentists', 'clinicians', 'therapists',
  'nurses', 'instructors', 'coaches', 'stylists', 'technicians', 'volunteers',
  'bios', 'biographies', 'profiles', 'whoweare', 'meettheteam', 'ourteam', 'ourstaff',
  'meetourteam', 'meetourstaff', 'ourpeople', 'meetus',
]);

// Recruiting. Contains "team" often enough to pass the test above, but lists jobs rather than people.
const DENY_TOKENS = new Set([
  'careers', 'career', 'jobs', 'job', 'join', 'joinus', 'apply', 'application', 'applications',
  'recruitment', 'recruiting', 'recruiter', 'vacancies', 'vacancy', 'hiring', 'employment',
  'opportunities', 'internships', 'apprenticeships',
]);

function pathOf(url) {
  const s = String(url || '').trim();
  if (!s) return null;
  try { return new URL(s).pathname || '/'; } catch (e) { /* fall through */ }
  // tolerate a scheme-less value ("acme.com/team")
  const m = /^[^/]+(\/.*)$/.exec(s.replace(/^\/\//, ''));
  return m ? m[1] : '/';
}

/** true when the URL's PATH names a group of people and is not a recruiting page. */
function isTeamPage(url) {
  const path = pathOf(url);
  if (path === null) return false;
  const segs = path.split('/').filter(Boolean);
  if (!segs.length) return false;                       // bare host = homepage
  let hit = false;
  for (let i = 0; i < segs.length; i++) {
    const bare = segs[i].toLowerCase().replace(/\.(html?|php|aspx?|jsp)$/, '');
    const tokens = bare.split(/[-_+.]+/).filter(Boolean);
    // The ambiguous words count only as the LAST segment, i.e. the page itself. As an interior segment
    // they name a section you are passing through, not a roster: /directory/products is a product list.
    if (i === segs.length - 1 && TEAM_SEGMENTS.has(bare)) hit = true;
    // the whole segment as one token too, so "meetourteam" matches without separators
    for (const t of [bare, ...tokens]) {
      if (DENY_TOKENS.has(t)) return false;
      if (TEAM_TOKENS.has(t)) hit = true;
    }
  }
  return hit;
}

module.exports = { isTeamPage, pathOf, TEAM_TOKENS, TEAM_SEGMENTS, DENY_TOKENS };

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const cc = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const t0 = Date.now();
  const query = { wildcard: { 'team_page.keyword': { value: '?*' } } };
  const sort = SAMPLE ? [{ _doc: 'asc' }] : [{ 'team_page.keyword': 'asc' }];

  let after = null, scanned = 0, keep = 0, drop = 0, cleared = 0;
  const dropped = [], kept = [];
  for (;;) {
    const body = { size: PAGE, query, _source: ['team_page'], sort };
    if (after) body.search_after = after;
    const hits = ((await cc.search({ index: co.INDEX, body })).body || {}).hits.hits;
    if (!hits || !hits.length) break;
    const actions = [];
    for (const h of hits) {
      scanned++;
      const v = h._source.team_page;
      if (isTeamPage(v)) {
        keep++;
        if (kept.length < 8) kept.push(String(v));
      } else {
        drop++;
        if (dropped.length < 12) dropped.push(String(v));
        if (!DRY) { actions.push({ update: { _index: co.INDEX, _id: h._id } }, { doc: { team_page: '' } }); cleared++; }
      }
    }
    if (actions.length) {
      try { await cc.bulk({ body: actions, refresh: false }); }
      catch (e) { console.error('  bulk failed:', e.message); }
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 100000 < PAGE) {
      const s = Math.max(1, (Date.now() - t0) / 1000);
      console.error(`  ${scanned.toLocaleString()} scanned | keep ${keep.toLocaleString()} | drop ${drop.toLocaleString()} | ${Math.round(scanned / s)}/s`);
    }
    if (LIMIT && scanned >= LIMIT) break;
  }
  if (!DRY) { try { await cc.indices.refresh({ index: co.INDEX }); } catch (e) { /* */ } }

  console.error(`\n====== ${DRY ? 'DRY RUN' : 'DONE'} · ${Math.round((Date.now() - t0) / 1000)}s ======`);
  console.error(`  team_page values seen : ${scanned.toLocaleString()}`);
  console.error(`  KEEP (real team page) : ${keep.toLocaleString()} (${(100 * keep / Math.max(1, scanned)).toFixed(1)}%)`);
  console.error(`  ${DRY ? 'would clear' : 'CLEARED'}       : ${(DRY ? drop : cleared).toLocaleString()} (${(100 * drop / Math.max(1, scanned)).toFixed(1)}%)`);
  console.error('\n  kept examples:');
  for (const s of kept) console.error('    ' + s);
  console.error('\n  cleared examples:');
  for (const s of dropped) console.error('    ' + s);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
