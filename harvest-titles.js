/**
 * harvest-titles.js — grow the job-title list from titles the pipeline captured but could NOT match.
 *
 *   OPENSEARCH_ENDPOINT=… node harvest-titles.js --dry [--min-count 25]
 *   OPENSEARCH_ENDPOINT=… node harvest-titles.js --out titles-additions.csv
 *
 * WHY: Title is populated on ~100% of contacts but Position only on 40.4%. The gap is titles the curated
 * list doesn't contain. The best of those come from two AUTHORITATIVE sources that state a job title
 * outright rather than inferring one:
 *
 *   schema.org jobTitle — extractRecord already reads it into Title, and already tries findPosition on it
 *   vCard TITLE        — the person's own published contact card, merged by vcard.applyVCardToRecord
 *
 * When either states "Mortgage Lender" and the list has never heard of it, Title gets the value and
 * Position stays blank forever. This finds exactly those: contacts WITH a title and WITHOUT a position,
 * grouped and thresholded, so a real job title used by many people surfaces and one-off page furniture
 * does not.
 *
 * Writes a CSV of candidates rather than editing "Titles in Order .csv" in place — the list is ordered by
 * specificity and matching is first-match-wins, so where a new title is inserted changes behaviour.
 */
const fs = require('fs');
const path = require('path');
const os = require('./opensearch');
const ex = require('./extractor');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const MIN_COUNT = Math.max(2, Number(arg('--min-count', '25')) || 25);
const OUT = arg('--out', path.join(__dirname, 'titles-additions.csv'));
const TOP = Number(arg('--top', '20000')) || 20000;

// The Title field is POLLUTED: it holds whatever text the page offered, which is usually the page <title>
// and often the person's own name. A first pass over production returned "admin" (115k), "Nothing Found",
// "One moment, please...", "No Results Found", "Select which cookies you accept" — CMS furniture, not jobs.
// So a candidate must positively LOOK like a job title, not merely fail to look like junk.

// A real job title almost always contains a role noun. Requiring one is what makes this high-precision.
const ROLE_HINT = new RegExp('\\b(' + [
  'officer', 'director', 'manager', 'supervisor', 'coordinator', 'administrator', 'executive', 'president',
  'chair', 'chairman', 'chairwoman', 'principal', 'partner', 'associate', 'assistant', 'analyst', 'specialist',
  'consultant', 'advisor', 'adviser', 'agent', 'broker', 'realtor', 'attorney', 'lawyer', 'solicitor',
  'barrister', 'paralegal', 'counsel', 'banker', 'lender', 'originator', 'underwriter', 'processor',
  'accountant', 'auditor', 'bookkeeper', 'controller', 'treasurer', 'secretary', 'clerk', 'registrar',
  'engineer', 'architect', 'developer', 'designer', 'scientist', 'researcher', 'technician', 'technologist',
  'physician', 'doctor', 'surgeon', 'dentist', 'nurse', 'practitioner', 'therapist', 'pathologist',
  'pharmacist', 'veterinarian', 'counselor', 'counsellor', 'psychologist', 'psychiatrist', 'dietitian',
  'teacher', 'professor', 'lecturer', 'instructor', 'educator', 'dean', 'provost', 'superintendent',
  'planner', 'strategist', 'buyer', 'trader', 'appraiser', 'inspector', 'adjuster', 'estimator',
  'representative', 'rep', 'ambassador', 'liaison', 'recruiter', 'trainer', 'coach', 'mentor',
  'owner', 'founder', 'cofounder', 'proprietor', 'operator', 'contractor', 'builder', 'installer',
  'stylist', 'groomer', 'chef', 'sommelier', 'pilot', 'captain', 'officer', 'paramedic', 'chiropractor',
  'optometrist', 'orthodontist', 'podiatrist', 'radiologist', 'anesthesiologist', 'oncologist',
  'ceo', 'cfo', 'coo', 'cto', 'cio', 'cmo', 'cco', 'evp', 'svp', 'vp', 'gm', 'md', 'esq',
].join('|') + ')\\b', 'i');

// Page furniture, CMS strings and non-title text seen in production.
const JUNK = /^(home|about|about us|contact|contact us|team|our team|staff|meet the team|meet our team|profile|my profile|biography|bio|welcome|news|blog|careers|privacy|terms|sitemap|search|login|menu|page not found|404|admin|contacts|members|people|directory|leadership|our staff|our people|faculty|who we are|archives|nothing found|no results found|administration)$/i;
const JUNK_SUBSTR = /(one moment|please wait|no results|nothing found|cookies?|javascript|enable|loading|error|untitled|click here|read more|learn more|coming soon|under construction|lorem ipsum|autor:|author:|\[email|@|https?:\/\/|www\.|\.com|\.co\.|\.org|\.net|\.nz|\.au)/i;

const looksLikeTitle = (t) => {
  const s = String(t || '').trim();
  if (s.length < 3 || s.length > 60) return false;
  if (JUNK.test(s)) return false;
  if (JUNK_SUBSTR.test(s)) return false;
  if (/[|<>{}\[\]•·»«]|\.{2,}/.test(s)) return false;           // page-title separators, ellipses
  if (/[?!]/.test(s)) return false;                             // questions/exclamations are copy, not titles
  if (!/[a-z]/i.test(s)) return false;
  if (/\d{3,}/.test(s)) return false;                           // ids, phone fragments
  if (s.split(/\s+/).length > 6) return false;                  // a sentence, not a title
  if (!ROLE_HINT.test(s)) return false;                         // the decisive test
  return true;
};

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  console.error(`harvesting unmatched titles${DRY ? '  [DRY RUN]' : ''} — threshold >=${MIN_COUNT} contacts\n`);

  // Titles on contacts the title list could NOT resolve to a Position.
  const body = {
    size: 0,
    query: { bool: { filter: [{ term: { position: '' } }], must_not: [{ term: { 'title.kw': '' } }] } },
    aggs: { t: { terms: { field: 'title.kw', size: TOP, order: { _count: 'desc' } } } },
  };
  const t0 = Date.now();
  const res = await client.search({ index: os.INDEX, body });
  const buckets = ((res.body || res).aggregations.t.buckets) || [];
  console.error(`distinct unmatched titles: ${buckets.length.toLocaleString()}\n`);

  const cands = [];
  const tally = { seen: buckets.length, junk: 0, tooFew: 0, nowMatches: 0, added: 0 };
  for (const b of buckets) {
    const title = String(b.key || '').trim();
    if (b.doc_count < MIN_COUNT) { tally.tooFew++; continue; }
    if (!looksLikeTitle(title)) { tally.junk++; continue; }
    // If findPosition can already resolve it, the list isn't missing it — the record is just stale.
    if (ex.findPosition(title, '')) { tally.nowMatches++; continue; }
    cands.push({ title, count: b.doc_count });
    tally.added++;
  }

  console.error('top candidates for the title list:');
  for (const c of cands.slice(0, 30)) console.error(`  ${String(c.count.toLocaleString()).padStart(9)}  ${c.title}`);
  console.error(`\n  below threshold      : ${tally.tooFew.toLocaleString()}`);
  console.error(`  rejected as junk     : ${tally.junk.toLocaleString()}`);
  console.error(`  already matchable    : ${tally.nowMatches.toLocaleString()}  (stale records — re-extract to fill Position)`);
  console.error(`  NEW candidates       : ${tally.added.toLocaleString()}`);
  const reach = cands.reduce((n, c) => n + c.count, 0);
  console.error(`  contacts they would give a Position to: ${reach.toLocaleString()}`);

  if (!DRY && cands.length) {
    fs.writeFileSync(OUT, 'title,contacts\n' + cands.map((c) => `"${c.title.replace(/"/g, '""')}",${c.count}`).join('\n') + '\n', 'utf8');
    console.error(`\nwrote ${cands.length.toLocaleString()} candidate(s) -> ${OUT}`);
    console.error('Review, then merge into "Titles in Order .csv". Order matters: matching is first-match-wins,');
    console.error('so put the MORE SPECIFIC title above any shorter one it contains ("Senior Loan Officer" before "Loan Officer").');
  }
  console.error(`\n${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.stack || e.message || e)); process.exit(1); });
