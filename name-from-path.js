/**
 * name-from-path.js — derive a person's first/last name from a URL "last path" slug.
 *
 * Cleaning steps (shared by the crawler's nameFromSlug and AI Search):
 *   1. drop a trailing file extension (.aspx / .html / .htm / .pdf / .php)
 *   2. turn "-", "_", "+", ".", and "%20" into spaces
 *   3. keep alphabetic tokens only, and remove honorific / credential / page-word terms
 *   4. first name = first remaining token, last name = last remaining token
 *
 * STRICT MODE (opts.strict) additionally REFUSES to invent a person when the slug plainly is not
 * one. Measured on 2026-11-19 against 12,000 live records: on the slug-derived records this gate
 * can reach it rejects 36.2% of the un-gendered rows and 0.0% of the real people. That matters
 * because 7.2M of 17.4M stored contacts carry no gender, and sampling showed the commonest
 * "first names" among them are admin, board, the, our, meet, info -- URL words, not people.
 * Strict mode is used for the URL-slug path only; a schema.org name is authoritative and is never
 * passed through it.
 */

// Terms to strip out of a slug before reading a name (case-insensitive, whole tokens).
const STRIP_TERMS = new Set([
  'bio', 'biography', 'about', 'dr', 'mr', 'mrs', 'ms', 'hon', 'rev', 'prof',
  'phd', 'md', 'esq', 'cpa', 'facs', 'sr', 'jr', 'ii', 'iii', 'iv', 'mba',
  // Directory/role words that prefix a profile slug ("/realtor-jane-smith-jane/"). None of these is ever
  // a given name, but they sit in first position, so the parser read them as one: 2,715 contacts were
  // stored as First="Realtor" / "Agent" / "Broker" / "Profile", 1,868 on myrealtor.nz alone.
  'realtor', 'realtors', 'agent', 'agents', 'broker', 'brokers', 'profile', 'profiles',
  // Never a surname either, so these are removed wherever they sit -- not just in first position.
  // 'executive' is the locumtenens.com defect: 'marc-singh-executive-bio' stored Last="Executive",
  // 33 records on that domain and 278 index-wide. 'director'/'team' were the next two commonest.
  'executive', 'executives', 'director', 'directors', 'team', 'teams',
]);

// Mail and corporate noise. These arrive when the slug is really an email address or a company
// name -- "selinalandrew@cmu.edu" becomes "Selinalandrew Cmu Edu", and the TLD is then read as the
// surname. "Com" was the single commonest surname in the database (986 of a 6,000-row sample, and
// it also sits on records that ARE real people). Stripped unconditionally: none of these is ever
// part of a person's name, so there is nothing to lose by removing it even from a trusted source.
const NOISE_TERMS = new Set([
  'com', 'net', 'org', 'edu', 'gov', 'info', 'biz', 'io', 'mail', 'email', 'www',
  'gmail', 'yahoo', 'hotmail', 'outlook', 'aol', 'icloud', 'msn',
  'ltd', 'llc', 'inc', 'gmbh', 'plc', 'llp', 'pty', 'corp', 'mbh', 'bv', 'nv', 'srl',
]);

// Words that are never a GIVEN name. Several ARE real surnames (White, Crown, Board), so these are
// not stripped from the slug -- they only disqualify the name when they land in first position,
// which is where /our-board/, /meet-the-team/ and /admin/ put them.
const NOT_A_GIVEN_NAME = new Set([
  'admin', 'administrator', 'board', 'info', 'the', 'our', 'ours', 'us', 'team', 'teams', 'staff',
  'meet', 'forum', 'executive', 'executives', 'director', 'directors', 'platform', 'new', 'crown',
  'european', 'ranks', 'home', 'main', 'contact', 'contacts', 'member', 'members', 'welcome',
  'office', 'online', 'buy', 'buying', 'sales', 'support', 'help', 'login', 'search', 'news',
  'blog', 'shop', 'store', 'service', 'services', 'product', 'products', 'company', 'group',
  'people', 'person', 'employee', 'employees', 'leadership', 'management', 'committee', 'council',
  'faculty', 'department', 'enquiry', 'enquiries', 'impressum', 'kontakt', 'mitarbeiter', 'unser',
  'unsere', 'ueber', 'igreja', 'conservatory', 'accademia', 'ielts', 'professor', 'best', 'real',
  'official', 'graduate', 'advising', 'mobile', 'gaming', 'sports', 'queens', 'riverside',
  'advancement', 'white', 'adm', 'cllr', 'site', 'pharmacie', 'calendar', 'editor',
]);

// Longest given name in the shipped 131,327-name census list is 13 characters. A longer token is a
// run-together slug ("codegowithdev", "jknottseztouse"), not a first name: 13.6% of un-gendered
// records tripped this and 0.0% of gendered ones did.
const MAX_GIVEN_NAME = 14;

const properCase = (s) =>
  String(s || '').replace(/\w\S*/g, (w) => w[0].toUpperCase() + w.slice(1).toLowerCase());

function pathNameTokens(lastPath) {
  const cleaned = String(lastPath || '').replace(/\.(aspx?|html?|pdf|php)$/i, '');
  return cleaned
    .replace(/%20/gi, ' ')
    .split(/[-_+.\s]+/)
    .map((t) => t.trim())
    .filter((t) => /^[A-Za-z]+$/.test(t))
    .filter((t) => !STRIP_TERMS.has(t.toLowerCase()))
    .filter((t) => !NOISE_TERMS.has(t.toLowerCase()));
}

// Is this token a name a person is actually given? Accepts the gender map (an object), a Set, or a
// predicate, so callers can pass whatever they already hold.
function isKnownGivenName(token, knownFirst) {
  if (!knownFirst || !token) return false;
  const t = String(token).toLowerCase();
  if (typeof knownFirst === 'function') return !!knownFirst(t);
  if (knownFirst instanceof Set) return knownFirst.has(t);
  if (knownFirst instanceof Map) return !!knownFirst.get(t);
  return !!(knownFirst[t] || knownFirst[token]);
}

// Best-effort { first, last } from a URL slug.
//
// opts.strict      apply the plausibility gate below (URL slugs only -- see the header note)
// opts.knownFirst  census of given names; consulted ONLY to adjudicate a lone token, never to
//                  reject a name that has a surname beside it. A two-token slug like
//                  "loukisa-panagiota" is still accepted though neither word is in the list,
//                  because the original rule stands: an unusual name is not a wrong one.
function nameFromPath(lastPath, opts = {}) {
  let toks = pathNameTokens(lastPath);
  // drop a leading single-letter initial when a fuller name follows: "m-spencer-cook" -> Spencer Cook
  if (toks.length >= 3 && toks[0].length === 1) toks = toks.slice(1);
  // Some directories append the given name again after the full name:
  // "realtor-christine-vivienne-tallott-christine" -> Christine Vivienne Tallott. Taking the last token
  // as the surname yields "Christine Christine". When the final token merely repeats the first and a
  // real surname sits between them, drop the repeat. Requires 3+ tokens, so a genuine "morgan-morgan"
  // is left alone.
  if (toks.length >= 3 && toks[0].toLowerCase() === toks[toks.length - 1].toLowerCase()) toks = toks.slice(0, -1);
  if (!toks.length) return { first: '', last: '' };

  if (opts.strict) {
    const first = toks[0], lower = first.toLowerCase();
    // A page word in the given-name slot means the URL is a section, not a person.
    if (NOT_A_GIVEN_NAME.has(lower)) return { first: '', last: '' };
    // Run-together slug rather than a name.
    if (first.length >= MAX_GIVEN_NAME) return { first: '', last: '' };
    // No vowel and longer than an initial. Two-letter initials (Cj, Wm, Jd) are left alone --
    // they were the ONLY real people this rule cost in the 12,000-record measurement.
    if (first.length >= 3 && !/[aeiouy]/i.test(first)) return { first: '', last: '' };
    // A lone token has no surname to corroborate it, so it must be a name we recognise. This is
    // the one place the census list can reject: with nothing else on the page saying "person",
    // "/calendar/" and "/herbartem/" are indistinguishable from "/okonkwo/" without it.
    if (toks.length === 1 && opts.knownFirst && !isKnownGivenName(lower, opts.knownFirst)) {
      return { first: '', last: '' };
    }
  }

  return {
    first: properCase(toks[0]),
    last: toks.length > 1 ? properCase(toks[toks.length - 1]) : '',
  };
}

// "John Smith" (or "" if nothing usable).
function nameStringFromPath(lastPath, opts = {}) {
  const { first, last } = nameFromPath(lastPath, opts);
  return [first, last].filter(Boolean).join(' ');
}

module.exports = { nameFromPath, nameStringFromPath, pathNameTokens,
  NOISE_TERMS, NOT_A_GIVEN_NAME, MAX_GIVEN_NAME, isKnownGivenName };

// ---- offline self-test: node name-from-path.js --selftest ----
if (require.main === module && process.argv.includes('--selftest')) {
  let p = 0, f = 0;
  const ok = (l, c) => { console.log((c ? '\u2713' : '\u2717') + ' ' + l); c ? p++ : f++; };
  const eq = (l, got, first, last) => ok(l + `  -> "${got.first} ${got.last}".trim()`,
    got.first === first && got.last === last);
  // a small stand-in for the 131k-name census list
  const census = { jane: 1, marc: 1, sarah: 1, spencer: 1, patrik: 1, sibel: 1, okonkwo: 0 };
  const S = { strict: true, knownFirst: census };

  // --- ordinary bio slugs must be untouched by the gate ---
  eq('plain first-last', nameFromPath('jane-doe-bio', S), 'Jane', 'Doe');
  eq('honorific stripped', nameFromPath('dr-sarah-okafor', S), 'Sarah', 'Okafor');
  eq('leading initial dropped', nameFromPath('m-spencer-cook', S), 'Spencer', 'Cook');
  // An unusual name is not a wrong one: two tokens are accepted with no census lookup at all.
  eq('unknown two-token name still accepted', nameFromPath('loukisa-panagiota', S), 'Loukisa', 'Panagiota');

  // --- the locumtenens.com defect, fixed at source ---
  // "marc-singh-executive-bio" stored Last="Executive" on 33 records of that domain, 278 index-wide.
  eq('role word is never a surname', nameFromPath('marc-singh-executive-bio', S), 'Marc', 'Singh');
  eq('...and not in loose mode either', nameFromPath('carol-ko-executive-bio'), 'Carol', 'Ko');
  eq('director stripped the same way', nameFromPath('ana-reyes-director', S), 'Ana', 'Reyes');

  // --- email-derived slugs: a TLD is never a surname ---
  // "Com" was the commonest surname in the database (986 of a 6,000-row sample).
  eq('TLD dropped from an email slug', nameFromPath('patrik-kovar-gmail-com', S), 'Patrik', 'Kovar');
  ok('company suffix dropped', nameFromPath('acme-ltd').last !== 'Ltd');

  // --- section pages must not become people ---
  const none = (l, slug) => ok(l + `  ("${slug}")`, !nameFromPath(slug, S).first);
  none('page word in the given-name slot', 'our-board');
  none('...meet-the-team', 'meet-the-team');
  none('...bare admin', 'admin');
  none('run-together slug is not a first name', 'codegowithdev-com');
  none('no vowel', 'xkcd-ltd');
  none('lone token the census does not know', 'calendar');

  // --- things the gate must NOT do ---
  eq('lone token the census DOES know is kept', nameFromPath('jane', S), 'Jane', '');
  // Two-letter initials have no vowel and are real: Cj, Wm, Jd were the only real people the
  // vowel rule cost before it was limited to tokens of 3+ characters.
  eq('two-letter initials survive', nameFromPath('cj-admin', S), 'Cj', 'Admin');
  // Without a census list a lone token cannot be adjudicated, so it is kept rather than guessed at.
  eq('no census -> lone token kept', nameFromPath('okonkwo', { strict: true }), 'Okonkwo', '');
  // Loose mode is what schema.org / vCard names use and must stay permissive.
  eq('loose mode keeps a page word', nameFromPath('admin'), 'Admin', '');

  console.log(`\nname-from-path self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
}
