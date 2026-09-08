/**
 * name-from-path.js — derive a person's first/last name from a URL "last path" slug.
 *
 * Cleaning steps (shared by the crawler's nameFromSlug and AI Search):
 *   1. drop a trailing file extension (.aspx / .html / .htm / .pdf / .php)
 *   2. turn "-", "_", "+", ".", and "%20" into spaces
 *   3. keep alphabetic tokens only, and remove honorific / credential / page-word terms
 *   4. first name = first remaining token, last name = last remaining token
 */

// Terms to strip out of a slug before reading a name (case-insensitive, whole tokens).
const STRIP_TERMS = new Set([
  'bio', 'biography', 'about', 'dr', 'mr', 'mrs', 'ms', 'hon', 'rev', 'prof',
  'phd', 'md', 'esq', 'cpa', 'facs', 'sr', 'jr', 'ii', 'iii', 'iv', 'mba',
  // Directory/role words that prefix a profile slug ("/realtor-jane-smith-jane/"). None of these is ever
  // a given name, but they sit in first position, so the parser read them as one: 2,715 contacts were
  // stored as First="Realtor" / "Agent" / "Broker" / "Profile", 1,868 on myrealtor.nz alone.
  'realtor', 'realtors', 'agent', 'agents', 'broker', 'brokers', 'profile', 'profiles',
]);

const properCase = (s) =>
  String(s || '').replace(/\w\S*/g, (w) => w[0].toUpperCase() + w.slice(1).toLowerCase());

function pathNameTokens(lastPath) {
  const cleaned = String(lastPath || '').replace(/\.(aspx?|html?|pdf|php)$/i, '');
  return cleaned
    .replace(/%20/gi, ' ')
    .split(/[-_+.\s]+/)
    .map((t) => t.trim())
    .filter((t) => /^[A-Za-z]+$/.test(t))
    .filter((t) => !STRIP_TERMS.has(t.toLowerCase()));
}

// Best-effort { first, last } from a URL slug.
function nameFromPath(lastPath) {
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
  return {
    first: properCase(toks[0]),
    last: toks.length > 1 ? properCase(toks[toks.length - 1]) : '',
  };
}

// "John Smith" (or "" if nothing usable).
function nameStringFromPath(lastPath) {
  const { first, last } = nameFromPath(lastPath);
  return [first, last].filter(Boolean).join(' ');
}

module.exports = { nameFromPath, nameStringFromPath, pathNameTokens };
