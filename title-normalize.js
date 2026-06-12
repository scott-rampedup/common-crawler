/**
 * title-normalize.js — validate + canonicalize a job title.
 *
 * Given a raw title (e.g. from AI Search), snap it to a clean canonical form from
 * data/job-titles.json when it's a recognized title (so "vp of sales" and
 * "Vice President of Sales" both become "Vice President of Sales", "ceo" -> "CEO").
 * Anything not in the list is kept as-is but cleanly title-cased. No network, no deps.
 */
const fs = require('fs');
const path = require('path');

// Token-level abbreviation expansion, applied to BOTH sides when matching only.
const ABBR = {
  vp: 'vice president', svp: 'senior vice president', evp: 'executive vice president',
  avp: 'assistant vice president', sr: 'senior', jr: 'junior', mgr: 'manager', dir: 'director',
  asst: 'assistant', exec: 'executive', ops: 'operations',
  hr: 'human resources', it: 'information technology',
  // C-suite acronyms -> full titles. The last few are ambiguous (e.g. CSO/CCO/CDO/CPO have
  // multiple real meanings); these use the most common expansion — adjust if your data differs.
  ceo: 'chief executive officer', coo: 'chief operating officer', cto: 'chief technology officer',
  cfo: 'chief financial officer', cmo: 'chief marketing officer', cio: 'chief information officer',
  ciso: 'chief information security officer', chro: 'chief human resources officer',
  cro: 'chief revenue officer', cpo: 'chief product officer', cso: 'chief strategy officer',
  cdo: 'chief data officer', clo: 'chief legal officer', cco: 'chief compliance officer',
  // (intentionally omit genuinely-ambiguous ones like "admin"/"dev" — expanding them
  //  produces wrong results, e.g. "Admin Assistant" -> "Administrator Assistant".)
};

function norm(s) {
  let t = String(s || '').toLowerCase().replace(/&/g, ' and ');
  t = t.replace(/[^a-z0-9]+/g, ' ').trim();
  t = t.split(' ').filter(Boolean).map((w) => ABBR[w] || w).join(' ');
  return t.replace(/\s+/g, ' ').trim();
}

const ACRONYMS = new Set([
  'ceo', 'coo', 'cto', 'cfo', 'cmo', 'cio', 'ciso', 'chro', 'cro', 'cpo', 'cso', 'cdo', 'clo', 'cco',
  'vp', 'svp', 'evp', 'avp', 'hr', 'it', 'pr', 'qa', 'ux', 'ui', 'seo', 'sem', 'us', 'usa', 'uk', 'llc', 'inc',
]);
const SMALL = new Set(['of', 'the', 'and', 'for', 'to', 'in', 'at', 'on', 'a', 'an', 'by', 'with']);

function smartTitleCase(s) {
  const words = String(s || '').trim().split(/\s+/).filter(Boolean);
  return words.map((w, i) => {
    const bare = w.toLowerCase().replace(/[^a-z]/g, '');
    if (ACRONYMS.has(bare)) return w.toUpperCase();
    if (i > 0 && SMALL.has(bare)) return w.toLowerCase();
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  }).join(' ');
}

let NORM_SET = null;
function load() {
  if (NORM_SET) return;
  NORM_SET = new Map();
  let list = [];
  try { list = JSON.parse(fs.readFileSync(path.join(__dirname, 'data', 'job-titles.json'), 'utf8')); }
  catch (e) { list = []; }
  for (const t of list) { const n = norm(t); if (n && !NORM_SET.has(n)) NORM_SET.set(n, t); }
}

// Expand known role abbreviations (VP, Sr, IT, Mgr, Ops, HR, ...) anywhere in a title.
function expandAbbr(s) {
  return String(s || '').replace(/[A-Za-z][A-Za-z0-9]*/g, (w) => ABBR[w.toLowerCase()] || w);
}

// Snap a recognized title to its canonical display form; otherwise expand abbreviations
// and clean the casing.
function normalizeTitle(raw) {
  load();
  const r = String(raw || '').trim();
  if (!r) return r;
  const hit = NORM_SET.get(norm(r));
  return hit || smartTitleCase(expandAbbr(r));
}

module.exports = { normalizeTitle, _norm: norm, _smartTitleCase: smartTitleCase };
