/**
 * serp-lookup.js — SERP Look Up tool.
 * ---------------------------------------------------------------------------------------------------
 * Input: rows of { first, last, employer, website, title }. For each person it runs ONE serper.dev
 * query ("First Last" + employer + title) and pulls, from the organic results:
 *   - LinkedIn Contact URL  (a linkedin.com/in/ profile)
 *   - Bio URL               (a page on the person's employer website, else a generic bio/contact page)
 *   - Snippet               (the matching result's snippet)
 * Returns the same fields plus those three. 1 serper credit per row.
 *
 *   const { lookupOne, parseCsv, toCsv, IN_COLS, OUT_COLS } = require('./serp-lookup');
 */
const { serperSearch } = require('./serper');
const { isBioOrContactUrl } = require('./cc-engine');

const IN_COLS = ['First Name', 'Last Name', 'Employer', 'Website', 'Title'];
const OUT_COLS = [...IN_COLS, 'LinkedIn URL', 'Bio URL', 'Snippet'];

function hostOf(u) {
  const t = String(u || '').trim();
  if (!t) return '';
  try { return new URL(/^https?:\/\//i.test(t) ? t : 'https://' + t).hostname.replace(/^www\./i, '').toLowerCase(); }
  catch { return t.replace(/^https?:\/\//i, '').replace(/^www\./i, '').split('/')[0].toLowerCase(); }
}
const cleanUrl = (u) => String(u || '').split('#')[0].trim();
const isSocial = (u) => /(^|\.)(linkedin|facebook|twitter|x|instagram|youtube|tiktok|pinterest)\.com/i.test(u);

// One person -> { linkedin, bio, snippet } from a single serper query.
async function lookupOne(row, { apiKey } = {}) {
  const first = String(row.first || '').trim();
  const last = String(row.last || '').trim();
  const employer = String(row.employer || '').trim();
  const title = String(row.title || '').trim();
  const siteHost = hostOf(row.website || '');
  const name = [first, last].filter(Boolean).join(' ');
  if (!name) return { linkedin: '', bio: '', snippet: '', query: '', error: 'no name' };

  const query = ['"' + name + '"', employer, title].filter(Boolean).join(' ');
  const res = await serperSearch(query, { apiKey, num: 10 });
  const organic = (res && res.organic) || [];

  let linkedin = '', bio = '', snippet = '';
  for (const o of organic) {
    const link = o.link || '';
    if (!linkedin && /(^|\.)linkedin\.com\/in\//i.test(link)) { linkedin = cleanUrl(link); if (!snippet) snippet = o.snippet || ''; }
    if (!bio && siteHost && hostOf(link) === siteHost) { bio = cleanUrl(link); if (!snippet) snippet = o.snippet || ''; }
    if (linkedin && bio) break;
  }
  // Fallback bio: a generic bio/contact page (not a social profile) if no employer-site match was found.
  if (!bio) {
    for (const o of organic) {
      const link = o.link || '';
      if (isSocial(link)) continue;
      if (isBioOrContactUrl(link)) { bio = cleanUrl(link); if (!snippet) snippet = o.snippet || ''; break; }
    }
  }
  if (!snippet && organic[0]) snippet = organic[0].snippet || '';
  return { linkedin, bio, snippet, query, error: (res && res.error) || '', credits: (res && res.credits) || 0 };
}

// --- CSV in/out (tolerant: header names matched loosely; quoted fields with commas/newlines supported) ---
function parseCsvLine(line) {
  const out = []; let cur = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (q) { if (c === '"') { if (line[i + 1] === '"') { cur += '"'; i++; } else q = false; } else cur += c; }
    else if (c === '"') q = true;
    else if (c === ',') { out.push(cur); cur = ''; }
    else cur += c;
  }
  out.push(cur);
  return out;
}
function colKey(s) { return String(s || '').toLowerCase().replace(/[^a-z]/g, ''); }
const ALIASES = {
  first: 'first', firstname: 'first', fname: 'first', givenname: 'first',
  last: 'last', lastname: 'last', lname: 'last', surname: 'last', familyname: 'last',
  employer: 'employer', company: 'employer', organization: 'employer', org: 'employer',
  website: 'website', site: 'website', url: 'website', domain: 'website', web: 'website',
  title: 'title', jobtitle: 'title', role: 'title', position: 'title',
};
// Parse CSV text -> [{ first, last, employer, website, title, _orig }]. _orig keeps the raw input cells.
function parseCsv(text) {
  const lines = String(text || '').split(/\r?\n/).filter((l) => l.trim() !== '');
  if (!lines.length) return [];
  const header = parseCsvLine(lines[0]).map((h) => ALIASES[colKey(h)] || '');
  const hasHeader = header.some(Boolean);
  const idx = {}; header.forEach((k, i) => { if (k && !(k in idx)) idx[k] = i; });
  const rows = [];
  const start = hasHeader ? 1 : 0;
  // positional fallback if no recognizable header: First,Last,Employer,Website,Title
  const pos = { first: 0, last: 1, employer: 2, website: 3, title: 4 };
  for (let i = start; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]);
    const get = (k) => (hasHeader ? (idx[k] != null ? cells[idx[k]] : '') : cells[pos[k]]);
    const r = { first: (get('first') || '').trim(), last: (get('last') || '').trim(),
      employer: (get('employer') || '').trim(), website: (get('website') || '').trim(), title: (get('title') || '').trim() };
    if (r.first || r.last || r.employer) rows.push(r);
  }
  return rows;
}
const esc = (v) => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
function toCsv(results) {
  const out = [OUT_COLS.join(',')];
  for (const r of results) {
    out.push([r.first, r.last, r.employer, r.website, r.title, r.linkedin, r.bio, r.snippet].map(esc).join(','));
  }
  return out.join('\n') + '\n';
}

module.exports = { lookupOne, hostOf, parseCsv, toCsv, parseCsvLine, IN_COLS, OUT_COLS };
