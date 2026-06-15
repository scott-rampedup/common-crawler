/**
 * email-pattern.js — learn a company's email format from known (first,last,email)
 * samples, then MODEL a new person's address from that format.
 *
 * Used for bio pages that publish a name (+ phone / LinkedIn) but no email — common
 * on modern corporate sites that route contact through a form (e.g. rsmuk.com). In
 * sitemap / webpage mode we keep those people and fill in a best-guess email here,
 * labelled Email Type "Modelled" so a guess is never mistaken for a verified address.
 *
 * Dependency-free + offline-testable (`node email-pattern.js`).
 */

// Local-part templates we recognise, MOST-SPECIFIC FIRST (ties break toward the top).
// Tokens: {first} {last} = full names; {f} {l} = first letters.
const TEMPLATES = [
  '{first}.{last}', '{first}_{last}', '{first}-{last}', '{first}{last}',
  '{f}.{last}', '{f}_{last}', '{f}-{last}', '{f}{last}',
  '{last}.{first}', '{last}_{first}', '{last}{first}',
  '{last}.{f}', '{last}{f}',
  '{first}.{l}', '{first}{l}',
  '{first}', '{last}',
];

// strip accents + anything but a–z (names only; separators come from the template)
const norm = (s) => String(s || '').toLowerCase().normalize('NFKD').replace(/[^a-z]/g, '');

// Render a template into a local-part for a given name, or "" if the name is unusable.
function render(tpl, first, last) {
  const f = norm(first), l = norm(last);
  if (!f || !l) return '';
  return tpl
    .replace(/\{first\}/g, f)
    .replace(/\{last\}/g, l)
    .replace(/\{f\}/g, f[0])
    .replace(/\{l\}/g, l[0]);
}

// Which template produced this local-part for this person? "" if none match.
function templateFor(localPart, first, last) {
  const local = String(localPart || '').toLowerCase().trim();
  if (!local) return '';
  for (const tpl of TEMPLATES) {
    const rendered = render(tpl, first, last);
    if (rendered && rendered === local) return tpl;
  }
  return '';
}

// Learn the dominant { pattern, domain } from [{first,last,email}] samples, or null.
function inferEmailPattern(samples) {
  const patternVotes = new Map();   // template -> count
  const domainVotes = new Map();    // email domain -> count
  for (const s of (samples || [])) {
    const email = String(s && s.email || '').toLowerCase().trim();
    const at = email.indexOf('@');
    if (at < 1) continue;
    const local = email.slice(0, at);
    const domain = email.slice(at + 1);
    if (!domain) continue;
    const tpl = templateFor(local, s.first, s.last);
    if (!tpl) continue;                                   // local-part doesn't fit the name
    patternVotes.set(tpl, (patternVotes.get(tpl) || 0) + 1);
    domainVotes.set(domain, (domainVotes.get(domain) || 0) + 1);
  }
  if (!patternVotes.size || !domainVotes.size) return null;

  // most-voted pattern (TEMPLATES order already encodes the specificity tie-break,
  // because Map preserves insertion order and we seed votes in iteration order)
  let pattern = '', best = -1;
  for (const tpl of TEMPLATES) {
    const v = patternVotes.get(tpl) || 0;
    if (v > best) { best = v; pattern = tpl; }
  }
  let domain = '', bestD = -1;
  for (const [d, v] of domainVotes) if (v > bestD) { bestD = v; domain = d; }

  return { pattern, domain, samples: best };
}

// Model an email for `first`/`last` from the company's known samples, or "" if the
// pattern can't be learned (e.g. the company publishes no parseable emails at all).
function modelEmail(samples, first, last) {
  const p = inferEmailPattern(samples);
  if (!p) return '';
  const local = render(p.pattern, first, last);
  if (!local) return '';
  return `${local}@${p.domain}`;
}

module.exports = { inferEmailPattern, modelEmail, templateFor, render, TEMPLATES };

// ---------------------------------------------------------------- self-test
if (require.main === module) {
  let pass = 0, fail = 0;
  const ok = (label, cond) => { if (cond) { pass++; } else { fail++; console.log('FAIL:', label); } };

  ok('templateFor first.last', templateFor('john.smith', 'John', 'Smith') === '{first}.{last}');
  ok('templateFor flast', templateFor('jsmith', 'John', 'Smith') === '{f}{last}');
  ok('templateFor first', templateFor('john', 'John', 'Smith') === '{first}');
  ok('templateFor no-match', templateFor('info', 'John', 'Smith') === '');

  const firmA = [
    { first: 'John', last: 'Smith', email: 'john.smith@acme.com' },
    { first: 'Jane', last: 'Doe', email: 'jane.doe@acme.com' },
    { first: 'Al', last: 'King', email: 'al.king@acme.com' },
  ];
  const pA = inferEmailPattern(firmA);
  ok('infer pattern first.last', pA && pA.pattern === '{first}.{last}' && pA.domain === 'acme.com');
  ok('model new person', modelEmail(firmA, 'Mary', 'Lee') === 'mary.lee@acme.com');

  const firmB = [
    { first: 'John', last: 'Smith', email: 'jsmith@beta.io' },
    { first: 'Jane', last: 'Doe', email: 'jdoe@beta.io' },
  ];
  ok('model flast pattern', modelEmail(firmB, 'Mary', 'Lee') === 'mlee@beta.io');

  ok('accented names normalise', modelEmail(
    [{ first: 'José', last: 'Núñez', email: 'jose.nunez@x.com' }], 'Renée', 'Smith') === 'renee.smith@x.com');

  ok('no samples -> no model', modelEmail([], 'Mary', 'Lee') === '');
  ok('unparseable samples -> no model', modelEmail(
    [{ first: 'Mary', last: 'Lee', email: 'info@x.com' }], 'Mary', 'Lee') === '');

  console.log(`\nemail-pattern self-test: ${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
}
