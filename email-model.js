/**
 * email-model.js — fill a missing Professional email for an email-less bio from the company's known
 * email pattern. Shared by the job/site-search pipeline (ui-server) AND the worker fleet, so the
 * fleet captures email-less bios it would otherwise drop at the email-keyed upsert.
 *
 * For each email-less person (First+Last+Gender, no email), group by company domain, learn the
 * Professional-email pattern from samples (the records in hand + optionally the central DB via
 * `dbQuery(domain) -> records[]`), and synthesize an address tagged Email Type 'Modelled'.
 */
const { cleanEmail, classifyEmail } = require('./extractor');
const { modelEmail, render, inferEmailPattern } = require('./email-pattern');

// Candidate local-part templates tried (in order) when verifying against the validation API.
const CANDIDATE_TEMPLATES = ['{first}.{last}', '{f}{last}', '{first}{last}', '{first}_{last}', '{last}.{first}', '{f}.{last}', '{first}'];

function rootDomain(url) {
  const t = String(url || '').trim(); if (!t) return '';
  try { return new URL(t).hostname.replace(/^www\./i, '').toLowerCase(); }
  catch { return t.replace(/^https?:\/\//i, '').split('/')[0].replace(/^www\./i, '').toLowerCase(); }
}
// Records from the job pipeline carry 'Domain'; raw extractRecord output (the worker) does not — fall
// back to the source URL's host so modelling works for both.
const domainOf = (r) => String((r && r['Domain']) || '').toLowerCase() || rootDomain(r && r['Web Source URL']);

// registrable-ish domain (strip subdomains); keeps two-part TLDs like co.uk together. Used as the email
// domain for the default-pattern fallback so a bio on a subdomain (financialprofessionals.massmutual.com)
// models against the parent (massmutual.com).
function registrableDomain(host) {
  const h = String(host || '').toLowerCase().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, '').trim();
  if (!h) return '';
  const p = h.split('.');
  if (p.length <= 2) return h;
  const last2 = p.slice(-2).join('.');
  return /^(co|com|org|net|gov|edu|ac)\.[a-z]{2}$/.test(last2) ? p.slice(-3).join('.') : last2;
}

// Mutate `records` IN PLACE: model emails for the email-less bios. Returns the count modelled.
// `dbQuery(domain)` (optional, async) returns existing records for that domain to learn the pattern.
// `defaultPattern` (optional, e.g. '{first}.{last}'): after learning fails, model ANY still-email-less
// record that has a gender (i.e. a recognized person) with this pattern @ its registrable domain — so
// no named/gendered bio is dropped for want of an email. All modelled emails are tagged 'Modelled'.
// `verify` (optional, async email->{ok,good,catchAll,noMx}): when set, each modelled address is checked
// against the deliverability API and different patterns/domains are tried until one is GOOD (verified
// mailbox or catch-all domain). `requireGood` (default true when verifying): if no candidate is GOOD,
// leave the record unmodelled rather than inventing a bad address (API errors fall back to a best guess).
async function modelMissingEmails(records, { dbQuery = null, patternQuery = null, defaultPattern = null, verify = null, requireGood = true } = {}) {
  const list = records || [];
  const missing = list.filter((r) => !cleanEmail(r['Email Address']) && r['First'] && r['Last'] && r['Gender']);
  if (!missing.length) return 0;

  const byDomain = new Map();
  for (const r of missing) {
    const d = domainOf(r);
    if (!d) continue;
    if (!byDomain.has(d)) byDomain.set(d, []);
    byDomain.get(d).push(r);
  }

  let modelled = 0;
  const domainClass = new Map();   // emailDomain -> 'catch_all' | 'no_mx' | 'verifiable' (cache across people)

  for (const [domain, people] of byDomain) {
    // Resolve a stored company model + a pattern learned from samples (both feed candidate ordering).
    let stored = null;
    if (patternQuery) { try { stored = await patternQuery(domain); } catch (e) { /* */ } }
    const samples = [];
    const addSample = (r) => { const email = cleanEmail(r['Email Address']); if (email && r['First'] && r['Last'] && classifyEmail(email) === 'Professional') samples.push({ first: r['First'], last: r['Last'], email }); };
    for (const r of list) if (domainOf(r) === domain) addSample(r);
    if (dbQuery) { try { for (const r of (await dbQuery(domain)) || []) addSample(r); } catch (e) { /* */ } }
    const learned = samples.length ? inferEmailPattern(samples) : null;

    if (!verify) {
      // ---- Unverified path (existing behavior): stored -> learned -> default ----
      if (stored && stored.pattern && stored.email_domain) {
        for (const r of people) { const l = render(stored.pattern, r['First'], r['Last']); if (l) { r['Email Address'] = `${l}@${stored.email_domain}`; r['Email Type'] = 'Modelled'; modelled++; } }
        continue;
      }
      if (learned) {
        for (const r of people) { const email = modelEmail(samples, r['First'], r['Last']); if (email) { r['Email Address'] = email; r['Email Type'] = 'Modelled'; modelled++; } }
      }
      continue;   // remaining handled by the default-fallback block below
    }

    // ---- Verified path: try candidate (pattern @ email-domain) until GOOD ----
    const patterns = [];
    const pushP = (t) => { if (t && !patterns.includes(t)) patterns.push(t); };
    pushP(stored && stored.pattern); pushP(learned && learned.pattern); pushP(defaultPattern);
    for (const t of CANDIDATE_TEMPLATES) pushP(t);
    const emailDomains = [];
    const pushD = (d) => { if (d && !emailDomains.includes(d)) emailDomains.push(d); };
    pushD(stored && stored.email_domain); pushD(learned && learned.domain); pushD(registrableDomain(domain)); pushD(domain);

    for (const r of people) {
      let set = false;
      for (const ed of emailDomains) {
        const cls = domainClass.get(ed);
        if (cls === 'no_mx') continue;
        if (cls === 'catch_all') { const l = render(patterns[0], r['First'], r['Last']); if (l) { r['Email Address'] = `${l}@${ed}`; r['Email Type'] = 'Modelled'; modelled++; set = true; } break; }
        for (const tpl of patterns) {
          const l = render(tpl, r['First'], r['Last']); if (!l) continue;
          const email = `${l}@${ed}`;
          const v = await verify(email);
          if (!v || !v.ok) { r['Email Address'] = email; r['Email Type'] = 'Modelled'; modelled++; set = true; break; }   // API error → best-guess, don't drop
          if (v.noMx) { domainClass.set(ed, 'no_mx'); break; }              // dead domain → try next email domain
          if (v.catchAll) { domainClass.set(ed, 'catch_all'); r['Email Address'] = email; r['Email Type'] = 'Modelled'; modelled++; set = true; break; }
          if (v.good) { r['Email Address'] = email; r['Email Type'] = 'Modelled'; modelled++; set = true; break; }
          // else BAD → try the next pattern
        }
        if (set) break;
        if (!domainClass.get(ed)) domainClass.set(ed, 'verifiable');        // remember: this domain needs per-mailbox checks
      }
      if (!set && !requireGood) {                                           // opt-in best-guess when nothing verified
        const dom = registrableDomain(domain); const l = render(defaultPattern || CANDIDATE_TEMPLATES[0], r['First'], r['Last']);
        if (dom && l) { r['Email Address'] = `${l}@${dom}`; r['Email Type'] = 'Modelled'; modelled++; }
      }
    }
  }

  // Default fallback (UNVERIFIED path only): model every still-email-less gendered bio with the default
  // pattern @ its registrable domain, so no named/gendered person is dropped for want of an email.
  if (defaultPattern && !verify) {
    for (const r of missing) {
      if (cleanEmail(r['Email Address'])) continue;
      const dom = registrableDomain(domainOf(r));
      if (!dom) continue;
      const local = render(defaultPattern, r['First'], r['Last']);
      if (!local) continue;
      r['Email Address'] = `${local}@${dom}`;
      r['Email Type'] = 'Modelled';
      modelled++;
    }
  }
  return modelled;
}

module.exports = { modelMissingEmails, domainOf, rootDomain, registrableDomain };

// ---------------------------------------------------------------------------------------------
if (require.main === module && process.argv.includes('--selftest')) {
  (async () => {
    let p = 0, f = 0; const ok = (n, c) => { if (c) { p++; console.log('  ✓', n); } else { f++; console.log('  ✗', n); } };

    // 1) models from a same-batch Professional sample (no DB needed)
    const recs = [
      { First: 'Jane', Last: 'Doe', Gender: 'F', 'Email Address': 'jane.doe@acme.com', 'Web Source URL': 'https://acme.com/team/jane-doe' },
      { First: 'John', Last: 'Roe', Gender: 'M', 'Email Address': '', 'Web Source URL': 'https://acme.com/team/john-roe' },
    ];
    const n1 = await modelMissingEmails(recs);
    ok('models an email-less bio from a same-batch sample', n1 === 1 && recs[1]['Email Address'] === 'john.roe@acme.com' && recs[1]['Email Type'] === 'Modelled');

    // 2) no sample anywhere -> not modelled
    const recs2 = [{ First: 'Amy', Last: 'Poe', Gender: 'F', 'Email Address': '', 'Web Source URL': 'https://nobody.com/team/amy-poe' }];
    ok('leaves a bio unmodelled when no pattern is known', (await modelMissingEmails(recs2)) === 0 && !recs2[0]['Email Address']);

    // 3) learns the pattern from the central DB (dbQuery) when the batch has no sample
    const recs3 = [{ First: 'Zoe', Last: 'Kim', Gender: 'F', 'Email Address': '', 'Web Source URL': 'https://firmx.com/people/zoe-kim' }];
    const dbQuery = async (domain) => domain === 'firmx.com'
      ? [{ First: 'Bob', Last: 'Lee', 'Email Address': 'blee@firmx.com' }] : [];
    const n3 = await modelMissingEmails(recs3, { dbQuery });
    ok('learns the pattern from the central DB via dbQuery', n3 === 1 && recs3[0]['Email Address'] === 'zkim@firmx.com');

    // 4) skips bios missing First/Last/Gender (can't model a name)
    const recs4 = [{ First: 'Pat', Last: '', Gender: 'M', 'Email Address': '', 'Web Source URL': 'https://acme.com/team/pat' }];
    ok('skips a bio without a full name', (await modelMissingEmails(recs4, { dbQuery: async () => [{ First: 'Jane', Last: 'Doe', 'Email Address': 'jane.doe@acme.com' }] })) === 0);

    // 5) domainOf falls back to the source URL host when there is no Domain field
    ok('domainOf derives the domain from Web Source URL', domainOf({ 'Web Source URL': 'https://www.acme.com/team/x' }) === 'acme.com');

    // 6) prefers a STORED company pattern (patternQuery) over sample-learning
    const recs6 = [{ First: 'Kay', Last: 'Ng', Gender: 'F', 'Email Address': '', Domain: 'stored.com', 'Web Source URL': 'https://stored.com/team/kay-ng' }];
    const patternQuery = async (domain) => (domain === 'stored.com' ? { pattern: '{f}{last}', email_domain: 'mail.stored.com' } : null);
    const n6 = await modelMissingEmails(recs6, { patternQuery });
    ok('models from a stored company pattern via patternQuery', n6 === 1 && recs6[0]['Email Address'] === 'kng@mail.stored.com' && recs6[0]['Email Type'] === 'Modelled');

    console.log(`\nemail-model self-test: ${p} passed, ${f} failed`);
    process.exit(f ? 1 : 0);
  })().catch((e) => { console.error(e); process.exit(1); });
}
