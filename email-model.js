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
// The ^(co|com|org|net|gov|edu|ac)\.[a-z]{2}$ test got co.uk and com.br right and everything else
// wrong — 239,844 of the index's multi-label domains (4.84%), where it returned a bare public suffix
// and the email pattern was then modelled against a suffix rather than a company. See psl-suffix.js.
const psl = require('./psl-suffix');
function registrableDomain(host) {
  return psl.registrableOrHost(host);
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
// `onPattern(domain, pattern, emailDomain, sampleCount)` is called when a pattern is LEARNED from
// observed addresses and no stored model already covered the domain. It lets the caller persist what
// was worked out, so the next job on that domain reads it instead of re-deriving it -- and so a
// domain resolved once is resolved for good, which is the only way this scales past the handful of
// domains any single crawl happens to touch.
const MIN_SAMPLES_TO_CACHE = 2;   // one address is an anecdote, not a pattern -- never cache off it
// Before a domain's people are given addresses at a DIFFERENT domain, that many samples must agree,
// and that share of them. Deliberately stricter than MIN_SAMPLES_TO_CACHE: this is the error that
// sends mail to the wrong company entirely.
const CROSS_DOMAIN_MIN_SAMPLES = Number(process.env.EMAIL_CROSS_DOMAIN_MIN_SAMPLES) || 5;
const CROSS_DOMAIN_MIN_SHARE = Number(process.env.EMAIL_CROSS_DOMAIN_MIN_SHARE) || 0.7;
async function modelMissingEmails(records, { dbQuery = null, patternQuery = null, defaultPattern = null, verify = null, requireGood = true, onPattern = null } = {}) {
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
    // A sample must be an OBSERVED address, never one we invented.
    //
    // classifyEmail() reads the SHAPE of an address, not its provenance: a modelled
    // jane.smith@acme.com is not role-based and not free-mail, so it comes back 'Professional' and
    // would be fed straight back in as evidence for the pattern that produced it. The Postgres side
    // never had this problem -- dbQuery filters on the stored email_type column -- but records held
    // in the job itself were matched on shape alone, so a job carrying modelled rows could teach the
    // learner its own guesses and have them come back out as a "learned" pattern.
    const addSample = (r) => {
      const email = cleanEmail(r['Email Address']);
      if (!email || !r['First'] || !r['Last']) return;
      if (String(r['Email Type'] || '').trim().toLowerCase() === 'modelled') return;
      if (classifyEmail(email) !== 'Professional') return;
      samples.push({ first: r['First'], last: r['Last'], email });
    };
    for (const r of list) if (domainOf(r) === domain) addSample(r);
    if (dbQuery) { try { for (const r of (await dbQuery(domain)) || []) addSample(r); } catch (e) { /* */ } }
    let learned = samples.length ? inferEmailPattern(samples) : null;
    // Guard the cross-domain claim.
    //
    // Keeping the local-part pattern is low-risk -- the worst case is the wrong separator on the
    // site's own domain. Switching the MAIL DOMAIN is different: get it wrong and every address is
    // addressed to a company that has nothing to do with this person. Verified cases where it is
    // right (cumedicine.us -> ucdenver.edu, biox.stanford.edu -> stanford.edu) look identical in the
    // data to cases where it is wrong (myprivia.com -> sunnypeds.com, chosen by 4 samples on a
    // platform hosting hundreds of unrelated practices). The only thing that separates them is how
    // much of the evidence agrees, so require a real majority of a real sample before believing it.
    if (learned && learned.domain) {
      const own = registrableDomain(domain);
      const foreign = own && learned.domain !== own && learned.domain !== domain;
      const thin = (learned.domainSamples || 0) < CROSS_DOMAIN_MIN_SAMPLES
                || (learned.domainShare || 0) < CROSS_DOMAIN_MIN_SHARE;
      if (foreign && thin) learned = { ...learned, domain: own, crossDomainRejected: true };
    }
    // Persist a learned pattern, but only one with real evidence behind it. Caching a pattern
    // inferred from a single address would write a guess into the companies index, where every
    // later job would read it back as established fact -- the same feedback loop addSample now
    // refuses, one level up.
    // Never cache a pattern whose cross-domain claim we just refused: the fallback is a safe default
    // for this run, not a fact worth writing where every later job will read it as one.
    if (onPattern && learned && learned.pattern && !learned.crossDomainRejected && !stored && samples.length >= MIN_SAMPLES_TO_CACHE) {
      try { await onPattern(domain, learned.pattern, learned.domain || null, samples.length); }
      catch (e) { /* caching is best-effort; never let it break modelling */ }
    }

    if (!verify) {
      // ---- Unverified path (existing behavior): stored -> learned -> default ----
      if (stored && stored.pattern && stored.email_domain) {
        for (const r of people) { const l = render(stored.pattern, r['First'], r['Last']); if (l) { r['Email Address'] = `${l}@${stored.email_domain}`; r['Email Type'] = 'Modelled'; modelled++; } }
        continue;
      }
      if (learned && learned.pattern && learned.domain) {
        // Render from the GUARDED `learned`, not via modelEmail(samples, ...). modelEmail re-runs
        // inferEmailPattern on the raw samples internally, so it silently discarded the cross-domain
        // check above and kept addressing people at a tenant domain the evidence did not support.
        for (const r of people) {
          const l = render(learned.pattern, r['First'], r['Last']);
          if (!l) continue;
          r['Email Address'] = l + '@' + learned.domain;
          r['Email Type'] = 'Modelled';
          modelled++;
        }
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
