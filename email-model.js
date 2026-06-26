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
const { modelEmail } = require('./email-pattern');

function rootDomain(url) {
  const t = String(url || '').trim(); if (!t) return '';
  try { return new URL(t).hostname.replace(/^www\./i, '').toLowerCase(); }
  catch { return t.replace(/^https?:\/\//i, '').split('/')[0].replace(/^www\./i, '').toLowerCase(); }
}
// Records from the job pipeline carry 'Domain'; raw extractRecord output (the worker) does not — fall
// back to the source URL's host so modelling works for both.
const domainOf = (r) => String((r && r['Domain']) || '').toLowerCase() || rootDomain(r && r['Web Source URL']);

// Mutate `records` IN PLACE: model emails for the email-less bios. Returns the count modelled.
// `dbQuery(domain)` (optional, async) returns existing records for that domain to learn the pattern.
async function modelMissingEmails(records, { dbQuery = null } = {}) {
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
  for (const [domain, people] of byDomain) {
    const samples = [];
    const addSample = (r) => {
      const email = cleanEmail(r['Email Address']);
      if (email && r['First'] && r['Last'] && classifyEmail(email) === 'Professional') {
        samples.push({ first: r['First'], last: r['Last'], email });
      }
    };
    for (const r of list) if (domainOf(r) === domain) addSample(r);          // samples in this batch
    if (dbQuery) { try { for (const r of (await dbQuery(domain)) || []) addSample(r); } catch (e) { /* best-effort */ } }
    if (!samples.length) continue;                                            // no pattern to learn from
    for (const r of people) {
      const email = modelEmail(samples, r['First'], r['Last']);
      if (email) { r['Email Address'] = email; r['Email Type'] = 'Modelled'; modelled++; }
    }
  }
  return modelled;
}

module.exports = { modelMissingEmails, domainOf, rootDomain };

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

    console.log(`\nemail-model self-test: ${p} passed, ${f} failed`);
    process.exit(f ? 1 : 0);
  })().catch((e) => { console.error(e); process.exit(1); });
}
