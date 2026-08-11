/**
 * pipeline-contract-test.js — executable check that the CONTACT BUILD PIPELINE behaves the way the
 * process-flow document says it does. Every assertion calls the real module; nothing is mocked except
 * the network (injected fetchers) and the DB (injected query callbacks).
 *
 *   node pipeline-contract-test.js
 *
 * A failure here means the doc and the code have diverged — fix whichever is wrong. Grouped by the
 * pipeline stage it covers, so a failure names the stage.
 */
const path = require('path');
const ex = require('./extractor');
const vcard = require('./vcard');
const { inferEmailPattern, render, templateFor } = require('./email-pattern');
const { modelMissingEmails, registrableDomain, domainOf } = require('./email-model');
const liName = require('./li-name');
const { nameFromPath } = require('./name-from-path');

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
let pass = 0, fail = 0; const failures = [];
function ok(stage, name, cond, detail = '') {
  if (cond) { pass++; console.log(`  PASS  ${name}${detail ? '  — ' + detail : ''}`); }
  else { fail++; failures.push(`[${stage}] ${name}`); console.log(`  FAIL  ${name}${detail ? '  — ' + detail : ''}`); }
}
const H = (s) => console.log(`\n${s}\n${'-'.repeat(s.length)}`);

(async () => {
  // ---------------------------------------------------------------- 1. vCard reader
  H('STAGE 1 — vCard reader (premium source; augments, never clobbers a complete name)');
  {
    const card = ['BEGIN:VCARD', 'VERSION:3.0', 'N:Smith;Jane;;;', 'FN:Jane Smith', 'TITLE:Managing Partner',
      'EMAIL;TYPE=WORK:jane.smith@acme.com', 'TEL;TYPE=CELL:+1-609-413-6297',
      'ADR;TYPE=WORK:;;1 Main St;Trenton;NJ;08608;USA', 'END:VCARD'].join('\n');
    const parsed = vcard.parseVCard(card);

    // a COMPLETE card name overrides a page-derived name
    const r1 = { First: 'J', Last: 'S', Gender: '', 'Email Address': '', Title: '', 'Web Source URL': 'https://acme.com/team/j-s' };
    vcard.applyVCardToRecord(r1, parsed, { genderMap });
    ok('vcard', 'complete card name overrides the page name', r1.First === 'Jane' && r1.Last === 'Smith', `${r1.First} ${r1.Last}`);
    ok('vcard', 'gender re-derived from the corrected first name', r1.Gender === 'F', `Gender=${r1.Gender}`);
    ok('vcard', 'title filled when blank', r1.Title === 'Managing Partner');
    ok('vcard', 'email filled when blank + typed', r1['Email Address'] === 'jane.smith@acme.com' && r1['Email Type'] === 'Professional');
    ok('vcard', 'card ADR sets Phone Location (beats area code)', /Trenton/.test(r1['Phone Location'] || ''), r1['Phone Location']);

    // an existing email is NOT clobbered
    const r2 = { First: '', Last: '', 'Email Address': 'existing@acme.com', 'Web Source URL': 'https://acme.com/t/x' };
    vcard.applyVCardToRecord(r2, parsed, { genderMap });
    ok('vcard', 'existing email is never overwritten', r2['Email Address'] === 'existing@acme.com');

    // the hang guard (the bug that stalled corp-prospects)
    const t0 = Date.now();
    const res = await vcard.enrichRecords([{ vCard: 'https://x.test/a.vcf' }], { _fetch: () => new Promise(() => {}), timeoutMs: 800 });
    ok('vcard', 'a hanging fetcher is bounded, not fatal', Date.now() - t0 < 5000 && res.timedOut === 1, `${Date.now() - t0}ms`);
  }

  // ---------------------------------------------------------------- 2. Name assumption
  H('STAGE 2 — Name assumption (URL last path / email / LinkedIn slug)');
  {
    const n1 = nameFromPath('dr-jane-smith-phd.aspx');
    ok('name', 'last path: strips extension + honorifics/credentials', n1.first === 'Jane' && n1.last === 'Smith', JSON.stringify(n1));

    const che = require('./cc-home-enrich');
    const n2 = che.nameFromEmail('jane.smith@acme.com');
    ok('name', 'email: dotted local part yields First/Last', n2.first && n2.last, JSON.stringify(n2));
    const n3 = che.nameFromEmail('jsmith@acme.com');
    ok('name', 'email: non-dotted local part yields NO name (no guessing)', !(n3.first && n3.last), JSON.stringify(n3));

    ok('name', 'linkedin: separator slug parsed', (liName.inferFromLinkedin('https://linkedin.com/in/jane-smith-123') || {}).first === 'Jane');
    ok('name', 'linkedin: concatenated slug split via the names map', (liName.inferFromLinkedin('https://linkedin.com/in/angelomarino') || {}).first === 'Angelo');
    ok('name', 'linkedin: an existing name is only replaced when the slug yields a GENDER',
      liName.resolve({ first: 'Zephyrine', last: 'Blount', gender: '', linkedinUrl: 'https://linkedin.com/in/zephyrine-blount' }) === null);
    ok('name', 'linkedin: concatenated slug never re-splits a name the page already tokenized',
      liName.resolve({ first: 'Wanessa', last: 'Moore', gender: '', linkedinUrl: 'https://linkedin.com/in/wanessamoore' }) === null);
  }

  // ---------------------------------------------------------------- 3. Email modelling
  H('STAGE 3 — Email modelling (where pattern + domain come from)');
  {
    const learned = inferEmailPattern([
      { first: 'Jane', last: 'Doe', email: 'jane.doe@acme.com' },
      { first: 'Bob', last: 'Roe', email: 'bob.roe@acme.com' },
    ]);
    ok('model', 'pattern LEARNED from same-company samples', learned && learned.pattern === '{first}.{last}' && learned.domain === 'acme.com', JSON.stringify(learned));
    ok('model', 'templateFor recognises the local part that produced it', templateFor('jdoe', 'Jane', 'Doe') === '{f}{last}', templateFor('jdoe', 'Jane', 'Doe'));
    ok('model', 'render builds the local part from the template', render('{f}.{last}', 'Jane', 'Doe') === 'j.doe');

    // precedence: STORED company model beats sample-learned
    const recs = [{ First: 'Kay', Last: 'Ng', Gender: 'F', 'Email Address': '', Domain: 'stored.com', 'Web Source URL': 'https://stored.com/team/kay-ng' },
      { First: 'A', Last: 'B', 'Email Address': 'a.b@stored.com', Domain: 'stored.com', 'Web Source URL': 'https://stored.com/team/a-b' }];
    await modelMissingEmails(recs, { patternQuery: async (d) => (d === 'stored.com' ? { pattern: '{f}{last}', email_domain: 'mail.stored.com' } : null) });
    ok('model', 'STORED company model wins over sample-learned', recs[0]['Email Address'] === 'kng@mail.stored.com', recs[0]['Email Address']);
    ok('model', 'modelled address is tagged Email Type = Modelled', recs[0]['Email Type'] === 'Modelled');

    // gating: no name or no gender -> never modelled
    const noGender = [{ First: 'Pat', Last: 'Lee', Gender: '', 'Email Address': '', 'Web Source URL': 'https://acme.com/t/p' }];
    ok('model', 'a record with NO gender is never modelled', (await modelMissingEmails(noGender, { dbQuery: async () => [{ First: 'J', Last: 'D', 'Email Address': 'j.d@acme.com' }] })) === 0);

    // default-pattern fallback + registrable domain
    const sub = [{ First: 'Ann', Last: 'Poe', Gender: 'F', 'Email Address': '', 'Web Source URL': 'https://advisors.massmutual.com/team/ann-poe' }];
    await modelMissingEmails(sub, { defaultPattern: '{first}.{last}' });
    ok('model', 'default pattern falls back to the REGISTRABLE domain (subdomain -> parent)', sub[0]['Email Address'] === 'ann.poe@massmutual.com', sub[0]['Email Address']);
    ok('model', 'registrableDomain keeps two-part TLDs together', registrableDomain('team.acme.co.uk') === 'acme.co.uk', registrableDomain('team.acme.co.uk'));
    ok('model', 'domainOf falls back to the source URL host', domainOf({ 'Web Source URL': 'https://www.acme.com/t/x' }) === 'acme.com');
  }

  // ---------------------------------------------------------------- 4. Email validation
  H('STAGE 4 — Email validation of MODELLED addresses (what a verdict does to the record)');
  {
    const mk = () => [{ First: 'Zoe', Last: 'Kim', Gender: 'F', 'Email Address': '', Domain: 'v.com', 'Web Source URL': 'https://v.com/t/zoe-kim' }];
    // GOOD -> kept
    let r = mk(); await modelMissingEmails(r, { defaultPattern: '{first}.{last}', verify: async () => ({ ok: true, good: true }) });
    ok('verify', 'GOOD verdict keeps the modelled address', !!r[0]['Email Address'], r[0]['Email Address']);
    // BAD everywhere + requireGood -> record left WITHOUT an invented address
    r = mk(); await modelMissingEmails(r, { defaultPattern: '{first}.{last}', verify: async () => ({ ok: true, good: false }), requireGood: true });
    ok('verify', 'all-BAD + requireGood leaves the record unmodelled (no invented address)', !r[0]['Email Address'], JSON.stringify(r[0]['Email Address']));
    // BAD everywhere + requireGood:false -> best guess kept
    r = mk(); await modelMissingEmails(r, { defaultPattern: '{first}.{last}', verify: async () => ({ ok: true, good: false }), requireGood: false });
    ok('verify', 'requireGood:false keeps a best guess', !!r[0]['Email Address'], r[0]['Email Address']);
    // catch-all domain -> accepted without per-mailbox proof
    r = mk(); await modelMissingEmails(r, { defaultPattern: '{first}.{last}', verify: async () => ({ ok: true, good: false, catchAll: true }) });
    ok('verify', 'CATCH-ALL domain accepts the address', !!r[0]['Email Address'], r[0]['Email Address']);
    // API error -> do NOT drop the record
    r = mk(); await modelMissingEmails(r, { defaultPattern: '{first}.{last}', verify: async () => ({ ok: false }) });
    ok('verify', 'API error falls back to a best guess rather than dropping', !!r[0]['Email Address'], r[0]['Email Address']);
  }

  // ---------------------------------------------------------------- 5. Location assumption
  H('STAGE 5 — Location assumption (TLD / phone block / libphonenumber)');
  {
    ok('location', 'TLD -> country calling code (.co.uk => 44)', ex.countryCodeFromDomain('acme.co.uk') === '44', ex.countryCodeFromDomain('acme.co.uk'));
    ok('location', 'unknown TLD assumes US (1)', ex.countryCodeFromDomain('acme.xyz') === '1');
    const loc = await ex.geocodePhone('+16094136297', '1');
    ok('location', 'US number geocodes to a place', !!loc && /,/.test(loc), loc || '(none)');
    const intl = await ex.geocodePhone('+442071838750', '44');
    ok('location', 'UK number geocodes to a place', !!intl, intl || '(none)');
  }

  // ---------------------------------------------------------------- 6. Title / Position
  H('STAGE 6 — Title + Position (Position matched from the curated title list)');
  {
    ok('title', 'position matched from a job title in the text', !!ex.findPosition('Managing Partner', ''), ex.findPosition('Managing Partner', ''));
    ok('title', 'position matched out of a description', !!ex.findPosition('', 'She is a Loan Officer at Acme.'), ex.findPosition('', 'She is a Loan Officer at Acme.'));
    ok('title', 'no false positive on a plain sentence', !ex.findPosition('', 'Welcome to our website.'), JSON.stringify(ex.findPosition('', 'Welcome to our website.')));
  }

  // ---------------------------------------------------------------- 7. Gender assignment
  H('STAGE 7 — Gender assignment (131k names map; never guessed)');
  {
    ok('gender', 'known first name resolves', liName.genderOf('michael') === 'M' && liName.genderOf('michelle') === 'F');
    ok('gender', 'unknown first name stays blank (no guess)', liName.genderOf('zephyrine') === '');
    ok('gender', 'map is loaded at scale', Object.keys(genderMap).length > 100000, `${Object.keys(genderMap).length.toLocaleString()} names`);
    // Regression: a plain {} leaks Object.prototype, so a first name like "Constructor" returned a
    // FUNCTION as its gender and it was written to the record. Found in production (1 doc).
    for (const proto of ['constructor', 'toString', 'valueOf', 'hasOwnProperty']) {
      ok('gender', `prototype key "${proto}" does not leak a function as a gender`,
        genderMap[proto] === undefined && liName.genderOf(proto) === '', `typeof=${typeof genderMap[proto]}`);
    }
  }

  // ---------------------------------------------------------------- 8. Email classification
  H('STAGE 8 — Email classification (Professional / Personal / Role-Based)');
  {
    ok('classify', 'role inbox -> Role-Based', ex.classifyEmail('info@acme.com') === 'Role-Based');
    ok('classify', 'free provider -> Personal', ex.classifyEmail('jane@gmail.com') === 'Personal');
    ok('classify', 'company mailbox -> Professional', ex.classifyEmail('jane.doe@acme.com') === 'Professional');
    ex.setAdminRoleTerms(['leasing']);
    ok('classify', 'admin term adds to Role-Based', ex.classifyEmail('leasing@acme.com') === 'Role-Based');
    ok('classify', 'admin term does not catch a person', ex.classifyEmail('leslie@acme.com') === 'Professional');
    ex.setAdminRoleTerms([]);
  }

  // ---------------------------------------------------------------- 9. Write path
  H('STAGE 9 — Write path (score gate + name/gender backfill at ingest)');
  {
    const os = require('./opensearch');
    const R = (o) => os.recordToDoc(Object.assign({ First: '', Last: '', Gender: '', 'Email Address': '', 'LinkedIn URL': '', 'Web Source URL': 'https://acme.com/t/x' }, o));
    let d = R({ 'Email Address': 'x@acme.com', 'LinkedIn URL': 'https://linkedin.com/in/michael-jordan' });
    ok('write', 'ingest backfills name+gender from the LinkedIn slug', d.first === 'Michael' && d.gender === 'M');
    d = R({ 'Email Address': 'jane.doe@acme.com' });
    ok('write', 'ingest backfills name from a dotted email', d.first === 'Jane' && d.last === 'Doe');
    d = R({ First: 'Jane', Last: 'Doe', Gender: 'F', 'Email Address': 'j@acme.com', Title: 'Partner', Phone: '+16094136297' });
    ok('write', 'score counts populated high-value fields', d.score >= 4, `score=${d.score}`);
    ok('write', 'company LinkedIn/company pages are stripped from linkedin_url',
      os.cleanContactLinkedin('https://www.linkedin.com/company/acme') === '');
  }

  console.log(`\n${'='.repeat(64)}`);
  console.log(`${pass} passed, ${fail} failed`);
  if (failures.length) { console.log('\nDIVERGENCES (doc vs code):'); for (const f of failures) console.log('  - ' + f); }
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error('HARNESS ERROR', e && e.stack || e); process.exit(1); });
