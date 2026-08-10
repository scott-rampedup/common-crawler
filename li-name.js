/**
 * li-name.js — the ONE implementation of "recover a person's name from their linkedin.com/in URL".
 *
 * Two parsers, tried in order:
 *   1) separator slugs  (firstname-lastname-123 -> First Last)  via extractor.nameFromSlug
 *   2) concatenated slugs (angelomarino -> Angelo Marino) via a dictionary split that uses the 131k
 *      names map as the first-name lexicon. Conservative — org/vanity slugs (superior-negotiators,
 *      txlender) are rejected by the org-word + length guards.
 *
 * INGEST RULE (opensearch.ensureNameGender) and BACKFILL (li-name-backfill.js) both call resolve(),
 * so a contact crawled today and a contact crawled last year get the identical treatment:
 *
 *   - no name at all            -> take the LinkedIn name (gendered or not; a name beats nothing)
 *   - name but NO gender        -> take the LinkedIn name ONLY if it resolves to a gender, since the
 *                                  gender is the real-person signal we're trying to recover. This can
 *                                  REPLACE a scraped name (people list a western name on LinkedIn).
 *   - name and gender           -> nothing to do
 *
 * When resolve() reports nameChanged, any email that was MODELLED off the old name is now wrong —
 * callers re-model it (ingest does so by running before email-model; the backfill re-keys the doc).
 */
const path = require('path');

let _gmap = null, _che = null;
// Lazily loaded + cached: this module is required by the ingest hot path (every doc) and by the Lambda
// package, so the 131k-row CSV must never be parsed more than once per process, and a missing file must
// degrade to "no gender" rather than throw.
function genderMap() {
  if (!_gmap) {
    try { _gmap = require('./extractor').loadGenderMap(path.join(__dirname, 'names-genders.csv')); }
    catch (e) { _gmap = {}; }
  }
  return _gmap;
}
function che() {
  if (_che === null) { try { _che = require('./cc-home-enrich'); } catch (e) { _che = false; } }
  return _che;
}

const cap = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '');
const trim = (s) => String(s == null ? '' : s).trim();
// Gender lookup mirrors the existing ingest convention (plain lowercase key), NOT extractor's
// normalizeForMatching — changing the key shape here would silently re-gender millions of contacts.
const genderOf = (first) => genderMap()[String(first || '').toLowerCase()] || '';

// Org/vanity words that make a concatenated slug a firm, not a person.
const ORG = /loan|lender|mortgage|realt|group|team|llc|inc|onlus|negotiat|insurance|agency|propert|homes|lawyer|consult|solutions|services|marketing|digital|media|studio|academy|foundation|network|global|partners|associates|capital|ventures|advisor|financial/;

// Pull the /in/<slug> out of a LinkedIn URL. Returns '' for company pages and non-LinkedIn URLs.
function slugOf(url) {
  const m = String(url || '').match(/linkedin\.com\/in\/([^/?#]+)/i);
  return m ? m[1] : '';
}

// angelomarino -> {first:'Angelo', last:'Marino'}. Separator-less slugs only (the separator parser owns
// hyphenated ones); first name >= 4 chars and present in the names map, last 3..15.
function splitConcat(slug) {
  const raw = String(slug || '').toLowerCase();
  if (!raw || /[-_.]/.test(raw)) return null;
  const s = raw.replace(/[^a-z]/g, '');
  if (s.length < 6 || s.length > 22 || ORG.test(s)) return null;
  const g = genderMap();
  for (let i = Math.min(s.length - 3, 12); i >= 4; i--) {
    const f = s.slice(0, i), l = s.slice(i);
    if (g[f] && l.length >= 3 && l.length <= 15) return { first: cap(f), last: cap(l) };
  }
  return null;
}

// The name a linkedin.com/in URL implies, or null. Separator parser first, then the dictionary split.
// `source` says which one won — 'separator' names carry the profile's own tokenization and are trusted to
// replace a name; 'concat' names are a guess and are heavily restricted by resolve() (see below).
function inferFromLinkedin(url) {
  const slug = slugOf(url);
  if (!slug) return null;
  const c = che();
  if (c) {
    const via = c.nameFromLinkedin(url);
    if (via && via.first && via.last) return { first: cap(via.first), last: cap(via.last), source: 'separator' };
  }
  const sc = splitConcat(slug);
  return sc ? { ...sc, source: 'concat' } : null;
}

const letters = (s) => String(s || '').toLowerCase().replace(/[^a-z]/g, '');

/**
 * Decide what a contact's name/gender should become given its LinkedIn URL.
 * Returns null when nothing should change, else { first, last, name, gender, nameChanged }.
 */
function resolve({ first, last, gender, linkedinUrl }) {
  const f = trim(first), l = trim(last), g = trim(gender);
  const hasName = !!(f && l);
  if (hasName && g) return null;                       // already complete
  let nm = inferFromLinkedin(linkedinUrl);
  if (!nm || !nm.first || !nm.last) return null;

  // A CONCATENATED slug (wanessamoore) carries no tokenization — where the first name ends is a dictionary
  // guess, while the page already gave us a human-written split. Guessing again corrupts names that were
  // already right (Wanessa Moore -> "Wane Ssamoore", Nissar Ahamed -> "Nissa Rahamed"). So splitConcat may
  // CREATE a name for a blank record, but may only REPLACE an existing one in the single case where the
  // tokens we already hold provably concatenate to the slug in the other order — a pure Last/First order
  // fix (Ziemann Holger + /in/holgerziemann -> Holger Ziemann), where nothing is being guessed at all.
  if (hasName && nm.source === 'concat') {
    const slug = letters(slugOf(linkedinUrl));
    if (!slug || letters(l) + letters(f) !== slug) return null;
    nm = { first: cap(l), last: cap(f), source: 'swap' };   // use the KNOWN tokens, not the guessed split
  }

  const derivedG = genderOf(nm.first);
  // A name already on the record is only overwritten when the LinkedIn name buys us a gender.
  if (hasName && !derivedG) return null;

  const nameChanged = nm.first.toLowerCase() !== f.toLowerCase() || nm.last.toLowerCase() !== l.toLowerCase();
  const out = { first: nm.first, last: nm.last, name: `${nm.first} ${nm.last}`.trim(), gender: derivedG || g, nameChanged };
  // No-op guard: same name AND no gender gained (blank-name records whose slug is ungendered still
  // count as a change, because the name itself is new).
  if (!nameChanged && out.gender === g) return null;
  return out;
}

// ---- display-record adapter ('First'/'Last'/'Gender'/'LinkedIn URL' — the shape the pipelines use) ----
// Mutates in place. Returns { changed, nameChanged }.
function applyToRecord(rec) {
  if (!rec) return { changed: false, nameChanged: false };
  const r = resolve({ first: rec['First'], last: rec['Last'], gender: rec['Gender'], linkedinUrl: rec['LinkedIn URL'] });
  if (!r) return { changed: false, nameChanged: false };
  rec['First'] = r.first; rec['Last'] = r.last; rec['Gender'] = r.gender;
  return { changed: true, nameChanged: r.nameChanged };
}

// Run over a batch before email modelling, so a modelled address is built from the corrected name
// instead of having to be re-modelled afterwards. Returns { changed, nameChanged } totals.
function applyToRecords(records) {
  let changed = 0, nameChanged = 0;
  for (const rec of records || []) {
    try { const r = applyToRecord(rec); if (r.changed) { changed++; if (r.nameChanged) nameChanged++; } }
    catch (e) { /* per-record, never break a batch */ }
  }
  return { changed, nameChanged };
}

module.exports = { resolve, inferFromLinkedin, splitConcat, slugOf, genderOf, applyToRecord, applyToRecords };

// ---------------------------------------------------------------------------------------------
if (require.main === module && process.argv.includes('--selftest')) {
  let p = 0, f = 0;
  const ok = (n, c) => { if (c) { p++; console.log('  ✓', n); } else { f++; console.log('  ✗', n); } };
  const G = genderOf('michael') ? 'loaded' : 'MISSING';
  console.log(`names map: ${G} (${Object.keys(genderMap()).length.toLocaleString()} names)\n`);

  ok('parses a separator slug', (() => { const n = inferFromLinkedin('https://www.linkedin.com/in/michael-jordan-123'); return n && n.first === 'Michael' && n.last === 'Jordan'; })());
  ok('splits a concatenated slug', (() => { const n = inferFromLinkedin('https://linkedin.com/in/angelomarino'); return n && n.first === 'Angelo' && n.last === 'Marino'; })());
  ok('rejects an org slug', inferFromLinkedin('https://linkedin.com/in/superiornegotiators') === null);
  ok('rejects a company page', inferFromLinkedin('https://www.linkedin.com/company/acme') === null);
  ok('rejects a non-LinkedIn URL', inferFromLinkedin('https://twitter.com/in/michael-jordan') === null);

  // blank name -> take the LinkedIn name even without a gender
  ok('fills a blank name from LinkedIn', (() => {
    const r = resolve({ first: '', last: '', gender: '', linkedinUrl: 'https://linkedin.com/in/michael-jordan' });
    return r && r.first === 'Michael' && r.gender === 'M' && r.nameChanged;
  })());

  // name present, gender missing, LinkedIn name IS gendered -> overwrite
  ok('replaces an ungendered name with a gendered LinkedIn name', (() => {
    const r = resolve({ first: 'Xiuying', last: 'Chen', gender: '', linkedinUrl: 'https://linkedin.com/in/michelle-chen' });
    return r && r.first === 'Michelle' && r.last === 'Chen' && r.gender === 'F' && r.nameChanged;
  })());

  // name present, gender missing, LinkedIn name NOT gendered -> leave alone ('Zephyrine' is not in the
  // 131k names map, so the slug buys us nothing and the scraped name stands).
  ok('keeps the scraped name when LinkedIn adds no gender', resolve({ first: 'Zephyrine', last: 'Blount', gender: '', linkedinUrl: 'https://linkedin.com/in/zephyrine-blount' }) === null);
  // initials-only slugs parse to a one-letter "first name" that is never in the map -> rejected, so an
  // existing name is never clobbered by /in/j-r-smith.
  ok('keeps the scraped name when the slug is initials', resolve({ first: 'Zephyrine', last: 'Smith', gender: '', linkedinUrl: 'https://linkedin.com/in/j-r-smith' }) === null);

  // complete record -> untouched
  ok('skips a record that already has name + gender', resolve({ first: 'Michael', last: 'Jordan', gender: 'M', linkedinUrl: 'https://linkedin.com/in/someone-else' }) === null);

  // same name, gender gained -> change with nameChanged=false (no email re-model needed)
  ok('gains gender without a name change', (() => {
    const r = resolve({ first: 'Michael', last: 'Jordan', gender: '', linkedinUrl: 'https://linkedin.com/in/michael-jordan' });
    return r && r.gender === 'M' && r.nameChanged === false;
  })());

  ok('no LinkedIn URL -> nothing to do', resolve({ first: '', last: '', gender: '', linkedinUrl: '' }) === null);

  // ---- regressions from the first production dry run: a concatenated slug must never RE-SPLIT a name
  // the page already tokenized correctly. All three of these were being corrupted. ----
  ok('does not re-split Wanessa Moore', resolve({ first: 'Wanessa', last: 'Moore', gender: '', linkedinUrl: 'https://linkedin.com/in/wanessamoore' }) === null);
  ok('does not re-split Nissar Ahamed', resolve({ first: 'Nissar', last: 'Ahamed', gender: '', linkedinUrl: 'https://ca.linkedin.com/in/nissarahamed' }) === null);
  ok('does not re-split Stef Dyankov', resolve({ first: 'Stef', last: 'Dyankov', gender: '', linkedinUrl: 'https://linkedin.com/in/dyankov91' }) === null);
  // ...but a provable Last/First ORDER fix is still allowed (nothing is guessed: holger+ziemann IS the slug)
  ok('still fixes Last/First order from a concatenated slug', (() => {
    const r = resolve({ first: 'Ziemann', last: 'Holger', gender: '', linkedinUrl: 'https://linkedin.com/in/holgerziemann' });
    return r && r.first === 'Holger' && r.last === 'Ziemann' && r.gender === 'M' && r.nameChanged;
  })());
  // ...and a blank record can still be named by the splitter (it is creating, not overwriting)
  ok('concatenated slug still names a blank record', (() => {
    const r = resolve({ first: '', last: '', gender: '', linkedinUrl: 'https://linkedin.com/in/wanessamoore' });
    return r && r.first === 'Wane' && r.nameChanged;
  })());
  // separator slugs keep their authority to replace a junk name (this is the main win of the whole rule)
  ok('separator slug still replaces a junk name', (() => {
    const r = resolve({ first: 'Black', last: 'Nonprofits', gender: '', linkedinUrl: 'https://www.linkedin.com/in/akshita-sankepally/' });
    return r && r.first === 'Akshita' && r.last === 'Sankepally' && r.gender === 'F';
  })());

  // record adapter mutates in place
  ok('applyToRecord mutates the display record', (() => {
    const rec = { First: '', Last: '', Gender: '', 'LinkedIn URL': 'https://linkedin.com/in/angelomarino' };
    const r = applyToRecord(rec);
    return r.changed && rec.First === 'Angelo' && rec.Last === 'Marino' && rec.Gender === 'M';
  })());

  console.log(`\nli-name self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
}
