/**
 * normalize.js — contact-normalization rules applied on EVERY central-DB write (db.upsertMany).
 *
 * Some sources reintroduce known-bad field values on every sync (e.g. the Google Sheet stores
 * Bankers Life agents' office hours in Position/Title, so a one-off DB fix gets reverted on the
 * next import). Encoding the rule here makes it durable: it runs at the single write chokepoint,
 * so re-imports, crawls, and Site Search all produce the corrected value. Add rules to RULES.
 */
const { lastPathFromUrl, nameSlugFromUrl, lastPathSeg, splitExtension } = require('./extractor');
const { countryForDomain } = require('./tld-lookup');
const { classifyLineType, blockLocation, loadPhoneBlocks, nanpDigits, PHONE_BLOCKS_CSV } = require('./wireless-block-classifier');

// memoized phone-block dataset { wireless:Set, location:Map } (see wireless-block-classifier.js)
const blocks = () => loadPhoneBlocks(PHONE_BLOCKS_CSV);
const mainNumber = (p) => splitExtension(String(p || '')).main;   // drop " ext. N" before classify/geocode

// US states + DC/PR and Canadian provinces, full names (lowercased). Used to recognize a COARSE
// phone location ("New Jersey, United States") so we can upgrade it to the block's city, without
// clobbering a real city-level location ("Brooklyn, NY", a remax slug city, a vCard ADR).
const STATE_NAMES = new Set([
  'alabama','alaska','arizona','arkansas','california','colorado','connecticut','delaware','florida',
  'georgia','hawaii','idaho','illinois','indiana','iowa','kansas','kentucky','louisiana','maine',
  'maryland','massachusetts','michigan','minnesota','mississippi','missouri','montana','nebraska',
  'nevada','new hampshire','new jersey','new mexico','new york','north carolina','north dakota','ohio',
  'oklahoma','oregon','pennsylvania','rhode island','south carolina','south dakota','tennessee','texas',
  'utah','vermont','virginia','washington','west virginia','wisconsin','wyoming','district of columbia',
  'puerto rico','alberta','british columbia','manitoba','new brunswick','newfoundland and labrador',
  'northwest territories','nova scotia','nunavut','ontario','prince edward island','quebec',
  'saskatchewan','yukon',
]);

// A location is "coarse" (safe to replace with a city-level block location) when it's blank, a single
// segment (a bare country like "United States", or a lone city), or its first segment is a state/
// province full name. A value like "Brooklyn, NY" or "Hackensack, NJ, United States" is NOT coarse.
function isCoarseLocation(loc) {
  const s = String(loc || '').trim();
  if (!s) return true;
  if (s.indexOf(',') < 0) return true;
  return STATE_NAMES.has(s.split(',')[0].trim().toLowerCase());
}

const RULES = [
  {
    // Bankers Life agent bios put office hours in Position/Title — force the role. Matches by
    // email domain (reliable) or the source/Domain being bankerslife.com (any subdomain).
    name: 'bankerslife-agent',
    match: (r) =>
      /[@.]bankerslife\.com$/i.test(String(r['Email Address'] || '').trim()) ||
      /(?:^|\.)bankerslife\.com$/i.test(String(r['Domain'] || '').trim().toLowerCase()),
    apply: (r) => { r['Position'] = 'Bankers Life Agent'; r['Title'] = 'Bankers Life Agent'; },
  },
  {
    // Last Path should be the segment the NAME came from, not a trailing record id. Backfill when
    // blank, AND correct it when the stored value is the trailing id (e.g. century21
    // /agent/detail/.../agnes-aaron/aid-… stored "aid-…" -> "Agnes Aaron"). Only overwrites when the
    // URL actually has a trailing id (name-bearing segment differs from the raw last segment), so
    // ordinary Last Paths are untouched.
    name: 'last-path-from-url',
    match: (r) => {
      const url = String(r['Web Source URL'] || '');
      if (!/^https?:\/\//i.test(url)) return false;
      const want = lastPathFromUrl(url);
      if (!want) return false;
      const cur = String(r['Last Path'] || '').trim();
      if (!cur) return true;                                            // blank -> backfill
      return cur !== want && nameSlugFromUrl(url) !== lastPathSeg(url); // stored a trailing id -> correct it
    },
    apply: (r) => { const lp = lastPathFromUrl(r['Web Source URL']); if (lp) r['Last Path'] = lp; },
  },
  {
    // Phone line type from the carrier block table (NANP) — SMS-SAFE. The engine treats an sms: link
    // as authoritative (an sms-textable number is Mobile, applied AFTER block classification — see
    // extractor.js), so sms wins over the block. A stored "Mobile" may be sms-derived and we can't tell
    // post-hoc, so we NEVER downgrade Mobile->Direct here. We only: set Toll Free (unambiguous,
    // area-code based), UPGRADE to Mobile on a wireless block, or FILL an empty/Unknown type with the
    // block's type. SAFEGUARDS: skip non-NANP (keep libphonenumber's intl type), skip blocks the
    // dataset doesn't cover, and skip records carrying a vCard (its TEL;TYPE=CELL is authoritative).
    // Existing landline-block records mislabeled Mobile thus self-correct on RE-CRAWL (engine:
    // block->Direct, then sms->Mobile if textable), not here.
    name: 'phone-type-from-block',
    match: (r) => {
      if (String(r['vCard'] || '').trim()) return false;
      return [['Phone', 'Phone Type'], ['Phone 2', 'Phone 2 Type']].some(([pf, tf]) => typeUpgrade(r[pf], r[tf]));
    },
    apply: (r) => {
      for (const [pf, tf] of [['Phone', 'Phone Type'], ['Phone 2', 'Phone 2 Type']]) {
        const nt = typeUpgrade(r[pf], r[tf]); if (nt) r[tf] = nt;
      }
    },
  },
  {
    // Phone location to the block's CITY (NANP) when the stored value is only coarse (state/country)
    // — block data is city-level where libphonenumber returns just the state. Never overrides a real
    // city (address-/slug-/vCard-derived), so it's safe at the write chokepoint.
    name: 'phone-location-block-city',
    match: (r) =>
      [['Phone', 'Phone Location'], ['Phone 2', 'Phone 2 Location']].some(([pf, lf]) =>
        isCoarseLocation(r[lf]) && blockLocation(mainNumber(r[pf]), blocks().location)),
    apply: (r) => {
      for (const [pf, lf] of [['Phone', 'Phone Location'], ['Phone 2', 'Phone 2 Location']]) {
        const loc = blockLocation(mainNumber(r[pf]), blocks().location);
        if (loc && isCoarseLocation(r[lf])) r[lf] = loc;
      }
    },
  },
  {
    // Location = Phone Location + TLD lookup: when there's still no Phone Location, fall back to the
    // domain's TLD-lookup country (e.g. a .ao domain -> "Angola") so the Location field is
    // consistently populated + searchable. (Crawled records already get this via geocodeRecords.)
    name: 'phone-location-tld-country',
    match: (r) => !String(r['Phone Location'] || '').trim(),
    apply: (r) => { const c = countryForDomain(r['Domain'] || r['Web Source URL'] || ''); if (c) r['Phone Location'] = c; },
  },
];

// Block-table line type for a stored phone, or "" when it shouldn't override the existing type
// (non-NANP, or a block the dataset doesn't cover). Toll Free is always trusted (area-code based).
function blockType(phone) {
  const main = mainNumber(phone);
  if (!main) return '';
  const c = classifyLineType(main, blocks().wireless);
  if (c.type === 'Unknown') return '';
  if (c.type === 'Toll Free') return 'Toll Free';
  const known = blocks().location.has(c.block) || blocks().wireless.has(c.block);
  return known ? c.type : '';
}

// SMS-SAFE line-type correction: the new type to write, or "" for no change. Never downgrades a
// stored Mobile to Direct (it may be an sms-textable number — sms wins over the carrier block).
// Allowed: -> Toll Free (unambiguous), -> Mobile on a wireless block, and -> Direct ONLY to fill a
// missing/Unknown type.
function typeUpgrade(phone, cur) {
  const t = blockType(phone);
  if (!t) return '';
  cur = String(cur || '').trim();
  if (t === 'Toll Free') return cur === 'Toll Free' ? '' : 'Toll Free';
  if (t === 'Mobile') return cur === 'Mobile' ? '' : 'Mobile';
  return (!cur || cur === 'Unknown') ? 'Direct' : '';   // landline block: fill only, never downgrade
}

// Apply all matching rules to a record (mutates + returns it).
function normalizeContact(rec) {
  if (!rec || typeof rec !== 'object') return rec;
  for (const rule of RULES) { try { if (rule.match(rec)) rule.apply(rec); } catch { /* skip bad rule */ } }
  return rec;
}

module.exports = { normalizeContact, RULES, isCoarseLocation, blockType, typeUpgrade };

// ---- offline self-test: node normalize.js --selftest ----
if (require.main === module && process.argv.includes('--selftest')) {
  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? '✓' : '✗') + ' ' + l); c ? p++ : f++; };

  // isCoarseLocation
  ok('coarse: blank', isCoarseLocation(''));
  ok('coarse: bare country', isCoarseLocation('United States'));
  ok('coarse: state full-name first', isCoarseLocation('New Jersey, United States'));
  ok('NOT coarse: city + 2-letter state', !isCoarseLocation('Hackensack, NJ, United States'));
  ok('NOT coarse: city + full state name', !isCoarseLocation('Brooklyn, New York'));

  // line type SMS-SAFE: a landline (WIRE/Office) block KEEPS a stored Mobile (it may be sms-textable;
  // sms wins over the carrier block). The bug fix for these happens on re-crawl, not here.
  const r1 = { Phone: '+12012012345', 'Phone Type': 'Mobile' };
  normalizeContact(r1);
  ok('landline block keeps Mobile (sms wins over block)', r1['Phone Type'] === 'Mobile');

  // line type: a landline block FILLS an empty type with Direct (no existing value to protect)
  const r1b = { Phone: '+12012012345', 'Phone Type': '' };
  normalizeContact(r1b);
  ok('landline block fills empty type -> Direct', r1b['Phone Type'] === 'Direct');

  // line type: a wireless (PCS) block upgrades a stored Direct -> Mobile
  const r2 = { Phone: '+12012042888', 'Phone Type': 'Direct' };
  normalizeContact(r2);
  ok('PCS block Direct -> Mobile', r2['Phone Type'] === 'Mobile');

  // line type: a toll-free number is corrected (unambiguous, area-code based)
  const r2b = { Phone: '+18002551212', 'Phone Type': 'Mobile' };
  normalizeContact(r2b);
  ok('toll-free corrected -> Toll Free', r2b['Phone Type'] === 'Toll Free');

  // vCard record is left alone (the card's CELL signal wins)
  const r3 = { Phone: '+12012042888', 'Phone Type': 'Direct', vCard: 'https://x/a.vcf' };
  normalizeContact(r3);
  ok('vCard record: line type untouched', r3['Phone Type'] === 'Direct');

  // non-NANP keeps its (intl) type
  const r4 = { Phone: '+442079460958', 'Phone Type': 'Direct' };
  normalizeContact(r4);
  ok('non-NANP: line type untouched', r4['Phone Type'] === 'Direct');

  // location: coarse state-only -> upgraded to the block city
  const r5 = { Phone: '+12012012345', 'Phone Location': 'New Jersey, United States' };
  normalizeContact(r5);
  ok('coarse location -> block city', /Hackensack, NJ/.test(r5['Phone Location']));

  // location: a real city is preserved (not overwritten by the phone's block city)
  const r6 = { Phone: '+12012012345', 'Phone Location': 'East Stroudsburg, PA' };
  normalizeContact(r6);
  ok('real city location preserved', r6['Phone Location'] === 'East Stroudsburg, PA');

  console.log(`\nnormalize self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
}
