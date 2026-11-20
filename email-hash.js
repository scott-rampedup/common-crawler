/**
 * email-hash.js — SHA-256 over the Public Prospects canonical address form.
 *
 *   const { ppNormalizeEmail, emailHash } = require('./email-hash');
 *   emailHash('  Amp.Rewitt13+news@GoogleMail.com ')  -> '9f0c...'
 *
 * WHY THIS IS A PORT AND NOT A FRESH NORMALISER. A hashed email is only useful if the SAME address
 * produces the SAME hash in every store that holds one. RampedUp, Public Prospects and the Contact
 * Crawler have to agree exactly; one character of divergence and a join silently returns nothing,
 * which looks like "no overlap" rather than like a bug. So this reproduces Public Prospects' own
 * normaliser rather than inventing a second one.
 *
 * SOURCE OF TRUTH: PublicProspects/pilot/load_sightings.py (NORM_LOCAL, NORM_DOM and the CASE that
 * joins them). NOT PublicProspects/sql/003_normalize_fn.sql, which is STALE -- that file still carries
 * the entity-strip bug described below, as do pilot/dm_detailed.py, pilot/overlap_matrix.py,
 * pilot/poi_extract.py and pilot/phone_backfill.py. The SQL file's own header warns that two
 * implementations of a normaliser will drift; five of them now have.
 *
 * THE SEMICOLON IS LOAD-BEARING. An earlier version stripped bare entity NAMES,
 * '^(u00..|amp|quot|gt|lt)+', which ate those letters off the front of any local part that legitimately
 * began with them: amprewitt13@gmail.com was stored as rewitt13@gmail.com -- a different address, one
 * nobody ever published. 'gt' and 'lt' were the worst of it, because gtaylor, gthomas and ltdsales are
 * ordinary addresses. An entity is only an entity with its terminating ';', so that is required here.
 * 'u00xx' is still stripped bare, because it is escape debris in every observed case.
 *
 * POSTGRES SEMANTICS, matched deliberately:
 *   trim(x)                 strips SPACES only, not all whitespace -- JS .trim() would also take tabs
 *                           and newlines and produce a different answer for an address carrying one
 *   split_part(s, d, 1)     the whole string when d is absent
 *   split_part(s, d, 2)     EMPTY STRING when d is absent (not the whole string)
 *   rtrim(s, '.')           removes every trailing dot, not just one
 *
 * The validity guard (empty local, or a domain with no dot -> no hash) comes from the SQL function,
 * which returns NULL for those. They are not addresses, so they get no hash rather than a hash of
 * garbage that would collide across records.
 *
 * VERIFIED against a real SQL engine, not just read off the source. The load_sightings expression was
 * run in DuckDB (Postgres-compatible split_part / rtrim / regexp_replace) over the 24 cases below and
 * compared with this port: all 20 VALID addresses matched character for character, including every
 * documented regression (amprewitt13, gtaylor, ltdsales, quotes, the &amp;/u003e runs, -info, .info,
 * _m2t-dev). The only four differences are the non-addresses, where the SQL expression yields
 * malformed output ('notanemail@', '@example.com', 'jane@localhost') and this returns '' -- the guard
 * described above, deliberate.
 */
const crypto = require('crypto');

// PG trim(): spaces only.
const pgTrim = (s) => String(s == null ? '' : s).replace(/^ +| +$/g, '');
// PG split_part(s, d, n), 1-based; '' when that field does not exist.
const splitPart = (s, d, n) => { const parts = String(s).split(d); return n <= parts.length ? parts[n - 1] : ''; };

// The Public Prospects canonical form. Returns '' where PP would store NULL.
function ppNormalizeEmail(raw) {
  const e = pgTrim(raw).toLowerCase();
  if (!e) return '';
  let lo = splitPart(e, '@', 1)
    .replace(/^(&?(amp|quot|gt|lt);|u00[0-9a-f]{2})+/, '')   // entity debris -- ';' REQUIRED for names
    .replace(/^[-.]+/, '');                                   // parse debris: "-info@x.com" is info@x.com
  const dom = splitPart(e, '@', 2).replace(/\.+$/, '');       // rtrim(dom, '.')
  if (!lo || !dom.includes('.')) return '';
  if (dom === 'gmail.com' || dom === 'googlemail.com') {
    // dot-stripping is GMAIL-ONLY; applied globally it corrupts addresses on providers where dots
    // are significant.
    return splitPart(lo, '+', 1).replace(/\./g, '') + '@gmail.com';
  }
  return splitPart(lo, '+', 1) + '@' + dom;
}

// SHA-256 hex of the canonical form. '' when the input is not an address.
function emailHash(raw) {
  const n = ppNormalizeEmail(raw);
  return n ? crypto.createHash('sha256').update(n, 'utf8').digest('hex') : '';
}

module.exports = { ppNormalizeEmail, emailHash };

// ---------------------------------------------------------------- self-test
if (require.main === module) {
  let pass = 0, fail = 0;
  const T = (raw, want, why) => {
    const got = ppNormalizeEmail(raw);
    if (got === want) pass++;
    else { fail++; console.log('FAIL  ' + JSON.stringify(raw) + '\n      got ' + JSON.stringify(got) + ' want ' + JSON.stringify(want) + (why ? '\n      ' + why : '')); }
  };

  T('Jane.Doe@Example.COM', 'jane.doe@example.com', 'lower-cased, dots kept off gmail');
  T('  jane@example.com  ', 'jane@example.com', 'spaces trimmed');
  T('jane+newsletter@example.com', 'jane@example.com', 'plus-tag dropped');
  T('Jane.Doe+promo@gmail.com', 'janedoe@gmail.com', 'gmail: dots stripped, tag dropped');
  T('jane.doe@googlemail.com', 'janedoe@gmail.com', 'googlemail folds to gmail');
  T('jane.doe@example.com', 'jane.doe@example.com', 'dots significant off gmail');
  T('jane@example.com...', 'jane@example.com', 'rtrim removes EVERY trailing dot');

  // the regression the semicolon exists to prevent
  T('amprewitt13@gmail.com', 'amprewitt13@gmail.com', "'amp' without ';' is just letters");
  T('gtaylor@example.com', 'gtaylor@example.com', "'gt' without ';' is just letters");
  T('ltdsales@example.com', 'ltdsales@example.com', "'lt' without ';' is just letters");
  T('quotes@example.com', 'quotes@example.com', "'quot' without ';' is just letters");
  T('&amp;jane@example.com', 'jane@example.com', 'a real entity IS stripped');
  T('amp;jane@example.com', 'jane@example.com', 'entity without the & is still an entity');
  T('&gt;&lt;jane@example.com', 'jane@example.com', 'a run of entities');
  T('u003ejane@example.com', 'jane@example.com', 'u00xx escape debris stripped bare');
  T('u003eu0026jane@example.com', 'jane@example.com', 'a run of escape debris');

  // leading punctuation
  T('-info@example.com', 'info@example.com', 'leading dash is parse debris');
  T('.info@example.com', 'info@example.com', 'a local part cannot begin with a dot');
  T('_m2t-dev@eclipse.org', '_m2t-dev@eclipse.org', 'leading underscore is VALID and kept');

  // non-addresses get no canonical form
  T('', '', 'empty');
  T('   ', '', 'spaces only');
  T('notanemail', '', 'no @ -> split_part(...,2) is empty -> no dot in domain');
  T('@example.com', '', 'empty local part');
  T('jane@localhost', '', 'domain with no dot');
  T('&amp;@example.com', '', 'local part was entirely entity debris');
  T('jane@example.com@evil.com', 'jane@example.com', 'split_part takes the SECOND field, not the last');

  // the hash itself
  const h1 = emailHash('Jane.Doe+promo@GMAIL.com'), h2 = emailHash('janedoe@gmail.com');
  if (h1 && h1 === h2) pass++; else { fail++; console.log('FAIL  two spellings of one gmail address must hash alike'); }
  if (/^[0-9a-f]{64}$/.test(h1)) pass++; else { fail++; console.log('FAIL  hash is not 64 hex characters: ' + h1); }
  if (emailHash('notanemail') === '') pass++; else { fail++; console.log('FAIL  a non-address must get no hash, not a hash of garbage'); }
  // the known SHA-256 of the canonical form, so a change to the hashing itself is caught
  const known = crypto.createHash('sha256').update('janedoe@gmail.com', 'utf8').digest('hex');
  if (h2 === known) pass++; else { fail++; console.log('FAIL  hash is not plain SHA-256 of the canonical form'); }

  console.log(pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
}
