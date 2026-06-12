/**
 * intl-mobile.js — mark an international phone as Mobile from its E.164 number.
 *
 * The US/Canada (NANP) classifier (wireless-block-classifier.js) returns
 * "Unknown" for non-NANP numbers. This fills that gap using a country-code +
 * mobile-prefix table (data/intl-mobile-prefixes.json, built from
 * "International Mobiles.csv"): given an E.164 number, find its country code,
 * then check whether the national number starts with a known mobile prefix.
 *
 * The table contains only non-NANP country codes, so +1 numbers never match.
 */
const fs = require('fs');
const path = require('path');

let MAP = null;     // { "<countryCode>": ["<prefix>", ...] }
let CODES = null;   // Set of country codes (all 2-3 digits, prefix-free)
function load() {
  if (MAP) return;
  try { MAP = JSON.parse(fs.readFileSync(path.join(__dirname, 'data', 'intl-mobile-prefixes.json'), 'utf8')); }
  catch { MAP = {}; }
  CODES = new Set(Object.keys(MAP));
}

/**
 * @param {string} e164  an E.164 number, e.g. "+447700900123" (a leading "+" is
 *   required; loose/national-format numbers should be passed through toE164 first)
 * @returns {string} "Mobile" if the number matches a known mobile prefix for its
 *   country, else "" (unknown country, NANP, or a non-mobile prefix).
 */
function intlMobileType(e164) {
  load();
  const s = String(e164 || '').trim();
  if (!s.startsWith('+')) return '';           // require explicit international form
  const digits = s.replace(/\D/g, '');
  if (!digits) return '';

  // longest country code that prefixes the number (table codes are 2-3 digits)
  let cc = '';
  for (const len of [3, 2]) { const p = digits.slice(0, len); if (CODES.has(p)) { cc = p; break; } }
  if (!cc) return '';

  const national = digits.slice(cc.length);
  for (const prefix of MAP[cc]) if (national.startsWith(prefix)) return 'Mobile';
  return '';
}

module.exports = { intlMobileType };
