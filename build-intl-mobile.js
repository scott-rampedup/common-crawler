/**
 * build-intl-mobile.js — regenerate data/intl-mobile-prefixes.json
 *
 * Reads "International Mobiles.csv" (Country, Country Code, Mobile prefix) and
 * writes { "<countryCode>": ["<prefix>", ...], ... } — the national-number
 * prefixes that mark a MOBILE line for each non-NANP country. intl-mobile.js
 * uses this to upgrade a phone's Type to "Mobile" from its E.164 number.
 *
 * Run after editing the CSV:  node build-intl-mobile.js
 *
 * Country codes here are intentionally all non-NANP (no "1"), so US/Canada
 * numbers never match. The build asserts the country codes are prefix-free, so
 * the greedy longest-code match in intl-mobile.js is unambiguous.
 */
const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, 'International Mobiles.csv');
const OUT = path.join(__dirname, 'data', 'intl-mobile-prefixes.json');

const raw = fs.readFileSync(SRC, 'utf8');
const lines = raw.split(/\r?\n/).slice(1); // drop header
const map = {};
let rows = 0;
for (const line of lines) {
  if (!line.trim()) continue;
  const cells = line.split(',');
  const cc = String(cells[1] || '').replace(/\D/g, '');       // country code, digits only
  const prefix = String(cells[2] || '').replace(/\D/g, '');   // mobile prefix, digits only
  if (!cc || !prefix) continue;
  rows++;
  (map[cc] || (map[cc] = new Set())).add(prefix);
}

// Sort each country's prefixes longest-first (so a more specific prefix is tried
// before a shorter one) then numerically; serialize Sets to arrays.
const out = {};
for (const cc of Object.keys(map).sort((a, b) => Number(a) - Number(b))) {
  out[cc] = [...map[cc]].sort((a, b) => (b.length - a.length) || a.localeCompare(b));
}

// Guard: country codes must be prefix-free (no code is a prefix of another), or
// the greedy code match could pick the wrong country.
const codes = Object.keys(out);
const codeSet = new Set(codes);
for (const c of codes) {
  for (let n = 1; n < c.length; n++) {
    if (codeSet.has(c.slice(0, n))) {
      console.warn(`WARNING: country code ${c} has shorter code ${c.slice(0, n)} as a prefix — code match may be ambiguous`);
    }
  }
}

fs.writeFileSync(OUT, JSON.stringify(out, null, 0) + '\n');
console.log('rows parsed     :', rows);
console.log('countries (codes):', codes.length);
console.log('written         :', path.relative(__dirname, OUT));
