/**
 * tld-lookup.js — map a domain to a { type, country } using "TLD Look UP.csv".
 *
 * The CSV is "TLD,Type,Country" (e.g. "attorney,Legal,United States" / "ac.uk,Education,
 * United Kingdom" / "com,Business,United States"). We match the LONGEST TLD suffix of the
 * domain (so "ox.ac.uk" -> "ac.uk", "acme.com" -> "com", "smith.attorney" -> "attorney").
 */
const fs = require('fs');
const path = require('path');

const CSV_FILE = path.join(__dirname, 'TLD Look UP.csv');

let MAP = null;   // lower(tld) -> { type, country }

function load() {
  if (MAP) return;
  MAP = new Map();
  let text = '';
  try { text = fs.readFileSync(CSV_FILE, 'utf8'); } catch (e) { return; }
  const lines = text.split(/\r?\n/);
  for (let i = 1; i < lines.length; i++) {          // skip header row
    const line = lines[i];
    if (!line || !line.trim()) continue;
    const cols = line.split(',');
    const tld = (cols[0] || '').trim().toLowerCase().replace(/^\./, '');
    const type = (cols[1] || '').trim();
    const country = (cols[2] || '').trim();
    if (tld && !MAP.has(tld)) MAP.set(tld, { type, country });
  }
}

// Find the { type, country } for a domain, matching the longest TLD suffix. null if none.
function lookupDomain(domain) {
  load();
  if (!MAP.size) return null;
  const d = String(domain || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0];
  if (!d) return null;
  const labels = d.split('.').filter(Boolean);
  for (let take = Math.min(3, labels.length); take >= 1; take--) {
    const key = labels.slice(labels.length - take).join('.');
    if (MAP.has(key)) return MAP.get(key);
  }
  return null;
}

function typeForDomain(domain) { const r = lookupDomain(domain); return r ? r.type : ''; }
function countryForDomain(domain) { const r = lookupDomain(domain); return r ? r.country : ''; }

module.exports = { lookupDomain, typeForDomain, countryForDomain };
