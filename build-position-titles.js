/**
 * build-position-titles.js — regenerate data/position-titles.json
 *
 * Reads "Titles in Order .csv" (the observed job-title list), dedupes, drops
 * generic single-word terms (see DROP_GENERIC below), and writes the titles
 * sorted longest-first (most characters) — the order findPosition() relies on so
 * the most specific title wins.
 *
 * Run after editing the CSV or the drop-list:  node build-position-titles.js
 *
 * DROP_GENERIC removes ONLY single-word entries that are generic English words,
 * fields, departments, or adjectives rather than an individual's job title
 * (e.g. "Marketing", "Community", "Operations", "Nursing", "Surgery"). Real
 * one-word occupations ("Accountant", "Engineer", "Nurse", "Welder", "Chef") and
 * every multi-word title are kept. Acronyms (CEO/CTO/VP/IT…) are kept — they are
 * matched case-sensitively (uppercase only) at runtime, so they don't fire on
 * lowercase prose. Edit this set to tune precision.
 */
const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, 'Titles in Order .csv');
const OUT = path.join(__dirname, 'data', 'position-titles.json');

const DROP_GENERIC = new Set([
  'accounting', 'administration', 'advertising', 'anesthesia', 'anesthesiology',
  'banking', 'benefits', 'brand', 'broadcasting', 'budgeting', 'cardiology',
  'carpet', 'chief', 'choreography', 'clergy', 'communication', 'community',
  'compensation', 'construction', 'content', 'council', 'counter', 'crane',
  'creative', 'cutters', 'dentistry', 'dermatology', 'design', 'devops',
  'digital', 'editorial', 'electrical', 'elementary', 'embalmers', 'employee',
  'endocrinology', 'enterprise', 'entertainment', 'equipment', 'events',
  'excavating', 'expert', 'extraction', 'extruding', 'fabric', 'faculty',
  'family', 'farming', 'finance', 'forestry', 'freight', 'fundraising',
  'gastroenterology', 'genetics', 'gynecology', 'health', 'heavy', 'hematology',
  'hospice', 'hospital', 'host', 'housekeeping', 'hvac', 'installation',
  'inventory', 'journalism', 'judicial', 'labor', 'lathe', 'leasing', 'life',
  'logistics', 'maintenance', 'management', 'manufacturing', 'marketing',
  'marriage', 'maxillofacial', 'media', 'medical', 'medicine', 'member', 'model',
  'network', 'nephrology', 'neurology', 'neuromusculoskeletal', 'nursing',
  'obstetrics', 'oncology', 'operations', 'ophthalmology', 'osteopathy',
  'otolaryngology', 'pathology', 'payables', 'payroll', 'pediatrics',
  'pharmaceuticals', 'philosophy', 'police', 'preschool', 'proctology',
  'procurement', 'psychiatry', 'publishing', 'pulmonary', 'radio', 'radiology',
  'receivables', 'recruiting', 'register', 'rehabilitation', 'repair', 'research',
  'reservation', 'retail', 'revenue', 'safety', 'sales', 'sanitation', 'security',
  'server', 'sewer', 'shipping', 'signal', 'software', 'staffing', 'strategy',
  'support', 'surgery', 'surveying', 'talent', 'technology', 'telephony',
  'teletransportation', 'textile', 'theology', 'therapy', 'title',
  'transportation', 'treasury', 'trucking', 'urology', 'warehousing', 'welding',
]);

const normKey = (s) =>
  s.toLowerCase().replace(/&/g, ' and ').replace(/[^a-z0-9]+/g, ' ').trim().replace(/\s+/g, ' ');

const raw = fs.readFileSync(SRC, 'utf8');
const lines = raw.split(/\r?\n/).slice(1); // drop header
const seen = new Set();
const out = [];
let dropped = 0;
for (const line of lines) {
  if (!line.trim()) continue;
  const m = line.match(/^(.*),(\d+)\s*$/); // split on the LAST comma (Name,Count)
  const name = (m ? m[1] : line).trim();
  if (!name) continue;
  // drop generic single-word entries (multi-word titles are always kept)
  if (/^[A-Za-z]+$/.test(name) && DROP_GENERIC.has(name.toLowerCase())) { dropped++; continue; }
  const k = normKey(name);
  if (!k || seen.has(k)) continue;
  seen.add(k);
  out.push(name);
}
out.sort((a, b) => (b.length - a.length) || a.localeCompare(b)); // longest (most chars) first

fs.writeFileSync(OUT, '[\n' + out.map((s) => '  ' + JSON.stringify(s)).join(',\n') + '\n]\n');
console.log('rows in:', lines.filter((l) => l.trim()).length);
console.log('generic single-worders dropped:', dropped);
console.log('titles written:', out.length, '->', path.relative(__dirname, OUT));
