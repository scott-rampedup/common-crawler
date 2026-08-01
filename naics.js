/**
 * naics.js — shared NAICS association used at INGESTION time (and by the UI typeahead endpoint).
 *
 * Single source of truth = the curated "Category to NAICS" lookup (bundled as data-category-naics.csv so it
 * ships in the Fly image; falls back to the parent-dir CSV for local dev). Columns:
 *   Source, Category or Key Word, NAICS 6 Code, NAICS 6 Title
 *
 *   assignNaics(doc)  -> mutates+returns doc, setting doc.naics_code / doc.naics_title from doc.category
 *                        (or doc.industry when category is blank). No-ops (leaves doc untouched) on no match.
 *   codeList()        -> distinct [{ code, title, label:"code — title" }] sorted by code, for the typeahead.
 *
 * Precedence mirrors naics-enrich.js exactly: category first, industry as the fallback signal.
 */
const fs = require('fs');
const path = require('path');

const CSV_CANDIDATES = [
  path.join(__dirname, 'data-category-naics.csv'),   // bundled (deploys to Fly)
  path.join(__dirname, '..', 'Category to NAICS.csv'), // local dev fallback
];

// minimal quoted-CSV parser (fields may contain commas inside double quotes)
function parseCsv(text) {
  const rows = []; let row = [], field = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
    else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows;
}

let _map = null;        // normalizedKey -> [code, title]
let _codeList = null;   // sorted distinct [{ code, title, label }]

function load() {
  if (_map) return;
  _map = {};
  const file = CSV_CANDIDATES.find((f) => { try { return fs.existsSync(f); } catch { return false; } });
  if (!file) { _codeList = []; return; }   // no CSV present -> assignNaics becomes a no-op, codeList empty
  const rows = parseCsv(fs.readFileSync(file, 'utf8'));
  rows.shift(); // header
  const byCode = new Map();
  for (const r of rows) {
    const key = String(r[1] || '').trim().toLowerCase();
    const code = String(r[2] || '').trim();
    const title = String(r[3] || '').trim();
    if (!key || !code) continue;
    if (!_map[key]) _map[key] = [code, title];   // first mapping wins (file lists higher-signal rows first)
    if (!byCode.has(code)) byCode.set(code, title);
  }
  _codeList = [...byCode.entries()]
    .map(([code, title]) => ({ code, title, label: title ? `${code} — ${title}` : code }))
    .sort((a, b) => a.code.localeCompare(b.code));
}

// category first, industry as fallback — set naics_code/naics_title on a match, else leave the doc as-is.
function assignNaics(doc) {
  if (!doc || doc.naics_code) return doc;   // don't clobber an already-assigned code
  load();
  let cat = doc.category;
  if (cat == null || String(cat).trim() === '') cat = doc.industry;
  if (cat == null || String(cat).trim() === '') return doc;
  const v = _map[String(cat).trim().toLowerCase()];
  if (v) { doc.naics_code = v[0]; doc.naics_title = v[1]; }
  return doc;
}

function codeList() { load(); return _codeList; }

module.exports = { assignNaics, codeList };
