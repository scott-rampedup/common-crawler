/**
 * db.js — central contacts store
 * -------------------------------------------------------------
 * One growing, de-duplicated table of every contact we've found, keyed by email
 * (case-insensitive). Persisted as a single JSON file on the data volume, written
 * atomically (temp file + rename) so it can't be left half-written.
 *
 * This is intentionally dependency-free. If we ever need SQL/scale, swap the guts
 * for SQLite behind the same upsertMany/all/stats interface.
 */
const fs = require('fs');
const path = require('path');

function emailKey(r) {
  return String(r['Email Address'] || '').trim().toLowerCase();
}

// completeness: how many useful fields are filled — used to keep the richer duplicate
const SCORE_FIELDS = ['First', 'Last', 'Title', 'Position', 'Phone', 'Phone 2',
  'LinkedIn URL', 'Gender', 'Phone Location', 'Image URL', 'Description', 'Google Maps'];
function score(r) {
  let s = 0;
  for (const f of SCORE_FIELDS) if (String(r[f] || '').trim()) s++;
  return s;
}

function makeDb(dir) {
  const file = path.join(dir, 'contacts.json');
  const byEmail = new Map();

  function load() {
    try {
      const arr = JSON.parse(fs.readFileSync(file, 'utf8'));
      for (const r of arr) { const k = emailKey(r); if (k) byEmail.set(k, r); }
      console.log(`Central DB: loaded ${byEmail.size.toLocaleString()} contact(s) from ${file}`);
    } catch (e) { /* no DB yet */ }
  }

  function persist() {
    try {
      const tmp = `${file}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify([...byEmail.values()]));
      fs.renameSync(tmp, file);   // atomic
    } catch (e) { console.error('Central DB persist failed:', e.message); }
  }

  // Merge records in; keep the richer record per email. Returns {added, updated, total}.
  function upsertMany(records) {
    let added = 0, updated = 0;
    for (const r of (records || [])) {
      const k = emailKey(r);
      if (!k) continue;
      const cur = byEmail.get(k);
      if (!cur) { byEmail.set(k, { ...r }); added++; }
      else if (score(r) >= score(cur)) { byEmail.set(k, { ...r }); updated++; }
    }
    if (added || updated) persist();
    return { added, updated, total: byEmail.size };
  }

  function all() { return [...byEmail.values()]; }
  function stats() { return { total: byEmail.size }; }
  function clear() { byEmail.clear(); persist(); return { total: 0 }; }

  load();
  return { upsertMany, all, stats, clear, emailKey };
}

module.exports = { makeDb };
