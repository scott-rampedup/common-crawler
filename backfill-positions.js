/**
 * backfill-positions.js — recompute Position for every contact using the current
 * findPosition() logic (standalone, longest-first; data/position-titles.json) and
 * OVERWRITE the stored Position, blanking it when no dictionary title is found.
 *
 * Also refreshes the denormalized `search` and `score` columns, which include
 * Position, so filters/sorting stay consistent with the new value.
 *
 * Usage (run where the DB lives, e.g. on the Fly machine so DATA_DIR=/data):
 *   node backfill-positions.js            # DRY RUN — reports what would change, writes nothing
 *   node backfill-positions.js --apply    # back up contacts.db*, then apply in one transaction
 *
 * Idempotent: running --apply twice produces the same result.
 */
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const { findPosition } = require('./extractor');

const APPLY = process.argv.includes('--apply');
const DATA_DIR = process.env.DATA_DIR || __dirname;
const FILE = path.join(DATA_DIR, 'contacts.db');

// SCORE columns (mirror db.js SCORE_FIELDS) used to recompute `score`.
const SCORE_COLS = ['first', 'last', 'title', 'position', 'phone', 'phone_2',
  'linkedin_url', 'gender', 'phone_location', 'image_url', 'description', 'google_maps'];

if (!fs.existsSync(FILE)) { console.error('No contacts.db at ' + FILE); process.exit(1); }
console.log((APPLY ? 'APPLY' : 'DRY RUN') + '  db=' + FILE);

const db = new DatabaseSync(FILE);

if (APPLY) {
  // Back up all three WAL-mode files so a full restore (copy them back) is consistent.
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  for (const ext of ['', '-wal', '-shm']) {
    const src = FILE + ext;
    if (fs.existsSync(src)) { fs.copyFileSync(src, src + '.bak-' + ts); }
  }
  console.log('Backed up contacts.db* -> *.bak-' + ts);
}

const rows = db.prepare('SELECT * FROM contacts').all();
const upd = db.prepare('UPDATE contacts SET position = ?, search = ?, score = ? WHERE email = ?');

let blanked = 0, set = 0, modified = 0, unchanged = 0;
const samples = [];

function run() {
  for (const r of rows) {
    const oldPos = String(r.position || '');
    const newPos = findPosition(r.title, r.description) || '';
    if (newPos === oldPos) { unchanged++; continue; }

    if (!newPos) blanked++; else if (!oldPos) set++; else modified++;
    if (samples.length < 25) samples.push({ title: r.title || '', old: oldPos, neu: newPos });

    if (APPLY) {
      const search = SCORE_COLS.length && [r.first, r.last, r.email_address, r.title, newPos,
        r.domain, r.phone, r.phone_2, r.description].map((v) => String(v || '')).join(' ').toLowerCase();
      let score = 0;
      for (const c of SCORE_COLS) {
        const v = c === 'position' ? newPos : r[c];
        if (String(v || '').trim()) score++;
      }
      upd.run(newPos, search, score, r.email);
    }
  }
}

if (APPLY) { db.exec('BEGIN'); try { run(); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; } }
else { run(); }

console.log('\nSample changes (title -> old Position => new Position):');
for (const s of samples) console.log('  ' + JSON.stringify(s.title).slice(0, 60) + '  ' + JSON.stringify(s.old) + ' => ' + JSON.stringify(s.neu));

const changed = blanked + set + modified;
console.log('\n--- ' + (APPLY ? 'APPLIED' : 'DRY RUN (no writes)') + ' ---');
console.log('total rows      : ' + rows.length);
console.log('unchanged       : ' + unchanged);
console.log('changed         : ' + changed);
console.log('  blanked (->"") : ' + blanked + '   (had a Position, new logic finds none)');
console.log('  newly set      : ' + set);
console.log('  modified       : ' + modified);
if (!APPLY) console.log('\nRe-run with --apply to write these changes (a backup is made first).');
