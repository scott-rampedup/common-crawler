/**
 * backfill-phone-types.js — mark existing international numbers as Mobile.
 *
 * For every contact, runs intlMobileType() (intl-mobile.js, country-code +
 * mobile-prefix table) over the stored Phone / Phone 2 E.164 values and, when it
 * matches, sets that line's Type to "Mobile".
 *
 * NON-DESTRUCTIVE: only UPGRADES a line to "Mobile". It never downgrades, never
 * touches NANP/+1 numbers (not in the table), and never changes any other field.
 * Phone Type isn't part of the search/score columns, so nothing else needs a refresh.
 *
 * Usage (run where the DB lives, e.g. on the Fly machine so DATA_DIR=/data):
 *   node backfill-phone-types.js          # DRY RUN — reports what would change
 *   node backfill-phone-types.js --apply  # back up contacts.db*, then apply
 *
 * Idempotent: a second --apply changes nothing.
 */
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const { intlMobileType } = require('./intl-mobile');

const APPLY = process.argv.includes('--apply');
const DATA_DIR = process.env.DATA_DIR || __dirname;
const FILE = path.join(DATA_DIR, 'contacts.db');

const basePhone = (p) => String(p || '').split(/\s*ext\.?\s*/i)[0].trim(); // drop " ext. NNN"

if (!fs.existsSync(FILE)) { console.error('No contacts.db at ' + FILE); process.exit(1); }
console.log((APPLY ? 'APPLY' : 'DRY RUN') + '  db=' + FILE);

const db = new DatabaseSync(FILE);

if (APPLY) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  for (const ext of ['', '-wal', '-shm']) {
    const src = FILE + ext;
    if (fs.existsSync(src)) fs.copyFileSync(src, src + '.bak-' + ts);
  }
  console.log('Backed up contacts.db* -> *.bak-' + ts);
}

const rows = db.prepare('SELECT email, phone, phone_type, phone_2, phone_2_type FROM contacts').all();
const updP1 = db.prepare("UPDATE contacts SET phone_type = 'Mobile' WHERE email = ?");
const updP2 = db.prepare("UPDATE contacts SET phone_2_type = 'Mobile' WHERE email = ?");

let p1 = 0, p2 = 0, rowsChanged = 0;
const samples = [];

function run() {
  for (const r of rows) {
    let changed = false;
    if (intlMobileType(basePhone(r.phone)) === 'Mobile' && r.phone_type !== 'Mobile') {
      if (APPLY) updP1.run(r.email);
      p1++; changed = true;
      if (samples.length < 25) samples.push({ ph: r.phone, was: r.phone_type || '' });
    }
    if (intlMobileType(basePhone(r.phone_2)) === 'Mobile' && r.phone_2_type !== 'Mobile') {
      if (APPLY) updP2.run(r.email);
      p2++; changed = true;
    }
    if (changed) rowsChanged++;
  }
}

if (APPLY) { db.exec('BEGIN'); try { run(); db.exec('COMMIT'); } catch (e) { db.exec('ROLLBACK'); throw e; } }
else { run(); }

console.log('\nSample Phone upgrades (phone : old Type => Mobile):');
for (const s of samples) console.log('  ' + s.ph + '   ' + JSON.stringify(s.was) + ' => "Mobile"');

console.log('\n--- ' + (APPLY ? 'APPLIED' : 'DRY RUN (no writes)') + ' ---');
console.log('total rows         : ' + rows.length);
console.log('Phone   -> Mobile  : ' + p1);
console.log('Phone 2 -> Mobile  : ' + p2);
console.log('rows changed       : ' + rowsChanged);
if (!APPLY) console.log('\nRe-run with --apply to write these changes (a backup is made first).');
