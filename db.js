/**
 * db.js — central contacts store (SQLite, built on node:sqlite)
 * -------------------------------------------------------------
 * One growing, de-duplicated table of every contact, keyed by lowercased email.
 * On-disk + indexed so it scales to millions of rows: incremental upserts (no
 * whole-file rewrites) and server-side paginated/filtered queries (the UI never
 * loads the whole table).
 *
 * Dependency-free: uses Node's built-in node:sqlite (Node 22.5+/24).
 */
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const { typeForDomain } = require('./tld-lookup');

// Record fields we persist (incl. Image URL for the thumbnail; CSV export drops it).
const FIELDS = ['Time Stamp', 'Source', 'Web Source URL', 'Directory', 'Path ID', 'Last Path',
  'Bio Check', 'First', 'Last', 'Gender', 'Title', 'Position', 'Description', 'Image URL',
  'Email Address', 'Email Type', 'LinkedIn URL', 'Google Maps', 'Phone', 'Phone Type',
  'Phone Location', 'Phone 2', 'Phone 2 Type', 'Phone 2 Location', 'Type'];
const colName = (f) => f.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
const COLS = FIELDS.map(colName);                 // stable snake_case columns
const FIELD_BY_COL = Object.fromEntries(FIELDS.map((f) => [colName(f), f]));

// columns the UI can filter/sort by -> the actual DB column
const SORT_COLS = new Set([...COLS, 'domain']);

function rootDomain(url) {
  const t = String(url || '').trim();
  if (!t) return '';
  try { return new URL(t).hostname.replace(/^www\./i, '').toLowerCase(); }
  catch { return t.replace(/^https?:\/\//i, '').split('/')[0].replace(/^www\./i, '').toLowerCase(); }
}

// Emails must never contain encoded spaces (%20) or whitespace. cleanEmail strips them
// (preserving case); emailKey is the lowercased primary-key form.
function cleanEmail(s) { return String(s == null ? '' : s).replace(/%20/gi, '').replace(/\s+/g, ''); }
function emailKey(s) { return cleanEmail(s).toLowerCase(); }

const SCORE_FIELDS = ['First', 'Last', 'Title', 'Position', 'Phone', 'Phone 2', 'LinkedIn URL',
  'Gender', 'Phone Location', 'Image URL', 'Description', 'Google Maps'];
function score(r) { let s = 0; for (const f of SCORE_FIELDS) if (String(r[f] || '').trim()) s++; return s; }

function makeDb(dir) {
  const file = path.join(dir, 'contacts.db');
  const db = new DatabaseSync(file);
  db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;');

  const colDefs = COLS.map((c) => `"${c}" TEXT`).join(', ');
  db.exec(`CREATE TABLE IF NOT EXISTS contacts (
    email TEXT PRIMARY KEY,
    ${colDefs},
    domain TEXT,
    search TEXT,
    score INTEGER,
    updated_at TEXT
  );`);
  for (const c of ['directory', 'domain', 'gender', 'phone_type', 'email_type', 'last', 'first']) {
    db.exec(`CREATE INDEX IF NOT EXISTS idx_contacts_${c} ON contacts("${c}");`);
  }

  // Additive migration: add any FIELD columns missing from an older DB (e.g. a DB
  // created before 'Phone 2 Location' existed). CREATE TABLE IF NOT EXISTS won't
  // add columns to an existing table, so backfill them here.
  const haveCols = new Set(db.prepare('PRAGMA table_info(contacts)').all().map((r) => r.name));
  for (const c of COLS) if (!haveCols.has(c)) db.exec(`ALTER TABLE contacts ADD COLUMN "${c}" TEXT`);

  const insertCols = ['email', ...COLS, 'domain', 'search', 'score', 'updated_at'];
  const placeholders = insertCols.map(() => '?').join(', ');
  const updates = [...COLS, 'domain', 'search', 'score', 'updated_at']
    .map((c) => `"${c}" = excluded."${c}"`).join(', ');
  // keep the richer record on conflict (only overwrite when the new score is >= existing)
  const upsertSql = `INSERT INTO contacts (${insertCols.map((c) => `"${c}"`).join(', ')})
    VALUES (${placeholders})
    ON CONFLICT(email) DO UPDATE SET ${updates} WHERE excluded.score >= contacts.score;`;
  const upsertStmt = db.prepare(upsertSql);

  function rowValues(r) {
    const emailField = cleanEmail(r['Email Address']);   // strip %20 / whitespace, keep case
    const email = emailField.toLowerCase();              // primary key
    if (!email) return null;
    const domain = rootDomain(r['Web Source URL']);
    const type = typeForDomain(domain);                  // domain-TLD category (computed, not from r)
    const search = [r['First'], r['Last'], emailField, r['Title'], r['Position'], domain,
      r['Phone'], r['Phone 2'], r['Description']].map((v) => String(v || '')).join(' ').toLowerCase();
    const vals = [email];
    for (const f of FIELDS) vals.push(
      f === 'Email Address' ? emailField :
      f === 'Type' ? type :
      String(r[f] == null ? '' : r[f])
    );
    vals.push(domain, search, score(r), new Date().toISOString());
    return vals;
  }

  function upsertMany(records) {
    let n = 0;
    const before = count();
    db.exec('BEGIN');
    try {
      for (const r of (records || [])) {
        const v = rowValues(r);
        if (!v) continue;
        upsertStmt.run(...v);
        n++;
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    const after = count();
    return { processed: n, added: after - before, total: after };
  }

  function rowToRecord(row) {
    const rec = {};
    for (const c of COLS) rec[FIELD_BY_COL[c]] = row[c] || '';
    rec.Domain = row.domain || '';
    return rec;
  }

  // Build the WHERE clause + params from filter options.
  function whereFor(opts = {}) {
    const where = []; const params = [];
    const eqCI = (col, val) => { where.push(`lower("${col}") = ?`); params.push(String(val).toLowerCase()); };
    if (opts.directory) eqCI('directory', opts.directory);
    if (opts.emailType) eqCI('email_type', opts.emailType);
    if (opts.phoneType) eqCI('phone_type', opts.phoneType);
    if (opts.type) eqCI('type', opts.type);
    if (opts.domain) { where.push('domain = ?'); params.push(String(opts.domain).toLowerCase()); }
    // Position keyword: substring match on the Position field.
    if (opts.position) { where.push(`lower("position") LIKE ?`); params.push('%' + String(opts.position).toLowerCase() + '%'); }
    // Pasted domain list: match any (root domain or a subdomain of it).
    if (Array.isArray(opts.domains) && opts.domains.length) {
      const parts = [];
      for (const d of opts.domains) {
        const dl = String(d || '').trim().toLowerCase().replace(/^www\./, '');
        if (!dl) continue;
        parts.push('(domain = ? OR domain LIKE ?)');
        params.push(dl, '%.' + dl);
      }
      if (parts.length) where.push('(' + parts.join(' OR ') + ')');
    }
    if (opts.linkedin) where.push(`linkedin_url <> ''`);
    switch (opts.gender) {
      case 'male': where.push(`upper(gender) = 'M'`); break;
      case 'female': where.push(`upper(gender) = 'F'`); break;
      case 'all': where.push(`upper(gender) IN ('M','F')`); break;
      case 'none': where.push(`upper(gender) NOT IN ('M','F')`); break;
      default: break; // 'na' -> no filter
    }
    if (opts.search) { where.push('search LIKE ?'); params.push('%' + String(opts.search).toLowerCase() + '%'); }
    return { sql: where.length ? 'WHERE ' + where.join(' AND ') : '', params };
  }

  function query(opts = {}) {
    const pageSize = Math.min(500, Math.max(1, Number(opts.pageSize) || 50));
    const page = Math.max(1, Number(opts.page) || 1);
    const { sql: whereSql, params } = whereFor(opts);

    const total = db.prepare(`SELECT COUNT(*) c FROM contacts ${whereSql}`).get(...params).c;

    let sortCol = colName(opts.sort || '');
    if (opts.sort === 'Domain') sortCol = 'domain';
    if (!SORT_COLS.has(sortCol)) sortCol = 'last';
    const dir = Number(opts.dir) === -1 ? 'DESC' : 'ASC';
    const offset = (page - 1) * pageSize;

    const rows = db.prepare(
      `SELECT * FROM contacts ${whereSql} ORDER BY "${sortCol}"='' , "${sortCol}" COLLATE NOCASE ${dir} LIMIT ? OFFSET ?`
    ).all(...params, pageSize, offset);

    return { rows: rows.map(rowToRecord), total, page, pageSize };
  }

  function count() { return db.prepare('SELECT COUNT(*) c FROM contacts').get().c; }
  function stats() { return { total: count() }; }

  // distinct values for the filter dropdowns (indexed columns -> fast)
  function facets() {
    const distinct = (c) => db.prepare(`SELECT DISTINCT "${c}" v FROM contacts WHERE "${c}" <> '' ORDER BY "${c}"`).all().map((r) => r.v);
    return { directory: distinct('directory'), emailType: distinct('email_type'), phoneType: distinct('phone_type'), type: distinct('type') };
  }

  // Stream every (optionally filtered) record to a callback, in batches (for CSV export).
  function each(opts, cb) {
    const { sql: whereSql, params } = whereFor(opts || {});
    const stmt = db.prepare(`SELECT * FROM contacts ${whereSql} ORDER BY "last" COLLATE NOCASE`);
    for (const row of stmt.iterate(...params)) cb(rowToRecord(row));
  }

  // --- single-record edit / delete (used by the Search UI manual edit + AI enrich) ---
  const replaceCols = ['email', ...COLS, 'domain', 'search', 'score', 'updated_at'];
  const replaceStmt = db.prepare(
    `INSERT OR REPLACE INTO contacts (${replaceCols.map((c) => `"${c}"`).join(', ')})
     VALUES (${replaceCols.map(() => '?').join(', ')})`
  );

  function getByEmail(email) {
    const e = emailKey(email);
    if (!e) return null;
    const row = db.prepare('SELECT * FROM contacts WHERE email = ?').get(e);
    return row ? rowToRecord(row) : null;
  }

  function deleteByEmail(email) {
    const e = emailKey(email);
    if (e) db.prepare('DELETE FROM contacts WHERE email = ?').run(e);
  }

  // Force-write a record (no score gate) — overwrites by email primary key.
  function putForce(record) {
    const v = rowValues(record);   // [email, ...fields, domain, search, score, updated_at] or null
    if (!v) return false;
    replaceStmt.run(...v);
    return true;
  }

  // Apply a partial {field: value} patch to the record keyed by origEmail. Handles a
  // changed Email Address (the primary key) by moving the row to the new key.
  function updateRecord(origEmail, updates) {
    const oe = emailKey(origEmail);
    const orig = getByEmail(oe);
    if (!orig) return { ok: false, error: 'not_found' };
    const merged = { ...orig, ...(updates || {}) };
    const newEmail = emailKey(merged['Email Address']);
    if (!newEmail) return { ok: false, error: 'email_required' };
    db.exec('BEGIN');
    try {
      if (newEmail !== oe) deleteByEmail(oe);
      putForce(merged);
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    return { ok: true, record: getByEmail(newEmail) };
  }

  // Fill ONLY empty phone-location columns for a batch, in one transaction (startup backfill).
  // items: [{ email, loc1, loc2 }]. Never overwrites a location that's already set.
  const fillLoc1 = db.prepare(`UPDATE contacts SET phone_location = ? WHERE email = ? AND (phone_location IS NULL OR phone_location = '')`);
  const fillLoc2 = db.prepare(`UPDATE contacts SET phone_2_location = ? WHERE email = ? AND (phone_2_location IS NULL OR phone_2_location = '')`);
  function backfillLocations(items) {
    let n = 0;
    db.exec('BEGIN');
    try {
      for (const it of (items || [])) {
        const e = emailKey(it.email);
        if (!e) continue;
        if (it.loc1) n += fillLoc1.run(it.loc1, e).changes;
        if (it.loc2) n += fillLoc2.run(it.loc2, e).changes;
      }
      db.exec('COMMIT');
    } catch (err) { db.exec('ROLLBACK'); throw err; }
    return n;
  }

  // One-time (idempotent) cleanup: re-clean any stored emails that still contain "%20"
  // (encoded space) or whitespace, in either the primary key or the Email Address field.
  function cleanStoredEmails() {
    let rows = [];
    try {
      rows = db.prepare(
        `SELECT * FROM contacts
         WHERE email LIKE '%!%20%' ESCAPE '!' OR email LIKE '% %'
            OR email_address LIKE '%!%20%' ESCAPE '!' OR email_address LIKE '% %'`
      ).all();
    } catch (e) { return 0; }
    if (!rows.length) return 0;
    const del = db.prepare('DELETE FROM contacts WHERE email = ?');
    let fixed = 0;
    db.exec('BEGIN');
    try {
      for (const row of rows) {
        const v = rowValues(rowToRecord(row));   // rowValues strips %20/space -> clean key + field
        del.run(row.email);                       // remove the old dirty-keyed row
        if (v) { upsertStmt.run(...v); fixed++; } // re-insert/merge cleaned (score-gated keeps richer)
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    return fixed;
  }

  // One-time (idempotent) backfill: set Type from the domain TLD for rows that don't have one.
  function backfillTypes() {
    let rows = [];
    try { rows = db.prepare("SELECT email, domain FROM contacts WHERE (type IS NULL OR type = '') AND domain <> ''").all(); }
    catch (e) { return 0; }
    if (!rows.length) return 0;
    const upd = db.prepare('UPDATE contacts SET type = ? WHERE email = ?');
    let n = 0;
    db.exec('BEGIN');
    try { for (const r of rows) { const t = typeForDomain(r.domain); if (t) n += upd.run(t, r.email).changes; } db.exec('COMMIT'); }
    catch (e) { db.exec('ROLLBACK'); throw e; }
    return n;
  }

  // one-time import of the legacy JSON store, if present and the table is empty
  function importLegacyJson() {
    if (count() > 0) return;
    const jsonFile = path.join(dir, 'contacts.json');
    try {
      const arr = JSON.parse(fs.readFileSync(jsonFile, 'utf8'));
      if (Array.isArray(arr) && arr.length) {
        const r = upsertMany(arr);
        console.log(`Central DB: imported ${r.added} contact(s) from legacy contacts.json`);
      }
    } catch (e) { /* none */ }
  }

  importLegacyJson();
  const cleanedEmails = cleanStoredEmails();
  if (cleanedEmails) console.log(`Central DB: removed encoded spaces from ${cleanedEmails} email(s).`);
  const typedRows = backfillTypes();
  if (typedRows) console.log(`Central DB: set Type (from domain TLD) on ${typedRows} record(s).`);
  console.log(`Central DB (SQLite): ${count().toLocaleString()} contact(s) at ${file}`);
  return { upsertMany, query, each, stats, count, facets, getByEmail, updateRecord, deleteByEmail, backfillLocations };
}

module.exports = { makeDb };
