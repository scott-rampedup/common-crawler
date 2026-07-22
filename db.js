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
const { normalizeContact } = require('./normalize');

// Record fields we persist (incl. Image URL for the thumbnail; CSV export drops it).
const FIELDS = ['Time Stamp', 'Source', 'Web Source URL', 'Directory', 'Path ID', 'Last Path',
  'Bio Check', 'First', 'Last', 'Gender', 'Title', 'Position', 'Description', 'Image URL',
  'Email Address', 'Email Type', 'LinkedIn URL', 'Facebook', 'Twitter', 'WhatsApp', 'Google Maps', 'vCard', 'Phone', 'Phone Type',
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
  // WAL: readers never block the writer. busy_timeout: a 2nd process/connection (e.g. seed-monitor.js
  // running on the same box as the web app, or two seeders) WAITS up to 5s for a write lock instead of
  // failing with SQLITE_BUSY — our write transactions are short, so this makes concurrent writers safe.
  db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;');

  const colDefs = COLS.map((c) => `"${c}" TEXT`).join(', ');
  db.exec(`CREATE TABLE IF NOT EXISTS contacts (
    email TEXT PRIMARY KEY,
    ${colDefs},
    domain TEXT,
    search TEXT,
    score INTEGER,
    updated_at TEXT
  );`);
  for (const c of ['directory', 'domain', 'gender', 'phone_type', 'email_type', 'last', 'first', 'updated_at']) {
    db.exec(`CREATE INDEX IF NOT EXISTS idx_contacts_${c} ON contacts("${c}");`);
  }

  // Additive migration: add any FIELD columns missing from an older DB (e.g. a DB
  // created before 'Phone 2 Location' existed). CREATE TABLE IF NOT EXISTS won't
  // add columns to an existing table, so backfill them here.
  const haveCols = new Set(db.prepare('PRAGMA table_info(contacts)').all().map((r) => r.name));
  for (const c of COLS) if (!haveCols.has(c)) db.exec(`ALTER TABLE contacts ADD COLUMN "${c}" TEXT`);

  // --- Sitemap monitor (new-employee detection) ----------------------------------------------
  // watched_sitemaps: the bio-DEDICATED child sitemaps we re-check on a schedule (one row each).
  // bio_urls: the per-URL baseline we diff each pass against (present | departed).
  // observations: the append-only change feed (new_bio | reappeared | departed).
  db.exec(`CREATE TABLE IF NOT EXISTS watched_sitemaps (
    sitemap_url  TEXT PRIMARY KEY,
    parent_url   TEXT,
    domain       TEXT,
    bio_ratio    REAL,
    url_count    INTEGER,
    bio_count    INTEGER,
    last_lastmod TEXT,
    last_hash    TEXT,
    last_fetched TEXT,
    added_at     TEXT,
    status       TEXT DEFAULT 'active'
  );`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_watched_domain ON watched_sitemaps(domain);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_watched_parent ON watched_sitemaps(parent_url);`);
  db.exec(`CREATE TABLE IF NOT EXISTS bio_urls (
    url         TEXT PRIMARY KEY,
    domain      TEXT,
    sitemap_url TEXT,
    lastmod     TEXT,
    first_seen  TEXT,
    last_seen   TEXT,
    status      TEXT DEFAULT 'present',
    extracted   INTEGER DEFAULT 0
  );`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_bio_urls_sitemap ON bio_urls(sitemap_url);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_bio_urls_domain ON bio_urls(domain);`);
  db.exec(`CREATE TABLE IF NOT EXISTS observations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT,
    domain      TEXT,
    url         TEXT,
    event       TEXT,
    sitemap_url TEXT,
    details     TEXT
  );`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_obs_ts ON observations(ts);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_obs_event ON observations(event);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_obs_domain ON observations(domain);`);

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
        normalizeContact(r);                 // force known-bad fields (e.g. Bankers Life Position/Title) on every write
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
    // Location keyword: substring match on the Location field — Phone Location / Phone 2 Location,
    // which the normalizer fills with the geocoded phone location or the TLD-lookup country.
    if (opts.location) {
      const t = '%' + String(opts.location).toLowerCase() + '%';
      where.push(`(lower("phone_location") LIKE ? OR lower("phone_2_location") LIKE ?)`);
      params.push(t, t);
    }
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
    const offset = (page - 1) * pageSize;
    // Default (no explicit column): newest-scanned first — order by last-updated timestamp, descending.
    let orderBy;
    if (!SORT_COLS.has(sortCol)) {
      orderBy = `updated_at ${Number(opts.dir) === -1 ? 'ASC' : 'DESC'}`;
    } else {
      orderBy = `"${sortCol}"='' , "${sortCol}" COLLATE NOCASE ${Number(opts.dir) === -1 ? 'DESC' : 'ASC'}`;
    }

    const rows = db.prepare(
      `SELECT * FROM contacts ${whereSql} ORDER BY ${orderBy} LIMIT ? OFFSET ?`
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

  // Delete every record for a root domain (the apex AND any subdomain, e.g. century21.com +
  // www/agents.century21.com). Returns the deleted rows (as records, so they can be restored)
  // plus the deleted count.
  // The set of Web Source URLs already captured for a root domain (apex + subdomains) — used to
  // skip already-crawled pages on a gap-fill re-run. Query strings are dropped to match extraction.
  function existingUrls(domain) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    const set = new Set();
    if (!d) return set;
    const rows = db.prepare('SELECT web_source_url u FROM contacts WHERE (domain = ? OR domain LIKE ?)').all(d, '%.' + d);
    for (const r of rows) if (r.u) set.add(String(r.u).split('?')[0]);
    return set;
  }

  // One-time fix: re-derive Phone Location from the URL slug for remax records currently set to
  // RE/MAX HQ ("Denver, CO…"). `derive(url, first, last)` returns "City, ST" or "".
  function fixRemaxLocations(derive) {
    const rows = db.prepare("SELECT * FROM contacts WHERE (domain = 'remax.com' OR domain LIKE '%.remax.com') AND phone_location LIKE 'Denver, CO%'").all();
    let fixed = 0, unparsed = 0; const samples = [];
    db.exec('BEGIN');
    try {
      const upd = db.prepare('UPDATE contacts SET phone_location = ?, updated_at = ? WHERE email = ?');
      const now = new Date().toISOString();
      for (const row of rows) {
        const loc = derive(row.web_source_url, row.first, row.last);
        if (loc && loc !== row.phone_location) { upd.run(loc, now, row.email); fixed++; }
        else if (!loc) { unparsed++; if (samples.length < 12) samples.push(`${row.first || ''}|${row.last || ''}|${(String(row.web_source_url || '').split('/real-estate-agents/')[1] || row.web_source_url)}`); }
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    return { scanned: rows.length, fixed, unparsed, samples };
  }

  // Read-only field coverage for a root domain (apex + subdomains): how many records exist and
  // how many carry an email / phone. For reporting crawl quality.
  function domainStats(domain) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    if (!d) return { total: 0, withEmail: 0, withPhone: 0 };
    const w = '(domain = ? OR domain LIKE ?)';
    const p = [d, '%.' + d];
    const n = (extra) => db.prepare(`SELECT COUNT(*) c FROM contacts WHERE ${w}${extra}`).get(...p).c;
    return { total: n(''), withEmail: n(" AND email_address <> ''"), withPhone: n(" AND phone <> ''") };
  }

  function deleteByDomain(domain, opts = {}) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    if (!d) return { rows: [], deleted: 0 };
    let where = '(domain = ? OR domain LIKE ?)';
    const params = [d, '%.' + d];
    // opts.exceptSource: keep rows from this Source (e.g. 'Site API'), delete the rest — used to
    // purge only the early scrape "junk" while keeping the clean adapter-pulled records.
    if (opts.exceptSource) { where += ' AND source <> ?'; params.push(String(opts.exceptSource)); }
    const rows = db.prepare(`SELECT * FROM contacts WHERE ${where}`).all(...params).map(rowToRecord);
    const res = db.prepare(`DELETE FROM contacts WHERE ${where}`).run(...params);
    return { rows, deleted: res.changes };
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

  // Bulk relabel: set BOTH Position and Title = newValue for every record matching { domain }
  // and/or a Position { prefix }, rebuilding each matched row's search index. Returns
  // { matched, samples } (samples are the PRE-change email/domain/position/title rows, for audit).
  function bulkSetPosition({ domain = '', emailDomain = '', prefix = '' } = {}, newValue) {
    const where = []; const params = [];
    if (domain) { where.push('domain = ?'); params.push(String(domain).toLowerCase().replace(/^www\./, '')); }
    if (emailDomain) { where.push('email LIKE ?'); params.push('%@' + String(emailDomain).toLowerCase().replace(/^@/, '')); }
    if (prefix) { where.push("position LIKE ? ESCAPE '\\'"); params.push(String(prefix).replace(/([%_\\])/g, '\\$1') + '%'); }
    if (!where.length) return { matched: 0, updated: 0, samples: [], domains: [] };
    const w = where.join(' AND ');
    const matchRows = db.prepare(`SELECT email FROM contacts WHERE ${w}`).all(...params);
    const domains = db.prepare(`SELECT domain, COUNT(*) c FROM contacts WHERE ${w} GROUP BY domain ORDER BY c DESC LIMIT 15`).all(...params);
    const samples = db.prepare(`SELECT email, domain, position, title FROM contacts WHERE ${w} LIMIT 8`).all(...params)
      .map((r) => ({ email: r.email, domain: r.domain, position: r.position, title: r.title }));
    const sel = db.prepare('SELECT * FROM contacts WHERE email = ?');
    const upd = db.prepare('UPDATE contacts SET position = ?, title = ?, search = ?, updated_at = ? WHERE email = ?');
    const now = new Date().toISOString();
    db.exec('BEGIN');
    try {
      for (const { email } of matchRows) {
        const row = sel.get(email); if (!row) continue;
        const rec = rowToRecord(row); rec['Position'] = newValue; rec['Title'] = newValue;
        const search = [rec['First'], rec['Last'], rec['Email Address'], rec['Title'], rec['Position'], rec['Domain'], rec['Phone'], rec['Phone 2'], rec['Description']]
          .map((v) => String(v || '')).join(' ').toLowerCase();
        upd.run(newValue, newValue, search, now, email);
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    return { matched: matchRows.length, updated: matchRows.length, samples, domains };
  }
  const updatePositionByPrefix = (prefix, newValue) => bulkSetPosition({ prefix }, newValue);   // back-compat

  // One-off correction for a bad crawl that mislabelled UK records as Angola: rewrite leading
  // +244 -> +44 in Phone/Phone 2, and "Angola" -> "United Kingdom" in the location fields.
  // Rebuilds search. Returns the affected rows [{email, phone, phone_2, domain}] so the caller
  // can re-geocode the corrected numbers.
  function fixAngola() {
    const cond = "phone LIKE '+244%' OR phone_2 LIKE '+244%' OR lower(phone_location) LIKE '%angola%' OR lower(phone_2_location) LIKE '%angola%'";
    const emails = db.prepare(`SELECT email FROM contacts WHERE ${cond}`).all();
    const sel = db.prepare('SELECT * FROM contacts WHERE email = ?');
    const upd = db.prepare('UPDATE contacts SET phone = ?, phone_2 = ?, phone_location = ?, phone_2_location = ?, search = ?, updated_at = ? WHERE email = ?');
    const now = new Date().toISOString();
    const fixPhone = (p) => /^\+244/.test(String(p || '')) ? '+44' + String(p).slice(4) : String(p || '');
    const fixLoc = (l) => /angola/i.test(String(l || '')) ? 'United Kingdom' : String(l || '');
    const affected = [];
    db.exec('BEGIN');
    try {
      for (const { email } of emails) {
        const row = sel.get(email); if (!row) continue;
        const phone = fixPhone(row.phone), phone2 = fixPhone(row.phone_2);
        const loc1 = fixLoc(row.phone_location), loc2 = fixLoc(row.phone_2_location);
        const rec = rowToRecord(row); rec['Phone'] = phone; rec['Phone 2'] = phone2;
        const search = [rec['First'], rec['Last'], rec['Email Address'], rec['Title'], rec['Position'], rec['Domain'], phone, phone2, rec['Description']]
          .map((v) => String(v || '')).join(' ').toLowerCase();
        upd.run(phone, phone2, loc1, loc2, search, now, email);
        affected.push({ email, phone, phone_2: phone2, domain: row.domain });
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    return affected;
  }

  // --- Sitemap monitor methods ---------------------------------------------------------------
  const nowIso = () => new Date().toISOString();

  // Add/refresh a watched child sitemap. Preserves change-detection state (last_lastmod/last_hash/
  // last_fetched), status and added_at across re-discovery; only the descriptive meta is refreshed.
  const watchUpsertStmt = db.prepare(`
    INSERT INTO watched_sitemaps (sitemap_url, parent_url, domain, bio_ratio, url_count, bio_count, added_at, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'active')
    ON CONFLICT(sitemap_url) DO UPDATE SET
      parent_url = excluded.parent_url, domain = excluded.domain,
      bio_ratio = excluded.bio_ratio, url_count = excluded.url_count, bio_count = excluded.bio_count`);
  function upsertWatch(w) {
    if (!w || !w.sitemapUrl) return false;
    watchUpsertStmt.run(w.sitemapUrl, w.parentUrl || null, w.domain || rootDomain(w.sitemapUrl),
      w.bioRatio == null ? null : Number(w.bioRatio), w.urlCount == null ? null : (w.urlCount | 0),
      w.bioCount == null ? null : (w.bioCount | 0), nowIso());
    return true;
  }

  // All watches, each annotated with its current present/departed bio-URL counts (for the UI).
  function listWatches(opts = {}) {
    const where = []; const params = [];
    if (opts.status) { where.push('w.status = ?'); params.push(opts.status); }
    if (opts.domain) { where.push('w.domain = ?'); params.push(String(opts.domain).toLowerCase()); }
    const w = where.length ? 'WHERE ' + where.join(' AND ') : '';
    return db.prepare(`
      SELECT w.*,
        (SELECT COUNT(*) FROM bio_urls b WHERE b.sitemap_url = w.sitemap_url AND b.status = 'present')  AS present_count,
        (SELECT COUNT(*) FROM bio_urls b WHERE b.sitemap_url = w.sitemap_url AND b.status = 'departed') AS departed_count
      FROM watched_sitemaps w ${w} ORDER BY w.domain, w.sitemap_url`).all(...params);
  }
  function activeWatches() { return db.prepare(`SELECT * FROM watched_sitemaps WHERE status = 'active'`).all(); }

  const setWatchStateStmt = db.prepare(`UPDATE watched_sitemaps
    SET last_lastmod = ?, last_hash = ?, last_fetched = ? WHERE sitemap_url = ?`);
  function setWatchState(sitemapUrl, { lastLastmod = null, lastHash = null, lastFetched = nowIso() } = {}) {
    setWatchStateStmt.run(lastLastmod, lastHash, lastFetched, sitemapUrl);
  }
  function setWatchStatus(sitemapUrl, status) {
    if (status !== 'active' && status !== 'paused') return false;
    return db.prepare(`UPDATE watched_sitemaps SET status = ? WHERE sitemap_url = ?`).run(status, sitemapUrl).changes > 0;
  }
  function removeWatch(sitemapUrl) {
    db.exec('BEGIN');
    try {
      db.prepare('DELETE FROM bio_urls WHERE sitemap_url = ?').run(sitemapUrl);
      const r = db.prepare('DELETE FROM watched_sitemaps WHERE sitemap_url = ?').run(sitemapUrl);
      db.exec('COMMIT');
      return r.changes > 0;
    } catch (e) { db.exec('ROLLBACK'); throw e; }
  }

  // THE DIFF. Given the current bio URLs of a sitemap ([{url,lastmod}]), reconcile against the stored
  // baseline in one transaction: brand-new URLs are inserted (event new_bio), URLs that had departed
  // and came back flip to present (event reappeared), and present URLs no longer listed flip to
  // departed (event departed). opts.seed=true records the baseline WITHOUT emitting observations (the
  // initial population is not "new hires"). Returns { newUrls, reappearedUrls, departedUrls, present }.
  const obsInsertStmt = db.prepare(`INSERT INTO observations (ts, domain, url, event, sitemap_url, details) VALUES (?, ?, ?, ?, ?, ?)`);
  const bioInsertStmt = db.prepare(`INSERT INTO bio_urls (url, domain, sitemap_url, lastmod, first_seen, last_seen, status, extracted)
    VALUES (?, ?, ?, ?, ?, ?, 'present', 0)`);
  const bioSeenStmt = db.prepare(`UPDATE bio_urls SET last_seen = ?, lastmod = ? WHERE url = ?`);
  const bioReappearStmt = db.prepare(`UPDATE bio_urls SET last_seen = ?, lastmod = ?, status = 'present' WHERE url = ?`);
  const bioDepartStmt = db.prepare(`UPDATE bio_urls SET status = 'departed' WHERE url = ?`);
  function syncSitemapUrls(sitemapUrl, domain, currentEntries, opts = {}) {
    const seed = !!opts.seed;
    const now = nowIso();
    const dom = String(domain || rootDomain(sitemapUrl) || '').toLowerCase();
    const existing = new Map();
    for (const r of db.prepare('SELECT url, status FROM bio_urls WHERE sitemap_url = ?').all(sitemapUrl)) existing.set(r.url, r);
    const seenNow = new Set();
    const newUrls = [], reappearedUrls = [], departedUrls = [];
    db.exec('BEGIN');
    try {
      for (const e of (currentEntries || [])) {
        const url = String(e && e.url || '').trim();
        if (!url || seenNow.has(url)) continue;
        seenNow.add(url);
        const lastmod = e.lastmod || null;
        const prev = existing.get(url);
        if (!prev) {
          bioInsertStmt.run(url, rootDomain(url) || dom, sitemapUrl, lastmod, now, now);
          newUrls.push(url);
          if (!seed) obsInsertStmt.run(now, rootDomain(url) || dom, url, 'new_bio', sitemapUrl, null);
        } else if (prev.status === 'departed') {
          bioReappearStmt.run(now, lastmod, url);
          reappearedUrls.push(url);
          if (!seed) obsInsertStmt.run(now, rootDomain(url) || dom, url, 'reappeared', sitemapUrl, null);
        } else {
          bioSeenStmt.run(now, lastmod, url);
        }
      }
      for (const [url, prev] of existing) {
        if (prev.status === 'present' && !seenNow.has(url)) {
          bioDepartStmt.run(url);
          departedUrls.push(url);
          if (!seed) obsInsertStmt.run(now, rootDomain(url) || dom, url, 'departed', sitemapUrl, null);
        }
      }
      db.exec('COMMIT');
    } catch (e) { db.exec('ROLLBACK'); throw e; }
    return { newUrls, reappearedUrls, departedUrls, present: seenNow.size };
  }

  // Mark bio URLs as extracted (the monitor calls this after enqueuing the delta for extraction).
  function markExtracted(urls) {
    const upd = db.prepare(`UPDATE bio_urls SET extracted = 1 WHERE url = ?`);
    let n = 0;
    db.exec('BEGIN');
    try { for (const u of (urls || [])) if (u) n += upd.run(u).changes; db.exec('COMMIT'); }
    catch (e) { db.exec('ROLLBACK'); throw e; }
    return n;
  }

  // The change feed: most-recent observations first, optionally filtered.
  function recentObservations(opts = {}) {
    const limit = Math.min(2000, Math.max(1, Number(opts.limit) || 200));
    const where = []; const params = [];
    if (opts.event) { where.push('event = ?'); params.push(String(opts.event)); }
    if (opts.domain) { where.push('domain = ?'); params.push(String(opts.domain).toLowerCase()); }
    if (opts.sinceTs) { where.push('ts >= ?'); params.push(String(opts.sinceTs)); }
    const w = where.length ? 'WHERE ' + where.join(' AND ') : '';
    return db.prepare(`SELECT * FROM observations ${w} ORDER BY ts DESC, id DESC LIMIT ?`).all(...params, limit);
  }

  // Headline counts for the Monitor tab.
  function monitorStats() {
    const watches = db.prepare(`SELECT COUNT(*) c FROM watched_sitemaps`).get().c;
    const activeW = db.prepare(`SELECT COUNT(*) c FROM watched_sitemaps WHERE status = 'active'`).get().c;
    const present = db.prepare(`SELECT COUNT(*) c FROM bio_urls WHERE status = 'present'`).get().c;
    const departed = db.prepare(`SELECT COUNT(*) c FROM bio_urls WHERE status = 'departed'`).get().c;
    const extracted = db.prepare(`SELECT COUNT(*) c FROM bio_urls WHERE extracted = 1`).get().c;
    const lastPass = db.prepare(`SELECT MAX(last_fetched) m FROM watched_sitemaps`).get().m || null;
    const byEvent = {};
    for (const r of db.prepare(`SELECT event, COUNT(*) c FROM observations GROUP BY event`).all()) byEvent[r.event] = r.c;
    return { watches, activeWatches: activeW, present, departed, extracted, lastPass, observations: byEvent };
  }

  importLegacyJson();
  const cleanedEmails = cleanStoredEmails();
  if (cleanedEmails) console.log(`Central DB: removed encoded spaces from ${cleanedEmails} email(s).`);
  const typedRows = backfillTypes();
  if (typedRows) console.log(`Central DB: set Type (from domain TLD) on ${typedRows} record(s).`);
  console.log(`Central DB (SQLite): ${count().toLocaleString()} contact(s) at ${file}`);
  return { upsertMany, query, each, stats, count, facets, getByEmail, updateRecord, deleteByEmail, deleteByDomain, domainStats, existingUrls, fixRemaxLocations, backfillLocations, updatePositionByPrefix, bulkSetPosition, fixAngola,
    // sitemap monitor
    upsertWatch, listWatches, activeWatches, setWatchState, setWatchStatus, removeWatch, syncSitemapUrls, markExtracted, recentObservations, monitorStats };
}

module.exports = { makeDb };
