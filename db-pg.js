/**
 * db-pg.js — Postgres backend for the central contacts store (async port of db.js).
 * ---------------------------------------------------------------------------------
 * Same single de-duplicated `contacts` table keyed by lowercased email, same
 * score-gated upsert ("keep the richer record"), same query/facet/maintenance
 * interface — but on Postgres so a fleet of worker machines can share ONE store
 * (SQLite-on-one-volume can't). Every method is async (returns a Promise).
 *
 * Field plumbing (FIELDS/COLS/rowValues/score) mirrors db.js — db.js stays the
 * source of truth for the SQLite path; this is the shared-DB path. Business logic
 * (normalizeContact, typeForDomain) is required from the same modules, not copied.
 *
 *   const db = await makeDb({ connectionString });   // DATABASE_URL
 */
const { Pool } = require('pg');
const { typeForDomain } = require('./tld-lookup');
const { normalizeContact } = require('./normalize');

// --- field mapping (kept in lockstep with db.js) ---
const FIELDS = ['Time Stamp', 'Source', 'Web Source URL', 'Directory', 'Path ID', 'Last Path',
  'Bio Check', 'First', 'Last', 'Gender', 'Title', 'Position', 'Description', 'Image URL',
  'Email Address', 'Email Type', 'LinkedIn URL', 'Facebook', 'Twitter', 'WhatsApp', 'Google Maps', 'vCard', 'Phone', 'Phone Type',
  'Phone Location', 'Phone 2', 'Phone 2 Type', 'Phone 2 Location', 'Type'];
const colName = (f) => f.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

// Postgres text can't hold a NUL byte (0x00) — one in an extracted bio aborts the whole INSERT with
// "invalid byte sequence for encoding UTF8: 0x00", which used to crash the worker on a poison batch.
// Strip NULs and drop unpaired UTF-16 surrogates (also invalid UTF-8). Non-strings pass through.
function pgSafe(v) {
  if (typeof v !== 'string') return v;
  if (v.indexOf('\x00') === -1 && !/[\uD800-\uDFFF]/.test(v)) return v;
  return v.replace(/\x00/g, '')
          .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '');
}
const COLS = FIELDS.map(colName);
const FIELD_BY_COL = Object.fromEntries(FIELDS.map((f) => [colName(f), f]));
const SORT_COLS = new Set([...COLS, 'domain']);
const INSERT_COLS = ['email', ...COLS, 'domain', 'search', 'score', 'updated_at'];

const SCORE_FIELDS = ['First', 'Last', 'Title', 'Position', 'Phone', 'Phone 2', 'LinkedIn URL',
  'Gender', 'Phone Location', 'Image URL', 'Description', 'Google Maps'];
function score(r) { let s = 0; for (const f of SCORE_FIELDS) if (String(r[f] || '').trim()) s++; return s; }

function rootDomain(url) {
  const t = String(url || '').trim();
  if (!t) return '';
  try { return new URL(t).hostname.replace(/^www\./i, '').toLowerCase(); }
  catch { return t.replace(/^https?:\/\//i, '').split('/')[0].replace(/^www\./i, '').toLowerCase(); }
}
function cleanEmail(s) { return String(s == null ? '' : s).replace(/%20/gi, '').replace(/\s+/g, ''); }
function emailKey(s) { return cleanEmail(s).toLowerCase(); }

function searchBlob(r, domain, emailField) {
  return [r['First'], r['Last'], emailField, r['Title'], r['Position'], domain, r['Phone'], r['Phone 2'], r['Description']]
    .map((v) => String(v || '')).join(' ').toLowerCase();
}
// [email, ...FIELDS(in order), domain, search, score, updated_at] or null if no email
function rowValues(r) {
  const emailField = cleanEmail(r['Email Address']);
  const email = emailField.toLowerCase();
  if (!email) return null;
  const domain = rootDomain(r['Web Source URL']);
  const type = typeForDomain(domain);
  const vals = [email];
  for (const f of FIELDS) vals.push(f === 'Email Address' ? emailField : f === 'Type' ? type : String(r[f] == null ? '' : r[f]));
  vals.push(domain, searchBlob(r, domain, emailField), score(r), new Date().toISOString());
  return vals;
}
function rowToRecord(row) {
  const rec = {};
  for (const c of COLS) rec[FIELD_BY_COL[c]] = row[c] || '';
  rec.Domain = row.domain || '';
  return rec;
}

async function makeDb(opts = {}) {
  const connectionString = opts.connectionString || process.env.DATABASE_URL;
  if (!connectionString) throw new Error('db-pg: no connectionString / DATABASE_URL');
  const pool = new Pool({ connectionString, max: Number(opts.max) || 10, ssl: opts.ssl || false });
  const q = (text, params) => pool.query(text, params);

  // --- schema (idempotent) ---
  const colDefs = COLS.map((c) => `"${c}" TEXT DEFAULT ''`).join(', ');
  await q(`CREATE TABLE IF NOT EXISTS contacts (
    email TEXT PRIMARY KEY, ${colDefs},
    domain TEXT DEFAULT '', search TEXT DEFAULT '', score INTEGER DEFAULT 0, updated_at TEXT DEFAULT ''
  )`);
  for (const c of COLS) await q(`ALTER TABLE contacts ADD COLUMN IF NOT EXISTS "${c}" TEXT DEFAULT ''`);
  for (const c of ['directory', 'domain', 'gender', 'phone_type', 'email_type', 'last', 'first']) {
    await q(`CREATE INDEX IF NOT EXISTS idx_contacts_${c} ON contacts("${c}")`);
  }
  await q(`CREATE EXTENSION IF NOT EXISTS pg_trgm`);
  await q(`CREATE INDEX IF NOT EXISTS idx_contacts_search_trgm ON contacts USING gin (search gin_trgm_ops)`);
  await q(`CREATE INDEX IF NOT EXISTS idx_contacts_updated_at ON contacts(updated_at)`);   // default sort: newest first

  // --- WHERE builder (PG $N placeholders); mirrors db.js whereFor ---
  function whereFor(o = {}) {
    const where = []; const params = [];
    const P = (v) => { params.push(v); return '$' + params.length; };
    const eqCI = (col, val) => where.push(`lower("${col}") = ${P(String(val).toLowerCase())}`);
    if (o.directory) eqCI('directory', o.directory);
    if (o.emailType) eqCI('email_type', o.emailType);
    if (o.phoneType) eqCI('phone_type', o.phoneType);
    if (o.type) eqCI('type', o.type);
    if (o.domain) where.push(`domain = ${P(String(o.domain).toLowerCase())}`);
    if (o.position) where.push(`lower("position") LIKE ${P('%' + String(o.position).toLowerCase() + '%')}`);
    if (o.location) { const t = '%' + String(o.location).toLowerCase() + '%'; where.push(`(lower("phone_location") LIKE ${P(t)} OR lower("phone_2_location") LIKE ${P(t)})`); }
    if (Array.isArray(o.domains) && o.domains.length) {
      const parts = [];
      for (const d of o.domains) { const dl = String(d || '').trim().toLowerCase().replace(/^www\./, ''); if (!dl) continue; parts.push(`(domain = ${P(dl)} OR domain LIKE ${P('%.' + dl)})`); }
      if (parts.length) where.push('(' + parts.join(' OR ') + ')');
    }
    if (o.linkedin) where.push(`linkedin_url <> ''`);
    switch (o.gender) {
      case 'male': where.push(`upper(gender) = 'M'`); break;
      case 'female': where.push(`upper(gender) = 'F'`); break;
      case 'all': where.push(`upper(gender) IN ('M','F')`); break;
      case 'none': where.push(`upper(gender) NOT IN ('M','F')`); break;
      default: break;
    }
    if (o.search) where.push(`search LIKE ${P('%' + String(o.search).toLowerCase() + '%')}`);
    return { sql: where.length ? 'WHERE ' + where.join(' AND ') : '', params };
  }

  async function count() { return Number((await q('SELECT COUNT(*) c FROM contacts')).rows[0].c); }
  async function stats() { return { total: await count() }; }

  // Multi-row, score-gated upsert. Dedupe within the batch (max score wins; ties keep the last,
  // matching db.js's sequential overwrite), then chunk to stay under PG's param limit, INSERT ...
  // ON CONFLICT DO UPDATE ... WHERE EXCLUDED.score >= contacts.score. `added` = true inserts (xmax=0).
  async function upsertMany(records) {
    const byEmail = new Map();
    for (const r of (records || [])) {
      normalizeContact(r);
      const v = rowValues(r);
      if (!v) continue;
      const prev = byEmail.get(v[0]);
      const sIdx = INSERT_COLS.indexOf('score');
      if (!prev || v[sIdx] >= prev[sIdx]) byEmail.set(v[0], v);
    }
    const rows = [...byEmail.values()];
    if (!rows.length) return { processed: 0, added: 0, total: await count() };

    const colList = INSERT_COLS.map((c) => `"${c}"`).join(', ');
    const setList = INSERT_COLS.filter((c) => c !== 'email').map((c) => `"${c}" = EXCLUDED."${c}"`).join(', ');
    const perRow = INSERT_COLS.length;
    const maxRows = Math.max(1, Math.floor(60000 / perRow));   // stay under 65535 params
    const client = await pool.connect();
    let added = 0, processed = 0;
    try {
      await client.query('BEGIN');
      for (let i = 0; i < rows.length; i += maxRows) {
        const chunk = rows.slice(i, i + maxRows);
        const values = [];
        const tuples = chunk.map((row) => {
          const ph = row.map((val) => { values.push(pgSafe(val)); return '$' + values.length; });
          return '(' + ph.join(', ') + ')';
        });
        const sql = `INSERT INTO contacts (${colList}) VALUES ${tuples.join(', ')}
          ON CONFLICT (email) DO UPDATE SET ${setList} WHERE EXCLUDED.score >= contacts.score
          RETURNING (xmax = 0) AS inserted`;
        const res = await client.query(sql, values);
        processed += chunk.length;
        for (const r of res.rows) if (r.inserted) added++;
      }
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
    return { processed, added, total: await count() };
  }

  // Exact COUNT(*) over a multi-million-row table is a full scan on EVERY page load — the main
  // reason Search felt slow (and, once a query crossed Fly's request timeout, surfaced as
  // "unable to connect to the API"). Make the total cheap instead:
  //   - no filter  -> the planner's row estimate (instant); exact-count fallback if never analyzed.
  //   - filtered   -> count, but stop at COUNT_CAP so a broad keyword can't scan the whole table.
  // `approx` tells the UI whether to render the number as estimated ('~') or capped ('+').
  const COUNT_CAP = 10000;   // matches the UI's MAX_SELECT_ALL ceiling
  async function totalFor(whereSql, params) {
    if (!whereSql) {
      const est = Number((await q(
        `SELECT reltuples::bigint AS c FROM pg_class WHERE oid = 'contacts'::regclass`)).rows[0].c);
      if (est > 0) return { total: est, approx: 'estimate' };
      return { total: Number((await q('SELECT COUNT(*) c FROM contacts')).rows[0].c), approx: null };
    }
    const c = Number((await q(
      `SELECT COUNT(*) c FROM (SELECT 1 FROM contacts ${whereSql} LIMIT ${COUNT_CAP + 1}) t`,
      params)).rows[0].c);
    return c > COUNT_CAP ? { total: COUNT_CAP, approx: 'capped' } : { total: c, approx: null };
  }

  async function query(o = {}) {
    const pageSize = Math.min(500, Math.max(1, Number(o.pageSize) || 50));
    const page = Math.max(1, Number(o.page) || 1);
    const { sql: whereSql, params } = whereFor(o);
    const { total, approx } = await totalFor(whereSql, params);
    let sortCol = colName(o.sort || ''); if (o.sort === 'Domain') sortCol = 'domain';
    const offset = (page - 1) * pageSize;
    let orderBy;
    if (!SORT_COLS.has(sortCol)) {
      // Default (no explicit column): newest-scanned first. updated_at is an ISO string so it sorts
      // lexicographically = chronologically, and is index-backed (idx_contacts_updated_at).
      orderBy = `updated_at ${Number(o.dir) === -1 ? 'ASC' : 'DESC'}`;
    } else {
      orderBy = `("${sortCol}" = '') ASC, lower("${sortCol}") ${Number(o.dir) === -1 ? 'DESC' : 'ASC'}`;
    }
    const rows = (await q(
      `SELECT * FROM contacts ${whereSql} ORDER BY ${orderBy} LIMIT ${pageSize} OFFSET ${offset}`,
      params)).rows;
    return { rows: rows.map(rowToRecord), total, approx, page, pageSize };
  }

  async function facets() {
    const distinct = async (c) => (await q(`SELECT DISTINCT "${c}" v FROM contacts WHERE "${c}" <> '' ORDER BY "${c}"`)).rows.map((r) => r.v);
    return { directory: await distinct('directory'), emailType: await distinct('email_type'), phoneType: await distinct('phone_type'), type: await distinct('type') };
  }

  // Stream every (optionally filtered) record to cb, keyset-paginated by email (efficient at scale).
  async function each(o, cb) {
    const { sql: whereSql, params } = whereFor(o || {});
    const base = whereSql ? whereSql + ' AND' : 'WHERE';
    let last = '';
    for (;;) {
      const rows = (await q(`SELECT * FROM contacts ${base} email > $${params.length + 1} ORDER BY email LIMIT 5000`, [...params, last])).rows;
      if (!rows.length) break;
      for (const row of rows) cb(rowToRecord(row));
      last = rows[rows.length - 1].email;
      if (rows.length < 5000) break;
    }
  }

  async function getByEmail(email) {
    const e = emailKey(email); if (!e) return null;
    const row = (await q('SELECT * FROM contacts WHERE email = $1', [e])).rows[0];
    return row ? rowToRecord(row) : null;
  }
  async function deleteByEmail(email) { const e = emailKey(email); if (e) await q('DELETE FROM contacts WHERE email = $1', [e]); }

  async function putForce(record) {
    const v = rowValues(record); if (!v) return false;
    const colList = INSERT_COLS.map((c) => `"${c}"`).join(', ');
    const setList = INSERT_COLS.filter((c) => c !== 'email').map((c) => `"${c}" = EXCLUDED."${c}"`).join(', ');
    const ph = v.map((_, i) => '$' + (i + 1));
    await q(`INSERT INTO contacts (${colList}) VALUES (${ph.join(', ')}) ON CONFLICT (email) DO UPDATE SET ${setList}`, v);
    return true;
  }

  async function updateRecord(origEmail, updates) {
    const oe = emailKey(origEmail);
    const orig = await getByEmail(oe);
    if (!orig) return { ok: false, error: 'not_found' };
    const merged = { ...orig, ...(updates || {}) };
    const newEmail = emailKey(merged['Email Address']);
    if (!newEmail) return { ok: false, error: 'email_required' };
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      if (newEmail !== oe) await client.query('DELETE FROM contacts WHERE email = $1', [oe]);
      const v = rowValues(merged);
      const colList = INSERT_COLS.map((c) => `"${c}"`).join(', ');
      const setList = INSERT_COLS.filter((c) => c !== 'email').map((c) => `"${c}" = EXCLUDED."${c}"`).join(', ');
      const ph = v.map((_, i) => '$' + (i + 1));
      await client.query(`INSERT INTO contacts (${colList}) VALUES (${ph.join(', ')}) ON CONFLICT (email) DO UPDATE SET ${setList}`, v);
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
    return { ok: true, record: await getByEmail(newEmail) };
  }

  async function deleteByDomain(domain, o = {}) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    if (!d) return { rows: [], deleted: 0 };
    let where = '(domain = $1 OR domain LIKE $2)'; const params = [d, '%.' + d];
    if (o.exceptSource) { where += ' AND source <> $3'; params.push(String(o.exceptSource)); }
    const rows = (await q(`SELECT * FROM contacts WHERE ${where}`, params)).rows.map(rowToRecord);
    const res = await q(`DELETE FROM contacts WHERE ${where}`, params);
    return { rows, deleted: res.rowCount };
  }

  async function domainStats(domain) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    if (!d) return { total: 0, withEmail: 0, withPhone: 0 };
    const n = async (extra) => Number((await q(`SELECT COUNT(*) c FROM contacts WHERE (domain = $1 OR domain LIKE $2)${extra}`, [d, '%.' + d])).rows[0].c);
    return { total: await n(''), withEmail: await n(` AND email_address <> ''`), withPhone: await n(` AND phone <> ''`) };
  }

  // A few Professional-email samples for a domain — to learn its email pattern (worker email modelling).
  // Light + indexed (domain index): minimal columns, no count, capped — cheap to call per batch at scale.
  async function sampleProfessionalEmails(domain, limit = 25) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    if (!d) return [];
    const rows = (await q(
      `SELECT first, last, email_address FROM contacts
       WHERE domain = $1 AND email_type = 'Professional' AND email_address <> '' LIMIT $2`,
      [d, Math.max(1, limit | 0)])).rows;
    return rows.map((r) => ({ First: r.first, Last: r.last, 'Email Address': r.email_address }));
  }

  async function existingUrls(domain) {
    const d = String(domain || '').toLowerCase().replace(/^www\./, '').trim();
    const set = new Set(); if (!d) return set;
    const rows = (await q('SELECT web_source_url u FROM contacts WHERE (domain = $1 OR domain LIKE $2)', [d, '%.' + d])).rows;
    for (const r of rows) if (r.u) set.add(String(r.u).split('?')[0]);
    return set;
  }

  async function backfillLocations(items) {
    const client = await pool.connect(); let n = 0;
    try {
      await client.query('BEGIN');
      for (const it of (items || [])) {
        const e = emailKey(it.email); if (!e) continue;
        if (it.loc1) n += (await client.query(`UPDATE contacts SET phone_location = $1 WHERE email = $2 AND (phone_location IS NULL OR phone_location = '')`, [it.loc1, e])).rowCount;
        if (it.loc2) n += (await client.query(`UPDATE contacts SET phone_2_location = $1 WHERE email = $2 AND (phone_2_location IS NULL OR phone_2_location = '')`, [it.loc2, e])).rowCount;
      }
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
    return n;
  }

  async function bulkSetPosition({ domain = '', emailDomain = '', prefix = '' } = {}, newValue) {
    const where = []; const params = [];
    const P = (v) => { params.push(v); return '$' + params.length; };
    if (domain) where.push(`domain = ${P(String(domain).toLowerCase().replace(/^www\./, ''))}`);
    if (emailDomain) where.push(`email LIKE ${P('%@' + String(emailDomain).toLowerCase().replace(/^@/, ''))}`);
    if (prefix) where.push(`position LIKE ${P(String(prefix).replace(/([%_\\])/g, '\\$1') + '%')}`);
    if (!where.length) return { matched: 0, updated: 0, samples: [], domains: [] };
    const w = where.join(' AND ');
    const matchRows = (await q(`SELECT * FROM contacts WHERE ${w}`, params)).rows;
    const domains = (await q(`SELECT domain, COUNT(*) c FROM contacts WHERE ${w} GROUP BY domain ORDER BY c DESC LIMIT 15`, params)).rows;
    const samples = matchRows.slice(0, 8).map((r) => ({ email: r.email, domain: r.domain, position: r.position, title: r.title }));
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const now = new Date().toISOString();
      for (const row of matchRows) {
        const rec = rowToRecord(row); rec['Position'] = newValue; rec['Title'] = newValue;
        const search = searchBlob(rec, rec['Domain'], rec['Email Address']);
        await client.query('UPDATE contacts SET position = $1, title = $2, search = $3, updated_at = $4 WHERE email = $5', [newValue, newValue, search, now, row.email]);
      }
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
    return { matched: matchRows.length, updated: matchRows.length, samples, domains };
  }
  const updatePositionByPrefix = (prefix, newValue) => bulkSetPosition({ prefix }, newValue);

  async function fixAngola() {
    const cond = `phone LIKE '+244%' OR phone_2 LIKE '+244%' OR lower(phone_location) LIKE '%angola%' OR lower(phone_2_location) LIKE '%angola%'`;
    const rows = (await q(`SELECT * FROM contacts WHERE ${cond}`)).rows;
    const fixPhone = (p) => /^\+244/.test(String(p || '')) ? '+44' + String(p).slice(4) : String(p || '');
    const fixLoc = (l) => /angola/i.test(String(l || '')) ? 'United Kingdom' : String(l || '');
    const affected = []; const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const now = new Date().toISOString();
      for (const row of rows) {
        const phone = fixPhone(row.phone), phone2 = fixPhone(row.phone_2);
        const loc1 = fixLoc(row.phone_location), loc2 = fixLoc(row.phone_2_location);
        const rec = rowToRecord(row); rec['Phone'] = phone; rec['Phone 2'] = phone2;
        const search = searchBlob(rec, rec['Domain'], rec['Email Address']);
        await client.query('UPDATE contacts SET phone = $1, phone_2 = $2, phone_location = $3, phone_2_location = $4, search = $5, updated_at = $6 WHERE email = $7',
          [phone, phone2, loc1, loc2, search, now, row.email]);
        affected.push({ email: row.email, phone, phone_2: phone2, domain: row.domain });
      }
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
    return affected;
  }

  async function fixRemaxLocations(derive) {
    const rows = (await q(`SELECT * FROM contacts WHERE (domain = 'remax.com' OR domain LIKE '%.remax.com') AND phone_location LIKE 'Denver, CO%'`)).rows;
    let fixed = 0, unparsed = 0; const samples = []; const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const now = new Date().toISOString();
      for (const row of rows) {
        const loc = derive(row.web_source_url, row.first, row.last);
        if (loc && loc !== row.phone_location) { await client.query('UPDATE contacts SET phone_location = $1, updated_at = $2 WHERE email = $3', [loc, now, row.email]); fixed++; }
        else if (!loc) { unparsed++; if (samples.length < 12) samples.push(`${row.first || ''}|${row.last || ''}|${(String(row.web_source_url || '').split('/real-estate-agents/')[1] || row.web_source_url)}`); }
      }
      await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
    return { scanned: rows.length, fixed, unparsed, samples };
  }

  console.log(`Central DB (Postgres): ${(await count()).toLocaleString()} contact(s)`);
  return { upsertMany, query, each, stats, count, facets, getByEmail, updateRecord, deleteByEmail, deleteByDomain,
    domainStats, existingUrls, fixRemaxLocations, backfillLocations, updatePositionByPrefix, bulkSetPosition, fixAngola,
    putForce, sampleProfessionalEmails, _pool: pool, close: () => pool.end() };
}

module.exports = { makeDb, rowValues, FIELDS, COLS, INSERT_COLS };
