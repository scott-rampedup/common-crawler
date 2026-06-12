/**
 * users.js — user accounts, roles, and sessions (SQLite via node:sqlite).
 *
 * Roles (low -> high): user < analyst < admin.
 *   user    : Search Database only, read-only (no edit/delete/AI).
 *   analyst : Master + Search Database, all data functionality.
 *   admin   : everything + manage users (create/activate/deactivate/promote/demote/delete).
 *
 * Passwords are scrypt-hashed with a per-user salt. Sessions are random tokens stored in
 * the DB (survive restarts) and carried in an HttpOnly cookie.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');

const ROLES = ['user', 'analyst', 'admin'];
const roleRank = (r) => { const i = ROLES.indexOf(String(r || '')); return i < 0 ? 0 : i; };

function hashPassword(password, salt) {
  salt = salt || crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(password), salt, 64).toString('hex');
  return { salt, hash };
}
function verifyPassword(password, salt, hash) {
  if (!salt || !hash) return false;
  const h = crypto.scryptSync(String(password), salt, 64).toString('hex');
  const a = Buffer.from(h), b = Buffer.from(hash);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
function randomToken() { return crypto.randomBytes(32).toString('hex'); }
function tempPassword() {
  return crypto.randomBytes(12).toString('base64').replace(/[^A-Za-z0-9]/g, '').slice(0, 12) || 'Temp' + Date.now();
}

// Initial, editable legal verbiage (admins can edit these in the Admin tab). Plain text;
// rendered with paragraph spacing on the public pages. Replace bracketed placeholders.
const DEFAULT_PRIVACY = `Privacy Policy

Last updated: 2026

This Privacy Policy explains how RampedUp ("we", "us", or "our") collects, uses, and
protects information in connection with the RampedUp contact-research application (the
"Service"). By using the Service or creating an account, you agree to this Policy.

1. Information We Collect
- Account information you provide when signing up: first name, last name, company, title,
  email address, phone number, username, and password.
- Usage information generated as you use the Service, such as searches you run and records
  you view or export.
- Business contact information contained in the Service's database, which is compiled from
  publicly available sources on the web (for example, business websites and public profiles).

2. How We Use Information
- To provide, operate, secure, and improve the Service.
- To authenticate users, manage roles and permissions, and process account activations.
- To communicate with you about your account and the Service.

3. Sources of Contact Data
The business contact records available through the Service are gathered from publicly
accessible sources and are intended for legitimate business-to-business outreach. We are
not the originator of this information.

4. How We Share Information
We do not sell your account information. We may share information with service providers who
host or support the Service, and as required by law or to protect our rights.

5. Data Retention
We retain account information for as long as your account is active and as needed to provide
the Service, comply with our legal obligations, and resolve disputes.

6. Security
We use reasonable technical and organizational measures, including password hashing and
access controls, to protect information. No method of transmission or storage is 100% secure.

7. Your Choices and Rights
You may request access to, correction of, or deletion of your account information by
contacting an administrator. Individuals whose business contact information appears in the
database may request removal.

8. Cookies
The Service uses a strictly necessary session cookie to keep you signed in. It is not used
for advertising.

9. Children
The Service is intended for business use and is not directed to children under 16.

10. Changes to This Policy
We may update this Policy from time to time. Material changes will be reflected on this page
with a new "Last updated" date.

11. Contact
Questions about this Policy may be directed to your account administrator.`;

const DEFAULT_TERMS = `Terms of Use

Last updated: 2026

These Terms of Use ("Terms") govern your access to and use of the RampedUp contact-research
application (the "Service"). By creating an account or using the Service, you agree to these
Terms. If you do not agree, do not use the Service.

1. Accounts and Eligibility
You must provide accurate registration information and keep it current. Accounts require
administrator activation before access is granted. You are responsible for safeguarding your
password and for all activity under your account.

2. Acceptable Use
You agree to use the Service only for lawful, legitimate business purposes and in compliance
with all applicable laws, including those governing electronic communications, marketing, and
data protection. You will not misuse the Service, attempt to gain unauthorized access, or use
the data to harass, defraud, or harm any person.

3. Data and Compliance
Business contact data is provided for business-to-business use. You are solely responsible for
ensuring your use of any contact information (including outreach) complies with applicable laws
and the recipients' rights, and for honoring opt-out and do-not-contact requests.

4. Intellectual Property
The Service and its software are owned by us and our licensors. You receive a limited,
non-exclusive, non-transferable right to use the Service while your account is active.

5. Disclaimers
The Service and all data are provided "as is" and "as available" without warranties of any
kind. We do not warrant that contact data is accurate, complete, or current.

6. Limitation of Liability
To the maximum extent permitted by law, we will not be liable for any indirect, incidental,
special, consequential, or punitive damages, or any loss arising from your use of the Service.

7. Termination
We may suspend or terminate your access at any time, including for violation of these Terms.

8. Changes
We may update these Terms from time to time. Continued use after changes constitutes
acceptance of the updated Terms.

9. Contact
Questions about these Terms may be directed to your account administrator.`;

function makeUsers(dir) {
  const file = path.join(dir, 'users.db');
  const db = new DatabaseSync(file);
  db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;');
  db.exec(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    pass_hash TEXT NOT NULL,
    pass_salt TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    active INTEGER NOT NULL DEFAULT 0,
    first TEXT DEFAULT '', last TEXT DEFAULT '', company TEXT DEFAULT '',
    title TEXT DEFAULT '', email TEXT DEFAULT '', phone TEXT DEFAULT '',
    created_at TEXT
  );`);
  db.exec(`CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    created_at TEXT,
    expires_at TEXT
  );`);
  db.exec(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);`);

  // Public-safe shape (never exposes hash/salt).
  const pub = (row) => row && {
    id: row.id, username: row.username, role: row.role, active: !!row.active,
    first: row.first || '', last: row.last || '', company: row.company || '',
    title: row.title || '', email: row.email || '', phone: row.phone || '',
    createdAt: row.created_at || '',
  };

  const norm = (u) => String(u || '').trim().toLowerCase();
  function getByUsername(u) { return db.prepare('SELECT * FROM users WHERE username = ?').get(norm(u)); }
  function getById(id) { return db.prepare('SELECT * FROM users WHERE id = ?').get(Number(id)); }
  function listUsers() {
    return db.prepare('SELECT * FROM users ORDER BY active ASC, role DESC, username ASC').all().map(pub);
  }
  function count() { return db.prepare('SELECT COUNT(*) c FROM users').get().c; }
  function activeAdminCount() { return db.prepare("SELECT COUNT(*) c FROM users WHERE role='admin' AND active=1").get().c; }

  function createUser(opts = {}) {
    const u = norm(opts.username);
    if (!u || !opts.password) return { ok: false, error: 'Username and password are required.' };
    if (getByUsername(u)) return { ok: false, error: 'That username is already taken.' };
    let role = ROLES.includes(opts.role) ? opts.role : 'user';
    const { salt, hash } = hashPassword(opts.password);
    db.prepare(`INSERT INTO users (username,pass_hash,pass_salt,role,active,first,last,company,title,email,phone,created_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      u, hash, salt, role, opts.active ? 1 : 0,
      opts.first || '', opts.last || '', opts.company || '', opts.title || '', opts.email || '', opts.phone || '',
      new Date().toISOString());
    return { ok: true, user: pub(getByUsername(u)) };
  }

  // Returns the raw row on success (caller checks .active), or null on bad credentials.
  function verify(username, password) {
    const row = getByUsername(username);
    if (!row || !verifyPassword(password, row.pass_salt, row.pass_hash)) return null;
    return row;
  }

  function setActive(id, active) { db.prepare('UPDATE users SET active=? WHERE id=?').run(active ? 1 : 0, Number(id)); }
  function setRole(id, role) { if (ROLES.includes(role)) db.prepare('UPDATE users SET role=? WHERE id=?').run(role, Number(id)); }
  function setPassword(id, password) {
    const { salt, hash } = hashPassword(password);
    db.prepare('UPDATE users SET pass_hash=?, pass_salt=? WHERE id=?').run(hash, salt, Number(id));
  }
  function deleteUser(id) {
    db.prepare('DELETE FROM sessions WHERE user_id=?').run(Number(id));
    db.prepare('DELETE FROM users WHERE id=?').run(Number(id));
  }

  // ---- sessions ----
  function createSession(userId) {
    const token = randomToken();
    const now = new Date();
    const exp = new Date(now.getTime() + 1000 * 60 * 60 * 24 * 14);   // 14 days
    db.prepare('INSERT INTO sessions (token,user_id,created_at,expires_at) VALUES (?,?,?,?)')
      .run(token, Number(userId), now.toISOString(), exp.toISOString());
    return token;
  }
  function sessionUser(token) {
    if (!token) return null;
    const s = db.prepare('SELECT * FROM sessions WHERE token = ?').get(String(token));
    if (!s) return null;
    if (new Date(s.expires_at) < new Date()) { db.prepare('DELETE FROM sessions WHERE token=?').run(String(token)); return null; }
    const row = getById(s.user_id);
    if (!row || !row.active) return null;     // deleted/deactivated users lose access immediately
    return row;
  }
  function destroySession(token) { if (token) db.prepare('DELETE FROM sessions WHERE token=?').run(String(token)); }
  function destroyUserSessions(userId) { db.prepare('DELETE FROM sessions WHERE user_id=?').run(Number(userId)); }

  // Reset a user's password to a fresh temporary one (returned so an admin can pass it on,
  // since email is not configured). All that user's sessions are invalidated.
  function resetPassword(id) {
    const pw = tempPassword();
    setPassword(id, pw);
    destroyUserSessions(id);
    return pw;
  }

  // ---- editable page content (Privacy Policy / Terms of Use) ----
  function getSetting(key) { const r = db.prepare('SELECT value FROM settings WHERE key = ?').get(String(key)); return r ? r.value : ''; }
  function setSetting(key, value) {
    db.prepare(`INSERT INTO settings (key,value,updated_at) VALUES (?,?,?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at`)
      .run(String(key), String(value == null ? '' : value), new Date().toISOString());
  }
  function seedPages() {
    if (!getSetting('privacy')) setSetting('privacy', DEFAULT_PRIVACY);
    if (!getSetting('terms')) setSetting('terms', DEFAULT_TERMS);
  }
  seedPages();

  // Seed a default admin the first time the system runs (no users yet).
  function seedDefaultAdmin() {
    if (count() > 0) return null;
    const username = (process.env.ADMIN_USERNAME || 'admin').trim().toLowerCase();
    const password = process.env.ADMIN_PASSWORD || tempPassword();
    createUser({ username, password, role: 'admin', active: 1, first: 'Site', last: 'Admin' });
    return { username, password, generated: !process.env.ADMIN_PASSWORD };
  }

  return {
    ROLES, roleRank, pub,
    getByUsername, getById, listUsers, count, activeAdminCount,
    createUser, verify, setActive, setRole, setPassword, deleteUser, resetPassword,
    createSession, sessionUser, destroySession, destroyUserSessions, seedDefaultAdmin,
    getSetting, setSetting,
  };
}

module.exports = { makeUsers, ROLES, roleRank };
