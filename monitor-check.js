// Read-only health check of the live Sitemap Monitor state (run on the Fly machine).
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');
let dir = process.env.DATA_DIR || '/data';
if (!fs.existsSync(path.join(dir, 'contacts.db'))) { for (const d of ['/data', '/app/data', '.']) if (fs.existsSync(path.join(d, 'contacts.db'))) { dir = d; break; } }
const file = path.join(dir, 'contacts.db');
console.log('DB:', file, '| now:', new Date().toISOString());
const db = new DatabaseSync(file);
const g = (s) => { try { return db.prepare(s).get(); } catch (e) { return { err: e.message }; } };
const has = (t) => g(`SELECT COUNT(*) c FROM ${t}`);
console.log('watched_sitemaps:', JSON.stringify(has('watched_sitemaps')), '| active:', JSON.stringify(g("SELECT COUNT(*) c FROM watched_sitemaps WHERE status='active'")));
console.log('bio_urls present:', JSON.stringify(g("SELECT COUNT(*) c FROM bio_urls WHERE status='present'")), '| extracted=1:', JSON.stringify(g('SELECT COUNT(*) c FROM bio_urls WHERE extracted=1')));
const ev = {}; try { for (const r of db.prepare('SELECT event,COUNT(*) c FROM observations GROUP BY event').all()) ev[r.event] = r.c; } catch (e) { ev.err = e.message; }
console.log('observations by event:', JSON.stringify(ev));
console.log('extract_cursor:', JSON.stringify(g("SELECT v FROM monitor_meta WHERE k='extract_cursor'")));
console.log('last pass (max last_fetched):', JSON.stringify(g('SELECT MAX(last_fetched) m FROM watched_sitemaps')));
try { console.log('recent new_bio:', db.prepare("SELECT ts,domain FROM observations WHERE event='new_bio' ORDER BY id DESC LIMIT 3").all().map((r) => `${r.ts} ${r.domain}`)); } catch (e) { /* */ }
