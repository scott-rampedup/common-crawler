/**
 * send-monitor-report.js — email the Sitemap Monitor's nightly summary to contact@common-crawler.com.
 * Reads the monitor SQLite READ-ONLY (no init side-effects) and sends via mailer.js (Fly SMTP secrets).
 *   node send-monitor-report.js [to] [--dry]
 * Run on Fly (where /data/contacts.db + SMTP live): fly ssh console -C "node /app/send-monitor-report.js --dry"
 */
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const mailer = require('./mailer');

const DATA_DIR = process.env.DATA_DIR || __dirname;
const FILE = path.join(DATA_DIR, 'contacts.db');
const args = process.argv.slice(2);
const DRY = args.includes('--dry');
// Same recipient the nightly report uses, so a manual send tests the real
// path rather than a different address. An argument still overrides it.
const TO = args.find((a) => a.includes('@')) || process.env.MONITOR_REPORT_TO || 'contact@common-crawler.com';

const db = new DatabaseSync(FILE, { readOnly: true });
const val = (sql, k = 'c') => { try { const r = db.prepare(sql).get(); return (r && r[k] != null) ? r[k] : 0; } catch (e) { return 0; } };

const activeW   = val(`SELECT COUNT(*) c FROM watched_sitemaps WHERE status='active'`);
const watches   = val(`SELECT COUNT(*) c FROM watched_sitemaps`);
const seenNew   = val(`SELECT COUNT(*) c FROM observations WHERE event='new_bio'`);
const processed = val(`SELECT COUNT(*) c FROM bio_urls WHERE extracted=1`);
const present   = val(`SELECT COUNT(*) c FROM bio_urls WHERE status='present'`);
const lastPass  = val(`SELECT MAX(last_fetched) m FROM watched_sitemaps`, 'm') || 'no pass recorded yet';
const asOf = new Date().toISOString().slice(0, 10);

const rows = [
  ['Sitemaps monitored nightly', `${activeW.toLocaleString()} active (${watches.toLocaleString()} total watched)`],
  ['New BIO URLs seen', seenNew.toLocaleString()],
  ['New BIO URLs processed', processed.toLocaleString()],
  ['BIO URLs currently tracked', present.toLocaleString()],
  ['Last monitor pass', String(lastPass)],
];
const subject = `Common Crawler — Sitemap Monitor report (${asOf})`;
const text = `Sitemap Monitor summary as of ${asOf}\n\n` + rows.map(([k, v]) => `${k}: ${v}`).join('\n') +
  `\n\n— Common Crawler`;
const tr = (k, v) => `<tr><td style="padding:6px 16px 6px 0;color:#6b7280">${k}</td><td style="padding:6px 0;font-weight:600;color:#111827">${v}</td></tr>`;
const html = `<div style="font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;max-width:520px;margin:0 auto;color:#111827">
  <h2 style="font-size:1.2rem;margin:0 0 4px">Sitemap Monitor — nightly report</h2>
  <p style="color:#6b7280;margin:0 0 14px">as of ${asOf}</p>
  <table style="border-collapse:collapse;font-size:14px">${rows.map(([k, v]) => tr(k, v)).join('')}</table>
  <p style="color:#9ca3af;font-size:12px;margin-top:24px;border-top:1px solid #eee;padding-top:12px">Common Crawler · common-crawler.com</p>
</div>`;

console.log(`DB: ${FILE}\nTo: ${TO}\n` + rows.map(([k, v]) => `  ${k}: ${v}`).join('\n'));
(async () => {
  if (DRY) { console.log('\n[--dry] not sending.'); return; }
  if (!mailer.mailEnabled()) { console.error('SMTP not configured (no SMTP_HOST/USER/PASS) — cannot send.'); process.exit(1); }
  const r = await mailer.sendMail({ to: TO, subject, text, html });
  console.log('\nsend result:', JSON.stringify(r));
})().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
