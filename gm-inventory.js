/**
 * gm-inventory.js — count what a Google Maps export actually CONTAINS, versus what the ETL keeps.
 *
 *   node gm-inventory.js --file "../Google Maps/Google_Maps_Contact_Info_11-08-2026_Part_1.csv"
 *
 * The current pipeline is built for NAMED contacts: extract-from-pointers ends every batch with
 * `.filter(d => d.first && d.last && d.email)`, and gm-build only emits a contact when it can name a
 * person. Under a named-contact product an email with no name is worthless, so it is dropped and never
 * counted. Under a hashed-email (HEM) product it is inventory.
 *
 * This measures the gap: distinct emails and LinkedIn profiles present in the export, split by the
 * classification that decides their value (Professional / Personal / Role-Based), and how many carry a
 * name-bearing local part (First.Last) that could be turned into a real contact for free.
 *
 * Read-only. Reuses gm-load's end-offset column positions, because the header claims 43 columns and the
 * data rows are 37 — the xb_* fields are only reliably addressable from the END of the row.
 */
const fs = require('fs');
const path = require('path');
const ex = require('./extractor');
const che = require('./cc-home-enrich');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const FILE = arg('--file', '');
const LIMIT = Number(arg('--limit', '0')) || 0;

const clean0 = (s) => String(s == null ? '' : s).trim().replace(/^"|"$/g, '');
const splitMulti = (s) => String(s || '').split(/[;,|\n]+/).map((x) => x.trim()).filter(Boolean);
const rootOf = (u) => { try { return new URL(/^https?:\/\//i.test(u) ? u : 'https://' + u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return ''; } };

// Streaming, quote-aware CSV -> rows (commas/newlines inside quoted fields are common here).
function* parseCsv(stream) { /* replaced by manual loop below */ }

(async () => {
  if (!FILE || !fs.existsSync(FILE)) { console.error('need --file <export.csv>'); process.exit(1); }
  const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));

  const t0 = Date.now();
  const emails = new Map();          // email -> type
  const liProfiles = new Set();
  const domains = new Set();
  let rows = 0, withEmail = 0, withLi = 0, withSite = 0;

  // Manual state-machine parse so a 900MB file streams in constant memory.
  const stream = fs.createReadStream(FILE, { encoding: 'utf8', highWaterMark: 1 << 20 });
  let field = '', row = [], q = false, pending = '';
  const onRow = (r) => {
    if (rows++ === 0) return;                       // header
    const len = r.length;
    const end = (n) => clean0(r[len - n]);
    const si = r.findIndex((c) => /^(Open|Closed|Temporarily closed|Permanently closed)$/i.test(clean0(c)));
    const site = si > -1 ? clean0(r[si + 1]) : '';
    const d = rootOf(site);
    if (d) { domains.add(d); withSite++; }
    for (const e of splitMulti(end(18))) {
      const c = ex.cleanEmail(e).toLowerCase();
      if (!c || !c.includes('@')) continue;
      if (!emails.has(c)) emails.set(c, ex.classifyEmail(c));
    }
    if (splitMulti(end(18)).length) withEmail++;
    for (const u of splitMulti(end(8))) if (/linkedin\.com\/in\//i.test(u)) liProfiles.add(u.trim().toLowerCase());
    if (splitMulti(end(8)).length) withLi++;
  };
  await new Promise((resolve, reject) => {
    stream.on('data', (chunk) => {
      const s = pending + chunk; pending = '';
      for (let i = 0; i < s.length; i++) {
        const c = s[i];
        if (q) { if (c === '"') { if (s[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
        else if (c === '"') q = true;
        else if (c === ',') { row.push(field); field = ''; }
        else if (c === '\n') { row.push(field); onRow(row); row = []; field = ''; if (LIMIT && rows > LIMIT) { stream.destroy(); resolve(); return; } }
        else if (c !== '\r') field += c;
      }
    });
    stream.on('close', resolve); stream.on('end', resolve); stream.on('error', reject);
  });
  if (field.length || row.length) { row.push(field); onRow(row); }

  // Classify the email inventory the way the product would value it.
  const byType = {};
  let nameable = 0;
  for (const [e, t] of emails) {
    byType[t] = (byType[t] || 0) + 1;
    const n = che.nameFromEmail(e);
    if (n && n.first && n.last) nameable++;          // First.Last -> a real contact, for free
  }

  const N = (n) => Number(n || 0).toLocaleString();
  console.log(`\n${path.basename(FILE)}`);
  console.log(`  rows                     ${N(rows - 1)}`);
  console.log(`  rows with a website      ${N(withSite)}   -> ${N(domains.size)} distinct root domains`);
  console.log(`  rows with xb_emails      ${N(withEmail)}`);
  console.log(`  rows with xb_linkedin    ${N(withLi)}`);
  console.log(`\n  DISTINCT EMAILS         ${N(emails.size)}`);
  for (const t of Object.keys(byType).sort((a, b) => byType[b] - byType[a])) {
    console.log(`    ${String(t || '(none)').padEnd(14)} ${N(byType[t])}  ${((byType[t] / emails.size) * 100).toFixed(1)}%`);
  }
  console.log(`    of these, First.Last  ${N(nameable)}  ${((nameable / Math.max(1, emails.size)) * 100).toFixed(1)}%  <- nameable into real contacts today`);
  console.log(`\n  DISTINCT LinkedIn /in/  ${N(liProfiles.size)}`);
  console.log(`\n  ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
