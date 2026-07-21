/**
 * bing-load.js — Phase 1 loader for the Bing Maps UK export. Same normalized "Location" output shape as
 * gm-load.js (so gm-build/gm-upsert consume it identically), adapted to Bing's cleaner schema:
 * header-based columns, no status field (keep all), no team/bio/LinkedIn/WhatsApp, but a COMPONENTIZED
 * address (city/State/country as separate columns — carried through so gm-build skips address parsing).
 *
 *   node bing-load.js [--limit N] [--src "../Bing Maps"] [--out "<dir>"]   -> bing-locations.ndjson
 * Bing header: ID,YPID,Business_name,Street,city,State,Zip,country,Address,Lat,Long,Phone,Email,Website,
 *              Facebook,Instagram,Category1,Category2,Category_code,Note,PageUrl
 */
const fs = require('fs');
const path = require('path');
const ex = require('./extractor');
const co = require('./companies');
const { rootDomain } = require('./email-model');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const LIMIT = Number(arg('--limit', '0')) || 0;
const SRC = arg('--src', path.join(__dirname, '..', 'Bing Maps'));
const OUT = arg('--out', SRC);
const B = { ypid: 1, name: 2, city: 4, state: 5, country: 7, address: 8, phone: 11, email: 12, website: 13, facebook: 14, instagram: 15, cat1: 16, cat2: 17 };
const COUNTRY = { GB: 'United Kingdom', UK: 'United Kingdom' };

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
try { ex.loadEmailBlocklist(path.join(__dirname, 'email-blocklist.txt')); } catch (e) { /* no list */ }
let dirRules = {}; try { dirRules = ex.loadDirectoryRules(path.join(__dirname, 'data', 'directory-rules.json')); } catch (e) { /* built-ins apply */ }
const clean = (s) => String(s == null ? '' : s).trim();
const abs = (u) => { u = clean(u); return /^https?:/i.test(u) ? u : (u ? 'https://' + u : ''); };
const PEOPLE_SUB = new Set(['agent', 'agents', 'advisor', 'advisors', 'realtor', 'realtors', 'provider', 'providers', 'doctor', 'doctors', 'physician', 'physicians']);
const LOC_SUB = new Set(['location', 'locations', 'store', 'stores', 'bank', 'banks']);
function websiteType(u) {
  u = abs(u); if (!u) return '';
  try { const labels = new URL(u).hostname.toLowerCase().replace(/^www\./, '').split('.'); if (labels.length > 2) { if (PEOPLE_SUB.has(labels[0])) return 'People'; if (LOC_SUB.has(labels[0])) return 'Location'; } } catch (e) {}
  try { const pid = ex.pathIdFromUrl(u); if (pid && pid.id) return pid.id; } catch (e) {}
  try { const d = ex.classifyDirectory(u, '', dirRules, genderMap); if (d && d !== 'Contact Us') return d; } catch (e) {}
  return '';
}

async function eachRecord(file, onRec) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file, { encoding: 'utf8' });
    let field = '', row = [], q = false, header = false, stopped = false;
    const endField = () => { row.push(field); field = ''; };
    const endRow = () => { endField(); if (!header) header = true; else if (!stopped) { if (onRec(row) === false) { stopped = true; stream.destroy(); } } row = []; };
    stream.on('data', (chunk) => { if (stopped) return; for (let i = 0; i < chunk.length; i++) { const c = chunk[i];
      if (q) { if (c === '"') { if (chunk[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
      else if (c === '"') q = true; else if (c === ',') endField(); else if (c === '\n') { endRow(); if (stopped) break; } else if (c !== '\r') field += c; } });
    stream.on('close', resolve); stream.on('end', resolve); stream.on('error', reject);
  });
}

(async () => {
  const files = fs.readdirSync(SRC).filter((f) => /\.csv$/i.test(f)).map((f) => path.join(SRC, f));
  console.error(`Bing CSVs: ${files.length} | out: ${OUT}`);
  const out = fs.createWriteStream(path.join(OUT, 'bing-locations.ndjson'));
  const now = new Date().toISOString();
  let seen = 0, kept = 0, skippedNoWeb = 0; const t0 = Date.now();
  for (const file of files) {
    await eachRecord(file, (r) => {
      seen++;
      if (LIMIT && kept >= LIMIT) return false;
      const dom = co.normDomain(clean(r[B.website])) || rootDomain(clean(r[B.website]));
      if (!dom) { skippedNoWeb++; return; }
      const email = ex.cleanEmail(clean(r[B.email])).toLowerCase();   // drops blocklist + junk emails
      const rec = {
        company_type: 'Location', root_domain: dom, cid: clean(r[B.ypid]),
        name: clean(r[B.name]), full_address: clean(r[B.address]),
        locality: clean(r[B.city]), region: clean(r[B.state]), country: COUNTRY[clean(r[B.country]).toUpperCase()] || clean(r[B.country]),
        website: 'https://' + dom + '/', website_type: websiteType(r[B.website]),
        category: clean(r[B.cat1]) || clean(r[B.cat2]),
        email, email_type: email ? ex.classifyEmail(email) : '',
        phone: clean(r[B.phone]), phone_type: '',                 // UK numbers: NANP line-typing N/A
        facebook: clean(r[B.facebook]), instagram: clean(r[B.instagram]), whatsapp: '',
        linkedin_contact: '', bio_url: '', team_page: '',
        time_stamp: now, source_map: 'Bing',
      };
      out.write(JSON.stringify(rec) + '\n'); kept++;
      if (kept % 50000 === 0) console.error(`  seen ${seen.toLocaleString()} | kept ${kept.toLocaleString()} | ${Math.round(kept / ((Date.now() - t0) / 1000))}/s`);
    });
    if (LIMIT && kept >= LIMIT) break;
  }
  out.end();
  console.error(`DONE: ${seen.toLocaleString()} seen -> ${kept.toLocaleString()} Location records | ${skippedNoWeb.toLocaleString()} no-website | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
