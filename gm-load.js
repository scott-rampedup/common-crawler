/**
 * gm-load.js — Phase 1 of the Google-Maps -> Company Crawler ETL: parse the Google Maps Contact-Info
 * CSVs, keep only OPEN businesses that have a website, and transform each into a normalized "Location"
 * record (Company Crawler field shape) using the shared Common-Crawler logic. Emits:
 *   - gm-locations.ndjson : one JSON Location record per line (grouped/rolled up in Phase 2)
 *   - gm-bio-urls.txt     : the validated bio URLs (isBioOrContactUrl) to push into Hop-2 extraction
 *
 * The source CSV header is unreliable (43 cols w/ duplicates) but the DATA rows are a clean 37 columns;
 * we read by fixed position. Fields with commas/quotes/newlines need the streaming state-machine parser.
 *
 *   node gm-load.js [--limit N] [--src "../Google Maps"] [--out "../Google Maps"]
 */
const fs = require('fs');
const path = require('path');
const ex = require('./extractor');
const co = require('./companies');
const { rootDomain } = require('./email-model');
let ccEngine = null; try { ccEngine = require('./cc-engine'); } catch (e) { /* isBioOrContactUrl optional */ }

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const LIMIT = Number(arg('--limit', '0')) || 0;
const SRC = arg('--src', path.join(__dirname, '..', 'Google Maps'));
const OUT = arg('--out', SRC);

const clean0 = (s) => String(s == null ? '' : s).trim();
// The scrapes ship in TWO layouts (clean 37-col + a messy 43-col). Rather than fixed maps, anchor on the
// status value ("Open"/"Closed"/…) — website/category/phone sit at fixed offsets around it — and read the
// xb_* enrichment fields from FIXED offsets off the END of the row (identical tail in both layouts).
const STATUS_RE = /^(Open|Closed|Permanently closed|Temporarily closed)$/i;
function fields(r) {
  const len = r.length;
  const si = r.findIndex((v) => STATUS_RE.test(clean0(v)));
  if (si < 3 || si + 4 >= len) return null;                         // couldn't locate the status block
  let name = '';
  for (let i = 4; i < si - 1; i++) { const v = clean0(r[i]); if (v && v !== '#N/A' && !/^\d+$/.test(v) && !/^(m|f|male|female|unisex)$/i.test(v) && !/^https?:/i.test(v) && v.length > name.length) name = v; }
  if (!name) name = clean0(r[4]) === '#N/A' ? '' : clean0(r[4]);
  const end = (n) => clean0(r[len - n]);                            // xb_* live at fixed offsets from the end
  // the cid COLUMN is Excel-mangled (scientific notation) — take the real one from the Maps URL (col 2).
  const cid = (String(r[2] || '').match(/[?&]cid=(-?\d+)/) || [])[1] || clean0(r[1]);
  return {
    cid, address: clean0(r[3]), name,
    status: clean0(r[si]), website_url: clean0(r[si + 1]), website_location: clean0(r[si + 2]),
    category: clean0(r[si + 4]), phone: clean0(r[si - 1]),
    xb_emails: end(18), xb_whatsapp: end(14), xb_facebook: end(13), xb_instagram: end(12),
    xb_linkedin_company: end(9), xb_linkedin_profile: end(8), xb_team_profile_urls: end(5), xb_team_page: end(3), xb_crawled_at: end(1),
  };
}

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
try { ex.loadEmailBlocklist(path.join(__dirname, 'email-blocklist.txt')); console.error('email blocklist loaded'); } catch (e) { console.error('email blocklist not loaded'); }
const wbc = require('./wireless-block-classifier');
let wireless = null; try { wireless = wbc.loadWirelessBlocks(wbc.PHONE_BLOCKS_CSV); console.error('phone-blocks loaded for line-type classification'); } catch (e) { console.error('phone-blocks not loaded -> phone_type blank'); }
const phoneType = (p) => { if (!wireless || !p) return ''; try { const t = wbc.classifyLineType(p, wireless); return (t && t.type && t.type !== 'Unknown') ? t.type : ''; } catch (e) { return ''; } };
let dirRules = {}; try { dirRules = ex.loadDirectoryRules(path.join(__dirname, 'data', 'directory-rules.json')); } catch (e) { /* built-in BIO_DIRS still apply */ }

const clean = (s) => String(s == null ? '' : s).trim();
const abs = (u) => { u = clean(u); return /^https?:/i.test(u) ? u : (u ? 'https://' + u : ''); };
const splitMulti = (s) => clean(s).split(/\s*\|\|\s*|\s*;\s*|\s*,\s*/).map((x) => x.trim()).filter(Boolean);
// LinkedIn Contact = personal profile (linkedin.com/in, from xb_linkedin_profile);
// LinkedIn URL = company page (linkedin.com/company, from xb_linkedin_company). Enforce BOTH the
// source column AND the URL shape so the two never cross-contaminate.
const LI_IN = /linkedin\.com\/in\//i;
const LI_CO = /linkedin\.com\/(company|school|showcase)\//i;
const liProfiles = (s) => splitMulti(s).filter((u) => LI_IN.test(u));
const liCompanies = (s) => splitMulti(s).filter((u) => LI_CO.test(u));

const PEOPLE_SUB = new Set(['agent', 'agents', 'advisor', 'advisors', 'realtor', 'realtors', 'provider', 'providers', 'doctor', 'doctors', 'physician', 'physicians']);
const LOC_SUB = new Set(['location', 'locations', 'store', 'stores', 'bank', 'banks']);
function websiteType(websiteLocation) {
  const u = abs(websiteLocation); if (!u) return '';
  try {
    const host = new URL(u).hostname.toLowerCase().replace(/^www\./, '');
    const labels = host.split('.');
    if (labels.length > 2) { const first = labels[0]; if (PEOPLE_SUB.has(first)) return 'People'; if (LOC_SUB.has(first)) return 'Location'; }
  } catch (e) { /* fall through */ }
  try { const pid = ex.pathIdFromUrl(u); if (pid && pid.id) return pid.id; } catch (e) { /* */ }
  try { const d = ex.classifyDirectory(u, '', dirRules, genderMap); if (d && d !== 'Contact Us') return d; } catch (e) { /* */ }
  return '';
}
const isBio = (u) => { try { return ccEngine ? ccEngine.isBioOrContactUrl(abs(u), dirRules, genderMap) : /\/(team|people|our-team|staff|attorney|agent|advisor|profile|bio|about-us)\//i.test(u); } catch (e) { return false; } };

// ---- streaming CSV state-machine parser (quotes + embedded newlines) ----
async function eachRecord(file, onRec) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file, { encoding: 'utf8' });
    let field = '', row = [], q = false, header = false, stopped = false;
    const endField = () => { row.push(field); field = ''; };
    const endRow = () => { endField(); if (!header) header = true; else if (!stopped) { if (onRec(row) === false) { stopped = true; stream.destroy(); } } row = []; };
    stream.on('data', (chunk) => {
      if (stopped) return;
      for (let i = 0; i < chunk.length; i++) {
        const c = chunk[i];
        if (q) { if (c === '"') { if (chunk[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
        else if (c === '"') q = true;
        else if (c === ',') endField();
        else if (c === '\n') { endRow(); if (stopped) break; }
        else if (c !== '\r') field += c;
      }
    });
    stream.on('close', resolve); stream.on('end', resolve); stream.on('error', reject);
  });
}

(async () => {
  // --file <path> ingests ONE CSV (e.g. a newly-arrived export); default = every CSV in --src.
  const oneFile = arg('--file', '');
  const files = oneFile ? [oneFile]
    : fs.readdirSync(SRC).filter((f) => /\.csv$/i.test(f)).map((f) => path.join(SRC, f));
  console.error(`source CSVs: ${files.length}${oneFile ? ` (${path.basename(oneFile)})` : ''} | out: ${OUT}`);
  const locOut = fs.createWriteStream(path.join(OUT, 'gm-locations.ndjson'));
  const bioSet = new Set();
  let seen = 0, kept = 0, skippedClosed = 0, skippedNoWeb = 0; const t0 = Date.now();

  for (const file of files) {
    await eachRecord(file, (r) => {
      seen++;
      if (LIMIT && kept >= LIMIT) return false;
      const f = fields(r);
      if (!f || f.status !== 'Open') { skippedClosed++; return; }  // skip unparseable + non-Open
      const webLoc = f.website_location || f.website_url;
      const dom = co.normDomain(webLoc) || rootDomain(webLoc);
      if (co.isBadCompanyDomain(dom)) { skippedNoWeb++; return; }  // skip website-less + free-mail/social/shared (domain must be the real web source)
      // capture EVERY team-profile URL the source found — these ARE team-member pages, so don't drop them to
      // the isBio heuristic (that was losing valid bios). Quality is enforced downstream at extraction
      // (extract-from-pointers rejects non-person junk). Keep basic URL sanity + dedupe.
      const bio = [...new Set(splitMulti(f.xb_team_profile_urls).map((u) => abs(u.trim())).filter((u) => /^https?:\/\/[^\s/]+\.[^\s]/i.test(u)))];
      for (const b of bio) bioSet.add(b);
      // pull EVERY email in xb_emails (not just the first), each run through cleanEmail — which drops the
      // ones we don't import (blocklist + junk: images/example.com/phone-misreads). email = primary; the
      // full list feeds contact-building downstream so no address is lost.
      const emails = [...new Set(splitMulti(f.xb_emails).map((e) => ex.cleanEmail(e).toLowerCase()).filter(Boolean))];
      const email = emails[0] || '';
      const out = {
        company_type: 'Location', root_domain: dom, cid: f.cid,
        name: f.name, full_address: f.address,
        website: 'https://' + dom + '/', website_type: websiteType(webLoc),
        category: f.category,
        email, emails, email_type: email ? ex.classifyEmail(email) : '',
        phone: f.phone, phone_type: phoneType(f.phone),
        whatsapp: f.xb_whatsapp, facebook: f.xb_facebook, instagram: f.xb_instagram,
        linkedin_contact: liProfiles(f.xb_linkedin_profile).join('; '),   // personal profiles (linkedin.com/in)
        linkedin_url: liCompanies(f.xb_linkedin_company).join('; '),      // company page (linkedin.com/company)
        bio_url: bio.join('; '), team_page: f.xb_team_page,
        time_stamp: f.xb_crawled_at,
      };
      locOut.write(JSON.stringify(out) + '\n');
      kept++;
      if (kept % 50000 === 0) console.error(`  seen ${seen.toLocaleString()} | kept ${kept.toLocaleString()} | ${Math.round(kept / ((Date.now() - t0) / 1000))}/s`);
    });
    if (LIMIT && kept >= LIMIT) break;
  }
  fs.writeFileSync(path.join(OUT, 'gm-bio-urls.txt'), [...bioSet].join('\n') + '\n');
  locOut.end();
  console.error(`DONE: ${seen.toLocaleString()} seen -> ${kept.toLocaleString()} Location records | skipped ${skippedClosed.toLocaleString()} closed + ${skippedNoWeb.toLocaleString()} no-website | ${bioSet.size.toLocaleString()} bio URLs | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
