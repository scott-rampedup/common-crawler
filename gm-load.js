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
// Which xb_ field each end-offset was standing in for. The offsets stay as the fallback for a
// headerless file; when a header is present XB_IDX wins, so a changed column count cannot silently
// repoint a field at an empty column.
const XB_BY_END = { 18: 'xb_emails', 16: 'xb_phones', 15: 'xb_phone_details', 14: 'xb_whatsapp',
  13: 'xb_facebook', 12: 'xb_instagram', 9: 'xb_linkedin_company', 8: 'xb_linkedin_profile',
  5: 'xb_team_profile_urls', 3: 'xb_team_page', 1: 'xb_crawled_at' };
let XB_IDX = null;                       // set from the header by setHeaderIndex()

// Record where each xb_ column actually is. Called once per file with the parsed header row.
function setHeaderIndex(headerRow) {
  XB_IDX = null;
  if (!Array.isArray(headerRow) || !headerRow.length) return;
  const idx = {};
  headerRow.forEach((h, i) => {
    const k = String(h == null ? '' : h).replace(/^﻿/, '').trim().toLowerCase();
    if (k.startsWith('xb_') && idx[k] == null) idx[k] = i;
  });
  const found = Object.keys(idx).length;
  if (found) {
    XB_IDX = idx;
    console.error(`  header: ${headerRow.length} column(s), ${found} xb_ field(s) located by name`);
  } else {
    console.error(`  header: ${headerRow.length} column(s), no xb_ names found — falling back to end-offsets`);
  }
}

function fields(r, siHint) {
  const len = r.length;
  // Prefer the status column the HEADER declares. Scanning for the first cell matching Open|Closed|...
  // cannot see a BLANK status, so those rows were discarded as unparseable -- 25,616 of them in the
  // 08-10 export. Both known layouts (37 and 43 columns) put website_url/category/phone at the same
  // offsets around the status column, so only si differs between them.
  const si = (siHint != null && siHint >= 0) ? siHint : r.findIndex((v) => STATUS_RE.test(clean0(v)));
  if (si < 3 || si + 4 >= len) return null;                         // couldn't locate the status block
  let name = '';
  for (let i = 4; i < si - 1; i++) { const v = clean0(r[i]); if (v && v !== '#N/A' && !/^\d+$/.test(v) && !/^(m|f|male|female|unisex)$/i.test(v) && !/^https?:/i.test(v) && v.length > name.length) name = v; }
  if (!name) name = clean0(r[4]) === '#N/A' ? '' : clean0(r[4]);
  // xb_* BY NAME when the header gave us positions, by end-offset only as a fallback.
  //
  // The end-offsets held for the 37- and 43-column layouts, but the 19-10 export arrived with 155
  // columns: the same 37 real ones followed by 118 EMPTY trailing columns. end(18) then addressed
  // column 137 instead of 19, so xb_emails/xb_phones/xb_phone_details/xb_whatsapp all read '' and the
  // run would have produced ~500k Location records with no contact data while reporting success.
  // A name is stable under that; a distance from the end is not.
  const end = (n) => {
    const name = XB_BY_END[n];
    if (name && XB_IDX && XB_IDX[name] != null) return clean0(r[XB_IDX[name]]);
    return clean0(r[len - n]);
  };
  // the cid COLUMN is Excel-mangled (scientific notation) — take the real one from the Maps URL (col 2).
  const cid = (String(r[2] || '').match(/[?&]cid=(-?\d+)/) || [])[1] || clean0(r[1]);
  return {
    cid, address: clean0(r[3]), name,
    status: clean0(r[si]), website_url: clean0(r[si + 1]), website_location: clean0(r[si + 2]),
    category: clean0(r[si + 4]), phone: clean0(r[si - 1]),
    xb_emails: end(18), xb_phones: end(16), xb_phone_details: end(15), xb_whatsapp: end(14), xb_facebook: end(13), xb_instagram: end(12),
    xb_linkedin_company: end(9), xb_linkedin_profile: end(8), xb_team_profile_urls: end(5), xb_team_page: end(3), xb_crawled_at: end(1),
  };
}

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
try { ex.loadEmailBlocklist(path.join(__dirname, 'email-blocklist.txt')); console.error('email blocklist loaded'); } catch (e) { console.error('email blocklist not loaded'); }
const wbc = require('./wireless-block-classifier');
let wireless = null; try { wireless = wbc.loadWirelessBlocks(wbc.PHONE_BLOCKS_CSV); console.error('phone-blocks loaded for line-type classification'); } catch (e) { console.error('phone-blocks not loaded -> phone_type blank'); }
const phoneType = (p) => { if (!wireless || !p) return ''; try { const t = wbc.classifyLineType(p, wireless); return (t && t.type && t.type !== 'Unknown') ? t.type : ''; } catch (e) { return ''; } };
// xb_phone_details is "<number>|<Type> || <number>|<Type> || ..." and mixes the real line with mangled
// variants of itself: +16463510882 arrives alongside +100386463510882 and +64635108822, and the junk
// gets a type too ("Fixed Line", "Other"). So prefer a Mobile, then a Fixed Line, never "Other", and
// require a plausible E.164 length -- 10 to 13 digits -- which is what separates the real number from
// the digit-shifted noise. Returns the business line, which for these one- and two-person businesses
// is usually the owner's; for a larger company it is the shared number, not a personal DDI.
function bestPhone(details) {
  const rank = (t) => (/^mobile$/i.test(t) ? 0 : /^fixed line$/i.test(t) ? 1 : 9);
  const items = splitMulti(details)
    .map((s) => { const p = String(s).split("|"); return { num: (p[0] || "").trim(), type: (p[1] || "").trim() }; })
    .filter((x) => { const d = x.num.replace(/[^0-9]/g, ""); return d.length >= 10 && d.length <= 13 && rank(x.type) < 9; })
    .sort((a, b) => rank(a.type) - rank(b.type) || a.num.length - b.num.length);
  return items[0] || null;
}
let dirRules = {}; try { dirRules = ex.loadDirectoryRules(path.join(__dirname, 'data', 'directory-rules.json')); } catch (e) { /* built-in BIO_DIRS still apply */ }

const clean = (s) => String(s == null ? '' : s).trim();
const abs = (u) => { u = clean(u); return /^https?:/i.test(u) ? u : (u ? 'https://' + u : ''); };
// Google appends redirect tracking to the website URL in the Maps export:
//   https://www.midflorida.com/locations/kissimmee-branch&opi=79508299&sa=U&ved=0ahUKEwj...
// Note the bare "&" with no "?" -- it is not even a valid query string. Left in place these URLs
// resolve against nothing in Common Crawl, which would quietly defeat the per-location crawl: the
// field looks populated and every lookup misses. Strip from the first tracking key.
const GOOG_TRACK = /[?&](?:opi|sa|ved|usg|sca_esv|source|cd|cad|rct|uact)=/;
const cleanUrl = (u) => { const s = String(u || '').trim(); if (!s) return ''; return s.split(GOOG_TRACK)[0].replace(/[?&]+$/, ''); };
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
async function eachRecord(file, onRec, onHeader) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file, { encoding: 'utf8' });
    let field = '', row = [], q = false, header = false, stopped = false;
    const endField = () => { row.push(field); field = ''; };
    const endRow = () => { endField(); if (!header) { header = true; if (onHeader) onHeader(row); } else if (!stopped) { if (onRec(row) === false) { stopped = true; stream.destroy(); } } row = []; };
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

// Exported so gm-bio-scan.js reads these CSVs through EXACTLY this code. The column mapping is the
// fragile part of the format — the 19-10 export arrived with 155 columns and end-offset parsing would
// have produced locations with no emails or phones — so a second reader that drifts from this one is a
// defect waiting to happen. Guarded below so requiring the module does not start an ingest.
module.exports = { eachRecord, fields, setHeaderIndex, websiteType, isBio, abs, clean, splitMulti, cleanUrl };

if (require.main === module) (async () => {
  // --file <path> ingests ONE CSV (e.g. a newly-arrived export); default = every CSV in --src.
  const oneFile = arg('--file', '');
  const files = oneFile ? [oneFile]
    : fs.readdirSync(SRC).filter((f) => /\.csv$/i.test(f)).map((f) => path.join(SRC, f));
  console.error(`source CSVs: ${files.length}${oneFile ? ` (${path.basename(oneFile)})` : ''} | out: ${OUT}`);
  const locOut = fs.createWriteStream(path.join(OUT, 'gm-locations.ndjson'));
  const bioSet = new Set();
  let seen = 0, kept = 0, skippedClosed = 0, skippedNoWeb = 0, unparseable = 0; const t0 = Date.now();
  const byStatus = new Map();          // what we actually kept, by company_status

  for (const file of files) {
    let siHint = -1;                 // company_status column, straight from this file's header
    await eachRecord(file, (r) => {
      seen++;
      if (LIMIT && kept >= LIMIT) return false;
      const f = fields(r, siHint);
      if (!f) { unparseable++; return; }                           // no locatable status block
      // Only PERMANENTLY closed is a dead business. This previously dropped every non-Open row, which
      // threw away two things that are not closed: "Temporarily closed" (16,443 in the 05-10 export --
      // live businesses that reopen) and a BLANK status (61,792 -- unknown, not closed). 78k a run.
      const status = f.status || "";
      if (/^(permanently closed|closed)$/i.test(status)) { skippedClosed++; return; }   // bare "Closed" too
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
        // Closure is a PER-LOCATION fact, never per-domain: one branch of a chain closing says nothing
        // about the chain -- coldwellbanker.com carries 67,339 contacts behind closed listings. Stored
        // on the location record so it can be filtered without condemning the whole domain.
        company_status: status,
        name: f.name, full_address: f.address,
        // KEEP THE LOCATION'S OWN PAGE.
        //
        // webLoc is the specific page the Maps export gave for THIS place
        // (…/locations/manchester-blood-donor-centre). It was previously used only to derive `dom` and
        // the website_type label and then discarded, so every Location stored the bare root instead:
        // all 4,510,566 of them, three different NHS blood donor centres all pointing at blood.co.uk.
        // That makes a per-location crawl impossible -- it would fetch the same homepage for every
        // branch -- and it inverts the rule that the location website beats the plain website.
        // `website` is now the page; `root_domain` above still carries the domain for joins.
        website: cleanUrl(abs(webLoc)) || ('https://' + dom + '/'),
        page_url: cleanUrl(abs(webLoc)) || '',            // explicit: what a Location-scoped crawl should fetch
        website_type: websiteType(webLoc),
        category: f.category,
        email, emails, email_type: email ? ex.classifyEmail(email) : '',
        phone: f.phone, phone_type: phoneType(f.phone),
        // the enrichment phone, kept separate from the listing phone above so neither overwrites the other
        xb_phone: (bestPhone(f.xb_phone_details) || {}).num || "",
        xb_phone_type: (bestPhone(f.xb_phone_details) || {}).type || "",
        whatsapp: f.xb_whatsapp, facebook: f.xb_facebook, instagram: f.xb_instagram,
        linkedin_contact: liProfiles(f.xb_linkedin_profile).join('; '),   // personal profiles (linkedin.com/in)
        linkedin_url: liCompanies(f.xb_linkedin_company).join('; '),      // company page (linkedin.com/company)
        bio_url: bio.join('; '), team_page: f.xb_team_page,
        time_stamp: f.xb_crawled_at,
      };
      locOut.write(JSON.stringify(out) + '\n');
      byStatus.set(status || '(blank)', (byStatus.get(status || '(blank)') || 0) + 1);
      kept++;
      if (kept % 50000 === 0) console.error(`  seen ${seen.toLocaleString()} | kept ${kept.toLocaleString()} | ${Math.round(kept / ((Date.now() - t0) / 1000))}/s`);
    }, (hdr) => {
      siHint = hdr.findIndex((h) => String(h || '').trim().toLowerCase() === 'company_status');
      setHeaderIndex(hdr);              // locate the xb_ columns by name for THIS file
    });
    if (LIMIT && kept >= LIMIT) break;
  }
  console.error('  kept by status: ' + [...byStatus].sort((a, b) => b[1] - a[1]).map(([k, v]) => k + ' ' + v.toLocaleString()).join(' | '));
  fs.writeFileSync(path.join(OUT, 'gm-bio-urls.txt'), [...bioSet].join('\n') + '\n');
  // Which export these bio URLs came out of. gm-run names the Data Importer list after it, so a pushed
  // list can be traced back to the file that produced it instead of appearing as an anonymous batch.
  fs.writeFileSync(path.join(OUT, 'gm-bio-urls.meta.json'), JSON.stringify({
    sourceFiles: files.map((f) => path.basename(f)),
    count: bioSet.size,
    at: new Date().toISOString(),
  }, null, 1));
  locOut.end();
  console.error(`DONE: ${seen.toLocaleString()} seen -> ${kept.toLocaleString()} Location records | skipped ${skippedClosed.toLocaleString()} permanently-closed + ${skippedNoWeb.toLocaleString()} no-website + ${unparseable.toLocaleString()} unparseable | ${bioSet.size.toLocaleString()} bio URLs | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
