/**
 * sheet-import.js — one-way import of a Google Sheet of contacts into the Master DB.
 *
 * Reads the sheet (public CSV export, or the service-account API via gsheets.js when
 * GOOGLE_SERVICE_ACCOUNT_JSON is set), maps its columns to the Master DB record shape
 * (reusing the extractor's derivations so imported rows match crawled ones), dedupes by
 * bio URL (keeping the newest scan), and upserts via db.upsertMany. Import-only — no crawling.
 */
const https = require("https");
const {
  cleanEmail, classifyEmail, classifyDirectory, pathIdFromUrl, toE164, countryCodeFromDomain, getBaseDomain,
} = require("./extractor");
let gsheets = null; try { gsheets = require("./gsheets"); } catch { /* optional */ }

// ---- robust CSV parser: handles quoted fields with embedded commas, quotes, and newlines ----
function parseCsv(text){
  const rows = []; let row = []; let field = ""; let inQ = false;
  const s = String(text || "");
  for(let i = 0; i < s.length; i++){
    const c = s[i];
    if(inQ){
      if(c === '"'){
        if(s[i + 1] === '"'){ field += '"'; i++; }
        else inQ = false;
      } else field += c;
    } else {
      if(c === '"') inQ = true;
      else if(c === ","){ row.push(field); field = ""; }
      else if(c === "\n"){ row.push(field); rows.push(row); row = []; field = ""; }
      else if(c === "\r"){ /* swallow; \n handles the break */ }
      else field += c;
    }
  }
  if(field.length || row.length){ row.push(field); rows.push(row); }
  return rows;
}

// Large-body GET that follows redirects (the CSV export 302s to googleusercontent). No size cap.
function fetchText(url, redirectsLeft = 5){
  return new Promise((resolve, reject) => {
    let u; try{ u = new URL(url); }catch{ return reject(new Error("bad url")); }
    const req = https.request(u, { method: "GET", timeout: 60000, headers: { "User-Agent": "RampedUp-CC-Engine/0.1" } }, (res) => {
      const sc = res.statusCode || 0;
      if(sc >= 300 && sc < 400 && res.headers.location && redirectsLeft > 0){
        res.resume();
        let next; try{ next = new URL(res.headers.location, u).toString(); }catch{ return reject(new Error("bad redirect")); }
        return resolve(fetchText(next, redirectsLeft - 1));
      }
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve({ status: sc, body: Buffer.concat(chunks).toString("utf8") }));
      res.on("error", reject);
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
    req.end();
  });
}

function parseRef(s){
  const str = String(s || "").trim();
  const idm = str.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/) || str.match(/^([a-zA-Z0-9_-]{30,})$/);
  const gidm = str.match(/[?#&]gid=(\d+)/);
  return { spreadsheetId: idm ? idm[1] : "", gid: gidm ? Number(gidm[1]) : 0 };
}

// Read the sheet as rows[][] — service account if configured, else the public CSV export.
async function readSheetRows(ref){
  if(gsheets && gsheets.loadServiceAccount && gsheets.loadServiceAccount()){
    const { rows } = await gsheets.readSheet(ref.spreadsheetId, { gid: ref.gid });
    return rows;
  }
  const url = `https://docs.google.com/spreadsheets/d/${ref.spreadsheetId}/export?format=csv&gid=${ref.gid}`;
  const { status, body } = await fetchText(url);
  if(status !== 200 || /^\s*<!DOCTYPE html/i.test(body)){
    throw new Error(`sheet not readable (HTTP ${status}). Make it 'Anyone with the link → Viewer', or set GOOGLE_SERVICE_ACCOUNT_JSON + share with the service account.`);
  }
  return parseCsv(body);
}

// header label (normalized) -> our accessor key
const norm = (h) => String(h || "").trim().toLowerCase().replace(/\s+/g, " ");
function headerIndex(headerRow){
  const idx = {};
  headerRow.forEach((h, i) => { const k = norm(h); if(k && !(k in idx)) idx[k] = i; });
  return idx;
}

const normGender = (g) => { const c = String(g || "").trim().toUpperCase()[0]; return c === "M" ? "M" : c === "F" ? "F" : ""; };
const yn = (v) => /^(y|yes|true|1)$/i.test(String(v || "").trim()) ? "Y" : "";
function normLinkedIn(v){
  const s = String(v || "").trim(); if(!s) return "";
  if(/^https?:\/\//i.test(s)) return s;
  if(/linkedin\.com/i.test(s)) return "https://" + s.replace(/^\/+/, "");
  return s;
}

// Build a Master DB record from one sheet row, mirroring extractRecord's derived fields.
function rowToRecord(get, genderMap, today){
  const url = String(get("web source url") || get("url") || "").trim();
  if(!/^https?:\/\//i.test(url)) return null;                  // need a real bio URL
  const clean = url.split("?")[0].split("#")[0];
  const email = cleanEmail(get("email address") || get("email"));
  const domain = getBaseDomain(clean) || String(get("root domain") || "").trim().toLowerCase();
  const cc = countryCodeFromDomain(domain);
  const pathHit = pathIdFromUrl(clean);
  const directory = pathHit ? "Team" : classifyDirectory(clean, "", {}, genderMap);
  const ts = String(get("scan timestamp") || "").slice(0, 10);
  return {
    "Time Stamp": /^\d{4}-\d{2}-\d{2}$/.test(ts) ? ts : today,
    "Source": "Sheet Import",
    "Web Source URL": clean,
    "Directory": directory,
    "Path ID": pathHit ? pathHit.id : "",
    "Domain": domain,
    "Last Path": String(get("last path") || "").trim(),
    "Bio Check": yn(get("bio check")) || (directory === "BIO URL" ? "Y" : ""),
    "First": String(get("first") || "").trim(),
    "Last": String(get("last") || "").trim(),
    "Gender": normGender(get("gender")),
    "Title": String(get("title") || "").trim(),
    "Position": String(get("position") || "").trim(),
    "Description": String(get("description") || "").trim().slice(0, 600),
    "Image URL": String(get("image url") || "").trim(),
    "Email Address": email,
    "Email Type": classifyEmail(email),
    "LinkedIn URL": normLinkedIn(get("linkedin url") || get("linkedin")),
    "Google Maps": String(get("google maps") || "").trim(),
    "Phone": toE164(get("phone"), cc),
    "Phone Type": "",
    "Phone Location": "",
    "Phone 2": toE164(get("phone 2") || get("phone2"), cc),
    "Phone 2 Type": "",
    "Phone 2 Location": "",
  };
}

// Read + map + dedupe (by bio URL, newest scan wins). Returns { records, stats }.
async function buildRecords(ref, { genderMap = {} } = {}){
  const rows = await readSheetRows(ref);
  if(!rows.length) return { records: [], stats: { read: 0, mapped: 0, unique: 0, skipped: 0 } };
  const idx = headerIndex(rows[0]);
  const today = new Date().toISOString().slice(0, 10);
  const byUrl = new Map();                                     // bio URL -> { rec, ts }
  let mapped = 0, skipped = 0;
  for(let r = 1; r < rows.length; r++){
    const row = rows[r];
    if(!row || !row.length) continue;
    const get = (label) => { const i = idx[label]; return i == null ? "" : (row[i] || ""); };
    const rec = rowToRecord(get, genderMap, today);
    if(!rec){ skipped++; continue; }
    mapped++;
    const key = rec["Web Source URL"].toLowerCase();
    const prev = byUrl.get(key);
    if(!prev || rec["Time Stamp"] > prev["Time Stamp"]) byUrl.set(key, rec);   // newest scan wins
  }
  const records = [...byUrl.values()];
  return { records, stats: { read: rows.length - 1, mapped, unique: records.length, skipped } };
}

// Full import into the Master DB. db = makeDb(...) instance. Returns a result summary.
async function importSheet(db, sheetRefOrUrl, { genderMap = {} } = {}){
  const ref = parseRef(sheetRefOrUrl);
  if(!ref.spreadsheetId) throw new Error("could not parse a spreadsheet id from: " + sheetRefOrUrl);
  const { records, stats } = await buildRecords(ref, { genderMap });
  const withEmail = records.filter((r) => r["Email Address"]).length;
  const merged = db.upsertMany(records);                       // DB is email-keyed; email-less rows are skipped
  return {
    ...stats, withEmail, withoutEmail: records.length - withEmail,
    imported: merged.processed, added: merged.added, dbTotal: merged.total,
    spreadsheetId: ref.spreadsheetId, gid: ref.gid,
  };
}

module.exports = { importSheet, buildRecords, readSheetRows, parseCsv, parseRef, rowToRecord };

// ---- CLI: dry-run a sheet (read + map + dedupe, print stats + samples; no DB write) ----
if(require.main === module){
  (async () => {
    const ref = parseRef(process.argv[2] || "");
    if(!ref.spreadsheetId){ console.error("usage: node sheet-import.js <sheetUrlOrId>"); process.exit(1); }
    let genderMap = {};
    try { genderMap = require("./extractor").loadGenderMap("names-genders.csv"); } catch {}
    const t0 = Date.now();
    const { records, stats } = await buildRecords(ref, { genderMap });
    console.log(`Read ${stats.read} data rows in ${Date.now() - t0}ms`);
    console.log(`  mapped: ${stats.mapped}   unique bio URLs: ${stats.unique}   skipped (no URL): ${stats.skipped}`);
    console.log(`  with email: ${records.filter((r) => r["Email Address"]).length}   without: ${records.filter((r) => !r["Email Address"]).length}`);
    console.log("\nSample records:");
    for(const r of records.slice(0, 4)){
      console.log("  " + ["First", "Last", "Gender", "Directory", "Title", "Email Address", "Email Type", "Phone", "LinkedIn URL", "Web Source URL"]
        .map((k) => `${k}=${r[k] || "-"}`).join("  |  "));
    }
  })().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
}
