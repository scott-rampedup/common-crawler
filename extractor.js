/**
 * extractor.js  —  Phase 0 core
 * -------------------------------------------------------------
 * Turns one page (HTML + its URL) into one contact record in the RampedUp
 * schema, or returns null if the page fails the quality gate.
 *
 * The SAME function is used for both data sources:
 *   • Common Crawl  -> pass the archived HTML
 *   • Live Inspector -> pass the freshly fetched HTML
 *
 * Design notes
 *  - Dependency-free so it runs with plain `node` (no install needed).
 *    HTML is parsed with focused regexes, mirroring the original extension's
 *    approach. For production you may swap in cheerio for robustness — only the
 *    small parse helpers change; the extraction LOGIC below stays identical.
 *  - Phone line type comes from ./wireless-block-classifier (IMS block table).
 *  - Phone LOCATION is a pluggable hook (`geocode`) — wire it to libphonenumber's
 *    offline geocoder in production. A minimal area-code fallback is included.
 *  - Gender uses a pluggable map (`genderMap`) — load the full name->gender CSV
 *    that already ships in the extension. A tiny sample is built in for testing.
 *
 * Quality gate (your rule): keep a record only if it has an email OR a
 * LinkedIn URL OR it is a BIO URL (an individual's own profile page, which is
 * itself mineable). Everything else is dropped.
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { classifyLineType, loadPhoneBlocks, blockLocation, PHONE_BLOCKS_CSV } = require("./wireless-block-classifier");
const { intlMobileType } = require("./intl-mobile");
const { modelEmail, templateFor } = require("./email-pattern");

// ---------------------------------------------------------------- config
const FREE_DOMAINS = new Set(["gmail.com","yahoo.com","hotmail.com","outlook.com",
  "icloud.com","aol.com","proton.me","protonmail.com","gmx.com","mail.com","live.com","msn.com"]);
const ROLE_LOCALS = new Set(["info","sales","support","admin","contact","hello","hr","billing",
  "careers","office","team","marketing","help","service","press","media","jobs","accounts",
  "webmaster","postmaster","abuse","noreply","no-reply","enquiries","inquiries"]);
// path segments that signal an individual profile (when followed by a person-ish slug)
// Authoritative person/bio directory path segments (provided by RampedUp). Stored
// NORMALIZED (lowercased, separators -> spaces) to match classifyDirectory's normalized
// URL segments. An individual page under any of these classifies as "BIO URL".
const BIO_DIR_RAW = [
  "accountant","accountants","admissions-staff","advisor","advisors","find-an-advisor","our-advisors",
  "agent","agent-detail","agent-details","agente","agents","find-an-agent","agentprofile","associates",
  "lawyers","abogados","attorney","attorney-profile","attorney-profiles","attorneys","attorneys-staff",
  "barrister","barristers","council-staff","find-a-lawyer","lawyer","our-attorneys","our-lawyers","bankers",
  "board","board_of_directors","board-committees","board-directors","board-members","board-of-director",
  "board-of-directors","board-of-management","bod","boe","broker","brokers","our-brokers","clinicians",
  "find-a-clinician","commissioners","consultant","consultants","dentist","doctor","doctor-profile","doctors",
  "findadoctor","find-a-doctor","find-doctor","find-doctors-physicians","doctors-providers","findadoc",
  "financial-advisor","wealth-management","wealth-management-team","loan-advisors","loan-officer","loan-officers",
  "LoanOriginator","our-partners","partner","partners","physician","physician-directory","physician-finder",
  "physicians","principals","find-a-provider","our-providers","provider","provider-directory","providers",
  "providers.","provider-search","realestateagent","real-estate-agent","real-estate-agents","realtor","realtors",
  "insurance-agent","insurance-agents",
  "recruiters","find-a-rep","find-a-representative","find-a-sales-rep","find-a-sales-representative","find-rep",
  "find-representative","find-sales-rep","rep-locator","reps","sales-rep-locator","sales-representative",
  "sales-representatives","sales-team","find-a-researcher","researcher","research-staff","specialists",
  "meet-the-teachers","res-teachers-and-staff","teachers","Teachers_and_Staff","Teachers_Staff","travel-agents",
  "our-vets","aboutleadership","about-leadership","administration","administration-staff-directory",
  "board-and-management","board-leadership","board-management","companyleadership","company-leadership",
  "corporate-leadership","corporate-officers","directors","directors-and-management","directors-management",
  "directors-officers","executive_team","Executive-Bios","executive-board","executive-committee",
  "executive-committee-bios","executive-leadership","executive-leadership-team","executive-management",
  "executive-management-team","executive-officers","executive-profiles","executives","executive-staff",
  "executive-team","firm-leadership","judges","key-management","leaders","leaders-bios","leadership",
  "leadership-and-governance","leadership-and-staff","leadership-bios","leadership-board","leadership-directory",
  "leadership-governance","leadership-group","leadership-staff","leadership-team","management","management_team",
  "management-and-directors","management-board","management-directors","management-profile","management-team",
  "meet-our-leadership","officers","officers-and-directors","officers-directors","our-board","our-leaders",
  "our-leadership","our-leadership-team","our-management","our-management-team","our-officers","school-leadership",
  "search-staff-directory-leadership","senior-leadership","senior-leadership-team","senior-management",
  "senior-management-team","senior-staff","staff-board","staff-leadership","about-our-team","about-team",
  "all-staff","anwaelte","author","authors","bio","biographies","biography","bios","business-team",
  "campus-directory","chamber-staff","city-staff-directory","contact_us","contacts","corporate-team","cpa",
  "cpt_team","department-directory","diocesan-directory","diocesan-staff","directories","directory",
  "directory.aspx","directory-staff","district-staff","district-staff-directory","dt_team","employee",
  "employee-directory","employees","equipe","expert","Expertos","experts","facstaff","fac-staff","faculty",
  "faculty_and_staff","faculty_staff","facultyandstaff","faculty-and-staff","faculty-and-staff-directory",
  "faculty-directory","facultystaff","faculty-staff","facultystaff-directory","faculty-staff-directory",
  "faculty-staff-directory.aspx","financial-aid-staff","industry-expertise","key-people","key-staff",
  "local-media-contact-list","media-contacts","meet-our-experts","meet-our-people","meet-our-staff",
  "meet-our-team","meet-team","meet-the-staff","MeetTheTeam","meet-the-team","meet-us","member","members",
  "Mitarbeitende","more-people","mortgage-team","news-events","nos-experts","office-staff","our_staff",
  "our_team","our+people","our-experts","ourpeople","our-people","our-professionals","our-staff","ourteam",
  "our-team","people","people_and_places","people-and-offices","people-directory","people-profiles",
  "people-search","person","personen","personnel","personnes","persons","phone-directory","press-contacts",
  "professional","professional-directory","professionals","profiel","profile","profiles","profissional",
  "schooldirectory","school-directory","school-staff","staff","staff.aspx","staff.cfm","staff_directory",
  "staff_directory.php","staff-2","staff-by-alphabet","staff-by-state","staff-category","staff-contacts",
  "staff-council","staffdir","staffdirectory","staff-directory","StaffDirectory.aspx","staff-directory.html",
  "staff-directory-10","staff-directory-2","staff-directory-list","staff-information","staff-item","staff-list",
  "staff-listing","staff-listings","staff-member","staff-members","staff-profile","staff-profiles",
  "state-directory","support-staff","team","team_member","team_members","team1","team-1","team-2","teambio",
  "team-details","team-directory","teaminfo","teammates","teammember","team-member","teammembers","team-members",
  "teams","the-staff","the-team","university-directory","unser-team","ymca-staff","your-team",
];
const BIO_DIRS = new Set(BIO_DIR_RAW.map((s) => normalizeForMatching(s)).filter(Boolean));

// ---------------------------------------------------------------- text utils
const fromCP = (n, fallback) => { try{ return (n > 0 && n <= 0x10FFFF) ? String.fromCodePoint(n) : fallback; }catch{ return fallback; } };
const decode = s => String(s||"")
  .replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">")
  .replace(/&quot;/g,'"').replace(/&#0?39;|&apos;/g,"'").replace(/&nbsp;/g," ")
  .replace(/&#x([0-9a-fA-F]+);/g, (m,h) => fromCP(parseInt(h,16), m))   // hex char refs, e.g. &#x40; -> @
  .replace(/&#(\d+);/g, (m,d) => fromCP(parseInt(d,10), m))            // decimal char refs, e.g. &#064; -> @
  .trim();
const stripTags = s => decode(String(s||"").replace(/<[^>]*>/g," ").replace(/\s+/g," "));
const properCase = s => String(s||"").replace(/\w\S*/g, w => w[0].toUpperCase()+w.slice(1).toLowerCase());

// Honorifics / credentials / suffixes to strip from a display path.
const LASTPATH_REMOVE = new Set(["dr","mr","mrs","ms","phd","md","esq","cpa","facs","prof","sr","jr","iii","mba","biography"]);
// Tidy a URL slug into a readable name for the "Last Path" field:
// drop file extensions, turn -/_/+/. into spaces, drop honorifics/credentials and
// one-character tokens (stray initials), then Proper Case the result.
function cleanLastPath(seg){
  const s = String(seg || "")
    .replace(/\.(aspx?|html?|pdf|php)$/i, "")     // strip .aspx/.asp/.html/.htm/.pdf/.php
    .replace(/[-_+.]+/g, " ");                     // separators -> space
  const tokens = s.split(/\s+/)
    .filter(Boolean)
    .filter((t) => t.length > 1)                   // drop one-symbol tokens (e.g. middle initials)
    .filter((t) => !LASTPATH_REMOVE.has(t.toLowerCase()));
  return properCase(tokens.join(" "));
}

// ---- email obfuscation + cleanup ----
function deobfuscateEmail(s){
  return String(s || "")
    .replace(/\s*[\[(]\s*at\s*[\])]\s*/gi, "@")     // [at] / (at)  ->  @
    .replace(/\s*\(\s*@\s*\)\s*/g, "@")             // (@)          ->  @
    .replace(/\s*[\[(]\s*dot\s*[\])]\s*/gi, ".");   // [dot] / (dot) ->  .
}

// Cloudflare "Email Protection" hides the real address behind a hex blob (first octet = XOR key):
//   <a data-cfemail="HEX">[email&#160;protected]</a>  and  /cdn-cgi/l/email-protection#HEX
// Without decoding we'd see the "[email protected]" placeholder and MODEL a guessed address instead
// of the person's real one — so this is a premium recovery for email capture.
function decodeCfEmail(hex){
  hex = String(hex || "").trim();
  if(!/^[0-9a-f]{6,}$/i.test(hex) || hex.length % 2) return "";
  try{
    const k = parseInt(hex.substr(0, 2), 16); let out = "";
    for(let i = 2; i < hex.length; i += 2) out += String.fromCharCode(parseInt(hex.substr(i, 2), 16) ^ k);
    return /@/.test(out) ? out : "";
  }catch{ return ""; }
}
function cfEmailsFromHtml(html){
  const out = []; const s = String(html || ""); let m;
  const re1 = /data-cfemail=["']([0-9a-fA-F]+)["']/g;
  while((m = re1.exec(s))){ const e = decodeCfEmail(m[1]); if(e) out.push(e); }
  const re2 = /\/cdn-cgi\/l\/email-protection#([0-9a-fA-F]+)/g;
  while((m = re2.exec(s))){ const e = decodeCfEmail(m[1]); if(e) out.push(e); }
  return out;
}

let EMAIL_BLOCKLIST = new Set();
function setEmailBlocklist(emails){
  EMAIL_BLOCKLIST = new Set([...(emails || [])].map((e) => String(e).trim().toLowerCase()).filter(Boolean));
}
function loadEmailBlocklist(filePath){
  const set = new Set();
  try{
    for(const line of fs.readFileSync(filePath, "utf8").split(/\r?\n/)){
      const e = line.trim().toLowerCase();
      if(e && !e.startsWith("#")) set.add(e);
    }
  }catch{ /* no list yet */ }
  setEmailBlocklist(set);
  return EMAIL_BLOCKLIST;
}

const IMG_EXT_RE = /\.(png|jpe?g|gif|svg|webp|bmp|ico)$/i;
// Normalize/repair a scraped email; returns "" if it should be dropped.
function cleanEmail(raw){
  if(!raw) return "";
  let e = deobfuscateEmail(raw).replace(/%20/gi, "").replace(/\s+/g, "");   // de-obfuscate + drop %20 (encoded space) and stray spaces
  e = e.replace(/^mailto:/i, "").split("?")[0];
  e = e.replace(/[.,;:>)\]]+$/, "");                    // trailing sentence punctuation (e.g. "...com.")
  if(!e.includes("@")) return "";
  let [local, ...rest] = e.split("@");
  const domain = rest.join("@").toLowerCase();
  if(!local || !domain || !domain.includes(".")) return "";
  if(IMG_EXT_RE.test(domain)) return "";               // e.g. logo@2x.png
  if(domain === "example.com") return "";              // placeholder domain

  // leading digits = a phone number misread into the address: drop everything before the 1st letter
  if(/^[0-9]/.test(local)) local = local.replace(/^[^A-Za-z]+/, "");

  // domain echoed at the start of the local part: hilton.commainstreet@hilton.com -> mainstreet@hilton.com
  while(local.toLowerCase().startsWith(domain)){
    local = local.slice(domain.length).replace(/^[._\-]+/, "");
  }
  if(!local) return "";

  const out = local + "@" + domain;
  if(EMAIL_BLOCKLIST.has(out.toLowerCase())) return "";
  if(!/^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/.test(out)) return "";
  return out;
}

function extractEmailsFromText(html){
  const text = deobfuscateEmail(decode(String(html||"")
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')));   // decode entities + de-obfuscate so [at]/(at) emails are matchable
  const matches = Array.from(new Set((text.match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g) || [])));
  return matches;
}

function getBaseDomain(url){ try{ return new URL(url).hostname.replace(/^www\./,""); }catch{ return ""; } }
function toAbs(src, base){ try{ return new URL(src, base).href; }catch{ return src; } }

function lastPathSeg(url){
  try{
    const p = new URL(url).pathname.replace(/\/+$/,"");           // strip trailing slash
    const seg = p.split("/").filter(Boolean).pop() || "";
    return decodeURIComponent(seg).replace(/\.(html?|php|aspx?)$/i,"");
  }catch{ return ""; }
}
// The "Last Path" field value for a URL — the cleaned last path segment, EXACTLY as
// extractRecord computes it. Exported so the DB normalizer can backfill a blank Last Path.
function lastPathFromUrl(url){ return cleanLastPath(nameSlugFromUrl(url) || lastPathSeg(url)); }
// A bare record-id path segment: a number ("71955"), a UUID, or a long hex blob — the kind of
// thing that trails a name on profile URLs (e.g. /First-Last/<uuid>/<uuid> on evrealestate.com).
function isIdSegment(seg){
  const s = String(seg || "").replace(/\.(html?|php|aspx?)$/i,"");
  if(/^\d{2,}$/.test(s)) return true;                                                    // numeric id
  if(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) return true;  // uuid
  if(/^[0-9a-f]{16,}$/i.test(s)) return true;                                            // long hex id
  // Opaque alphanumeric id, optionally tagged — e.g. century21's
  // "aid-P00200000000033dyujCKdEsXQx08N4pMQLeFT62" (trails the name on /agent/detail/.../First-Last/aid-…).
  // After an optional short letter tag + hyphen, the rest is one >=16-char run of letters+digits that
  // MIXES IN at least one digit — which a real person-name slug never does, so this won't eat names.
  const blob = s.replace(/^[a-z]{1,6}-/i, "");
  if(/^[0-9a-z]{16,}$/i.test(blob) && /\d/.test(blob)) return true;
  return false;
}
// The path segment that holds a person's name: usually the last segment, but for profile URLs
// that END in one or more record ids (".../First-Last/71955" or ".../First-Last/<uuid>/<uuid>")
// it's the last NON-id segment.
function nameSlugFromUrl(url){
  try{
    const segs = new URL(url).pathname.replace(/\/+$/,"").split("/").filter(Boolean)
      .map((s) => decodeURIComponent(s).replace(/\.(html?|php|aspx?)$/i,""));
    if(!segs.length) return "";
    let i = segs.length - 1;
    while(i >= 1 && isIdSegment(segs[i])) i -= 1;                 // skip trailing id(s); the name is before them
    return segs[i] || "";
  }catch{ return ""; }
}
function pathSegments(url){ try{ return new URL(url).pathname.toLowerCase().split("/").filter(Boolean); }catch{ return []; } }
function normalizeForMatching(text){
  return String(text||"")
    .toLowerCase()
    .replace(/[-_\.\/]+/g," ")
    .replace(/[^a-z0-9 ]+/g,"")
    .replace(/\s+/g," ")
    .trim();
}
function isKnownDir(seg, known){
  if(!seg) return false;
  if(known.has(seg)) return true;
  for(const item of known){
    if(seg === item) return true;
    if(seg.startsWith(item + " ") || seg.endsWith(" " + item) || seg.includes(" " + item + " ")) return true;
  }
  return false;
}

function parseCsvRow(row){
  const cols = [];
  let cur = "";
  let inQuotes = false;
  for(let i = 0; i < row.length; i++){
    const ch = row[i];
    if(inQuotes){
      if(ch === '"'){
        if(row[i+1] === '"'){
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if(ch === '"'){
        inQuotes = true;
      } else if(ch === ','){
        cols.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
  }
  cols.push(cur);
  return cols;
}
// first/last from a URL slug, e.g. "dr-marcus-patel-bio.html" -> {first:"Marcus",last:"Patel"}.
// Cleaning (extension strip, separator split, honorific/credential/page-word removal) is shared
// with AI Search and lives in name-from-path.js.
const { nameFromPath, pathNameTokens } = require("./name-from-path");
function nameFromSlug(slug){ return nameFromPath(slug); }

function inferNameFromSlug(slug, genderMap){
  // The slug is already gated as a person elsewhere (looksLikePersonSlug /
  // classifyDirectory). genderMap is used ONLY to fill the Gender field later — we do
  // NOT drop a real name just because it isn't in the census list (e.g. "Sibel").
  return nameFromSlug(slug);
}

function colLettersToIndex(col){
  let idx = 0;
  for(const ch of String(col||"")){
    const code = ch.charCodeAt(0) - 64;
    if(code >= 1 && code <= 26) idx = idx*26 + code;
  }
  return idx - 1;
}
function readXlsxEntry(buffer, entryName){
  const needle = Buffer.from(entryName, "utf8");
  let position = 0;
  while(true){
    const idx = buffer.indexOf(needle, position);
    if(idx < 0) return null;
    const headerStart = idx - 30;
    if(headerStart < 0){ position = idx + 1; continue; }
    if(buffer.readUInt32LE(headerStart) !== 0x04034b50){ position = idx + 1; continue; }
    const fnameLen = buffer.readUInt16LE(headerStart + 26);
    const extraLen = buffer.readUInt16LE(headerStart + 28);
    const compMethod = buffer.readUInt16LE(headerStart + 8);
    const compSize = buffer.readUInt32LE(headerStart + 18);
    const dataStart = headerStart + 30 + fnameLen + extraLen;
    const data = buffer.slice(dataStart, dataStart + compSize);
    if(compMethod === 0) return data.toString("utf8");
    if(compMethod === 8) return zlib.inflateRawSync(data).toString("utf8");
    throw new Error(`Unsupported ZIP compression method ${compMethod}`);
  }
}
function parseSharedStrings(xml){
  if(!xml) return [];
  const out = [];
  const re = /<si>([\s\S]*?)<\/si>/gi;
  let m;
  while((m = re.exec(xml))){
    const inner = m[1];
    const pieces = [...inner.matchAll(/<t[^>]*>([\s\S]*?)<\/t>/gi)].map(p => decode(p[1]));
    out.push(pieces.join(""));
  }
  return out;
}
function parseSheetXml(xml, sharedStrings){
  const rows = [];
  const cellRe = /<c[^>]*r="([A-Z]+\d+)"[^>]*>([\s\S]*?)<\/c>/gi;
  let m;
  while((m = cellRe.exec(xml))){
    const ref = m[1];
    const body = m[2];
    const [,col,row] = ref.match(/^([A-Z]+)(\d+)$/i) || [null, null, "1"];
    const rowIndex = parseInt(row, 10) - 1;
    const colIndex = colLettersToIndex(col);
    rows[rowIndex] = rows[rowIndex] || [];
    const typeAttr = m[0].match(/ t="([^"]+)"/) || [];
    const type = typeAttr[1] || "";
    const valueMatch = body.match(/<v>([\s\S]*?)<\/v>/i);
    let value = valueMatch ? decode(valueMatch[1]) : "";
    if(type === "s" && value){
      const idx = parseInt(value, 10);
      value = !Number.isNaN(idx) && sharedStrings[idx] ? sharedStrings[idx] : value;
    }
    rows[rowIndex][colIndex] = value;
  }
  return rows.map(r => r ? r.map(c => c || "") : []);
}
function xlsxToCsv(filePath){
  const buffer = fs.readFileSync(filePath);
  const sharedStringsXml = readXlsxEntry(buffer, "xl/sharedStrings.xml");
  const sharedStrings = parseSharedStrings(sharedStringsXml);
  const sheetXml = readXlsxEntry(buffer, "xl/worksheets/sheet1.xml");
  if(!sheetXml) throw new Error(`Unable to locate worksheet XML in ${filePath}`);
  const rows = parseSheetXml(sheetXml, sharedStrings);
  const escapeCsv = v => {
    const s = String(v || "").replace(/"/g, '""');
    return /[",\n\r]/.test(s) ? `"${s}"` : s;
  };
  return rows.map(row => row.map(escapeCsv).join(",")).join("\n");
}
function loadRows(pathOrBuffer){
  const lines = String(pathOrBuffer).toLowerCase().endsWith(".xlsx")
    ? xlsxToCsv(pathOrBuffer).split(/\r?\n/)
    : fs.readFileSync(pathOrBuffer, "utf8").split(/\r?\n/);
  return lines.map(line => line.replace(/\uFEFF/g, ""));
}

const PERSON_SLUG_BLACKLIST = new Set([
  "contact","about","home","index","team","careers","jobs","privacy","terms",
  "our","process","content","cleaning","service","services","blog","news","local",
  "profile","profiles","support","help","staff","resources","portal","directory"
]);

function looksLikePersonSlug(slug, genderMap = {}){
  const name = nameFromSlug(slug);
  if(!name.first || !name.last) return false;
  const first = name.first.toLowerCase();
  const last = name.last.toLowerCase();
  if(PERSON_SLUG_BLACKLIST.has(first) || PERSON_SLUG_BLACKLIST.has(last)) return false;
  if(!/^[a-z]+$/.test(first) || !/^[a-z]+$/.test(last)) return false;
  if(genderMap && genderMap[first]) return true;
  if(first.length <= 2 || last.length <= 2) return false;
  return true;
}

// ---------------------------------------------------------------- classification
function classifyDirectory(url, html = "", rules = {}, genderMap = {}){
  const segs = pathSegments(url);
  let nIdx = segs.length - 1;                                   // index of the name-bearing segment…
  while(nIdx >= 1 && isIdSegment(segs[nIdx])) nIdx -= 1;       // …skip trailing record id(s) (/First-Last/71955 or /Name/<uuid>/<uuid>)
  const nameRaw = nameSlugFromUrl(url);                         // raw name seg (decoded, ext-stripped)
  const last = normalizeForMatching(nameRaw);
  const parent = nIdx >= 1 ? normalizeForMatching(segs[nIdx-1]) : "";
  const pageText = normalizeForMatching(stripTags(html));
  const extraDirs = rules.names || new Set();
  const extraTerms = rules.terms || new Set();
  const termMap = rules.termMap || new Map();
  const personSlug = looksLikePersonSlug(last, genderMap);
  const urlPath = normalizeForMatching(new URL(url).pathname);
  // A directory term can live in the SUBDOMAIN, not the path (e.g. agents.farmers.com,
  // agents.bankerslife.com, team.example.com). Treat the host's subdomain labels as dir context.
  const hostLabels = (() => { try { return new URL(url).hostname.toLowerCase().replace(/^www\./, "").split(".").slice(0, -2); } catch { return []; } })();
  const hostIsBioDir = hostLabels.some((l) => { const n = normalizeForMatching(l); return isKnownDir(n, BIO_DIRS) || isKnownDir(n, extraDirs); });

  for(const [term, name] of termMap.entries()){
    if(!term) continue;
    if(urlPath.includes(term)) return name;
  }

  if(/^contact(-us)?$/.test(last) || last === "contact") return "Contact Us";
  if(isKnownDir(last, extraDirs) || isKnownDir(parent, extraDirs) || isKnownDir(last, BIO_DIRS) || isKnownDir(parent, BIO_DIRS)){
    if(personSlug || [...extraTerms].some(term => pageText.includes(term))) return "BIO URL";
    if(isKnownDir(last, extraDirs) || isKnownDir(parent, extraDirs)){
      const matchedName = [...termMap.entries()].find(([term,name]) => term === last || term === parent);
      return matchedName ? matchedName[1] : "People";
    }
    return "People";
  }
  if(personSlug && (isKnownDir(parent, BIO_DIRS) || isKnownDir(parent, extraDirs) || /^[a-z]+[-_.][a-z]+$/i.test(last))) return "BIO URL";
  // Profile pages like /Agent/Detail/First-Last/<id>: the directory term ("Agent") sits
  // earlier in the path (not adjacent to the name), and the name has its own segment.
  // Recognise via a whole-path directory scan paired with a clean first-last name segment.
  if(personSlug && /^[a-z]+[-_.][a-z]+$/i.test(nameRaw) && pathIdFromUrl(url)) return "BIO URL";
  // Directory term in the SUBDOMAIN + a person-name leaf (agents.farmers.com/ca/calabasas/alex-sayeri):
  // the host says "agents" and the last segment is clearly a person, so it's an agent profile page.
  if(personSlug && hostIsBioDir) return "BIO URL";
  if(isKnownDir(last, BIO_DIRS)) return "People";
  if([...extraTerms].some(term => pageText.includes(term))) return "BIO URL";
  return "Company";
}

// When a page has no usable <title>, infer a role title from a keyword in the URL path.
const KEYWORD_TITLES = {
  "accountant":"Accountant","accountants":"Accountant","admissions-staff":"Admissions",
  "advisor":"Advisor","advisors":"Advisor","find-an-advisor":"Advisor","our-advisors":"Advisor",
  "agent":"Agent","agent-detail":"Agent","agent-details":"Agent","agente":"Agent","agents":"Agent",
  "find-an-agent":"Agent","agentprofile":"Agent","associates":"Associate",
  "lawyers":"Attorney","abogados":"Attorney","attorney":"Attorney","attorney-profile":"Attorney",
  "attorney-profiles":"Attorney","attorneys":"Attorney","attorneys-staff":"Attorney","barrister":"Attorney",
  "barristers":"Attorney","council-staff":"Attorney","find-a-lawyer":"Attorney","lawyer":"Attorney",
  "our-attorneys":"Attorney","our-lawyers":"Attorney","bankers":"Banker",
  "board":"Board Member","board_of_directors":"Board Member","board-committees":"Board Member",
  "board-directors":"Board Member","board-members":"Board Member","board-of-director":"Board Member",
  "board-of-directors":"Board Member","board-of-management":"Board Member","bod":"Board Member","boe":"Board Member",
  "broker":"Broker","brokers":"Broker","our-brokers":"Broker","clinicians":"Clinician","find-a-clinician":"Clinician",
  "commissioners":"Commisioner","consultant":"Consultant","consultants":"Consultant",
  "dentist-office":"Dentist","dentist":"Dentist","doctor":"Doctor","doctor-profile":"Doctor","doctors":"Doctor",
  "findadoctor":"Doctor","find-a-doctor":"Doctor","find-doctor":"Doctor","find-doctors-physicians":"Doctor",
  "doctors-providers":"Doctor","findadoc":"Doctor","financial-advisor":"Financial Advisor",
  "wealth-management":"Financial Advisor","wealth-management-team":"Financial Advisor",
  "loan-advisors":"Loan Officer","loan-officer":"Loan Officer","loan-officers":"Loan Officer","loanoriginator":"Loan Officer",
  "our-partners":"Partner","partner":"Partner","partners":"Partner","physician":"Physician",
  "physician-directory":"Physician","physician-finder":"Physician","physicians":"Physician","principals":"Principal",
  "find-a-provider":"Provider","our-providers":"Provider","provider":"Provider","provider-directory":"Provider",
  "providers":"Provider","providers.":"Provider","provider-search":"Provider","realestateagent":"Realtor",
  "real-estate-agent":"Realtor","real-estate-agents":"Realtor","realtors":"Realtor","recruiters":"Recruiter",
  "find-a-rep":"Representative","find-a-representative":"Representative","find-a-sales-rep":"Representative",
  "find-a-sales-representative":"Representative","find-rep":"Representative","find-representative":"Representative",
  "find-sales-rep":"Representative","rep-locator":"Representative","reps":"Representative",
  "sales-rep-locator":"Representative","sales-representative":"Representative","sales-representatives":"Representative",
  "sales-team":"Representative","find-a-researcher":"Researcher","researcher":"Researcher","research-staff":"Researcher",
  "specialists":"Specialist","meet-the-teachers":"Teacher","res-teachers-and-staff":"Teacher","teachers":"Teacher",
  "teachers_and_staff":"Teacher","teachers_staff":"Teacher","travel-agents":"Travel Agent","our-vets":"Veterinarian",
};
function titleFromUrlKeywords(url){
  let segs;
  try { segs = new URL(url).pathname.toLowerCase().split("/").filter(Boolean); }
  catch { return ""; }
  for(const seg of segs){ if(KEYWORD_TITLES[seg]) return KEYWORD_TITLES[seg]; }
  return "";
}

// Every known directory path term -> its role title ("" when none). Built from the
// directory list + the role map; all of these map to Directory Type "Team".
const DIR_TERMS = new Map();
for(const t of BIO_DIR_RAW) DIR_TERMS.set(t.toLowerCase(), KEYWORD_TITLES[t.toLowerCase()] || "");
for(const k of Object.keys(KEYWORD_TITLES)) if(!DIR_TERMS.has(k)) DIR_TERMS.set(k, KEYWORD_TITLES[k]);

// Find the URL path segment that identifies a directory (e.g. /attorneys/jane -> attorneys).
// Returns { id: "Attorneys", role: "Attorney" } or null. id is the matched term, prettified.
function pathIdFromUrl(url){
  let segs;
  try { segs = new URL(url).pathname.split("/").filter(Boolean); }
  catch { return null; }
  for(const seg of segs){
    const key = seg.toLowerCase();
    if(DIR_TERMS.has(key)){
      const id = properCase(seg.replace(/[-_.]+/g, " ").replace(/\b(aspx?|html?|php|cfm)\b/gi, "").trim());
      return { id, role: DIR_TERMS.get(key) };
    }
  }
  return null;
}

function classifyEmail(email){
  if(!email) return "";
  const [local, domain] = email.toLowerCase().split("@"); if(!domain) return "";
  const base = local.split(/[._-]/)[0];
  if(ROLE_LOCALS.has(local) || ROLE_LOCALS.has(base)) return "Role-Based";
  if(FREE_DOMAINS.has(domain)) return "Personal";
  return "Professional";
}

// ---------------------------------------------------------------- field extractors
function anchors(html){
  const out = [];
  const re = /<a\b[^>]*?href\s*=\s*["']([^"']+)["'][^>]*>/gi; let m;
  while((m = re.exec(html))) out.push(m[1]);
  return out;
}
function metaContent(html, key){
  // matches <meta property="og:description" content="..."> in either attr order
  const patterns = [
    new RegExp(`<meta[^>]+(?:property|name)\\s*=\\s*["']${key}["'][^>]*content\\s*=\\s*["']([^"']*)["']`, "i"),
    new RegExp(`<meta[^>]+content\\s*=\\s*["']([^"']*)["'][^>]*(?:property|name)\\s*=\\s*["']${key}["']`, "i"),
  ];
  for(const p of patterns){ const m = html.match(p); if(m) return decode(m[1]); }
  return "";
}
function pageTitle(html){
  const h1 = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
  if(h1 && stripTags(h1[1])) return stripTags(h1[1]);
  const t = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return t ? stripTags(t[1]).split(/[|\-–—]/)[0].trim() : "";
}
function findImage(html, url){
  const og = metaContent(html, "og:image"); if(og) return toAbs(og, url);
  const cand = html.match(/<img\b[^>]*(?:alt|class)\s*=\s*["'][^"']*(?:head|profile|portrait|bio|author|avatar)[^"']*["'][^>]*>/i);
  const src = (cand && cand[0].match(/\bsrc\s*=\s*["']([^"']+)["']/i)) || html.match(/<img\b[^>]*\bsrc\s*=\s*["']([^"']+)["']/i);
  return src ? toAbs(src[1], url) : "";
}
// ---- Position dictionary (data/position-titles.json) -----------------------
// ~6,300 observed job titles, pre-sorted longest-first. findPosition() scans the
// list and returns the FIRST title that appears as a STANDALONE phrase in the
// page's Title + Description. Because the list is longest-first, the most specific
// title wins: "Vice President of Sales" before "Vice President" before "President";
// "Director of Information Technology" before "Director".
//
// Matching is whole-token only (each candidate and the haystack are normalized to
// single-space-delimited, space-padded strings, then tested with includes(" term ")).
// That means a title is NEVER matched inside a larger word — "CTO" can't match
// inside "Dire-cto-r" or "Vi-cto-r". Short all-caps acronyms (<=3 letters: CEO,
// CTO, VP, IT, HR, AP, ...) are matched CASE-SENSITIVELY and only when they appear
// in UPPERCASE in the source, so ordinary prose ("...it is...", "...hr team...")
// never yields a false Position.
function buildPositionMatchers(){
  let list = [];
  try { list = JSON.parse(fs.readFileSync(path.join(__dirname, "data", "position-titles.json"), "utf8")); }
  catch { list = []; }
  const matchers = [];
  for(const title of list){
    const letters = title.replace(/[^A-Za-z]/g, "");
    const isAcronym = letters.length > 0 && letters.length <= 3 && letters === letters.toUpperCase();
    // acronyms: keep original case, only collapse punctuation; others: lowercase + &->and
    const key = isAcronym
      ? " " + title.replace(/[^A-Za-z0-9]+/g, " ").trim().replace(/\s+/g, " ") + " "
      : " " + title.toLowerCase().replace(/&/g, " and ").replace(/[^a-z0-9]+/g, " ").trim().replace(/\s+/g, " ") + " ";
    if(key.trim()) matchers.push({ title, key, firstToken: key.trim().split(" ")[0], cased: isAcronym });
  }
  return matchers;
}
let POSITION_MATCHERS = null;

function findPosition(title, description){
  if(!POSITION_MATCHERS) POSITION_MATCHERS = buildPositionMatchers();
  const text = String(title || "") + " " + String(description || "");
  // Two space-padded, single-spaced haystacks: lowercased for normal titles,
  // original-case for acronyms. Padding + single spaces make includes() a
  // standalone (whole-token) test.
  const lowerHay = " " + text.toLowerCase().replace(/&/g, " and ").replace(/[^a-z0-9]+/g, " ").trim().replace(/\s+/g, " ") + " ";
  const casedHay = " " + text.replace(/[^A-Za-z0-9]+/g, " ").trim().replace(/\s+/g, " ") + " ";
  // first-token sets prune the ~6,300 candidates to the few worth an includes() check
  const lowerTokens = new Set(lowerHay.trim().split(" "));
  const casedTokens = new Set(casedHay.trim().split(" "));
  for(const m of POSITION_MATCHERS){
    if(m.cased){
      if(casedTokens.has(m.firstToken) && casedHay.includes(m.key)) return m.title;
    } else {
      if(lowerTokens.has(m.firstToken) && lowerHay.includes(m.key)) return m.title;
    }
  }
  return "";
}

// ---------------------------------------------------------------- main
const EMPTY_SD = { person:false, name:"", email:"", phone:"", image:"", locality:"", region:"", country:"", sameAs:[] };
// First Facebook / Twitter-X / WhatsApp PROFILE link from a list of URLs. Skips share/intent
// endpoints and the site's OWN brand handle (e.g. facebook.com/remax on a remax page) so we keep
// the PERSON's profile, not the company's. Facebook/Twitter strip the query; WhatsApp keeps it
// (the number lives in wa.me/<num> or ?phone=).
function findSocials(urls, brand){
  const out = { facebook:"", twitter:"", whatsapp:"" };
  const b = String(brand || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const isBrand = (h) => b && String(h).toLowerCase().replace(/[^a-z0-9]/g, "") === b;
  const FB_SKIP = /^(sharer|share|share\.php|dialog|plugins|tr|profile\.php|pages|groups|events|hashtag|story\.php|permalink\.php|login|help|policies)$/i;
  const TW_SKIP = /^(intent|share|home|hashtag|search|i|explore|messages|notifications|privacy|tos|login)$/i;
  for(const raw of (urls || [])){
    const u = String(raw || "").split("#")[0].trim();
    if(!out.facebook){
      const m = u.match(/^https?:\/\/(?:[a-z0-9-]+\.)?facebook\.com\/([^/?#]+)/i) || u.match(/^https?:\/\/fb\.com\/([^/?#]+)/i);
      if(m && !FB_SKIP.test(m[1]) && !isBrand(m[1])) out.facebook = u.split("?")[0];
    }
    if(!out.twitter){
      const m = u.match(/^https?:\/\/(?:www\.)?(?:twitter|x)\.com\/([^/?#]+)/i);
      if(m && !TW_SKIP.test(m[1]) && !isBrand(m[1])) out.twitter = u.split("?")[0];
    }
    if(!out.whatsapp){
      if(/^https?:\/\/(?:wa\.me|(?:api|web|chat)\.whatsapp\.com)\//i.test(u) || /^whatsapp:\/\//i.test(u)) out.whatsapp = u;
    }
  }
  return out;
}
// Pull contact fields from a page's schema.org JSON-LD (Person / RealEstateAgent / LocalBusiness /
// Organization). A FREE, automatic fallback for sites that publish structured data but hide the
// email/phone from mailto:/tel: links, or use a name-polluting URL slug (e.g. remax's
// first-last-city-state). Name is taken ONLY from a Person-type object so a company name never
// lands in First/Last. Returns EMPTY_SD's shape.
function structuredFromHtml(html){
  const out = { person:false, name:"", email:"", phone:"", image:"", locality:"", region:"", country:"", sameAs:[] };
  const re = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while((m = re.exec(html))){
    let j; try{ j = JSON.parse(m[1].trim()); }catch{ continue; }
    const list = Array.isArray(j) ? j : (j && Array.isArray(j["@graph"]) ? j["@graph"] : [j]);
    for(const o of list){
      if(!o || typeof o !== "object") continue;
      const t = Array.isArray(o["@type"]) ? o["@type"].join(",") : String(o["@type"] || "");
      const isPerson = /\b(Person|RealEstateAgent|Agent|Attorney|Physician)\b/i.test(t);
      if(!isPerson && !/\b(LocalBusiness|Organization|RealEstate)/i.test(t)) continue;
      if(isPerson && !out.name && o.name){ out.name = String(o.name); out.person = true; }
      if(!out.email && o.email) out.email = String(o.email).replace(/^mailto:/i, "").trim();
      if(!out.phone && o.telephone) out.phone = String(o.telephone).trim();
      if(!out.image && o.image) out.image = typeof o.image === "string" ? o.image : ((o.image && (o.image.url || o.image["@id"])) || "");
      const a = (o.address && typeof o.address === "object") ? o.address : null;
      if(a && !out.locality){ out.locality = a.addressLocality || ""; out.region = a.addressRegion || ""; out.country = a.addressCountry || ""; }
      if(o.sameAs){ for(const s of (Array.isArray(o.sameAs) ? o.sameAs : [o.sameAs])) if(s) out.sameAs.push(String(s)); }
    }
  }
  return out;
}
/**
 * @param {string} html
 * @param {string} url
 * @param {object} deps  { wireless:Set, genderMap?:object, geocode?:fn, source?, timestamp? }
 * @returns {object|null} record, or null if it fails the quality gate
 */
function extractRecord(html, url, deps = {}){
  const { wireless, genderMap = {}, geocode = defaultGeocode, source = "", timestamp = new Date().toISOString().slice(0,10) } = deps;
  html = String(html || "");
  url = String(url || "").split("?")[0];        // drop the query string ("?...") from the source URL

  const directory = classifyDirectory(url, html, deps.directoryRules, deps.genderMap);
  const isBio = directory === "BIO URL";
  const pathHit = pathIdFromUrl(url);                  // {id, role} for the directory segment, or null
  const outDirectory = pathHit ? "Team" : directory;   // matched directory term -> Directory Type "Team"
  const last = lastPathSeg(url);
  const nameSlug = nameSlugFromUrl(url);               // name seg (skips a trailing record id, e.g. /First-Last/71955)
  let { first, last: lastName } = isBio ? inferNameFromSlug(nameSlug, deps.genderMap) : { first:"", last:"" };

  const hrefs = anchors(html);

  // FREE structured-data (schema.org JSON-LD) fallback — fills any gaps the normal extraction leaves.
  const sd = html.indexOf("ld+json") >= 0 ? structuredFromHtml(html) : EMPTY_SD;
  if(isBio && sd.person && sd.name){            // JSON-LD name beats a slug polluted with city/state
    const nm = nameFromSlug(String(sd.name).replace(/\s+/g, "-"));
    if(nm.first && nm.last && (!first || !lastName || lastName.length <= 2)){ first = nm.first; lastName = nm.last; }
  }

  // email(s): mailto first (precise), then a text scan. Every candidate runs through
  // cleanEmail (de-obfuscate, strip phone prefixes / echoed domain / trailing dots,
  // drop example.com + blocklisted) and only clean, valid addresses are kept.
  const emails = []; const seenE = new Set();
  const addEmail = (rawCandidate) => {
    const e = cleanEmail(decode(rawCandidate));
    if(e && !seenE.has(e.toLowerCase())){ seenE.add(e.toLowerCase()); emails.push(e); }
  };
  for(const h of hrefs){ if(/^mailto:/i.test(h)) addEmail(h.replace(/^mailto:/i,"").split("?")[0]); }
  for(const e of cfEmailsFromHtml(html)) addEmail(e);      // Cloudflare-protected addresses (decoded) — precise, like mailto
  if(!emails.length){
    for(const e of extractEmailsFromText(html)) addEmail(e);
  }
  if(!emails.length && sd.email) addEmail(sd.email);     // JSON-LD email when none in mailto/text
  let email = emails[0] || "";
  let emailType = "";                                     // "" -> classifyEmail(email); "Modelled" for a modelled guess

  // linkedin(s): /in/ only, not /company/
  const lis = []; const seenL = new Set();
  for(const h of hrefs){ if(/linkedin\.com\/in\//i.test(h) && !/linkedin\.com\/company\//i.test(h)){
    const li = h.split("#")[0].split("?")[0]; if(!seenL.has(li)){ seenL.add(li); lis.push(li); } } }
  const linkedin = lis[0] || "";

  // social profiles (facebook / twitter-x / whatsapp) from the page links + JSON-LD sameAs
  const socials = findSocials(hrefs.concat(sd.sameAs || []), getBaseDomain(url).split(".")[0] || "");

  // google maps (first)
  const maps = hrefs.find(h => /google\.[^/]+\/maps/i.test(h)) || "";

  // vCard: a downloadable .vcf contact card link (often carries the person's direct/cell number).
  // Store the absolute URL now; vcard.js reads + merges the card's fields on ingestion.
  const vcardHref = hrefs.find(h => /\.vcf($|[?#])/i.test(String(h))) || "";
  const vcard = vcardHref ? toAbs(vcardHref, url) : "";

  // phones from tel: AND sms: links. A number behind an sms: link is textable => it's a Mobile
  // number (important: many bio pages, e.g. century21, expose a "Call" tel: + a "Text" sms: for
  // the same cell). SAME-PAGE RULE: only attach to a person on a BIO URL page.
  const tels = []; const seenT = new Set(); const smsRaw = [];
  for(const h of hrefs){
    if(!/^(tel|sms):/i.test(h)) continue;
    const t = h.replace(/^(tel|sms):/i, "").trim();
    if(!t) continue;
    if(/^sms:/i.test(h)) smsRaw.push(t);
    const key = t.replace(/\D/g, "");                       // de-dupe tel:/sms: of the SAME number
    if(key && !seenT.has(key)){ seenT.add(key); tels.push(t); }
  }
  // Fallback: many bio pages PRINT the phone as plain text with no tel: link. On a BIO URL
  // (where the SAME-PAGE RULE makes a number safe to attach to the person), scan the visible
  // text for phone-shaped numbers — only when no tel:/sms: link was found, so existing pages are
  // unchanged. extractTextPhones is strict to avoid grabbing years, dates, zips, or IDs.
  if(isBio && !tels.length){
    for(const t of extractTextPhones(html)){ const key = t.replace(/\D/g,""); if(key && !seenT.has(key)){ seenT.add(key); tels.push(t); } }
  }
  if(isBio && !tels.length && sd.phone){ const k = String(sd.phone).replace(/\D/g,""); if(k){ seenT.add(k); tels.push(sd.phone); } }  // JSON-LD telephone
  let phone="", phoneType="", phoneLocation="", phone2="", phone2Type="";
  // Capture the phone NUMBER whenever we have a tel:/sms: (or text/JSON-LD) number on a bio page —
  // this NO LONGER requires the `wireless` block table, so the S3-direct Lambda (which omits the 40MB
  // table for speed) still captures numbers. The block table only adds the NANP line-TYPE + location;
  // when it's absent we still get the number + intl/sms-based Mobile flag, and NANP line-types can be
  // backfilled later from the stored E.164 values.
  if(isBio && tels.length){
    const cc = countryCodeFromDomain(getBaseDomain(url));   // country from the domain TLD (default US)
    const smsSet = new Set(smsRaw.map((s) => toE164(splitExtension(s).main, cc)).filter(Boolean));  // textable (Mobile) numbers
    const a = splitExtension(tels[0]);
    const e164a = toE164(a.main, cc);
    if(wireless) phoneType = classifyLineType(a.main, wireless).type;   // NANP line-type (needs the block table)…
    if(!phoneType || phoneType === "Unknown") phoneType = intlLineType(e164a) || phoneType;  // …non-NANP via libphonenumber
    if(intlMobileType(e164a)) phoneType = "Mobile";         // …non-NANP mobile by country prefix
    if(smsSet.has(e164a)) phoneType = "Mobile";             // …an sms: link means it's textable => Mobile
    phoneLocation = (wireless && phoneType !== "Toll Free") ? geocode(a.main) : "";   // block-level location (needs the table)
    phone = e164a + (a.ext ? ` ext. ${a.ext}` : "");        // …E.164 + standardized extension
    if(tels[1]){
      const b = splitExtension(tels[1]);
      const e164b = toE164(b.main, cc);
      if(wireless) phone2Type = classifyLineType(b.main, wireless).type;
      if(!phone2Type || phone2Type === "Unknown") phone2Type = intlLineType(e164b) || phone2Type;
      if(intlMobileType(e164b)) phone2Type = "Mobile";
      if(smsSet.has(e164b)) phone2Type = "Mobile";
      phone2 = e164b + (b.ext ? ` ext. ${b.ext}` : "");
    }
  }

  if(sd.locality || sd.region){                          // structured postal address beats the phone area code
    const sdLoc = [sd.locality, sd.region, sd.country].filter(Boolean).join(", ");
    if(sdLoc) phoneLocation = sdLoc;
  }

  const description = (metaContent(html,"og:description") || metaContent(html,"description")).slice(0,300);
  const pageTtl = pageTitle(html);
  // A matched directory Path ID with a role (e.g. /attorneys/ -> "Attorney") drives both
  // Title and Position; otherwise Title = page title and Position = page-derived role.
  const role = (pathHit && pathHit.role) ? pathHit.role : "";
  let title = role || pageTtl;
  let position = role || findPosition(pageTtl, description);
  let image = findImage(html, url);
  if(!image && sd.image) image = toAbs(sd.image, url);
  const gender = first ? (genderMap[first.toLowerCase()] || "") : "";

  // ---- Morgan Stanley advisor pages (advisor.morganstanley.com) ----
  // Server-rendered team-roster pages read from Common Crawl (the live site blocks datacenter IPs).
  // The lead advisor's OWN email isn't printed (compliance) — only the team's — and the page <title>
  // "Name | City, ST | Division" carries the office location + division. Improve the lead record:
  // take City/State + division from the title, and MODEL the lead's email from the Firstname.Lastname
  // pattern visible in the team's printed emails (labelled "Modelled" — never a verified address).
  if(isBio && first && lastName && /(^|\.)morganstanley\.com$/.test(getBaseDomain(url))){
    // the RAW <title> is "Name | City, ST | Division" (pageTitle() strips after the first "|")
    const rawTitle = decode(stripTags((html.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1] || ""));
    const parts = rawTitle.split("|").map((s) => s.trim()).filter(Boolean);
    const locPart = parts.find((p) => /,\s*[A-Z]{2}\b/.test(p) && !/morgan stanley/i.test(p));  // "Coral Gables, FL"
    if(locPart && !(sd.locality || sd.region)) phoneLocation = locPart;                          // office city beats the phone's area
    const divPart = parts.slice(1).find((p) => p !== locPart);                                   // "Morgan Stanley Private Wealth Management"
    if(divPart){ title = divPart; position = findPosition(divPart, description) || position; }
    // The advisor's OWN email is never printed (only the team's + a generic FAwebsites@ inbox). Keep a
    // found email ONLY if it's the lead's own (local-part fits the lead's name); otherwise MODEL it from
    // the team's Firstname.Lastname pattern, and never attribute a generic/team inbox to the advisor.
    const foundIsLead = email && templateFor(email.split("@")[0], first, lastName);
    if(!foundIsLead){
      const found = String(html).match(/[A-Za-z0-9._%+-]+@morganstanley(?:pwm)?\.com/gi) || [];
      const samples = [...new Set(found.map((e) => e.toLowerCase()))].map((e) => {
        const toks = e.split("@")[0].split(/[._-]/).filter(Boolean);
        return toks.length === 2 ? { first: toks[0], last: toks[1], email: e } : null;           // skip generics (FAwebsites@…)
      }).filter(Boolean);
      const modelled = samples.length >= 2 ? modelEmail(samples, first, lastName) : "";
      if(modelled){ email = modelled; emailType = "Modelled"; }
      else { email = ""; emailType = ""; }     // drop generic/team inbox — not the advisor's address
    }
  }

  // ---- QUALITY GATE ----
  // Normally every record needs a real email. EXCEPTION — when deps.allowNoEmail is set
  // (sitemap / webpage mode, where the user explicitly targets these URLs): keep a clean
  // person bio that publishes a name + assigned Gender but no email, so its address can be
  // MODELLED downstream from the company's known pattern (labelled Email Type "Modelled").
  // Guarded tightly to real people: a BIO URL with first+last+gender AND a 2–3 token slug
  // (e.g. /our-people/adam-gage) — this drops news headlines like /news/jane-doe-joins-as-... .
  if(!email){
    const slugTokens = isBio ? pathNameTokens(nameSlug).length : 0;
    const keepForModelling = deps.allowNoEmail && isBio && first && lastName && gender
      && (sd.person || (slugTokens >= 2 && slugTokens <= 3));   // JSON-LD confirming a person also clears the junk-slug guard
    if(!keepForModelling) return null;         // require a valid email address for every other record
  }

  return {
    "Time Stamp": timestamp,
    "Source": source,
    "Web Source URL": url,
    "Directory": outDirectory,
    "Path ID": pathHit ? pathHit.id : "",
    "Domain": getBaseDomain(url),
    "Last Path": cleanLastPath(nameSlug || last),   // the segment the NAME came from, not a trailing id
    "Bio Check": isBio ? "Y" : "",
    "First": first,
    "Last": lastName,
    "Gender": gender,
    "Title": title,
    "Position": position,
    "Description": description,
    "Image URL": image,
    "Email Address": email,
    "Email Type": emailType || classifyEmail(email),
    "LinkedIn URL": linkedin,
    "Facebook": socials.facebook,
    "Twitter": socials.twitter,
    "WhatsApp": socials.whatsapp,
    "Google Maps": maps,
    "vCard": vcard,
    "Phone": phone,
    "Phone Type": phoneType,
    "Phone Location": phoneLocation,
    "Phone 2": phone2,
    "Phone 2 Type": phone2Type,
    "Phone 2 Location": "",
  };
}

// Block-level City/State for a NANP number, from phone-blocks.csv (memoized load). City-level —
// far more precise than the old 10-area-code fallback. Non-NANP/unknown blocks return "" and get
// libphonenumber later (see geocodePhone / geocodeRecords).
function defaultGeocode(phone){
  return blockLocation(phone, loadPhoneBlocks(PHONE_BLOCKS_CSV).location);
}

const { countryForDomain } = require("./tld-lookup");   // domain-TLD country (Phone Location fallback)

// ---- real phone geocoding via libphonenumber (City, Region, Country) ----
// Lazy-loaded + wrapped so the engine still runs if the libs aren't installed.
let _geoTried = false, _phoneUtil = null, _PNT = null, _geocoder = null, _regionName = null;
function loadGeoLibs(){
  if(_geoTried) return;
  _geoTried = true;
  try{
    const glp = require("google-libphonenumber");
    _phoneUtil = glp.PhoneNumberUtil.getInstance();
    _PNT = glp.PhoneNumberType;
    _geocoder = require("libphonenumber-geo-carrier").geocoder;
    _regionName = new Intl.DisplayNames(["en"], { type: "region" });
  }catch(e){ _phoneUtil = null; _geocoder = null; }
}

// Line type for a NON-NANP E.164 number, via libphonenumber's getNumberType. Reliable
// outside the US/Canada (where number portability makes the type ambiguous — that's why
// NANP keeps the wireless-block classifier instead). Maps to our vocabulary
// (Mobile / Direct / Toll Free); "" when unavailable, invalid, or not classifiable.
function intlLineType(e164){
  loadGeoLibs();
  if(!_phoneUtil || !_PNT || !e164) return "";
  const s = String(e164).trim();
  if(!s.startsWith("+")) return "";                      // require explicit international form
  try{
    const num = _phoneUtil.parse(s);
    if(!_phoneUtil.isValidNumber(num)) return "";
    switch(_phoneUtil.getNumberType(num)){
      case _PNT.MOBILE: return "Mobile";
      case _PNT.TOLL_FREE: return "Toll Free";
      case _PNT.FIXED_LINE:
      case _PNT.FIXED_LINE_OR_MOBILE:
      case _PNT.VOIP:
      case _PNT.PERSONAL_NUMBER:
      case _PNT.UAN: return "Direct";
      default: return "";
    }
  }catch{ return ""; }
}

// "+19169291481" -> "Sacramento, CA, United States". Normalizes non-E.164 input first, and
// always returns at least the country for a valid number (incl. toll-free, which has no area).
// "" only when the number can't be parsed at all. `cc` = default country calling code.
async function geocodePhone(raw, cc){
  // Block-level "City, ST, Country" (US/Canada) is more precise than libphonenumber's rate-center
  // (which often returns only the state) — try it first, fall back to libphonenumber for intl /
  // uncovered blocks.
  const blk = blockLocation(raw, loadPhoneBlocks(PHONE_BLOCKS_CSV).location);
  if(blk) return blk;
  loadGeoLibs();
  if(!_phoneUtil || !_geocoder || !raw) return "";
  let e164 = String(raw).trim();
  if(!e164.startsWith("+")) e164 = toE164(e164, cc || "1");   // accept formatted / national numbers
  if(!e164) return "";
  try{
    const num = _phoneUtil.parse(e164);
    const region = _phoneUtil.getRegionCodeForNumber(num);
    let country = "";
    try{ country = region ? _regionName.of(region) : ""; }catch{ country = region || ""; }
    let desc = "";
    if(!(_PNT && _phoneUtil.getNumberType(num) === _PNT.TOLL_FREE)){   // toll-free -> country only
      const ccd = num.getCountryCode().toString();
      const nn = num.getNationalNumber().toString();
      try{ desc = await _geocoder({ nationalNumber: nn, countryCallingCode: ccd }, "en") || ""; }catch{ desc = ""; }
    }
    return [desc, country].filter(Boolean).join(", ") || country;
  }catch(e){ return ""; }
}

// Fill "Phone Location" for each record from its (E.164) Phone, cached per number.
// Async; run as a post-pass after collection. Leaves the existing value if no result.
async function geocodeRecords(records){
  loadGeoLibs();
  const cache = new Map();
  const lookup = async (raw, cc) => { const k = cc + "|" + raw; if(!cache.has(k)) cache.set(k, await geocodePhone(raw, cc)); return cache.get(k); };
  for(const r of records){
    const cc = countryCodeFromDomain(r["Domain"]);   // default calling code for non-E.164 numbers
    // Fill ONLY when blank — an address-derived location (from a JSON API or a vCard) is more
    // accurate than the phone's area code, so it wins.
    if(!String(r["Phone Location"] || "").trim()){
      const base = basePhone(r["Phone"]);
      let loc = base ? await lookup(base, cc) : "";
      if(!loc) loc = countryForDomain(r["Domain"]);  // fall back to the domain's TLD country
      if(loc) r["Phone Location"] = loc;
    }
    if(!String(r["Phone 2 Location"] || "").trim()){
      const base2 = basePhone(r["Phone 2"]);
      if(base2){ const loc2 = await lookup(base2, cc); if(loc2) r["Phone 2 Location"] = loc2; }
    }
  }
  return records;
}

// ---- phone normalization to E.164 ----
// country calling code by domain TLD; anything not listed (incl. .com/.org/.net…) -> US (1)
const TLD_CC = {
  us:"1", ca:"1", uk:"44", gb:"44", ie:"353", au:"61", nz:"64", in:"91", sg:"65", hk:"852", my:"60",
  ph:"63", de:"49", fr:"33", es:"34", it:"39", nl:"31", be:"32", ch:"41", at:"43", se:"46", no:"47",
  dk:"45", fi:"358", pt:"351", pl:"48", cz:"420", ro:"40", gr:"30", hu:"36", lu:"352", is:"354",
  mx:"52", br:"55", ar:"54", cl:"56", co:"57", pe:"51", za:"27", ng:"234", ke:"254", ae:"971",
  sa:"966", il:"972", tr:"90", jp:"81", kr:"82", cn:"86", tw:"886", ru:"7", ua:"380",
};
function countryCodeFromDomain(domain){
  const tld = String(domain || "").toLowerCase().split(".").pop();
  return TLD_CC[tld] || "1";   // assume US when unknown / not inferable
}
// Convert a messy phone string to E.164 (+<cc><number>, digits only). Best-effort.
function toE164(raw, cc){
  if(!raw) return "";
  raw = String(raw).replace(/\(\s*0\s*\)/g, "");         // drop a parenthesized trunk prefix, e.g. "+44 (0)1483…" -> "+44 1483…"
  const hasPlus = raw.trim().startsWith("+");
  let digits = raw.replace(/\D/g, "");
  if(!digits) return "";
  if(hasPlus) return "+" + digits.slice(0, 15);          // already international
  cc = cc || "1";
  if(cc === "1"){                                         // North American Numbering Plan
    if(digits.length === 11 && digits[0] === "1") return "+" + digits;
    if(digits.length === 10) return "+1" + digits;
    if(digits.length > 11) return "+" + digits.slice(0, 15);
    return "+1" + digits;                                 // short/ext — best effort
  }
  digits = digits.replace(/^0+/, "");                     // drop national trunk prefix
  if(digits.startsWith(cc)) return "+" + digits.slice(0, 15);
  return "+" + (cc + digits).slice(0, 15);
}

// Split a phone string into { main, ext }. Handles tel: ";ext=" / ",", and
// "ext", "extension", "x", "#" forms. ext is digits only ("" if none).
function splitExtension(raw){
  const s = String(raw || "");
  let m = s.match(/;\s*ext=\s*(\d{1,6})/i);                       // tel: URI ext param
  if(m) return { main: s.slice(0, m.index), ext: m[1] };
  m = s.match(/(?:\bext(?:ension)?\b\.?|[x#])\s*[:.]?\s*(\d{1,6})\s*$/i);  // ext/extension/x/#
  if(m) return { main: s.slice(0, m.index), ext: m[1] };
  m = s.match(/,\s*(\d{1,6})\s*$/);                               // tel: comma pause -> ext
  if(m) return { main: s.slice(0, m.index), ext: m[1] };
  return { main: s, ext: "" };
}

// the E.164 base of a phone field, dropping any " ext. NNN" suffix
function basePhone(p){ return String(p || "").split(/\s*ext\.?\s*/i)[0].trim(); }

// Pull plain-text phone numbers off a page (for bio pages that print a number with no tel:
// link). STRICT to avoid false positives: a candidate is kept only if it's international
// (leading + with 8–15 digits) or NANP-shaped (10–11 digits WITH separators). So years
// ("2026"), dates ("2026-04-30"), year ranges ("2010 - 2026"), zips, and IDs never match.
// Returns at most the first two distinct numbers, in document order.
const PHONE_TEXT_RE = /\+?\d[\d\s().\-]{6,18}\d/g;
function extractTextPhones(html){
  const text = stripTags(String(html || ""));
  const out = []; const seen = new Set();
  let m; PHONE_TEXT_RE.lastIndex = 0;
  while(out.length < 2 && (m = PHONE_TEXT_RE.exec(text))){
    const raw = m[0].trim();
    const hasPlus = raw.startsWith("+");
    const digits = raw.replace(/\D/g, "");
    if(hasPlus){
      if(digits.length < 8 || digits.length > 15) continue;
    } else {
      if(digits.length < 10 || digits.length > 11) continue;   // NANP-shaped only
      if(!/[\s().\-]/.test(raw)) continue;                      // require formatting (not a bare digit run)
    }
    const key = (hasPlus ? "+" : "") + digits;
    if(!seen.has(key)){ seen.add(key); out.push(raw); }
  }
  return out;
}

// Dataset-wide phone analysis (run AFTER all records are collected):
//  1. clear Phone 2 when it's the same line as Phone
//  2. a number seen more than once that is "Direct" becomes "Office" (shared line)
// Returns NEW record objects; does not mutate the inputs.
function analyzePhones(records){
  const rows = records.map((r) => ({ ...r }));

  for(const r of rows){
    const b1 = basePhone(r["Phone"]), b2 = basePhone(r["Phone 2"]);
    if(b1 && b2 && b1 === b2){ r["Phone 2"] = ""; r["Phone 2 Type"] = ""; }
  }

  const counts = new Map();
  const bump = (b) => { if(b) counts.set(b, (counts.get(b) || 0) + 1); };
  for(const r of rows){ bump(basePhone(r["Phone"])); bump(basePhone(r["Phone 2"])); }

  for(const r of rows){
    const b1 = basePhone(r["Phone"]);
    if(b1 && counts.get(b1) > 1 && r["Phone Type"] === "Direct") r["Phone Type"] = "Office";
    const b2 = basePhone(r["Phone 2"]);
    if(b2 && counts.get(b2) > 1 && r["Phone 2 Type"] === "Direct") r["Phone 2 Type"] = "Office";
  }
  return rows;
}

function loadGenderMap(filePath){
  const rows = loadRows(filePath);
  const map = {};
  for(const row of rows){
    const cols = parseCsvRow(row).map(s => s.trim().replace(/^"|"$/g, ""));
    const first = normalizeForMatching(cols[0] || "");
    const genderRaw = (cols[1] || "").trim().toUpperCase();
    const gender = genderRaw[0] === "M" ? "M" : genderRaw[0] === "F" ? "F" : "";
    if(first && gender) map[first] = gender;
  }
  return map;
}

function loadDirectoryRules(filePath){
  const rows = loadRows(filePath);
  const rules = { names: new Set(), terms: new Set(), termMap: new Map() };
  for(const row of rows){
    const cols = parseCsvRow(row).map(s => s.trim().replace(/^"|"$/g, "")).filter(Boolean);
    const rawName = (cols[0] || "").trim();
    const name = normalizeForMatching(rawName);
    if(!name) continue;
    rules.names.add(name);
    rules.termMap.set(name, rawName);
    for(let i = 1; i < cols.length; i++){
      const rawTerm = (cols[i] || "").trim();
      const term = normalizeForMatching(rawTerm);
      if(!term) continue;
      rules.terms.add(term);
      rules.termMap.set(term, rawName);
    }
  }
  return rules;
}

module.exports = { extractRecord, classifyEmail, classifyDirectory, nameFromSlug, loadGenderMap, loadDirectoryRules,
  cleanEmail, setEmailBlocklist, loadEmailBlocklist, analyzePhones, splitExtension,
  geocodeRecords, geocodePhone, findPosition,
  toE164, countryCodeFromDomain, pathIdFromUrl, getBaseDomain, nameSlugFromUrl, lastPathSeg, lastPathFromUrl, structuredFromHtml, findSocials };   // reused by the Sheet importer + Site Search + DB normalizer

// ---------------------------------------------------------------- self-test
if(require.main === module){
  const { loadWirelessBlocks } = require("./wireless-block-classifier");
  const wireless = loadWirelessBlocks(process.argv[2] || (__dirname + "/phone-blocks.csv"));
  const genderMap = { marcus:"M", nina:"F", maria:"F", omar:"M" };   // production: full census CSV

  const cases = [
    { label:"BIO page, full contact (wireless phone)",
      url:"https://acme.com/team/marcus-patel",
      html:`<title>Acme</title><h1>Marcus Patel</h1>
            <meta property="og:description" content="Marcus Patel is VP of Marketing at Acme.">
            <meta property="og:image" content="/img/marcus.jpg">
            <a href="mailto:marcus.patel@acme.com">email</a>
            <a href="tel:+12012012345">call</a>
            <a href="https://www.linkedin.com/in/marcus-patel?trk=x">li</a>` },
    { label:"BIO page, name only (no handles) — kept via BIO URL",
      url:"https://globex.io/people/nina-novak",
      html:`<h1>Nina Novak</h1><p>Senior Account Executive.</p>` },
    { label:"Contact page, role email — kept via email",
      url:"https://acme.com/contact",
      html:`<h1>Contact Us</h1><a href="mailto:info@acme.com">info</a><a href="tel:+18005550199">call</a>` },
    { label:"Contact page, nothing usable — DROPPED",
      url:"https://acme.com/contact",
      html:`<h1>Contact Us</h1><p>123 Main St. Open 9-5.</p>` },
    { label:"BIO page, HTML-entity-obfuscated email — decoded",
      url:"https://larsonllp.com/people/catherine-s-owens",
      html:`<h1>Catherine S. Owens</h1>` +
           `<a href="mailto:&#099;&#111;wens&#064;lar&#115;&#111;n&#108;&#108;p&#046;com">email</a>` },
  ];

  for(const c of cases){
    const r = extractRecord(c.html, c.url, { wireless, genderMap, source:"Test" });
    console.log("\n• " + c.label);
    if(!r){ console.log("   -> DROPPED (failed quality gate)"); continue; }
    console.log(`   Directory : ${r["Directory"]}   Bio:${r["Bio Check"]||"-"}`);
    console.log(`   Name      : ${r["First"]} ${r["Last"]} (${r["Gender"]||"-"})`);
    console.log(`   Email     : ${r["Email Address"]||"-"}  [${r["Email Type"]||"-"}]`);
    console.log(`   LinkedIn  : ${r["LinkedIn URL"]||"-"}`);
    console.log(`   Phone     : ${r["Phone"]||"-"}  [${r["Phone Type"]||"-"}]  ${r["Phone Location"]||""}`);
    console.log(`   Title/Pos : ${r["Title"]||"-"} / ${r["Position"]||"-"}`);
  }

  const ruleTest = { names: new Set(["content-cleaning"]), terms: new Set(["content-cleaning"]), termMap: new Map([["content-cleaning","content-cleaning"]]) };
  console.log("\n• Directory rule match test:", classifyDirectory("https://acme.com/charlotte/content-cleaning/", "", ruleTest) === "content-cleaning" ? "PASS" : "FAIL");

  const decodedEmail = extractRecord(
    `<h1>Catherine S. Owens</h1><a href="mailto:&#099;&#111;wens&#064;lar&#115;&#111;n&#108;&#108;p&#046;com">email</a>`,
    "https://larsonllp.com/people/catherine-s-owens", { wireless, genderMap, source:"Test" });
  console.log("• HTML-entity email decode test:", decodedEmail && decodedEmail["Email Address"] === "cowens@larsonllp.com" ? "PASS" : `FAIL (${decodedEmail && decodedEmail["Email Address"]})`);

  // ---- text-phone extraction + email-less bio (sitemap/webpage) ----
  let pp = 0, pf = 0;
  const okp = (label, cond) => { if(cond) pp++; else { pf++; console.log("  FAIL:", label); } };

  okp("toE164 drops (0) trunk prefix", toE164("+44 (0)1483 307090") === "+441483307090");
  okp("extractTextPhones grabs an intl number", extractTextPhones("<p>+44 (0)1483 307090</p>")[0] === "+44 (0)1483 307090");
  okp("extractTextPhones grabs a US number", (extractTextPhones("<p>Call (202) 555-0123 today</p>")[0] || "").includes("202"));
  okp("extractTextPhones ignores a year", extractTextPhones("<li>2026: promoted to Partner</li>").length === 0);
  okp("extractTextPhones ignores a date", extractTextPhones("<p>2026-04-30</p>").length === 0);
  okp("extractTextPhones ignores a year range", extractTextPhones("<p>2010 - 2026</p>").length === 0);
  okp("extractTextPhones ignores a bare digit run", extractTextPhones("<p>10000000000</p>").length === 0);

  // a bio page printing a phone as text (no tel: link) yields the normalized phone, and the
  // email-less person is kept because Gender is assigned (allowNoEmail = sitemap/webpage mode)
  const txtPhoneRec = extractRecord(
    `<title>Adam Gage - our people | RSM UK</title><h1>Adam Gage</h1>` +
    `<div class="profile-block"><p>Adam Gage</p><p>Partner</p><p>+44 (0)1483 307090</p></div>`,
    "https://www.rsmuk.com/our-people/adam-gage", { wireless, genderMap: { adam:"M" }, source:"Test", allowNoEmail:true });
  okp("bio text-phone captured + normalized", txtPhoneRec && txtPhoneRec["Phone"] === "+441483307090");
  okp("email-less bio kept when gender assigned", txtPhoneRec && txtPhoneRec["First"] === "Adam" && !txtPhoneRec["Email Address"]);

  // Morgan Stanley advisor page (advisor.morganstanley.com): server-rendered team roster where the
  // lead's own email isn't printed. Pull City/State + division from the "Name | City, ST | Division"
  // title, and MODEL the lead's email from the Firstname.Lastname pattern in the team's printed emails.
  const msRec = extractRecord(
    `<html><head><title>Adam E. Carlin | Coral Gables, FL | Morgan Stanley Private Wealth Management</title></head>` +
    `<body><h1>Adam E. Carlin</h1><a href="tel:+1-305-476-3302">call</a>` +
    `<input type="hidden" value="Luisa.Arias@morganstanleypwm.com">` +
    `<input type="hidden" value="Jan.Strusinski@morganstanleypwm.com">` +
    `<a href="mailto:FAwebsites@morganstanley.com">generic</a></body></html>`,
    "https://advisor.morganstanley.com/adam.e.carlin", { wireless, genderMap: { adam:"M" }, source:"Test", allowNoEmail:true });
  okp("MS: location from title", msRec && msRec["Phone Location"] === "Coral Gables, FL");
  okp("MS: division as title", msRec && /Private Wealth Management/.test(msRec["Title"]));
  okp("MS: lead email modelled from team pattern", msRec && msRec["Email Address"] === "adam.carlin@morganstanleypwm.com");
  okp("MS: modelled email labelled Modelled", msRec && msRec["Email Type"] === "Modelled");
  okp("MS: phone captured", msRec && msRec["Phone"] === "+13054763302");
  // a generic-only page (no parseable team pattern) must NOT fabricate an email
  const msGeneric = extractRecord(
    `<html><head><title>Jane Q. Roe | Austin, TX | Morgan Stanley</title></head><body><h1>Jane Q. Roe</h1>` +
    `<a href="mailto:FAwebsites@morganstanley.com">x</a></body></html>`,
    "https://advisor.morganstanley.com/jane.q.roe", { wireless, genderMap: { jane:"F" }, source:"Test", allowNoEmail:true });
  okp("MS: no email modelled without a team pattern", msGeneric && !msGeneric["Email Address"] && msGeneric["Phone Location"] === "Austin, TX");

  // non-NANP line type via libphonenumber (skipped if the lib isn't installed)
  if(intlLineType("+441483307090")){
    okp("intlLineType: UK landline -> Direct", intlLineType("+441483307090") === "Direct");
    okp("intlLineType: UK mobile -> Mobile", intlLineType("+447525281159") === "Mobile");
    okp("intlLineType: UK toll-free -> Toll Free", intlLineType("+448001234567") === "Toll Free");
    okp("intlLineType: invalid -> ''", intlLineType("+44123") === "");
    okp("UK landline bio classified Direct (not Unknown)", txtPhoneRec && txtPhoneRec["Phone Type"] === "Direct");
    const ukMobileRec = extractRecord(
      `<h1>Alessandra Raja</h1><div class="profile-block"><p>+44 7525 281159</p></div>`,
      "https://www.rsmuk.com/our-people/alessandra-raja", { wireless, genderMap: { alessandra:"F" }, source:"Test", allowNoEmail:true });
    okp("UK mobile bio classified Mobile", ukMobileRec && ukMobileRec["Phone Type"] === "Mobile");
  } else {
    console.log("  (skipped intlLineType tests — google-libphonenumber not available)");
  }

  // profile URLs with a trailing record id: /Agent/Detail/First-Last/<id> (name in the parent seg)
  okp("nameSlugFromUrl skips a trailing id", nameSlugFromUrl("https://x.com/Agent/Detail/Aarin-Burt/71955") === "Aarin-Burt");
  okp("classify /Agent/Detail/Name/id -> BIO URL", classifyDirectory("https://x.com/Agent/Detail/Aarin-Burt/71955") === "BIO URL");
  const agentRec = extractRecord(`<h1>x</h1>`, "https://x.com/Agent/Detail/Aaron-Foster/72909",
    { wireless, genderMap: { aaron:"M" }, source:"Test", allowNoEmail:true });
  okp("agent-detail name pulled from parent seg", agentRec && agentRec["First"] === "Aaron" && agentRec["Last"] === "Foster");
  okp("agent-detail role -> Title Agent / Directory Team", agentRec && agentRec["Title"] === "Agent" && agentRec["Directory"] === "Team");
  okp("no false BIO for /blog/2024/12345 (ids, no dir, no name)", classifyDirectory("https://x.com/blog/2024/12345") !== "BIO URL");

  // trailing UUID id(s): /our-advisors/First-Last/<uuid>/<uuid> (e.g. evrealestate.com)
  okp("isIdSegment matches a UUID", isIdSegment("cc2bac91-8a1a-49cf-80fb-8b0db880183e"));
  okp("nameSlugFromUrl skips two trailing UUIDs",
    nameSlugFromUrl("https://x.com/en/our-advisors/aaron-allred/cc2bac91-8a1a-49cf-80fb-8b0db880183e/cc2bac91-8a1a-49cf-80fb-8b0db880183e") === "aaron-allred");
  okp("classify /our-advisors/Name/<uuid>/<uuid> -> BIO URL",
    classifyDirectory("https://x.com/en/our-advisors/aaron-allred/cc2bac91-8a1a-49cf-80fb-8b0db880183e/cc2bac91-8a1a-49cf-80fb-8b0db880183e") === "BIO URL");

  // without an assigned Gender the email-less bio is still dropped (guards out junk slugs)
  const noGender = extractRecord(`<h1>Zzyk Vwxp</h1><p>+44 (0)1483 307090</p>`,
    "https://x.com/our-people/zzyk-vwxp", { wireless, genderMap:{}, source:"Test", allowNoEmail:true });
  okp("email-less bio dropped when no gender", noGender === null);

  // tel: link still wins (text fallback only runs when there's no tel:) and an emailed record is unaffected
  okp("tel: link unaffected by text fallback",
    (extractRecord(`<h1>Nina Novak</h1><a href="mailto:n@acme.com">e</a><a href="tel:+12012012345">c</a>`,
      "https://acme.com/team/nina-novak", { wireless, genderMap, source:"Test" }) || {})["Phone"] === "+12012012345");

  // sms: link => the number is textable => Phone Type "Mobile" (key for this project). A "Call"
  // tel: + "Text" sms: for the SAME number de-dupe to one Phone, and the trailing aid- id is NOT
  // used as the Last Path (the name segment is) — both century21 behaviours.
  const smsRec = extractRecord(
    `<h1>Agnes Aaron</h1><a href="mailto:a@c21.com">e</a><a href="tel:+16091234567">call</a><a href="sms:+16091234567">text</a>`,
    "https://www.century21.com/agent/detail/nj/medford/agents/agnes-aaron/aid-P00200000FZGd1opBBuOqLprx9TUD7AZamyzZbCg",
    { wireless, genderMap: { agnes:"F" }, source:"Test" });
  okp("sms: link classifies the number as Mobile", smsRec && smsRec["Phone"] === "+16091234567" && smsRec["Phone Type"] === "Mobile");
  okp("tel:+sms: of one number -> no duplicate Phone 2", smsRec && !smsRec["Phone 2"]);
  okp("Last Path = name segment, not the trailing aid- id", smsRec && smsRec["Last Path"] === "Agnes Aaron");

  // structured-data (schema.org JSON-LD) fallback: email/phone/location pulled from a Person block
  // even though the page has NO mailto:/tel: links (the free AI-Assist layer)
  const ldHtml = `<html><head><script type="application/ld+json">${JSON.stringify({
    "@type":"Person", name:"Jane Doe", email:"jane@firm.com", telephone:"(512) 555-0143",
    address:{ "@type":"PostalAddress", addressLocality:"Austin", addressRegion:"TX", addressCountry:"US" },
  })}</script></head><body><h1>Jane Doe</h1></body></html>`;
  const sdRec = extractRecord(ldHtml, "https://firm.com/our-people/jane-doe", { wireless, genderMap:{ jane:"F" }, source:"Test", allowNoEmail:true });
  okp("JSON-LD email extracted when no mailto on the page", sdRec && sdRec["Email Address"] === "jane@firm.com");
  okp("JSON-LD telephone extracted when no tel: link", sdRec && sdRec["Phone"] === "+15125550143");
  okp("JSON-LD postal address -> Phone Location", sdRec && sdRec["Phone Location"] === "Austin, TX, US");
  okp("structuredFromHtml ignores a page with no JSON-LD", structuredFromHtml("<html><body>nope</body></html>").email === "");

  // social profiles: keep the PERSON's, drop the site's own brand handle + share/intent endpoints
  const soc = findSocials(["https://www.facebook.com/remax", "https://facebook.com/janeagent", "https://twitter.com/intent/tweet", "https://x.com/janeagent", "https://wa.me/15551234567"], "remax");
  okp("findSocials keeps person's FB/Twitter/WhatsApp, drops brand + intent", soc.facebook === "https://facebook.com/janeagent" && soc.twitter === "https://x.com/janeagent" && soc.whatsapp === "https://wa.me/15551234567");

  console.log(`\nphone / email-less self-test: ${pp} passed, ${pf} failed`);
}
