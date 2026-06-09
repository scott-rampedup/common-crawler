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
const { classifyLineType } = require("./wireless-block-classifier");

// ---------------------------------------------------------------- config
const FREE_DOMAINS = new Set(["gmail.com","yahoo.com","hotmail.com","outlook.com",
  "icloud.com","aol.com","proton.me","protonmail.com","gmx.com","mail.com","live.com","msn.com"]);
const ROLE_LOCALS = new Set(["info","sales","support","admin","contact","hello","hr","billing",
  "careers","office","team","marketing","help","service","press","media","jobs","accounts",
  "webmaster","postmaster","abuse","noreply","no-reply","enquiries","inquiries"]);
// path segments that signal an individual profile (when followed by a person-ish slug)
const BIO_DIRS = new Set(["people","person","team","about","staff","bio","profile","profiles",
  "leadership","our-team","meet","employee","agents","agent","attorneys","doctors","providers",
  // common professional-services bio directories (single-word forms match the normalizer)
  "attorney","lawyer","lawyers","partner","partners","associate","associates","principals",
  "physician","physicians","doctor","provider","broker","brokers","realtor","realtors",
  "advisor","advisors","consultant","consultants","clinicians","specialists","member","members",
  "professionals","directory","biographies","biography","bios"]);
const COMMON_TITLES = ["chief executive officer","ceo","founder","co-founder","cto","cfo","coo","cmo",
  "president","vice president","vp","director","head of","manager","lead","engineer","designer",
  "account executive","partner","associate","analyst","consultant","specialist","coordinator","owner"];

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

function extractEmailsFromText(html){
  const text = decode(String(html||"")
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' '));   // decode entities so obfuscated emails (&#064; = @) are matchable
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
// "marcus-patel" / "marcus.patel" / "marcus_patel" -> {first,last}
function nameFromSlug(slug){
  const toks = String(slug||"").split(/[-_.+%20\s]+/).filter(t=>/^[a-z]+$/i.test(t));
  if(!toks.length) return {first:"",last:""};
  if(toks.length===1) return {first:properCase(toks[0]), last:""};
  return {first:properCase(toks[0]), last:properCase(toks[toks.length-1])};
}

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
  const last = normalizeForMatching(lastPathSeg(url));
  const parent = segs.length >= 2 ? normalizeForMatching(segs[segs.length-2]) : "";
  const pageText = normalizeForMatching(stripTags(html));
  const extraDirs = rules.names || new Set();
  const extraTerms = rules.terms || new Set();
  const termMap = rules.termMap || new Map();
  const personSlug = looksLikePersonSlug(last, genderMap);
  const urlPath = normalizeForMatching(new URL(url).pathname);

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
function findPosition(title, description){
  const hay = (title+" "+description).toLowerCase();
  for(const t of COMMON_TITLES){ if(hay.includes(t)) return properCase(t).replace(/\bCeo\b/,"CEO").replace(/\bVp\b/,"VP").replace(/\bCto\b/,"CTO"); }
  return "";
}

// ---------------------------------------------------------------- main
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
  const last = lastPathSeg(url);
  const { first, last: lastName } = isBio ? inferNameFromSlug(last, deps.genderMap) : { first:"", last:"" };

  const hrefs = anchors(html);

  // email(s): mailto only, like the original — precise, low junk
  const emails = []; const seenE = new Set();
  for(const h of hrefs){ if(/^mailto:/i.test(h)){ const e = decode(h.replace(/^mailto:/i,"").split("?")[0]);
    if(e && e.includes("@") && !/\.(png|jpg|gif)$/i.test(e) && !seenE.has(e)){ seenE.add(e); emails.push(e); } } }
  if(!emails.length){
    for(const e of extractEmailsFromText(html)){
      if(e && !/\.(png|jpg|gif)$/i.test(e) && !seenE.has(e)){
        seenE.add(e);
        emails.push(e);
      }
    }
  }
  const email = emails[0] || "";

  // linkedin(s): /in/ only, not /company/
  const lis = []; const seenL = new Set();
  for(const h of hrefs){ if(/linkedin\.com\/in\//i.test(h) && !/linkedin\.com\/company\//i.test(h)){
    const li = h.split("#")[0].split("?")[0]; if(!seenL.has(li)){ seenL.add(li); lis.push(li); } } }
  const linkedin = lis[0] || "";

  // google maps (first)
  const maps = hrefs.find(h => /google\.[^/]+\/maps/i.test(h)) || "";

  // phones from tel: links. SAME-PAGE RULE: only attach to a person on a BIO URL page.
  const tels = []; const seenT = new Set();
  for(const h of hrefs){ if(/^tel:/i.test(h)){ const t = h.replace(/^tel:/i,"").trim();
    if(t && !seenT.has(t)){ seenT.add(t); tels.push(t); } } }
  let phone="", phoneType="", phoneLocation="", phone2="", phone2Type="";
  if(isBio && tels.length && wireless){
    const cc = countryCodeFromDomain(getBaseDomain(url));   // country from the domain TLD (default US)
    const raw1 = tels[0];
    phoneType = classifyLineType(raw1, wireless).type;      // classify on the raw number…
    phoneLocation = phoneType === "Toll Free" ? "" : geocode(raw1);
    phone = toE164(raw1, cc);                               // …then standardize to E.164
    const raw2 = tels[1] || "";
    if(raw2){ phone2Type = classifyLineType(raw2, wireless).type; phone2 = toE164(raw2, cc); }
  }

  const description = (metaContent(html,"og:description") || metaContent(html,"description")).slice(0,300);
  let title = pageTitle(html);
  if(!title) title = titleFromUrlKeywords(url);   // fall back to a role inferred from the URL path
  const position = findPosition(title, description);
  const image = findImage(html, url);
  const gender = first ? (genderMap[first.toLowerCase()] || "") : "";

  // ---- QUALITY GATE ----
  if(!email) return null;                      // require a valid email address for every record

  return {
    "Time Stamp": timestamp,
    "Source": source,
    "Web Source URL": url,
    "Directory": directory,
    "ID": last || "",
    "Last Path": last || "",
    "Bio Check": isBio ? "Y" : "",
    "First": first,
    "Last": lastName,
    "Gender": gender,
    "Title": title,
    "Position": position,
    "Description": description,
    "Image URL": image,
    "Email Address": email,
    "Email Type": classifyEmail(email),
    "LinkedIn URL": linkedin,
    "Google Maps": maps,
    "Phone": phone,
    "Phone Type": phoneType,
    "Phone Location": phoneLocation,
    "Phone 2": phone2,
    "Phone 2 Type": phone2Type,
  };
}

// minimal area-code -> region fallback; replace with libphonenumber geocoder in production
const AREA_REGION = {212:"New York, NY",415:"San Francisco, CA",312:"Chicago, IL",404:"Atlanta, GA",
  617:"Boston, MA",305:"Miami, FL",206:"Seattle, WA",512:"Austin, TX",201:"Jersey City, NJ",214:"Dallas, TX"};
function defaultGeocode(phone){
  const d = String(phone).replace(/\D/g,""); const ten = d.length===11&&d[0]==="1"?d.slice(1):d;
  return AREA_REGION[Number(ten.slice(0,3))] || "";
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
  const hasPlus = String(raw).trim().startsWith("+");
  let digits = String(raw).replace(/\D/g, "");
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

module.exports = { extractRecord, classifyEmail, classifyDirectory, nameFromSlug, loadGenderMap, loadDirectoryRules };

// ---------------------------------------------------------------- self-test
if(require.main === module){
  const { loadWirelessBlocks } = require("./wireless-block-classifier");
  const wireless = loadWirelessBlocks(process.argv[2] || (__dirname + "/WIRELESS_BLOCKS.TXT"));
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
}
