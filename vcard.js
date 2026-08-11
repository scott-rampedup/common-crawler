/**
 * vcard.js — read .vcf vCard files linked from a bio page and merge their fields into the
 * contact record. extractRecord() captures the .vcf URL into the record's "vCard" field (cheap,
 * no fetch); then on ingestion enrichRecords() downloads each card and applyVCardToRecord() fills
 * in the contact's First/Last, Email, Title, and — importantly for this project — phones, where
 * TEL;TYPE=CELL becomes a **Mobile** number.
 *
 * Existing non-blank fields are preserved (the vCard augments, never clobbers), except that an
 * existing phone is UPGRADED to "Mobile" when the card marks that same number as a cell.
 */
const https = require("https");
const http = require("http");
const { cleanEmail, classifyEmail, toE164, getBaseDomain, countryCodeFromDomain, splitExtension, nameFromSlug } = require("./extractor");

const DESKTOP_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
const digits = (s) => String(s || "").replace(/\D/g, "");

// GET a text document (a .vcf), following a couple of redirects. Resolves to the body, or ""
// on any failure (a missing/blocked card just means no enrichment — never throws).
function fetchText(target, timeoutMs = 15000, rediromsLeft = 3){
  return new Promise((resolve) => {
    let u; try{ u = new URL(target); }catch{ return resolve(""); }
    if(u.protocol !== "http:" && u.protocol !== "https:") return resolve("");
    const lib = u.protocol === "http:" ? http : https;
    const req = lib.request(u, {
      method: "GET", timeout: timeoutMs,
      headers: { "User-Agent": DESKTOP_UA, "Accept": "text/vcard, text/x-vcard, text/directory, text/plain, */*" },
    }, (res) => {
      const status = res.statusCode || 0;
      if(status >= 300 && status < 400 && res.headers.location && rediromsLeft > 0){
        res.resume();
        let next; try{ next = new URL(res.headers.location, u).href; }catch{ return resolve(""); }
        return resolve(fetchText(next, timeoutMs, rediromsLeft - 1));
      }
      if(status !== 200){ res.resume(); return resolve(""); }
      const chunks = []; let bytes = 0;
      res.on("data", (c) => { bytes += c.length; if(bytes <= 1_000_000) chunks.push(c); else req.destroy(); });
      res.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      res.on("error", () => resolve(""));
    });
    req.on("error", () => resolve(""));
    req.on("timeout", () => { req.destroy(); resolve(""); });
    req.end();
  });
}

// vCard escaping: \n \, \; \\ -> their literals.
function unesc(s){ return String(s || "").replace(/\\n/gi, " ").replace(/\\([,;\\])/g, "$1").trim(); }

// Parse a .vcf into { fn, first, last, org, title, url, emails[], phones[{number,mobile,work}] }.
// Handles line folding (a line starting with space/tab continues the prior one), item-group
// prefixes (item1.TEL), bare and TYPE= params, and the first card only.
function parseVCard(text){
  const out = { fn: "", first: "", last: "", org: "", title: "", url: "", emails: [], phones: [], adr: null };
  let body = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").replace(/\n[ \t]/g, "");  // unfold
  const card = body.match(/BEGIN:VCARD([\s\S]*?)END:VCARD/i);
  for(const raw of (card ? card[1] : body).split("\n")){
    const line = raw.trim();
    if(!line) continue;
    const ci = line.indexOf(":");
    if(ci < 0) continue;
    const left = line.slice(0, ci);
    const value = line.slice(ci + 1).trim();
    const segs = left.split(";");
    const prop = segs[0].toUpperCase().replace(/^ITEM\d+\./i, "");   // strip item-group prefix
    const types = [];
    for(const p of segs.slice(1)){
      const eq = p.indexOf("=");
      if(eq < 0) types.push(p);                                       // bare type (vCard 2.1: TEL;CELL:)
      else if(/^TYPE$/i.test(p.slice(0, eq))) types.push(...p.slice(eq + 1).replace(/"/g, "").split(","));
    }
    const typeStr = types.join(",").toLowerCase();
    if(prop === "FN") out.fn = unesc(value);
    else if(prop === "N"){ const n = value.split(";"); out.last = unesc(n[0] || ""); out.first = unesc(n[1] || ""); }
    else if(prop === "ORG"){ if(!out.org) out.org = unesc(value.split(";")[0] || ""); }
    else if(prop === "TITLE"){ if(!out.title) out.title = unesc(value); }
    else if(prop === "URL"){ if(!out.url) out.url = value.replace(/^tel:/i, "").trim(); }
    else if(prop === "EMAIL"){ const e = value.trim(); if(e) out.emails.push(e); }
    else if(prop === "TEL"){
      if(/fax/.test(typeStr)) continue;                               // skip fax lines
      const number = value.replace(/^tel:/i, "").trim();
      if(number) out.phones.push({ number, mobile: /cell|mobile|iphone/.test(typeStr), work: /work/.test(typeStr) });
    }
    else if(prop === "ADR"){                                          // PObox;ext;street;city;region;postal;country
      const a = value.split(";");
      const adr = { city: unesc(a[3] || ""), region: unesc(a[4] || ""), country: unesc(a[6] || "") };
      if(!out.adr || /work/.test(typeStr)) out.adr = adr;             // prefer a WORK address
    }
  }
  // derive First/Last from FN when N was absent
  if((!out.first || !out.last) && out.fn){
    const nm = nameFromSlug(out.fn.replace(/\s+/g, "-"));
    out.first = out.first || nm.first || "";
    out.last = out.last || nm.last || "";
  }
  return out;
}

// Merge a parsed vCard into a record (in place). Fills blanks; upgrades an existing phone to
// Mobile when the card marks that number as a cell. Returns the record. `applied` flag tells the
// caller whether anything changed.
function applyVCardToRecord(record, v, opts = {}){
  const snap = () => JSON.stringify(["First", "Last", "Gender", "Email Address", "Phone", "Phone Type", "Phone 2", "Title", "Phone Location"].map((f) => record[f]));
  const before = snap();
  const cc = opts.cc || countryCodeFromDomain(getBaseDomain(record["Web Source URL"] || ("https://" + (record["Domain"] || "")))) || "1";
  const blank = (f) => !String(record[f] || "").trim();

  // Name: a COMPLETE name from the card is the person's real name, so it wins over a name parsed
  // from a page heading or URL slug. Re-derive Gender for the corrected first name when we can.
  if(v.first && v.last){
    record["First"] = v.first; record["Last"] = v.last;
    const g = (opts.genderMap || {})[v.first.toLowerCase()]; if(g) record["Gender"] = g;
  } else {
    if(blank("First") && v.first) record["First"] = v.first;
    if(blank("Last") && v.last) record["Last"] = v.last;
  }
  if(blank("Email Address") && v.emails.length){
    const e = cleanEmail(v.emails[0]);
    if(e){ record["Email Address"] = e; record["Email Type"] = classifyEmail(e); }
  }
  if(blank("Title") && v.title) record["Title"] = v.title;
  if(blank("Position") && v.title) record["Position"] = v.title;
  if(v.adr){                                                          // address on the card beats the phone area code
    const loc = [v.adr.city, v.adr.region, v.adr.country].filter(Boolean).join(", ");
    if(loc) record["Phone Location"] = loc;
  }

  // normalize the card's phones to E.164 + a type, mobile first, de-duped
  const phones = [];
  for(const p of v.phones){
    const e164 = toE164(splitExtension(p.number).main, cc);
    if(!e164 || phones.some((x) => x.e164 === e164)) continue;
    phones.push({ e164, type: p.mobile ? "Mobile" : (p.work ? "Direct" : "") });
  }
  phones.sort((a, b) => (b.type === "Mobile" ? 1 : 0) - (a.type === "Mobile" ? 1 : 0));

  if(phones.length){
    const curMain = digits(splitExtension(record["Phone"] || "").main);
    // upgrade an existing number to Mobile if the card says it's a cell
    if(curMain){
      const hit = phones.find((p) => digits(p.e164) === curMain || digits(p.e164).endsWith(curMain) || curMain.endsWith(digits(p.e164)));
      if(hit && hit.type === "Mobile" && record["Phone Type"] !== "Mobile") record["Phone Type"] = "Mobile";
    }
    if(blank("Phone")){ record["Phone"] = phones[0].e164; if(phones[0].type) record["Phone Type"] = phones[0].type; }
    // fill Phone 2 with a distinct card number when empty
    if(blank("Phone 2")){
      const main = digits(splitExtension(record["Phone"] || "").main);
      const other = phones.find((p) => digits(p.e164) !== main);
      if(other){ record["Phone 2"] = other.e164; if(other.type) record["Phone 2 Type"] = other.type; }
    }
  }
  return snap() !== before;
}

// Download + apply the vCard for every record that carries a "vCard" URL. Mutates records in
// place; bounded concurrency so a big job doesn't open thousands of sockets. Returns a small
// summary. _fetch is injectable for offline tests.
const TIMEOUT = Symbol("vcard-timeout");
async function enrichRecords(records, opts = {}){
  const list = (records || []).filter((r) => r && /^https?:\/\//i.test(String(r["vCard"] || "")));
  const fetchOne = opts._fetch || fetchText;
  const concurrency = Math.max(1, opts.concurrency || 8);
  const cap = opts.max || Number(process.env.VCARD_MAX) || 5000;     // safety ceiling per pass
  const todo = list.slice(0, cap);
  // HARD per-card ceiling. The built-in fetchText already times out, but callers inject their own fetcher
  // (extract-from-pointers passes ccEngine.fetchDoc, which goes through the proxy), and an injected
  // fetcher that hangs used to hang this function forever: enrichRecords is awaited inside the extract
  // flush, which is awaited by the fetch workers, so ONE wedged proxy socket stalled an entire run --
  // observed on the corp-prospects batch, which froze at 25,000 pages having written nothing. Never trust
  // an injected fetcher to be bounded, and never let one bad card take the batch down.
  const timeoutMs = Math.max(1000, Number(opts.timeoutMs) || Number(process.env.VCARD_TIMEOUT_MS) || 10000);
  let fetched = 0, applied = 0, cursor = 0, timedOut = 0, errors = 0;
  const fetchBounded = async (u) => {
    let timer;
    try {
      return await Promise.race([
        Promise.resolve(fetchOne(u)),
        // resolve (not reject) a sentinel: a rejection here would be an unhandled rejection for the
        // loser of the race in some Node versions, and a timeout isn't an error worth throwing.
        new Promise((resolve) => { timer = setTimeout(() => resolve(TIMEOUT), timeoutMs); }),
      ]);
    } finally { clearTimeout(timer); }
  };
  async function worker(){
    while(cursor < todo.length){
      const r = todo[cursor++];
      let text;
      try { text = await fetchBounded(String(r["vCard"])); }
      catch { errors++; continue; }                       // injected fetchers may throw; one card, not the batch
      if(text === TIMEOUT){ timedOut++; continue; }
      if(!text || !/BEGIN:VCARD/i.test(text)) continue;
      fetched++;
      try{ if(applyVCardToRecord(r, parseVCard(text), opts)) applied++; }catch{ /* skip bad card */ }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, todo.length) }, worker));
  if(list.length > cap) console.log(`vCard: capped at ${cap} of ${list.length} cards (raise VCARD_MAX to do more).`);
  if(timedOut || errors) console.log(`vCard: ${timedOut} timed out (>${timeoutMs}ms), ${errors} failed — skipped, batch continued.`);
  return { withVcard: list.length, fetched, applied, timedOut, errors };
}

module.exports = { parseVCard, applyVCardToRecord, enrichRecords, fetchText };

// ---- offline self-test: node vcard.js --selftest ----
if(require.main === module && process.argv.includes("--selftest")){
  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? "✓" : "✗") + " " + l); c ? p++ : f++; };
  const CARD = [
    "BEGIN:VCARD", "VERSION:3.0", "N:Aaron;Agnes;;;", "FN:Agnes Aaron",
    "ORG:CENTURY 21 Alliance", "TITLE:Sales Associate",
    "ADR;TYPE=WORK:;;400 Stokes Rd;Medford;NJ;08055;USA",
    "TEL;TYPE=CELL:+1-609-413-6297", "TEL;TYPE=WORK,VOICE:(609) 654-8797",
    "TEL;TYPE=FAX:+1-609-555-0000", "EMAIL;TYPE=INTERNET:aglong@verizon.net", "END:VCARD",
  ].join("\r\n");
  const url = "https://www.century21.com/agent/detail/nj/medford/agents/agnes-aaron/aid-X";

  const v = parseVCard(CARD);
  ok("parse: N -> first/last", v.first === "Agnes" && v.last === "Aaron");
  ok("parse: TITLE + ORG", v.title === "Sales Associate" && /CENTURY 21/.test(v.org));
  ok("parse: EMAIL", v.emails[0] === "aglong@verizon.net");
  ok("parse: keeps CELL + WORK, drops FAX", v.phones.length === 2 && v.phones.some((x) => x.mobile) && v.phones.some((x) => x.work));
  ok("parse: ADR -> city/region/country", v.adr && v.adr.city === "Medford" && v.adr.region === "NJ" && v.adr.country === "USA");

  // fill an empty record: cell => Mobile primary, work => Phone 2 Direct, Location from ADR, gender from name
  const r1 = { "Web Source URL": url };
  applyVCardToRecord(r1, v, { genderMap: { agnes:"F" } });
  ok("apply: name filled", r1["First"] === "Agnes" && r1["Last"] === "Aaron");
  ok("apply: gender derived from card name", r1["Gender"] === "F");
  ok("apply: email filled + classified", r1["Email Address"] === "aglong@verizon.net" && r1["Email Type"]);
  ok("apply: title/position filled", r1["Title"] === "Sales Associate" && r1["Position"] === "Sales Associate");
  ok("apply: Location assumed from ADR", r1["Phone Location"] === "Medford, NJ, USA");
  ok("apply: cell => Phone Mobile", r1["Phone"] === "+16094136297" && r1["Phone Type"] === "Mobile");
  ok("apply: work => Phone 2 Direct", r1["Phone 2"] === "+16096548797" && r1["Phone 2 Type"] === "Direct");

  // a COMPLETE card name overrides a different existing name (the card is authoritative); gender
  // is re-derived, the existing phone is upgraded to Mobile, and Phone 2 is filled
  const r2 = { "Web Source URL": url, "First": "Aggie", "Last": "A", "Gender": "", "Phone": "+16094136297", "Phone Type": "Direct" };
  applyVCardToRecord(r2, v, { genderMap: { agnes:"F" } });
  ok("apply: complete card name overrides existing name", r2["First"] === "Agnes" && r2["Last"] === "Aaron");
  ok("apply: gender re-derived on name override", r2["Gender"] === "F");
  ok("apply: existing phone upgraded to Mobile (card says cell)", r2["Phone Type"] === "Mobile");
  ok("apply: distinct work number fills empty Phone 2", r2["Phone 2"] === "+16096548797");

  // a PARTIAL card name (first only, no last) must NOT clobber an existing full name
  const vPartial = parseVCard("BEGIN:VCARD\r\nFN:Bob\r\nEMAIL:b@x.com\r\nEND:VCARD");
  const r3 = { "Web Source URL": url, "First": "Robert", "Last": "Smith" };
  applyVCardToRecord(r3, vPartial);
  ok("apply: partial card name does not clobber a full existing name", r3["First"] === "Robert" && r3["Last"] === "Smith");

  (async () => {
    // enrichRecords downloads + applies for records carrying a vCard URL (fetch injected)
    const recs = [
      { "Web Source URL": url, "vCard": "https://x/agnes.vcf" },
      { "Web Source URL": url, "vCard": "" },                       // no card -> untouched
    ];
    const sum = await enrichRecords(recs, { _fetch: async () => CARD });
    ok("enrich: only records with a vCard URL are fetched", sum.withVcard === 1 && sum.fetched === 1 && sum.applied === 1);
    ok("enrich: card data merged into the record", recs[0]["Phone Type"] === "Mobile" && recs[0]["Email Address"] === "aglong@verizon.net");
    ok("enrich: record without a vCard URL untouched", !recs[1]["Phone"]);
    console.log(`\nvcard self-test: ${p} passed, ${f} failed`);
    process.exit(f ? 1 : 0);
  })();
}
