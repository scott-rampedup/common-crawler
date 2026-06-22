/**
 * site-apis.js — per-site JSON-API adapters for large people-directories whose pages are
 * client-rendered (the crawler only ever sees an empty HTML shell, e.g. <div id="root">, so
 * scraping finds nothing). For a site worth the effort — the project rule of thumb is a
 * domain/sitemap with 500+ Bio URLs — we call the SAME public JSON API the site's own frontend
 * uses, look the person up by the id in their bio URL, then render a minimal bio HTML that
 * extractRecord() turns into a normal record.
 *
 * Rendering to HTML + reusing extractRecord (rather than building the record by hand) means ALL
 * the existing logic applies unchanged — name/gender split, phone classification incl.
 * sms: => Mobile, email cleaning, Last Path, the quality gate — and the output is identical in
 * shape to a scraped record, so the rest of the pipeline can't tell the difference.
 *
 * Add a site by pushing an adapter { name, match(url), fetchRecord(url, deps) } to ADAPTERS.
 */
const https = require("https");
const zlib = require("zlib");
const { extractRecord, getBaseDomain, nameFromSlug } = require("./extractor");
let HttpsProxyAgent = null;
try { ({ HttpsProxyAgent } = require("https-proxy-agent")); } catch { /* proxies unavailable -> direct only */ }

const DESKTOP_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";
// Any social/profile URL in a blob (API JSON or page HTML). Emitted into the synthetic bio HTML so
// extractRecord captures LinkedIn + Facebook/Twitter/WhatsApp (it drops the site's own brand handle).
const SOCIAL_RE = /https?:\/\/(?:[a-z0-9.-]+\.)?(?:facebook\.com|fb\.com|twitter\.com|x\.com|linkedin\.com|wa\.me|(?:api|web|chat)\.whatsapp\.com)\/[^"'\s<>\\)]+/gi;
const socialAnchors = (blob) => [...new Set(String(blob || "").match(SOCIAL_RE) || [])].slice(0, 30).map((s) => `<a href="${esc(s)}">s</a>`).join("");

// Outbound transports to try, cheapest-first: datacenter proxy, then residential, then direct.
// Many site APIs (e.g. century21's, behind Akamai) BLOCK datacenter IPs even though they serve the
// page shell — so the same NetNut proxy the live crawler uses is required for the API call too.
// Direct is the local-dev / last-resort path.
function apiTransports(){
  const t = [];
  if(HttpsProxyAgent && process.env.PROXY_URL) t.push(process.env.PROXY_URL);
  if(HttpsProxyAgent && process.env.PROXY_FALLBACK_URL) t.push(process.env.PROXY_FALLBACK_URL);
  t.push("");                                                  // direct
  return t;
}
let _apiFailLogged = false;

function getJsonVia(target, headers, proxyUrl, timeoutMs){
  return new Promise((resolve) => {
    let u; try{ u = new URL(target); }catch{ return resolve({ status: 0 }); }
    let agent;
    if(proxyUrl){ try{ agent = new HttpsProxyAgent(proxyUrl, { keepAlive: false }); }catch{ /* direct */ } }
    const req = https.request(u, {
      method: "GET", timeout: timeoutMs, agent,
      headers: { "User-Agent": DESKTOP_UA, "Accept": "application/json", ...headers },
    }, (res) => {
      const status = res.statusCode || 0;
      if(status !== 200){ res.resume(); return resolve({ status }); }
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => { try{ resolve({ status, json: JSON.parse(Buffer.concat(chunks).toString("utf8")) }); }catch{ resolve({ status, json: null }); } });
      res.on("error", () => resolve({ status: 0 }));
    });
    req.on("error", () => resolve({ status: 0 }));
    req.on("timeout", () => { req.destroy(); resolve({ status: 0 }); });
    req.end();
  });
}

// GET a JSON document, escalating across transports until one returns parseable JSON. Resolves to
// the parsed object, or null on total failure (never throws — the caller then falls back to scraping).
async function getJson(target, headers = {}, timeoutMs = 20000){
  let lastStatus = 0;
  for(const px of apiTransports()){
    const r = await getJsonVia(target, headers, px, timeoutMs);
    if(r.status === 200 && r.json != null) return r.json;
    lastStatus = r.status || lastStatus;
  }
  if(!_apiFailLogged){ _apiFailLogged = true; console.log(`Site API: all transports failed for ${String(target).split("?")[0]} (last HTTP ${lastStatus}) — falling back to scraping`); }
  return null;
}

// GET an HTML page (gzip-aware), one transport. For PAGE adapters (e.g. remax) the data is in the
// server-rendered HTML, so a normal fetch works — direct is cheapest, proxies are the fallback.
function getTextVia(target, proxyUrl, timeoutMs){
  return new Promise((resolve) => {
    let u; try{ u = new URL(target); }catch{ return resolve({ status: 0 }); }
    let agent;
    if(proxyUrl){ try{ agent = new HttpsProxyAgent(proxyUrl, { keepAlive: false }); }catch{ /* direct */ } }
    const req = https.request(u, {
      method: "GET", timeout: timeoutMs, agent,
      headers: { "User-Agent": DESKTOP_UA, "Accept": "text/html,application/xhtml+xml", "Accept-Encoding": "gzip, deflate, br" },
    }, (res) => {
      const status = res.statusCode || 0;
      if(status !== 200){ res.resume(); return resolve({ status }); }
      let stream = res; const enc = (res.headers["content-encoding"] || "").toLowerCase();
      try{
        if(enc === "gzip") stream = res.pipe(zlib.createGunzip());
        else if(enc === "deflate") stream = res.pipe(zlib.createInflate());
        else if(enc === "br") stream = res.pipe(zlib.createBrotliDecompress());
      }catch{ res.resume(); return resolve({ status: 0 }); }
      const chunks = []; let bytes = 0;
      stream.on("data", (c) => { bytes += c.length; if(bytes <= 8_000_000) chunks.push(c); else req.destroy(); });
      stream.on("end", () => resolve({ status, text: Buffer.concat(chunks).toString("utf8") }));
      stream.on("error", () => resolve({ status: 0 }));
    });
    req.on("error", () => resolve({ status: 0 }));
    req.on("timeout", () => { req.destroy(); resolve({ status: 0 }); });
    req.end();
  });
}
async function getText(target, timeoutMs = 25000){
  const order = [""];                                          // direct first (cheap), then proxies
  if(HttpsProxyAgent && process.env.PROXY_URL) order.push(process.env.PROXY_URL);
  if(HttpsProxyAgent && process.env.PROXY_FALLBACK_URL) order.push(process.env.PROXY_FALLBACK_URL);
  for(const px of order){ const r = await getTextVia(target, px, timeoutMs); if(r.status === 200 && r.text) return r.text; }
  return "";
}

// Pull the first schema.org RealEstateAgent/Person object out of a page's JSON-LD (<script
// type="application/ld+json">). Returns the object (with name/email/telephone/address/image) or null.
function parseAgentLd(html){
  const re = /<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while((m = re.exec(String(html || "")))){
    let j; try{ j = JSON.parse(m[1].trim()); }catch{ continue; }
    const list = Array.isArray(j) ? j : (j && Array.isArray(j["@graph"]) ? j["@graph"] : [j]);
    for(const o of list){
      const t = o && o["@type"];
      const types = Array.isArray(t) ? t.join(",") : String(t || "");
      if(/RealEstateAgent|Person/i.test(types) && o.name) return o;
    }
  }
  return null;
}

const esc = (s) => String(s == null ? "" : s)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

function hostMatches(url, root){
  try{ return getBaseDomain(url) === root; }catch{ return false; }
}

// ---------------------------------------------------------------- century21.com
// The C21 React app calls GET /api/agents/{id}?brand=C21 with a public x-api-key (lifted from
// their frontend bundle); {id} is the "aid-…" segment of the bio URL. data.result carries
// fullName, email, cellPhone (mobile), businessPhone, profilePhoto, bio, agentLicenseType, etc.
const C21_API_KEY = process.env.C21_API_KEY || "svbyT7C7Hw7d8D7GxJsi";   // public read key
function c21AgentId(url){ const m = String(url || "").match(/\/aid-([A-Za-z0-9]+)/i); return m ? m[1] : ""; }
// "City, Region, Country" from the agent's office / primary place — more accurate than the
// phone's area code, so it populates the Location field directly.
function c21Location(r){
  const a = ((r.offices && r.offices[0]) || {}).address || {};
  const city = (r.primaryPlace && r.primaryPlace.displayName) || a.city || "";
  return [city, a.stateName || a.state || "", a.countryName || ""].filter(Boolean).join(", ");
}
const century21 = {
  name: "century21",
  match: (url) => hostMatches(url, "century21.com") && !!c21AgentId(url),
  async fetchRecord(url, deps = {}){
    const id = c21AgentId(url);
    if(!id) return null;
    const api = `https://www.century21.com/api/agents/${encodeURIComponent(id)}?brand=C21`;
    const fetchJson = deps._getJson || getJson;             // injectable for offline tests
    const j = await fetchJson(api, { "x-api-key": C21_API_KEY });
    const r = j && j.data && j.data.result;
    if(!r || !r.fullName) return null;
    const parts = [`<h1>${esc(r.fullName)}</h1>`];
    if(r.email) parts.push(`<a href="mailto:${esc(r.email)}">email</a>`);
    // Emit the CELL first (as tel: + sms:) so the Mobile number becomes the primary Phone, then
    // the business line. extractRecord de-dupes a shared number and the sms: link marks it Mobile.
    if(r.cellPhone) parts.push(`<a href="tel:${esc(r.cellPhone)}">call</a><a href="sms:${esc(r.cellPhone)}">text</a>`);
    if(r.businessPhone && r.businessPhone !== r.cellPhone) parts.push(`<a href="tel:${esc(r.businessPhone)}">office</a>`);
    // LinkedIn: a personal /in/ profile only (extractRecord drops /company/). socialMedia's shape
    // varies, so scan the whole result for a linkedin.com/in URL.
    const li = (JSON.stringify(r).match(/https?:\/\/[^"\\\s]*linkedin\.com\/in\/[^"\\\s]+/i) || [])[0] || "";
    if(li) parts.push(`<a href="${esc(li)}">linkedin</a>`);
    parts.push(socialAnchors(JSON.stringify(r)));     // facebook / twitter / whatsapp from the API result
    if(r.bio) parts.push(`<meta property="og:description" content="${esc(String(r.bio).slice(0, 300))}">`);
    if(r.profilePhoto) parts.push(`<meta property="og:image" content="${esc(r.profilePhoto)}">`);
    const html = `<!doctype html><html><head><title>${esc(r.fullName)}</title>${parts.join("")}</head><body></body></html>`;
    const rec = extractRecord(html, url, { allowNoEmail: true, ...deps, source: deps.source || "Site API" });
    if(!rec) return null;
    // The API knows the contact's REAL name — prefer it over the name parsed from the URL slug.
    const nm = nameFromSlug(String(r.fullName).replace(/\s+/g, "-"));
    if(nm.first && nm.last){
      rec["First"] = nm.first; rec["Last"] = nm.last;
      const g = (deps.genderMap || {})[nm.first.toLowerCase()]; if(g) rec["Gender"] = g;
    }
    const loc = c21Location(r);                              // office/place beats the phone area code
    if(loc) rec["Phone Location"] = loc;
    return rec;
  },
};

// ---------------------------------------------------------------- remax.com
// remax agent pages are SERVER-RENDERED HTML whose bio URL slug is polluted with city/state
// (e.g. /real-estate-agents/anne-mohr-newport-beach-ca/100071923 — the generic name/bio detector
// chokes on it). But each page embeds a schema.org RealEstateAgent JSON-LD block with the CLEAN
// name, email, telephone, image and postal address — so we fetch the page and read that.
function remaxAgentId(url){ const m = String(url || "").match(/\/real-estate-agents\/[^/]+\/(\d{3,})/i); return m ? m[1] : ""; }

const US_STATES = new Set("al ak az ar ca co ct de dc fl ga hi id il in ia ks ky la me md ma mi mn ms mo mt ne nv nh nj nm ny nc nd oh ok or pa ri sc sd tn tx ut vt va wa wv wi wy".split(" "));
// The remax slug is "<first>-<last>-<city>-<state>". Given the agent's last name, return the City +
// State that TRAIL it (state = the 2-letter last token, validated against US states). This is more
// reliable than the JSON-LD postal address, which falls back to RE/MAX HQ (Denver, CO) when the
// agent's office isn't listed. We anchor on the last-name token so a multi-word city (e.g. Newport
// Beach) or extra name parts don't throw it off. Returns "City, ST" or "".
const normTok = (t) => String(t).normalize("NFD").toLowerCase().replace(/[^a-z0-9]/g, "");   // accent/punctuation-insensitive
function cityStateFromSlug(slug, first, last){
  const raw = String(slug || "").split("-").map((t) => t.trim()).filter(Boolean);
  if(raw.length < 3) return "";
  const tok = raw.map(normTok);                                // accent/punctuation-insensitive tokens
  const state = tok[tok.length - 1];
  if(!US_STATES.has(state)) return "";
  const firstWords = String(first || "").split(/[\s-]+/).filter(Boolean).map(normTok);
  const lastWords = String(last || "").split(/[\s-]+/).filter(Boolean).map(normTok);
  let cityStart = -1;
  // 1) the full name (first + last) as a prefix of the slug -> city is whatever follows. Handles
  //    correct multi-token names (e.g. "Mary Jane Watson") and accents/punctuation.
  const nameTok = [...firstWords, ...lastWords];
  if(nameTok.length){
    let i = 0; while(i < nameTok.length && i < tok.length - 1 && tok[i] === nameTok[i]) i++;
    if(i === nameTok.length) cityStart = i;
  }
  // 2) fallback: the slug is first-last-city-state, so the city is everything after the first TWO
  //    tokens. We do NOT anchor on the stored last name — for pages with no clean JSON-LD name it
  //    was mis-derived from the slug (a city word like "Dunkirk"), which would land past the city.
  if(cityStart < 0) cityStart = Math.min(2, tok.length - 1);
  const cityTokens = raw.slice(cityStart, raw.length - 1);
  if(!cityTokens.length) return "";
  const city = cityTokens.map((t) => t.charAt(0).toUpperCase() + t.slice(1).toLowerCase()).join(" ");
  return city + ", " + state.toUpperCase();
}
function remaxLocationFromUrl(url, first, last){
  const m = String(url || "").match(/\/real-estate-agents\/([^/?#]+)\/\d/i);
  return m ? cityStateFromSlug(m[1], first, last) : "";
}

const remax = {
  name: "remax",
  match: (url) => hostMatches(url, "remax.com") && !!remaxAgentId(url),
  async fetchRecord(url, deps = {}){
    const html = await (deps._getText || getText)(url);        // engine page-fetcher in prod; getText standalone
    if(!html) return null;
    const ld = parseAgentLd(html);
    if(!ld || !ld.name) return null;
    const parts = [`<h1>${esc(ld.name)}</h1>`];
    if(ld.email) parts.push(`<a href="mailto:${esc(ld.email)}">email</a>`);
    if(ld.telephone) parts.push(`<a href="tel:${esc(ld.telephone)}">call</a>`);
    if(ld.image) parts.push(`<meta property="og:image" content="${esc(ld.image)}">`);
    parts.push(socialAnchors(html));                  // agent's facebook/twitter/whatsapp/linkedin from the page
    const synth = `<!doctype html><html><head><title>${esc(ld.name)}</title>${parts.join("")}</head><body></body></html>`;
    const rec = extractRecord(synth, url, { allowNoEmail: true, ...deps, source: deps.source || "Site API" });
    if(!rec) return null;
    // The JSON-LD name is the clean one — the URL slug has city/state mixed in, so prefer JSON-LD.
    const nm = nameFromSlug(String(ld.name).replace(/\s+/g, "-"));
    if(nm.first && nm.last){
      rec["First"] = nm.first; rec["Last"] = nm.last;
      const g = (deps.genderMap || {})[nm.first.toLowerCase()]; if(g) rec["Gender"] = g;
    }
    // Location: prefer the City/State in the slug (the agent's real market) over the JSON-LD
    // postal address, which is often the RE/MAX HQ (Denver, CO) when no office is listed.
    const a = ld.address || {};
    const ldLoc = [a.addressLocality, a.addressRegion, a.addressCountry].filter(Boolean).join(", ");
    const slugLoc = remaxLocationFromUrl(url, rec["First"], rec["Last"]);
    if(slugLoc || ldLoc) rec["Phone Location"] = slugLoc || ldLoc;
    return rec;
  },
};

const ADAPTERS = [century21, remax];

// The adapter whose site handles this URL, or null. (try/catch so a bad matcher never breaks a crawl)
function findSiteApi(url){
  for(const a of ADAPTERS){ try{ if(a.match(url)) return a; }catch{ /* ignore */ } }
  return null;
}

module.exports = { findSiteApi, ADAPTERS, getJson, remaxLocationFromUrl, _century21: century21 };

// ---- offline self-test: node site-apis.js --selftest (mocks the API via deps._getJson) ----
if(require.main === module && process.argv.includes("--selftest")){
  const { loadWirelessBlocks } = require("./wireless-block-classifier");
  const wireless = loadWirelessBlocks(__dirname + "/WIRELESS_BLOCKS.TXT");
  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? "✓" : "✗") + " " + l); c ? p++ : f++; };
  (async () => {
    const url = "https://www.century21.com/agent/detail/nj/medford/agents/agnes-aaron/aid-P00200000FZGd1opBBuOqLprx9TUD7AZamyzZbCg";
    const fix = (result) => async () => ({ data: { result } });

    ok("findSiteApi matches a c21 aid url", !!findSiteApi(url) && findSiteApi(url).name === "century21");
    ok("findSiteApi rejects a c21 url with no aid", !findSiteApi("https://www.century21.com/agent/detail/nj/medford"));
    ok("findSiteApi rejects a non-c21 url", !findSiteApi("https://example.com/agent/detail/x/aid-ABC123DEF456"));

    // shared cell + business number => one Mobile phone, no Phone 2; Last Path = name, not aid-
    const rec = await century21.fetchRecord(url, { wireless, genderMap: { agnes:"F" }, allowNoEmail: true,
      _getJson: fix({ fullName:"Agnes Aaron", email:"aglong@verizon.net", cellPhone:"+16094136297", businessPhone:"+16094136297", profilePhoto:"https://img/x.jpg", bio:"Realtor in Medford." }) });
    ok("adapter builds a record", !!rec);
    ok("name from API fullName", rec && rec["First"] === "Agnes" && rec["Last"] === "Aaron");
    ok("email captured + classified", rec && rec["Email Address"] === "aglong@verizon.net");
    ok("cellPhone => Phone Type Mobile", rec && rec["Phone"] === "+16094136297" && rec["Phone Type"] === "Mobile");
    ok("shared cell+business => no Phone 2", rec && !rec["Phone 2"]);
    ok("Last Path = name segment, not aid- id", rec && rec["Last Path"] === "Agnes Aaron");
    ok("image + bio carried through", rec && rec["Image URL"] === "https://img/x.jpg" && /Realtor in Medford/.test(rec["Description"]));

    // distinct cell + business => cell is the Mobile primary, business becomes Phone 2
    const rec2 = await century21.fetchRecord(url, { wireless, genderMap: { jane:"F" }, allowNoEmail: true,
      _getJson: fix({ fullName:"Jane Roe", email:"j@x.com", cellPhone:"+16094136297", businessPhone:"+12015550182" }) });
    ok("distinct business number => Phone 2",
      rec2 && rec2["Phone"] === "+16094136297" && rec2["Phone Type"] === "Mobile" && rec2["Phone 2"] === "+12015550182");

    // LinkedIn: a personal /in/ profile from socialMedia is mapped; a /company/ page is not
    const recLi = await century21.fetchRecord(url, { wireless, genderMap: {}, allowNoEmail: true,
      _getJson: fix({ fullName:"Agnes Aaron", email:"a@c21.com",
        socialMedia:[{ platform:"LinkedIn", url:"https://www.linkedin.com/in/agnes-aaron-12345" }] }) });
    ok("maps a linkedin.com/in profile from the API", recLi && recLi["LinkedIn URL"] === "https://www.linkedin.com/in/agnes-aaron-12345");
    const recCo = await century21.fetchRecord(url, { wireless, genderMap: {}, allowNoEmail: true,
      _getJson: fix({ fullName:"Agnes Aaron", email:"a@c21.com", socialMedia:[{ url:"https://www.linkedin.com/company/century-21" }] }) });
    ok("does NOT map a linkedin.com/company page", recCo && !recCo["LinkedIn URL"]);

    // real First/Last from fullName overrides a different URL-slug name; gender re-derived
    const bobUrl = "https://www.century21.com/agent/detail/tx/austin/agents/bob-smith/aid-P002Z";
    const recName = await century21.fetchRecord(bobUrl, { wireless, genderMap: { robert:"M" }, allowNoEmail: true,
      _getJson: fix({ fullName:"Robert Smith", email:"r@c21.com" }) });
    ok("API fullName overrides the URL-slug name", recName && recName["First"] === "Robert" && recName["Last"] === "Smith");
    ok("gender re-derived for the corrected first name", recName && recName["Gender"] === "M");

    // Location assumed from the office / primary place (not the phone area code)
    const recLoc = await century21.fetchRecord(url, { wireless, genderMap: {}, allowNoEmail: true,
      _getJson: fix({ fullName:"Agnes Aaron", email:"a@c21.com", cellPhone:"+16094136297",
        primaryPlace:{ displayName:"Medford" },
        offices:[{ address:{ city:"Medford", stateName:"New Jersey", countryName:"United States" } }] }) });
    ok("Location assumed from API office/place", recLoc && recLoc["Phone Location"] === "Medford, New Jersey, United States");

    // a missing/empty API result must not produce a record (falls back to scraping upstream)
    const none = await century21.fetchRecord(url, { wireless, _getJson: async () => null });
    ok("null API response => null record (graceful fallback)", none === null);

    // ---- remax: clean name/email/phone/location from the page's RealEstateAgent JSON-LD ----
    const remaxUrl = "https://www.remax.com/real-estate-agents/anne-mohr-newport-beach-ca/100071923";
    ok("findSiteApi routes a remax /real-estate-agents/<slug>/<id> url", findSiteApi(remaxUrl) && findSiteApi(remaxUrl).name === "remax");
    ok("findSiteApi rejects a remax url with no numeric id", !findSiteApi("https://www.remax.com/real-estate-agents/anne-mohr-newport-beach-ca"));
    const remaxHtml = '<html><head><script type="application/ld+json">' + JSON.stringify({
      "@type": "RealEstateAgent", name: "Anne Mohr", email: "amohr@remax.net", telephone: "(949) 887-1836",
      image: "https://img/x.jpg",
      address: { "@type": "PostalAddress", addressLocality: "Newport Beach", addressRegion: "CA", addressCountry: "US" },
    }) + '</script></head><body></body></html>';
    const recRm = await remax.fetchRecord(remaxUrl, { wireless, genderMap: { anne:"F" }, _getText: async () => remaxHtml });
    ok("remax: clean First/Last from JSON-LD (not the polluted slug)", recRm && recRm["First"] === "Anne" && recRm["Last"] === "Mohr");
    ok("remax: email from JSON-LD", recRm && recRm["Email Address"] === "amohr@remax.net");
    ok("remax: phone normalized from JSON-LD telephone", recRm && recRm["Phone"] === "+19498871836");
    const recRmFb = await remax.fetchRecord("https://www.remax.com/real-estate-agents/anne-mohr/100071923", { wireless, genderMap: {}, _getText: async () => remaxHtml });
    ok("remax: Location falls back to JSON-LD address when slug has no city/state", recRmFb && recRmFb["Phone Location"] === "Newport Beach, CA, US");
    const recRmNone = await remax.fetchRecord(remaxUrl, { wireless, _getText: async () => "<html><body>no json-ld</body></html>" });
    ok("remax: no JSON-LD => null (falls back to scraping)", recRmNone === null);
    ok("remax: Location from slug overrides JSON-LD HQ", recRm && recRm["Phone Location"] === "Newport Beach, CA");
    ok("remax slug parser: multi-word city + last-name==city word", remaxLocationFromUrl("https://www.remax.com/real-estate-agents/john-hill-chapel-hill-nc/123", "John", "Hill") === "Chapel Hill, NC");

    console.log(`\nsite-apis self-test: ${p} passed, ${f} failed`);
    process.exit(f ? 1 : 0);
  })().catch((e) => { console.error("FAILED:", e); process.exit(1); });
}
