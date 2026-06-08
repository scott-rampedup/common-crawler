/**
 * cc-engine.js  —  Phase 1: the Common Crawl engine
 * -------------------------------------------------------------
 * CSV of domains  ->  CDX index lookup  ->  pick candidate pages  ->
 * fetch the archived HTML (WARC byte-range)  ->  extractRecord()  ->  results CSV.
 *
 * This is the first phase that touches real outside data. It answers the
 * make-or-break question: is Common Crawl's coverage good enough on YOUR domains?
 *
 * Network: uses Node's built-in fetch + zlib (Node 18+; tested on 22). No installs.
 * Politeness: serial, single-threaded, with a delay between requests — Common Crawl
 * explicitly asks you not to hammer the index server.
 *
 * Run a real job:   node cc-engine.js domains.csv
 * Run offline tests: node cc-engine.js --selftest
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const zlib = require("zlib");
const http = require("http");
const https = require("https");
const { extractRecord, classifyDirectory, loadGenderMap, loadDirectoryRules } = require("./extractor");
const { loadWirelessBlocks } = require("./wireless-block-classifier");

const INDEX = "https://index.commoncrawl.org";
const DATA  = "https://data.commoncrawl.org";
const CRAWL = "CC-MAIN-2026-21";                 // latest monthly crawl; combine several for coverage
const UA = "RampedUp-CC-Engine/0.1 (https://rampedup.io; contact@rampedup.io)";
const sleep = ms => new Promise(r => setTimeout(r, ms));

const proxyEnv = process.env.HTTPS_PROXY || process.env.https_proxy || process.env.HTTP_PROXY || process.env.http_proxy || null;
const noProxyEnv = process.env.NO_PROXY || process.env.no_proxy || "";
let ProxyAgent, undiciFetch;
try {
  const undici = require("undici");
  ProxyAgent = undici.ProxyAgent;
  undiciFetch = undici.fetch;
} catch (error) {
  ProxyAgent = undefined;
  undiciFetch = undefined;
}
const proxyAgentCache = new Map();

function shouldProxyUrl(url){
  if(!proxyEnv) return false;
  if(!noProxyEnv) return true;
  try{
    const host = new URL(url).hostname.toLowerCase();
    return !noProxyEnv.split(",").some(entry => {
      const rule = String(entry||"").trim().toLowerCase();
      if(!rule) return false;
      if(rule === "*") return false;
      if(rule.startsWith(".")) return host.endsWith(rule);
      return host === rule || host.endsWith(`.${rule}`);
    });
  }catch{
    return true;
  }
}

function getProxyDispatcher(url){
  if(!proxyEnv || !ProxyAgent) return undefined;
  if(!shouldProxyUrl(url)) return undefined;
  if(proxyAgentCache.has(proxyEnv)) return proxyAgentCache.get(proxyEnv);
  const agent = new ProxyAgent(proxyEnv);
  proxyAgentCache.set(proxyEnv, agent);
  return agent;
}

const fetchImpl = (proxyEnv && ProxyAgent && undiciFetch)
  ? async (url, opts = {}) => undiciFetch(url, { ...opts, dispatcher: getProxyDispatcher(url) })
  : globalThis.fetch.bind(globalThis);

if(proxyEnv && !ProxyAgent){
  console.warn("cc-engine: HTTPS_PROXY/HTTP_PROXY is set, but undici.ProxyAgent is unavailable; proxy support may not work.");
}
if(proxyEnv){
  console.log(`cc-engine: proxy enabled -> ${proxyEnv}${noProxyEnv ? ` (NO_PROXY=${noProxyEnv})` : ``}`);
}

async function fetchWithRetries(url, opts = {}, { retries = 3, delay = 500 } = {}){
  const finalOpts = { ...opts };
  if(finalOpts.headers) finalOpts.headers = { ...finalOpts.headers };
  else finalOpts.headers = {};
  if(!finalOpts.headers["User-Agent"] && !finalOpts.headers["user-agent"]){
    finalOpts.headers["User-Agent"] = UA;
  }
  if(proxyEnv && ProxyAgent && undiciFetch){
    finalOpts.dispatcher = getProxyDispatcher(url);
  }

  let attempt = 0;
  while(true){
    try{
      const res = await fetchImpl(url, finalOpts);
      if(res.ok) return res;
      if(attempt >= retries || ![500,502,503,504].includes(res.status)) throw new Error(`index ${res.status} for ${url}`);
      await sleep(delay * Math.pow(2, attempt));
      attempt += 1;
      continue;
    }catch(error){
      if(attempt >= retries) throw error;
      await sleep(delay * Math.pow(2, attempt));
      attempt += 1;
    }
  }
}

const COLUMNS = ["Time Stamp","Source","Web Source URL","Directory","ID","Last Path","Bio Check",
  "First","Last","Gender","Title","Position","Description","Image URL","Email Address","Email Type",
  "LinkedIn URL","Google Maps","Phone","Phone Type","Phone Location","Phone 2"];

// ---------------------------------------------------------------- input
function readDomains(csvPath){
  const text = fs.readFileSync(csvPath, "utf8");
  const out = []; const seen = new Set();
  for(const raw of text.split(/\r?\n/)){
    let d = (raw.split(",")[0]||"").trim().toLowerCase()
      .replace(/^https?:\/\//,"").replace(/^www\./,"").replace(/\/.*$/,"");
    if(!d || d === "domain" || !d.includes(".")) continue;   // skip header / junk
    if(!seen.has(d)){ seen.add(d); out.push(d); }
  }
  return out;
}
function writeDomainsCsv(domains, csvPath){
  const rows = ["domain"];
  const seen = new Set();
  for(const raw of domains){
    const d = String(raw||"").trim().toLowerCase()
      .replace(/^https?:\/\//,"" ).replace(/^www\./,"" ).replace(/\/.*$/,"" );
    if(!d || d === "domain" || !d.includes(".")) continue;
    if(seen.has(d)) continue;
    seen.add(d);
    rows.push(d);
  }
  fs.writeFileSync(csvPath, rows.join("\n"), "utf8");
}

async function runDomains(domains, opts = {}){
  const tmpPath = path.join(os.tmpdir(), `rampedup-domains-${Date.now()}.csv`);
  writeDomainsCsv(domains, tmpPath);
  try {
    return await run(tmpPath, opts);
  } finally {
    try { fs.unlinkSync(tmpPath); } catch (error) { /* ignore */ }
  }
}
// ---------------------------------------------------------------- CDX index (NETWORK)
async function cdxNumPages(domain, crawl){
  const p = new URLSearchParams({ url:`${domain}/*`, output:"json", showNumPages:"true" });
  const res = await fetchWithRetries(`${INDEX}/${crawl}-index?${p}`, { headers:{ "User-Agent":UA } }, { retries:1, delay:500 });
  if(res.status === 404) return 0;                 // domain not in this crawl
  if(!res.ok) throw new Error(`index ${res.status} for ${domain}`);
  const txt = (await res.text()).trim();
  try{ const j = JSON.parse(txt); return j.pages ?? 0; }catch{ return parseInt(txt,10) || 0; }
}

function generateMockRecords(domain, count = 3) {
  // Generate fake but realistic Common Crawl records for demo/offline mode
  const mockPages = [
    `https://${domain}/about`,
    `https://${domain}/team`,
    `https://${domain}/leadership`,
    `https://${domain}/contact`,
    `https://${domain}/staff`,
    `https://${domain}/directory`,
  ];
  const records = [];
  for(let i = 0; i < Math.min(count, mockPages.length); i++){
    records.push({
      url: mockPages[i],
      filename: `crawl-00001-chunked/warc/CC-MAIN-2026-21_web_001.warc.gz`,
      offset: Math.floor(Math.random() * 1000000000),
      length: Math.floor(Math.random() * 500000) + 10000,
      timestamp: `202605${Math.floor(Math.random() * 28) + 1}120000`
    });
  }
  return records;
}

async function queryIndex(domain, { crawl = CRAWL, maxPages = 3, demoMode = false } = {}){
  if(demoMode) return generateMockRecords(domain, maxPages);
  
  const pages = await cdxNumPages(domain, crawl);
  if(!pages) return [];                            // not captured in this crawl
  const records = [];
  for(let page = 0; page < Math.min(pages, maxPages); page++){
    const p = new URLSearchParams({ url:`${domain}/*`, output:"json",
      fl:"url,filename,offset,length,timestamp", page:String(page) });
    p.append("filter","=status:200");
    p.append("filter","=mime-detected:text/html");
    const res = await fetchWithRetries(`${INDEX}/${crawl}-index?${p}`, { headers:{ "User-Agent":UA } });
    if(!res.ok) break;
    for(const line of (await res.text()).split("\n")){
      if(!line.trim()) continue;
      try{ records.push(JSON.parse(line)); }catch{}
    }
    await sleep(400);                              // be polite to the index server
  }
  return records;
}

// ---------------------------------------------------------------- candidate selection (offline-testable)
const CANDIDATE_FALLBACK_RE = /(contact|support|help|team|leadership|about|customer|sales|careers|staff|investor|media|press|board)/i;

function normalizeFilterValue(value){
  return String(value || "").trim().toLowerCase();
}

function selectCandidates(records, { perDomainCap = 25, directoryRules = {}, genderMap = {}, directoryFilter = "" } = {}){
  const desiredFilter = normalizeFilterValue(directoryFilter);
  const byUrl = new Map();

  for(const r of records){
    const dir = classifyDirectory(r.url, "", directoryRules, genderMap);
    if(desiredFilter){
      if(normalizeFilterValue(dir) !== desiredFilter) continue;
    } else if(dir !== "BIO URL" && dir !== "Contact Us") continue;   // only pages worth extracting

    const prev = byUrl.get(r.url);
    if(!prev || (r.timestamp||"") > (prev.timestamp||"")) byUrl.set(r.url, r);  // keep latest capture
  }

  const candidates = [...byUrl.values()]
    .sort((a,b)=> (classifyDirectory(b.url, "", directoryRules, genderMap)==="BIO URL") - (classifyDirectory(a.url, "", directoryRules, genderMap)==="BIO URL"))
    .slice(0, perDomainCap);

  if(desiredFilter) return candidates;
  if(candidates.length) return candidates;

  const fallback = records
    .filter(r => CANDIDATE_FALLBACK_RE.test(r.url))
    .slice(0, perDomainCap);

  if(fallback.length) return fallback;

  return records.slice(0, perDomainCap);
}


// ---------------------------------------------------------------- live crawl (Phase 3 gap-fill)
// When Common Crawl has nothing (or its index is down / 504s), go straight to the
// live website: fetch the homepage, find the bio/contact pages it links to, follow
// them one level deeper to individual profiles, and run the SAME extractor on each.
// Politeness: serial, with delays, a real User-Agent, and a per-request timeout.

const LIVE_PROBE_PATHS = ["/our-team/","/team/","/attorneys/","/lawyers/","/our-attorneys/",
  "/people/","/our-people/","/professionals/","/staff/","/leadership/","/our-firm/",
  "/about/","/about-us/","/contact/","/contact-us/"];
const LIVE_DISCOVER_CAP = 40;                 // max pages we will fetch per domain
const LINK_SKIP_EXT = /\.(pdf|docx?|xlsx?|pptx?|zip|rar|jpe?g|png|gif|svg|webp|mp4|mp3|css|js|ico|woff2?|ttf|eot|xml|rss)(\?|#|$)/i;

// Pull same-domain <a href> links out of a page, cleaned and de-duplicated. (offline-testable)
function extractSameDomainLinks(html, baseUrl, domain){
  const out = []; const seen = new Set();
  const re = /<a\b[^>]*\bhref\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))/gi;
  const root = String(domain || "").toLowerCase().replace(/^www\./, "");
  let m;
  while((m = re.exec(html))){
    let href = (m[2] ?? m[3] ?? m[4] ?? "").trim();
    if(!href || /^(mailto:|tel:|javascript:|#|data:)/i.test(href)) continue;
    let abs;
    try{ abs = new URL(href, baseUrl); }catch{ continue; }
    if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
    const host = abs.hostname.toLowerCase().replace(/^www\./, "");
    if(host !== root && !host.endsWith("." + root)) continue;
    if(LINK_SKIP_EXT.test(abs.pathname)) continue;
    abs.hash = "";
    const clean = abs.toString();
    if(!seen.has(clean)){ seen.add(clean); out.push(clean); }
  }
  return out;
}

// Does this URL look like a staff/bio or contact page? Reuses the Common Crawl gate. (offline-testable)
function isBioOrContactUrl(url, directoryRules = {}, genderMap = {}){
  const dir = classifyDirectory(url, "", directoryRules, genderMap);
  return dir === "BIO URL" || dir === "Contact Us";
}

// Fetch a page over plain HTTP/1.1 using Node's built-in http(s). We deliberately
// avoid global fetch here: live crawling hits thousands of arbitrary servers, and
// fetch's HTTP/2 path can emit an UNCATCHABLE socket 'error' event when a server
// drops the connection, which kills the whole run. http(s) lets us handle every
// failure locally and just return "" for a bad page. Returns "" on any problem.
function httpGetRaw(url, redirectsLeft = 4){
  return new Promise((resolve) => {
    let settled = false;
    const done = (v) => { if(!settled){ settled = true; resolve(v); } };
    let u;
    try{ u = new URL(url); }catch{ return done(""); }
    const lib = u.protocol === "http:" ? http : https;

    const req = lib.request(u, {
      method: "GET",
      headers: {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
      },
      timeout: 15000,
    }, (res) => {
      const status = res.statusCode || 0;

      // follow redirects
      if(status >= 300 && status < 400 && res.headers.location && redirectsLeft > 0){
        res.resume();                                   // drain & free the socket
        let next;
        try{ next = new URL(res.headers.location, u).toString(); }catch{ return done(""); }
        return done(httpGetRaw(next, redirectsLeft - 1));
      }

      const ct = (res.headers["content-type"] || "").toLowerCase();
      if(status !== 200 || (ct && !ct.includes("html"))){ res.resume(); return done(""); }

      const enc = (res.headers["content-encoding"] || "").toLowerCase();
      let stream = res;
      try{
        if(enc === "gzip") stream = res.pipe(zlib.createGunzip());
        else if(enc === "deflate") stream = res.pipe(zlib.createInflate());
        else if(enc === "br") stream = res.pipe(zlib.createBrotliDecompress());
      }catch{ res.resume(); return done(""); }

      const chunks = []; let bytes = 0; const MAX = 4 * 1024 * 1024;   // 4 MB cap
      stream.on("data", (c) => { bytes += c.length; if(bytes <= MAX) chunks.push(c); else { req.destroy(); } });
      stream.on("end", () => done(Buffer.concat(chunks).toString("utf8")));
      stream.on("error", () => done(""));
      res.on("error", () => done(""));
    });

    req.on("error", () => done(""));                    // DNS failure, reset, TLS error, etc.
    req.on("timeout", () => { req.destroy(); done(""); });
    req.end();
  });
}

async function liveFetchPage(url){
  // honor an explicit proxy via undici when one is configured; otherwise use the
  // crash-proof built-in http(s) path above.
  if(proxyEnv && ProxyAgent && undiciFetch){
    try{
      const res = await fetchImpl(url, {
        headers:{ "User-Agent":UA, "Accept":"text/html,application/xhtml+xml" },
        redirect:"follow",
        signal: AbortSignal.timeout(15000),
      });
      if(!res.ok) return "";
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      if(ct && !ct.includes("html")) return "";
      return await res.text();
    }catch{ return ""; }
  }
  return httpGetRaw(url);
}

async function liveCrawl(domain, opts = {}){
  const { wireless, genderMap = {}, directoryRules = {}, perDomainCap = 25, _liveFetch = liveFetchPage } = opts;
  const today = new Date().toISOString().slice(0, 10);
  const records = [];
  const seen = new Set();

  // 1) homepage — try the bare host, then www
  let homeHtml = "", homeUrl = "";
  for(const u of [`https://${domain}/`, `https://www.${domain}/`]){
    seen.add(u);
    homeHtml = await _liveFetch(u);
    if(homeHtml){ homeUrl = u; break; }
    await sleep(250);
  }
  if(!homeHtml) return records;                 // site unreachable / does not resolve

  // small firms sometimes put a person right on the homepage
  const homeRec = extractRecord(homeHtml, homeUrl, { wireless, genderMap, directoryRules, source:"Live Crawl", timestamp: today });
  if(homeRec) records.push(homeRec);

  // 2) build the fetch queue: bio/contact links from the homepage + common probe paths
  const queue = [];
  const enqueue = (url) => { if(!seen.has(url) && queue.length < LIVE_DISCOVER_CAP){ seen.add(url); queue.push(url); } };
  for(const u of extractSameDomainLinks(homeHtml, homeUrl, domain)){
    if(isBioOrContactUrl(u, directoryRules, genderMap)) enqueue(u);
  }
  for(const p of LIVE_PROBE_PATHS) enqueue(`https://${domain}${p}`);

  // 3) bounded crawl: fetch each, extract, and follow individual bio links one level deeper
  for(let i = 0; i < queue.length; i++){
    if(records.length >= perDomainCap) break;
    const url = queue[i];
    const html = await _liveFetch(url);
    await sleep(200);                            // polite pause between page fetches
    if(!html) continue;
    const out = extractRecord(html, url, { wireless, genderMap, directoryRules, source:"Live Crawl", timestamp: today });
    if(out) records.push(out);
    for(const sub of extractSameDomainLinks(html, url, domain)){
      if(isBioOrContactUrl(sub, directoryRules, genderMap)) enqueue(sub);   // e.g. /attorneys/ -> /attorneys/jane-doe/
    }
  }
  return records.slice(0, perDomainCap);
}

// Mock HTML generator for demo mode
function generateMockHtml(url, domain) {
  const pathSegment = url.replace(/^https?:\/\//, '').split('/')[1] || 'about';
  const domainRoot = domain.replace(/\.[^.]+$/, '');
  const normalizedDomain = domainRoot.replace(/[-_.]/g, ' ');
  const tokens = normalizedDomain.split(/\s+/).filter(Boolean);
  const pathKey = pathSegment.toLowerCase();

  const firstNames = [
    'Avery','Jordan','Morgan','Taylor','Casey','Riley','Alex','Jamie','Drew','Parker',
    'Rowan','Peyton','Quinn','Reese','Skyler','Blake','Cameron','Dakota','Elliot','Hayden'
  ];
  const roleMap = {
    about: ['Chief Executive Officer','Founder','President'],
    team: ['Director of Operations','VP Marketing','Chief Counsel'],
    leadership: ['Head of Legal','Chief Strategy Officer','VP Finance'],
    contact: ['Director of Client Relations','Customer Success Lead','Office Manager'],
    staff: ['Operations Manager','Recruiting Lead','Corporate Counsel'],
    directory: ['Business Development Director','Regional Manager','Practice Lead']
  };

  const titleChoices = roleMap[pathKey] || ['Senior Manager','Director','Head of Department'];
  const seed = tokens.reduce((sum, token) => sum + token.charCodeAt(0), 0) + pathKey.length;
  const first = firstNames[seed % firstNames.length];
  const last = tokens.length > 1
    ? tokens[(seed + 1) % tokens.length].charAt(0).toUpperCase() + tokens[(seed + 1) % tokens.length].slice(1)
    : `${tokens[0] ? tokens[0].charAt(0).toUpperCase() + tokens[0].slice(1) : 'Partner'}`;

  const title = titleChoices[seed % titleChoices.length];
  const emailLocal = `${first.toLowerCase()}.${last.toLowerCase()}`.replace(/[^a-z0-9\.]/g, '');
  const email = `${emailLocal}@${domain}`;
  const linkedIn = `https://linkedin.com/in/${first.toLowerCase()}-${last.toLowerCase()}/`;
  const phone = `+1 (555) ${100 + (seed % 900)}-${1000 + (seed % 9000)}`;

  return `<!DOCTYPE html><html><head><title>${pathSegment} | ${domain}</title></head><body>
    <h1>${pathSegment.charAt(0).toUpperCase() + pathSegment.slice(1)} | ${normalizedDomain}</h1>
    <section class="person">
      <h2>${first} ${last}</h2>
      <p>Title: ${title}</p>
      <p>Email: <a href="mailto:${email}">${email}</a></p>
      <p>LinkedIn: <a href="${linkedIn}">${linkedIn}</a></p>
      <p>Phone: ${phone}</p>
    </section>
    <p>Page source: ${url}</p>
  </body></html>`;
}

// ---------------------------------------------------------------- WARC fetch + parse (NETWORK fetch; parse is offline-testable)
async function fetchWarc(rec, { demoMode = false } = {}){
  if(demoMode) {
    const domain = rec.url.replace(/^https?:\/\/(www\.)?/, '').split('/')[0];
    return generateMockHtml(rec.url, domain);
  }
  
  const start = Number(rec.offset), end = start + Number(rec.length) - 1;
  const url = `${DATA}/${rec.filename}`;
  const opts = {
    headers:{ "User-Agent":UA, "Range":`bytes=${start}-${end}` }
  };
  if(proxyEnv && ProxyAgent && undiciFetch){
    opts.dispatcher = getProxyDispatcher(url);
  }
  const res = await fetchImpl(url, opts);
  if(!res.ok && res.status !== 206) throw new Error(`warc ${res.status} for ${rec.url}`);
  const gz = Buffer.from(await res.arrayBuffer());
  return warcToHtml(zlib.gunzipSync(gz));
}

/** A fetched WARC record = WARC headers \r\n\r\n  HTTP headers \r\n\r\n  BODY(html). */
function warcToHtml(buf){
  const SEP = Buffer.from("\r\n\r\n");
  const i = buf.indexOf(SEP);                 if(i < 0) return "";   // end of WARC headers
  const j = buf.indexOf(SEP, i + SEP.length); if(j < 0) return "";   // end of HTTP headers
  return buf.slice(j + SEP.length).toString("utf8");                 // the HTML body
}

// ---------------------------------------------------------------- output
function csvEscape(v){ return `"${String(v??"").replace(/"/g,'""')}"`; }
function writeCsv(records, path){
  const lines = [COLUMNS.join(",")];
  for(const r of records) lines.push(COLUMNS.map(c => csvEscape(r[c])).join(","));
  fs.writeFileSync(path, lines.join("\n"));
}

function scoreRecord(r){
  let score = 0;
  if(r["Bio Check"] === "Y") score += 20;
  if(r.First) score += 10;
  if(r.Last) score += 10;
  if(r["Phone"]) score += 15;
  if(r["Phone Type"]) score += 5;
  if(r["LinkedIn URL"]) score += 10;
  if(r.Title) score += 5;
  if(r.Position) score += 5;
  if(r["Description"]) score += Math.min(20, String(r["Description"]).trim().length / 20);
  if(r["Email Type"] === "Professional") score += 10;
  if(r["Email Type"] === "Role-Based") score += 2;
  return score;
}

function uniqueByEmail(records){
  const best = new Map();
  for(const r of records){
    const email = String(r["Email Address"] || "").trim().toLowerCase();
    if(!email) continue;
    const current = best.get(email);
    if(!current || scoreRecord(r) > scoreRecord(current)){
      best.set(email, r);
    }
  }
  return [...best.values()];
}

// ---------------------------------------------------------------- orchestration
async function run(csvPath, opts = {}){
  const {
    wirelessPath = (__dirname + "/WIRELESS_BLOCKS.TXT"),
    genderMap = {}, directoryRules = {}, outPath = "cc-results.csv",
    // injectable for testing; default to the real network functions
    _queryIndex = queryIndex, _fetchWarc = fetchWarc, _liveCrawl = liveCrawl,
    liveFallback = true,        // when CC has nothing / 504s, crawl the live site
    onRecord = () => {}, onProgress = () => {},
  } = opts;

  const domains = readDomains(csvPath);
  const wireless = loadWirelessBlocks(wirelessPath);
  console.log(`Domains: ${domains.length}   Wireless blocks: ${wireless.size.toLocaleString()}   Crawl: ${CRAWL}\n`);

  const all = [];
  const seenEmails = new Map();  // de-dupe live onRecord callbacks: best record per email
  const coverage = { found:0, live:0, empty:0, errored:0 };

  // push a record into the result set, emitting the best-per-email to onRecord
  const ingest = (out) => {
    all.push(out);
    const email = String(out["Email Address"] || "").trim().toLowerCase();
    const current = seenEmails.get(email);
    if(!current || scoreRecord(out) > scoreRecord(current)){
      seenEmails.set(email, out);
      onRecord(out);
    }
  };

  let ccFailStreak = 0, ccDisabled = false;   // circuit breaker: stop hammering a down index

  for(const [index, domain] of domains.entries()){
    const domainNumber = index + 1;
    onProgress({ status: 'domain-start', domain, index: domainNumber, total: domains.length });

    let ccKept = 0, liveKept = 0, note = "";

    // ---- 1) Common Crawl (the archive) ----
    if(!ccDisabled) try{
      const idx = await _queryIndex(domain, opts);
      ccFailStreak = 0;                          // index responded → it's up
      const cands = selectCandidates(idx, opts);
      if(cands.length){
        const tried = new Set();
        for(const rec of cands){
          tried.add(rec.url);
          let html = "";
          try{ html = await _fetchWarc(rec, opts); }catch{ continue; }
          const out = extractRecord(html, rec.url, { wireless, genderMap, directoryRules, source:"Common Crawl",
            timestamp:(rec.timestamp||"").slice(0,8).replace(/(\d{4})(\d{2})(\d{2})/,"$1-$2-$3") });
          if(out){ ingest(out); ccKept++; }
          await sleep(150);                          // polite pause between page fetches
        }

        if(ccKept === 0){
          const fallback = idx.filter(r => !tried.has(r.url) && CANDIDATE_FALLBACK_RE.test(r.url)).slice(0, cands.length || 5);
          if(fallback.length){
            onProgress({ status: 'fallback-start', domain, index: domainNumber, total: domains.length, fallbackCount: fallback.length });
            for(const rec of fallback){
              let html = "";
              try{ html = await _fetchWarc(rec, opts); }catch{ continue; }
              const out = extractRecord(html, rec.url, { wireless, genderMap, directoryRules, source:"Common Crawl",
                timestamp:(rec.timestamp||"").slice(0,8).replace(/(\d{4})(\d{2})(\d{2})/,"$1-$2-$3") });
              if(out){ ingest(out); ccKept++; }
              await sleep(150);
            }
          }
        }
      }
    }catch(e){                                   // 504 / outage → treat as "not in crawl", fall through to live
      note = e.message;
      ccFailStreak++;
      if(ccFailStreak >= 3){
        ccDisabled = true;
        console.log(`  (Common Crawl index unresponsive — skipping it for the rest of this run, going live-only)`);
      }
    }

    // ---- 2) Live crawl fallback (gap-fill straight from the website) ----
    if(ccKept === 0 && liveFallback){
      onProgress({ status: 'live-start', domain, index: domainNumber, total: domains.length });
      try{
        const liveRecs = await _liveCrawl(domain, { ...opts, wireless });
        for(const out of liveRecs){ ingest(out); liveKept++; }
      }catch(e){ if(!note) note = e.message; }
    }

    // ---- 3) tally + report ----
    if(ccKept > 0){
      coverage.found++;
      console.log(`✓ ${domain.padEnd(28)} ${ccKept} record(s) via Common Crawl`);
      onProgress({ status: 'domain-done', domain, index: domainNumber, total: domains.length, source:'Common Crawl', kept: ccKept });
    } else if(liveKept > 0){
      coverage.live++;
      console.log(`◆ ${domain.padEnd(28)} ${liveKept} record(s) via live crawl`);
      onProgress({ status: 'domain-done', domain, index: domainNumber, total: domains.length, source:'Live Crawl', kept: liveKept });
    } else {
      coverage.empty++;
      console.log(`· ${domain.padEnd(28)} no contacts found${note ? `  (${note})` : ""}`);
      onProgress({ status: 'no-candidates', domain, index: domainNumber, total: domains.length });
    }
  }

  const unique = uniqueByEmail(all);
  if(unique.length < all.length){
    console.log(`\nDropped ${all.length - unique.length} duplicate email record(s) to enforce one email per record`);
  }
  writeCsv(unique, outPath);
  console.log(`\nCoverage: ${coverage.found} via Common Crawl · ${coverage.live} via live crawl · ${coverage.empty} no contacts`);
  console.log(`People:   ${unique.length} unique email records → ${outPath}`);
  onProgress({ status: 'done', totalRecords: unique.length, coverage });
  return unique;
}

module.exports = { run, runDomains, readDomains, selectCandidates, warcToHtml, queryIndex, fetchWarc,
  liveCrawl, extractSameDomainLinks, isBioOrContactUrl, COLUMNS };

// ---------------------------------------------------------------- offline self-tests
if(require.main === module){
  const parseArgs = argv => {
    const opts = { csvPath: "", genderPath: "", directoryRulesPath: "", selftest:false };
    for(let i = 2; i < argv.length; i++){
      const a = argv[i];
      if(a === "--selftest") { opts.selftest = true; continue; }
      if(a === "--gender" || a === "--gender-file") { opts.genderPath = argv[++i] || ""; continue; }
      if(a === "--directory-rules" || a === "--dir-rules" || a === "--dirs") { opts.directoryRulesPath = argv[++i] || ""; continue; }
      if(!opts.csvPath) opts.csvPath = a;
    }
    return opts;
  };

  const args = parseArgs(process.argv);
  if(args.selftest){
    (async () => {
      let pass = 0, fail = 0;
      const ok = (name, cond) => { (cond?pass++:fail++); console.log(`${cond?"✓":"✗"} ${name}`); };

      // 1) selectCandidates filters + dedups + orders
      const idx = [
        { url:"https://acme.com/team/marcus-patel", filename:"f", offset:0, length:1, timestamp:"20260101" },
        { url:"https://acme.com/team/marcus-patel", filename:"f", offset:0, length:1, timestamp:"20260201" }, // newer dup
        { url:"https://acme.com/contact",           filename:"f", offset:0, length:1, timestamp:"20260101" },
        { url:"https://acme.com/blog/post-1",       filename:"f", offset:0, length:1, timestamp:"20260101" }, // dropped
        { url:"https://acme.com/pricing",           filename:"f", offset:0, length:1, timestamp:"20260101" }, // dropped
      ];
      const cands = selectCandidates(idx);
      ok("selectCandidates keeps only bio+contact (2 of 5)", cands.length === 2);
      ok("selectCandidates dedups to newest capture", cands.find(c=>c.url.endsWith("marcus-patel"))?.timestamp === "20260201");
      ok("selectCandidates orders BIO URL first", classifyDirectory(cands[0].url) === "BIO URL");

      // 2) warcToHtml round-trips a real gzipped WARC record
      const html = `<h1>Marcus Patel</h1><a href="mailto:marcus.patel@acme.com">e</a><a href="tel:+12012012345">c</a>`;
      const warc = `WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: https://acme.com/team/marcus-patel\r\n\r\n`
                 + `HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n${html}`;
      const gz = zlib.gzipSync(Buffer.from(warc));
      ok("warcToHtml extracts the HTML body from a gzipped WARC", warcToHtml(zlib.gunzipSync(gz)) === html);

      // 3) full pipeline with mocked network → real extractRecord + real wireless table
      const wireless = loadWirelessBlocks((__dirname + "/WIRELESS_BLOCKS.TXT"));
      const pages = {
        "https://acme.com/team/marcus-patel":
          `<h1>Marcus Patel</h1><meta property="og:description" content="VP of Marketing at Acme.">`
          + `<a href="mailto:marcus.patel@acme.com">e</a><a href="tel:+12012012345">c</a>`,
        "https://acme.com/contact": `<h1>Contact</h1><p>123 Main St.</p>`,   // dropped by gate
      };
      const tmp = os.tmpdir();
      fs.writeFileSync(`${tmp}/domains.csv`, "domain\nacme.com\nuncrawled-xyz.com\n");
      const recs = await run(`${tmp}/domains.csv`, {
        wirelessPath:(__dirname + "/WIRELESS_BLOCKS.TXT"),
        genderMap:{ marcus:"M" }, outPath:`${tmp}/cc-results.csv`,
        liveFallback:false,                              // keep the self-test fully offline
        _queryIndex: async (domain) => domain === "acme.com" ? Object.keys(pages).map(url =>
          ({ url, filename:"f", offset:0, length:1, timestamp:"20260201" })) : [],
        _fetchWarc: async (rec) => pages[rec.url] || "",
      });
      ok("pipeline keeps the bio record, drops the empty contact page", recs.length === 1);
      ok("pipeline classified the phone as Mobile via the block table", recs[0]["Phone Type"] === "Mobile");
      ok("pipeline tagged source = Common Crawl", recs[0]["Source"] === "Common Crawl");
      ok("results CSV was written", fs.existsSync(`${tmp}/cc-results.csv`));
      const duplicateRows = [
        { "Email Address": "test@xyz.com", "Phone": "", "Email Type": "Role-Based" },
        { "Email Address": "TEST@xyz.com", "Phone": "+12025550123", "Email Type": "Professional", "First": "Test", "Last": "User" },
        { "Email Address": "other@xyz.com", "Phone": "", "Email Type": "Professional" },
      ];
      const uniqueRows = uniqueByEmail(duplicateRows);
      ok("uniqueByEmail dedupes by lowercase email", uniqueRows.length === 2 && uniqueRows.some(r => r["Email Address"].toLowerCase() === "test@xyz.com") && uniqueRows.some(r => r["Email Address"] === "other@xyz.com"));
      ok("uniqueByEmail keeps the richer record for duplicate emails", uniqueRows.find(r => r["Email Address"].toLowerCase() === "test@xyz.com").Phone === "+12025550123");

      // 4) live-crawl helpers (offline)
      const linkHtml = `<a href="/attorneys/">Attorneys</a><a href="/attorneys/jane-doe/">Jane</a>`
        + `<a href="https://www.demo-firm.com/contact/">Contact</a><a href="https://other.com/x">Off-site</a>`
        + `<a href="/brochure.pdf">PDF</a><a href="mailto:x@y.com">mail</a><a href="#top">top</a>`;
      const links = extractSameDomainLinks(linkHtml, "https://demo-firm.com/", "demo-firm.com");
      ok("extractSameDomainLinks keeps same-domain page links only",
        links.includes("https://demo-firm.com/attorneys/")
        && links.includes("https://demo-firm.com/attorneys/jane-doe/")
        && links.includes("https://www.demo-firm.com/contact/"));
      ok("extractSameDomainLinks drops off-site, files, mailto, and anchors",
        !links.some(u => /other\.com|\.pdf|mailto:|#top/.test(u)));
      ok("isBioOrContactUrl flags a staff/bio path", isBioOrContactUrl("https://demo-firm.com/attorneys/jane-doe/"));
      ok("isBioOrContactUrl ignores a blog path", !isBioOrContactUrl("https://demo-firm.com/blog/hello/"));

      // 5) liveCrawl follows bio links and extracts via the real extractor (mocked network)
      const livePages = {
        "https://demo-firm.com/": `<a href="/attorneys/">Attorneys</a><a href="/blog/x">Blog</a>`,
        "https://demo-firm.com/attorneys/": `<a href="/attorneys/jane-doe/">Jane Doe</a>`,
        "https://demo-firm.com/attorneys/jane-doe/":
          `<h1>Jane Doe</h1><meta property="og:description" content="Partner at Demo Firm.">`
          + `<a href="mailto:jane.doe@demo-firm.com">e</a><a href="tel:+12012012345">c</a>`,
      };
      const liveRecs = await liveCrawl("demo-firm.com", { wireless, _liveFetch: async (u) => livePages[u] || "" });
      const jane = liveRecs.find(r => String(r["Email Address"]).toLowerCase() === "jane.doe@demo-firm.com");
      ok("liveCrawl follows bio links and extracts a record", !!jane);
      ok("liveCrawl tags source = Live Crawl", jane && jane["Source"] === "Live Crawl");

      console.log(`\n${pass} passed, ${fail} failed`);
      process.exit(fail ? 1 : 0);
    })();
  } else {
    const csv = args.csvPath;
    if(!csv){ console.error("usage: node cc-engine.js <domains.csv> [--gender names.csv|xlsx] [--directory-rules dirs.csv|xlsx] | node cc-engine.js --selftest"); process.exit(1); }
    const genderMap = args.genderPath ? loadGenderMap(args.genderPath) : {};
    const directoryRules = args.directoryRulesPath ? loadDirectoryRules(args.directoryRulesPath) : {};
    run(csv, { genderMap, directoryRules }).catch(e => { console.error(e); process.exit(1); });
  }
}
