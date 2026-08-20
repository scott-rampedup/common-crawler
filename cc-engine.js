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
const { extractRecord, classifyDirectory, loadGenderMap, loadDirectoryRules, analyzePhones, geocodeRecords } = require("./extractor");
const { findSiteApi } = require("./site-apis");
let _siteApiSeen = 0;   // count of records pulled via a Site-API adapter (for sampling the logs)
const { loadWirelessBlocks } = require("./wireless-block-classifier");

const INDEX = "https://index.commoncrawl.org";
const DATA  = "https://data.commoncrawl.org";
// ALWAYS use the latest Common Crawl corpus: resolveLatestCrawl() updates this from collinfo.json at
// startup. The literal is a recent fallback if that lookup fails; CC_CRAWL pins a specific crawl.
let CRAWL = process.env.CC_CRAWL || "CC-MAIN-2026-25";
const UA = "RampedUp-CC-Engine/0.1 (https://rampedup.io; contact@rampedup.io)";
// Browser UA for PROXIED live fetches (bot-protected sites flag the honest crawler UA).
// Direct fetches + robots.txt keep the honest UA above.
const BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Resolve the LATEST published Common Crawl corpus from collinfo.json (newest first) and point CRAWL
// at it, so we always read the freshest archive. Pin with CC_CRAWL to skip auto-resolution. On any
// failure, the recent literal fallback above stands. Call once at startup (before jobs run).
function _getJsonCC(u, timeoutMs = 8000){
  return new Promise((resolve) => {
    const req = https.get(u, { headers: { "User-Agent": UA }, timeout: timeoutMs }, (res) => {
      if(res.statusCode !== 200){ res.resume(); return resolve(null); }
      let b = ""; res.on("data", (d) => b += d); res.on("end", () => { try{ resolve(JSON.parse(b)); }catch{ resolve(null); } });
    });
    req.on("error", () => resolve(null));
    req.on("timeout", () => { req.destroy(); resolve(null); });
  });
}
async function resolveLatestCrawl(){
  if(process.env.CC_CRAWL){ CRAWL = process.env.CC_CRAWL; return CRAWL; }      // explicit pin
  const info = await _getJsonCC(INDEX + "/collinfo.json");
  const latest = Array.isArray(info) && info[0] && info[0].id;
  if(typeof latest === "string" && /^CC-MAIN-\d{4}-\d+$/.test(latest)){
    if(latest !== CRAWL) console.log(`Common Crawl: using latest corpus ${latest} (was ${CRAWL}).`);
    CRAWL = latest;
  } else {
    console.log(`Common Crawl: latest-corpus lookup failed; using ${CRAWL}.`);
  }
  return CRAWL;
}
function currentCrawl(){ return CRAWL; }

// Keep-alive agents so we reuse TCP/TLS connections (esp. when pulling many pages
// from one site) instead of paying a fresh handshake per request.
// 64 sockets suits a 48-way live crawl. The nightly sitemap sweep runs far wider than that, and a socket
// cap below the requested concurrency silently serialises it — the pass looks slow rather than blocked.
const MAX_SOCKETS = Math.max(16, Number(process.env.CC_MAX_SOCKETS) || 64);
const keepAliveHttp  = new http.Agent({ keepAlive: true, maxSockets: MAX_SOCKETS, maxFreeSockets: 16, timeout: 30000 });
const keepAliveHttps = new https.Agent({ keepAlive: true, maxSockets: MAX_SOCKETS, maxFreeSockets: 16, timeout: 30000 });

// Optional outbound proxies + unblocker for LIVE page fetches (e.g. NetNut). THREE tiers,
// escalated cheapest-first by liveFetchPage — a tier is tried only when the previous one is blocked:
//   PROXY_URL            = primary, a cheap DATACENTER gateway
//                          (http://USER-dc-any:PASS@gw.netnut.net:5959)
//   PROXY_FALLBACK_URL   = RESIDENTIAL gateway, for sites that block datacenter IPs
//                          (http://USER-res-us:PASS@gw.netnut.net:5959)
//   UNBLOCKER_API_URL    = WEBSITE UNBLOCKER API (runs a real browser, solves JS/TLS challenges)
//                          for Akamai/Cloudflare-JS sites a raw proxy can't pass. NetNut's is an
//                          HTTP API (POST {url,format:html}), NOT a proxy — and slow (~60-90s/page)
//                          + billed per request, so it fires LAST and only on still-blocked pages.
//                          (https://USER:PASS@unblocker.netnut.io/unblock)
// Each proxied request exits a different IP, so we crawl wide without burning one address, and we
// only spend pricier bandwidth on the sites that actually need it. Common Crawl traffic is NOT
// proxied (it's an API, not a blocking target, and saves $).
const mask = (s) => String(s || "").replace(/\/\/[^@]*@/, "//***:***@");
function makeProxyAgents(url){
  if(!url) return { http: null, https: null };
  try{
    const { HttpProxyAgent }  = require("http-proxy-agent");
    const { HttpsProxyAgent } = require("https-proxy-agent");
    // keepAlive:false -> a fresh connection (and thus a fresh rotating exit IP) per request.
    // Reusing a socket would pin one IP, defeating rotation + the retry-rolls-a-new-IP logic.
    return { http:  new HttpProxyAgent(url,  { keepAlive: false, maxSockets: MAX_SOCKETS }),
             https: new HttpsProxyAgent(url, { keepAlive: false, maxSockets: MAX_SOCKETS }) };
  }catch(e){ console.warn("proxy agent unavailable:", e.message); return { http: null, https: null }; }
}
const PROXY_URL = process.env.PROXY_URL || "";
const PROXY_FALLBACK_URL = process.env.PROXY_FALLBACK_URL || "";
const UNBLOCKER_API_URL = process.env.UNBLOCKER_API_URL || "";
// Live-fetch tuning. A blocked/unreachable page costs a full timeout, so keep it short (was hard-coded 15s)
// and BAIL a domain after a run of consecutive failures instead of grinding all LIVE_MAX_PAGES × timeout —
// the big win for batches of bot-protected sites (e.g. hospital provider directories behind Akamai).
const LIVE_TIMEOUT = Number(process.env.LIVE_FETCH_TIMEOUT) || 6000;
const LIVE_BLOCK_LIMIT = Number(process.env.LIVE_BLOCK_STREAK) || 6;
const _proxyPrimary   = makeProxyAgents(PROXY_URL);
const _proxyFallback  = makeProxyAgents(PROXY_FALLBACK_URL);
let proxyAgentHttp = _proxyPrimary.http, proxyAgentHttps = _proxyPrimary.https;
let proxyAgentHttpFb = _proxyFallback.http, proxyAgentHttpsFb = _proxyFallback.https;
if(PROXY_URL)          console.log(`Live-crawl proxy (primary): ON via ${mask(PROXY_URL)}`);
if(PROXY_FALLBACK_URL) console.log(`Live-crawl proxy (residential fallback): ON via ${mask(PROXY_FALLBACK_URL)}`);
if(UNBLOCKER_API_URL)  console.log(`Live-crawl website-unblocker API: ON via ${mask(UNBLOCKER_API_URL)}`);

// Tiny concurrency limiter: run() uses one per "lane" (across-domain pool, the
// global Common-Crawl lane, the per-site lane) to cap how many requests run at once.
function makeLimiter(maxConcurrent){
  let active = 0; const waiters = [];
  const next = () => { active--; const w = waiters.shift(); if(w) w(); };
  return function run(fn){
    return new Promise((resolve, reject) => {
      const start = () => {
        active++;
        Promise.resolve().then(fn).then(
          (v) => { resolve(v); next(); },
          (e) => { reject(e); next(); },
        );
      };
      if(active < maxConcurrent) start(); else waiters.push(start);
    });
  };
}

// Common Crawl is a shared public service — keep our total CC requests serialized
// (configurable) even when many domains run in parallel, so we never hammer it.
const ccLimit = makeLimiter(Number(process.env.CC_CONCURRENCY) || 1);
// The CC index server is rate-sensitive (keep it serial via ccLimit), but the WARC DATA store
// (data.commoncrawl.org, S3/CloudFront) scales — fetch archived pages massively in parallel.
const warcLimit = makeLimiter(Number(process.env.WARC_CONCURRENCY) || 32);

// GLOBAL crawl-concurrency cap, shared across ALL jobs in this process. Per-job pools
// each respect this, so running several big jobs at once can't multiply the in-flight
// load (two 48-concurrency jobs once = ~96 fetches -> heap OOM / SIGABRT).
const globalCrawlLimit = makeLimiter(Math.max(1, Number(process.env.DOMAIN_CONCURRENCY) || 6));
// Webpage/sitemap units are MUCH lighter than a whole-domain crawl: one (cached) index
// lookup + one WARC fetch, no per-domain page-walking. They get their own higher global
// cap so a big sitemap finishes fast without competing for the 6 heavy domain-crawl slots.
// Actual network is still bounded by warcLimit (CC) + the per-host gate (live).
const globalWebpageLimit = makeLimiter(Math.max(1, Number(process.env.WEBPAGE_GLOBAL_CONCURRENCY) || 32));

// Per-key (per-host) concurrency limiter: lets total concurrency be high while
// keeping the number of simultaneous requests to ANY single host small (polite +
// protects our IP). Used for live page fetches in webpage mode.
function makeKeyedLimiter(maxPerKey){
  const active = new Map();   // key -> in-flight count
  const queue = new Map();    // key -> [thunks]
  function launch(k, fn, resolve, reject){
    active.set(k, (active.get(k) || 0) + 1);
    Promise.resolve().then(fn).then(
      (v) => { done(k); resolve(v); },
      (e) => { done(k); reject(e); });
  }
  function done(k){
    active.set(k, (active.get(k) || 1) - 1);
    const q = queue.get(k);
    if(q && q.length) q.shift()();
    else if((active.get(k) || 0) <= 0){ active.delete(k); queue.delete(k); }
  }
  return (k, fn) => new Promise((resolve, reject) => {
    if((active.get(k) || 0) < maxPerKey) launch(k, fn, resolve, reject);
    else { if(!queue.has(k)) queue.set(k, []); queue.get(k).push(() => launch(k, fn, resolve, reject)); }
  });
}
function hostOf(u){ try{ return new URL(u).hostname.replace(/^www\./, "").toLowerCase(); }catch{ return String(u); } }
// Canonical key for matching a sitemap/job URL to a Common-Crawl capture: host (no www, lc) +
// path (no trailing slash), ignoring scheme/query/fragment. Used by the bulk domain index cache.
function ccUrlKey(u){
  try{ const x = new URL(u); return x.hostname.toLowerCase().replace(/^www\./, "") + (x.pathname.replace(/\/+$/, "") || "/"); }
  catch{ return String(u || "").toLowerCase(); }
}

// HTML pages are capped smaller than data files — bios are tiny; this cuts download
// + parse time and protects the event loop at high concurrency.
const HTML_MAX_BYTES = (Number(process.env.HTML_MAX_KB) || 1024) * 1024;

// lightweight network telemetry per run (so we can see if we're getting blocked)
let _net = { fetched: 0, blocked: 0 };
function resetNetStats(){ _net = { fetched: 0, blocked: 0 }; }
function getNetStats(){ return { ..._net }; }

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

// CSV output columns. "Image URL" is intentionally NOT here — the image still rides
// along on each record object (used for the UI thumbnail) but is not a CSV column.
const COLUMNS = ["Time Stamp","Source","Web Source URL","Directory","Path ID","Domain","Last Path","Bio Check",
  "First","Last","Gender","Title","Position","Description","Email Address","Email Type",
  "LinkedIn URL","Facebook","Twitter","WhatsApp","Google Maps","vCard","Phone","Phone Type","Phone Location","Phone 2","Phone 2 Type","Type"];

// ---------------------------------------------------------------- input
// Domain mode: reduce each line to a bare host (strip protocol/www/path), dedup.
function normalizeDomainList(lines){
  const out = []; const seen = new Set();
  for(const line of lines){
    for(const raw of String(line).split(/[|\r\n]+/)){          // accept pipe-delimited within a line ("a || b")
      const d = (String(raw).split(",")[0]||"").trim().toLowerCase()
        .replace(/^https?:\/\//,"").replace(/^www\./,"").replace(/\/.*$/,"");
      if(!d || d === "domain" || !d.includes(".")) continue;   // skip header / junk
      if(!seen.has(d)){ seen.add(d); out.push(d); }
    }
  }
  return out;
}
// Webpage mode: keep the FULL URL (path/query intact), just ensure a protocol + dedup.
function normalizeUrlList(lines){
  const out = []; const seen = new Set();
  for(const line of lines){
    for(let u of String(line).split(/[|\r\n]+/)){              // pipe- OR line-delimited (commas can be in a URL)
      u = (u.split(/,(?![^?]*=)/)[0]||"").trim();              // tolerate trailing CSV cols
      if(!u || /^(domain|url|webpage)$/i.test(u) || !u.includes(".")) continue;
      if(!/^https?:\/\//i.test(u)) u = "https://" + u;
      try{ new URL(u); }catch{ continue; }
      if(!seen.has(u)){ seen.add(u); out.push(u); }
    }
  }
  return out;
}
function readDomains(csvPath){
  return normalizeDomainList(fs.readFileSync(csvPath, "utf8").split(/\r?\n/));
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
  return run(null, { ...opts, _items: domains });   // pass the list straight through (no temp CSV)
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

  // hold the global CC lane for this domain's whole index lookup so concurrent
  // domains never hammer Common Crawl's shared index server.
  return ccLimit(async () => {
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
  });
}

// Look up ONE exact URL in the Common Crawl index (not a domain prefix). Returns the latest
// 200/text-html capture record { url, filename, offset, length, timestamp }, or null if the URL
// isn't archived. Lets webpage mode read a page from the archive when the live site blocks us
// (e.g. Cloudflare). Throws only when the index itself is unreachable, so callers can trip a breaker.
// Bulk-load EVERY 200/text-html capture for a domain (+ subdomains) from the CC index in ONE
// paginated query, returned as a Map(ccUrlKey -> latest record). Lets webpage/sitemap jobs do a
// single index query per domain instead of one per URL. Holds the (serial) index lane.
async function loadDomainIndex(domain, { crawl = CRAWL, maxPages = 100 } = {}){
  return ccLimit(async () => {
    const base = `${INDEX}/${crawl}-index`;
    let pages = 0;
    try{
      const np = new URLSearchParams({ url: domain, matchType:"domain", output:"json", showNumPages:"true" });
      const r = await fetchWithRetries(`${base}?${np}`, { headers:{ "User-Agent":UA } }, { retries:1, delay:500 });
      if(r.status === 404) return new Map();                    // domain not in this crawl
      const txt = (await r.text()).trim();
      try{ pages = JSON.parse(txt).pages ?? 0; }catch{ pages = parseInt(txt, 10) || 0; }
    }catch(e){
      if(/\b404\b/.test(String(e && e.message))) return new Map();
      throw e;
    }
    const m = new Map();
    for(let page = 0; page < Math.min(pages, maxPages); page++){
      const p = new URLSearchParams({ url: domain, matchType:"domain", output:"json",
        fl:"url,filename,offset,length,timestamp", page:String(page) });
      p.append("filter","=status:200");
      p.append("filter","=mime-detected:text/html");
      let res; try{ res = await fetchWithRetries(`${base}?${p}`, { headers:{ "User-Agent":UA } }); }catch{ break; }
      if(!res.ok) break;
      for(const line of (await res.text()).split("\n")){
        if(!line.trim()) continue;
        let rec; try{ rec = JSON.parse(line); }catch{ continue; }
        const k = ccUrlKey(rec.url); const prev = m.get(k);
        if(!prev || (rec.timestamp||"") > (prev.timestamp||"")) m.set(k, rec);   // keep latest capture
      }
      await sleep(400);                                         // polite between index pages
    }
    return m;
  });
}

async function queryIndexUrl(url, opts = {}){
  const { crawl = CRAWL, demoMode = false } = opts;
  if(demoMode){ const recs = generateMockRecords(url, 1); return recs[0] || null; }
  // BULK PATH: when a per-run domain cache is supplied (webpage/sitemap jobs), load the whole
  // domain's index once and answer every URL from memory — turns N index queries into ~1/domain.
  const cache = opts._domainIndexCache;
  if(cache){
    const host = hostOf(url);
    // Store the PROMISE synchronously (before any await) so the ~24 concurrent workers hitting
    // the same host all share ONE index load instead of each kicking off its own.
    if(!cache.has(host)) cache.set(host, loadDomainIndex(host, { crawl, maxPages: opts.bulkIndexPages || 100 }));
    const m = await cache.get(host);
    return m.get(ccUrlKey(url)) || null;
  }
  return ccLimit(async () => {
    const p = new URLSearchParams({ url, output:"json", fl:"url,filename,offset,length,timestamp" });
    p.append("filter","=status:200");
    p.append("filter","=mime-detected:text/html");
    let res;
    try{
      res = await fetchWithRetries(`${INDEX}/${crawl}-index?${p}`, { headers:{ "User-Agent":UA } });
    }catch(e){
      if(/\b404\b/.test(String(e && e.message))) return null;   // 404 = not captured in this crawl
      throw e;                                                  // 5xx / network = genuine index failure
    }
    let best = null;
    for(const line of (await res.text()).split("\n")){
      if(!line.trim()) continue;
      let rec; try{ rec = JSON.parse(line); }catch{ continue; }
      if(!best || (rec.timestamp||"") > (best.timestamp||"")) best = rec;   // keep the latest capture
    }
    return best;
  });
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
// live website. To uncover EVERY matching page we read robots.txt, follow its
// Sitemap(s) (incl. sitemap indexes and gzipped .xml.gz), and keep every same-domain
// URL that fits the bio/contact criteria — plus homepage links + probe paths as a
// backstop. We run the SAME extractor on each page, and we honor robots Disallow.
// Politeness: serial, with delays, a real User-Agent, and a per-request timeout.

const LIVE_PROBE_PATHS = ["/our-team/","/team/","/attorneys/","/lawyers/","/our-attorneys/",
  "/people/","/our-people/","/professionals/","/staff/","/leadership/","/our-firm/",
  "/about/","/about-us/","/contact/","/contact-us/"];
const LINK_SKIP_EXT =/\.(pdf|docx?|xlsx?|pptx?|zip|rar|jpe?g|png|gif|svg|webp|mp4|mp3|css|js|ico|woff2?|ttf|eot|xml|rss)(\?|#|$)/i;

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

// Location-page detector: a LEAF url pointing to a specific place under a location container path
// (…/locations/<slug>, …/stores/<slug>, …/dealers/<slug>) or a locations.<domain> subdomain. The
// trailing slug distinguishes a specific store/branch/office from the container/index page. Heuristic
// (like isBioOrContactUrl); the ratio threshold + name lexicon guard against false positives. (offline-testable)
const LOC_SEG = /^(locations?|stores?|store-locator|storelocator|branch(?:es)?|offices?|dealers?|dealerships?|agenc(?:y|ies)|showrooms?|clinics?|centers?|centres?|restaurants?|hotels?|cities|find-a-store|find-a-location|find-us)$/i;
function isLocationUrl(url){
  let u; try{ u = new URL(url); }catch{ return false; }
  if(u.protocol !== "http:" && u.protocol !== "https:") return false;
  if(LINK_SKIP_EXT.test(u.pathname)) return false;
  const segs = u.pathname.split("/").filter(Boolean);
  const leafOk = (s) => /^[a-z0-9][a-z0-9\-]*$/i.test(s || "") && String(s).length >= 2;
  // locations.<domain>/<place> style subdomain
  const sub = u.hostname.replace(/^www\./, "").toLowerCase().split(".")[0];
  if((sub === "locations" || sub === "location" || sub === "stores" || sub === "store") && segs.length >= 1) return leafOk(segs[segs.length - 1]);
  // …/<container>/<place>[/…] — a location container segment followed by at least one more segment
  for(let i = 0; i < segs.length - 1; i++){ if(LOC_SEG.test(segs[i]) && leafOk(segs[segs.length - 1])) return true; }
  return false;
}

// ---- robots.txt + sitemaps: how we discover EVERY matching page ----

// Parse robots.txt into { sitemaps:[urls], rules:[{allow,path}] } for our user-agent. (offline-testable)
function parseRobots(text, ua = UA){
  const uaLower = String(ua || "").toLowerCase();
  const sitemaps = [];
  const groups = []; let current = null; let lastWasAgent = false;

  for(const raw of String(text || "").split(/\r?\n/)){
    const line = raw.replace(/#.*$/, "").trim();
    if(!line) continue;
    const idx = line.indexOf(":");
    if(idx < 0) continue;
    const field = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();

    if(field === "sitemap"){ if(value) sitemaps.push(value); continue; }
    if(field === "user-agent"){
      if(!lastWasAgent || !current){ current = { agents: [], rules: [] }; groups.push(current); }
      current.agents.push(value.toLowerCase());
      lastWasAgent = true;
      continue;
    }
    if(field === "disallow" || field === "allow"){
      if(!current){ current = { agents: ["*"], rules: [] }; groups.push(current); }
      current.rules.push({ allow: field === "allow", path: value });
    }
    lastWasAgent = false;
  }

  // prefer rules from a group naming our agent; otherwise fall back to the "*" group
  let rules = [];
  for(const g of groups){ if(g.agents.some(a => a && a !== "*" && uaLower.includes(a))) rules = rules.concat(g.rules); }
  if(!rules.length){ for(const g of groups){ if(g.agents.includes("*")) rules = rules.concat(g.rules); } }
  return { sitemaps, rules };
}

function robotsPathMatches(pathname, pattern){
  let p = pattern, anchored = false;
  if(p.endsWith("$")){ anchored = true; p = p.slice(0, -1); }
  const re = new RegExp("^" + p.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*") + (anchored ? "$" : ""));
  return re.test(pathname);
}

// Is a path allowed by the robots rules? Longest-match wins; Allow beats equal-length Disallow. (offline-testable)
function robotsAllows(pathname, rules = []){
  let best = null;   // { allow, len }
  for(const r of rules){
    if(r.path === ""){ if(!r.allow && (!best || best.len === 0)) best = best || { allow: true, len: 0 }; continue; }
    if(robotsPathMatches(pathname, r.path)){
      const len = r.path.length;
      if(!best || len > best.len || (len === best.len && r.allow)) best = { allow: r.allow, len };
    }
  }
  return best ? best.allow : true;
}

// Pull <loc> URLs out of a sitemap (or sitemap index) XML blob, pairing each <loc> with its
// sibling <lastmod> when present. Returns { isIndex, locs:[url], entries:[{loc,lastmod}] } — `locs`
// is kept (string array) for back-compat; `entries` carries the lastmod the monitor uses to skip
// unchanged child sitemaps. (offline-testable)
function extractSitemapLocs(xml){
  const text = String(xml || "");
  const isIndex = /<sitemapindex[\s>]/i.test(text);
  const decode = (u) => u.replace(/&amp;/g, "&").replace(/&#38;/g, "&").trim();
  const entries = [];
  // Parse per <url>/<sitemap> block so a <loc> binds to the <lastmod> in the SAME block.
  const blockRe = /<(url|sitemap)\b[^>]*>([\s\S]*?)<\/\1>/gi; let b; let matchedBlock = false;
  while((b = blockRe.exec(text))){
    matchedBlock = true;
    const inner = b[2];
    const lm = /<loc>\s*([^<]+?)\s*<\/loc>/i.exec(inner);
    if(!lm) continue;
    const loc = decode(lm[1]);
    if(!loc) continue;
    const lmod = /<lastmod>\s*([^<]+?)\s*<\/lastmod>/i.exec(inner);
    entries.push({ loc, lastmod: lmod ? lmod[1].trim() : null });
  }
  if(!matchedBlock){                                 // malformed/blockless sitemap -> flat <loc> scan
    const re = /<loc>\s*([^<]+?)\s*<\/loc>/gi; let m;
    while((m = re.exec(text))){
      const u = decode(m[1]);
      if(u) entries.push({ loc: u, lastmod: null });
    }
  }
  return { isIndex, locs: entries.map(e => e.loc), entries };
}

// Walk a site's sitemaps and return every same-domain bio/contact URL that robots allows.
async function collectSitemapCandidates(domain, opts, sitemaps, rules){
  const { directoryRules = {}, genderMap = {}, _fetchDoc = fetchDoc,
          maxSitemaps = 60, maxUrls = 60000, candidateCap = 2000 } = opts;
  const root = String(domain || "").toLowerCase().replace(/^www\./, "");
  const out = new Set();
  const seenSm = new Set();
  const queue = [...sitemaps];
  let fetched = 0, scanned = 0;

  while(queue.length && fetched < maxSitemaps && scanned < maxUrls && out.size < candidateCap){
    const sm = queue.shift();
    if(seenSm.has(sm)) continue;
    seenSm.add(sm);
    const xml = await _fetchDoc(sm);
    fetched++;
    await sleep(150);                              // polite pause between sitemap fetches
    if(!xml) continue;

    const { isIndex, locs } = extractSitemapLocs(xml);
    if(isIndex){
      for(const loc of locs){ if(seenSm.size + queue.length < maxSitemaps * 4) queue.push(loc); }
      continue;
    }
    for(const loc of locs){
      if(scanned++ > maxUrls || out.size >= candidateCap) break;
      let abs; try{ abs = new URL(loc); }catch{ continue; }
      if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
      const host = abs.hostname.toLowerCase().replace(/^www\./, "");
      if(host !== root && !host.endsWith("." + root)) continue;
      if(LINK_SKIP_EXT.test(abs.pathname)) continue;
      if(!isBioOrContactUrl(abs.toString(), directoryRules, genderMap)) continue;
      if(!robotsAllows(abs.pathname, rules)) continue;
      abs.hash = "";
      out.add(abs.toString());
    }
  }
  return [...out];
}

// Extract bio/contact page URLs from USER-SUPPLIED sitemaps (uploaded or pasted), as opposed
// to collectSitemapCandidates which walks a known domain's own sitemaps. Accepts inline XML
// `content` (a urlset OR a sitemapindex) and/or a list of sitemap `urls` to fetch. Sitemap-index
// entries are fetched and recursed; gzipped sitemaps are handled transparently by fetchDoc.
// Returns every <loc> page URL that passes isBioOrContactUrl, deduped — ready to run as a
// 'webpage' job. (offline-testable via opts._fetchDoc)
async function extractBioUrlsFromSitemaps(opts = {}){
  const { content = "", urls = [], directoryRules = {}, genderMap = {}, _fetchDoc = fetchDoc,
          maxSitemaps = 100, maxUrls = 200000, candidateCap = 100000 } = opts;
  const bio = new Set();
  const seenSm = new Set();
  const queue = [];
  if(String(content || "").trim()) queue.push({ inline: content });
  for(const u of urls){ if(u) queue.push({ url: String(u).trim() }); }
  let fetched = 0, fetchedOk = 0, totalUrls = 0;

  while(queue.length && fetched < maxSitemaps && totalUrls < maxUrls && bio.size < candidateCap){
    const item = queue.shift();
    let xml = "";
    if(item.inline != null){
      xml = item.inline;
    } else {
      if(seenSm.has(item.url)) continue;
      seenSm.add(item.url);
      xml = await _fetchDoc(item.url);
      fetched++;
      if(xml) fetchedOk++;                           // distinguishes "blocked/empty" from "fetched but no matches"
      await sleep(120);                              // polite pause between sitemap fetches
    }
    if(!xml) continue;

    const { isIndex, locs } = extractSitemapLocs(xml);
    if(isIndex){                                     // sitemap index -> its <loc>s are child sitemaps
      for(const loc of locs){
        if(seenSm.has(loc)) continue;
        if(seenSm.size + queue.length >= maxSitemaps * 4) break;
        queue.push({ url: loc });
      }
      continue;
    }
    for(const loc of locs){                          // urlset -> its <loc>s are page URLs
      if(totalUrls++ >= maxUrls || bio.size >= candidateCap) break;
      let abs; try{ abs = new URL(loc); }catch{ continue; }
      if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
      if(LINK_SKIP_EXT.test(abs.pathname)) continue;
      if(!isBioOrContactUrl(abs.toString(), directoryRules, genderMap) && !findSiteApi(abs.toString())) continue;
      abs.hash = "";
      bio.add(abs.toString());
    }
  }
  return { bioUrls: [...bio], totalUrls, sitemapsFetched: fetched, sitemapsOk: fetchedOk };
}

// Like extractBioUrlsFromSitemaps, but yields the bio URLs ONE SITEMAP AT A TIME instead of
// merging every sitemap into a single (often too-large) list. Each leaf <urlset> is its own
// group; a <sitemapindex> is expanded so every child sitemap becomes its own group. The optional
// async `onGroup({ index, source, bioUrls })` callback fires as each sitemap finishes — letting
// the caller start a SEPARATE job per sitemap, so a combined job that would overflow request/
// memory limits never has to be built. Page URLs are de-duped across the whole run. When no
// onGroup is given the groups are collected and returned. (offline-testable via opts._fetchDoc)
async function extractBioUrlGroups(opts = {}){
  const { content = "", urls = [], directoryRules = {}, genderMap = {}, _fetchDoc = fetchDoc,
          onGroup = null, maxSitemaps = 100, maxUrls = 200000, candidateCap = 100000 } = opts;
  const seenSm = new Set();
  const seenBio = new Set();                          // de-dupe page URLs across all sitemaps
  const queue = [];
  if(String(content || "").trim()) queue.push({ inline: content, source: "(pasted sitemap)" });
  for(const u of urls){ if(u) queue.push({ url: String(u).trim(), source: String(u).trim() }); }
  const groups = [];
  let fetched = 0, fetchedOk = 0, totalUrls = 0, totalBio = 0, groupIndex = 0;

  while(queue.length && fetched < maxSitemaps && totalUrls < maxUrls && totalBio < candidateCap){
    const item = queue.shift();
    let xml = "";
    if(item.inline != null){
      xml = item.inline;
    } else {
      if(seenSm.has(item.url)) continue;
      seenSm.add(item.url);
      xml = await _fetchDoc(item.url);
      fetched++;
      if(xml) fetchedOk++;
      await sleep(120);                                // polite pause between sitemap fetches
    }
    if(!xml) continue;

    const { isIndex, locs } = extractSitemapLocs(xml);
    if(isIndex){                                       // index -> each child sitemap is its own group
      for(const loc of locs){
        if(seenSm.has(loc)) continue;
        if(seenSm.size + queue.length >= maxSitemaps * 4) break;
        queue.push({ url: loc, source: loc });
      }
      continue;
    }
    const bio = [];                                    // THIS sitemap's bio URLs only
    for(const loc of locs){
      if(totalUrls++ >= maxUrls || totalBio >= candidateCap) break;
      let abs; try{ abs = new URL(loc); }catch{ continue; }
      if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
      if(LINK_SKIP_EXT.test(abs.pathname)) continue;
      // keep a URL if the generic bio detector likes it OR a site adapter handles it (e.g. remax,
      // whose slugs the generic detector misses but the adapter reads cleanly from the page).
      if(!isBioOrContactUrl(abs.toString(), directoryRules, genderMap) && !findSiteApi(abs.toString())) continue;
      abs.hash = "";
      const s = abs.toString();
      if(seenBio.has(s)) continue;
      seenBio.add(s); bio.push(s); totalBio++;
    }
    if(bio.length){
      const group = { index: groupIndex++, source: item.source || "(sitemap)", bioUrls: bio };
      if(onGroup){ await onGroup(group); } else { groups.push(group); }
    }
  }
  return { groups, totalGroups: groupIndex, totalBioUrls: totalBio, totalUrls,
           sitemapsFetched: fetched, sitemapsOk: fetchedOk };
}

// Find the CHILD sitemaps that are *dedicated to people/bio pages* — the ones worth monitoring for
// new hires. Walks an index (or a single urlset), and for every leaf <urlset> computes its bio-ratio
// (fraction of <loc>s that pass isBioOrContactUrl). A child qualifies as a watch when it has at least
// `minBioCount` bio URLs AND bioRatio >= `minBioRatio` (so `agents-sitemap.xml` qualifies but a mixed
// `pages-sitemap.xml` or a `blog-sitemap.xml` does not). Each watch carries the child's <lastmod> from
// its parent index entry (lets the nightly pass skip unchanged children) and its bio URLs paired with
// their own <lastmod>. Returns { watches:[{sitemapUrl,parentUrl,lastmod,urlCount,bioCount,bioRatio,
// domain,bioUrls:[{url,lastmod}]}], sitemapsFetched, totalUrls }. (offline-testable via opts._fetchDoc)
async function discoverBioSitemaps(opts = {}){
  const { content = "", urls = [], directoryRules = {}, genderMap = {}, _fetchDoc = fetchDoc,
          minBioRatio = 0.30, minBioCount = 3, maxSitemaps = 200, maxUrls = 500000,
          bioSitemapNames = null, sourceUrl = "" } = opts;
  // A child sitemap whose FILENAME is a known people/bio sitemap (e.g. agents-sitemap.xml) is treated
  // as bio-dedicated regardless of its bio-ratio, and ALL its <loc>s are captured (the slug may not
  // match the generic detector). bioSitemapNames is a Set of lowercased filenames.
  const fileNameOf = (u) => String(u || "").split("?")[0].split("#")[0].split("/").pop().toLowerCase();
  const seenSm = new Set();
  const queue = [];
  if(String(content || "").trim()) queue.push({ inline: content, url: sourceUrl || "(pasted sitemap)", parent: null, lastmod: null });
  for(const u of urls){ if(u) queue.push({ url: String(u).trim(), parent: null, lastmod: null }); }
  const watches = [];
  let fetched = 0, totalUrls = 0;

  while(queue.length && fetched < maxSitemaps && totalUrls < maxUrls){
    const item = queue.shift();
    let xml = "";
    if(item.inline != null){ xml = item.inline; }
    else {
      if(seenSm.has(item.url)) continue;
      seenSm.add(item.url);
      xml = await _fetchDoc(item.url);
      fetched++;
      await sleep(120);
    }
    if(!xml) continue;

    const { isIndex, entries } = extractSitemapLocs(xml);
    if(isIndex){                                       // index -> queue each child, carrying its <lastmod>
      for(const e of entries){
        if(seenSm.has(e.loc)) continue;
        if(seenSm.size + queue.length >= maxSitemaps * 4) break;
        queue.push({ url: e.loc, parent: item.url || null, lastmod: e.lastmod });
      }
      continue;
    }
    // leaf urlset -> score how bio-dedicated it is (or accept outright if its filename is a known bio sitemap)
    const nameHit = !!(bioSitemapNames && bioSitemapNames.has(fileNameOf(item.url)));
    let total = 0; const bio = [];
    for(const e of entries){
      total++; totalUrls++;
      let abs; try{ abs = new URL(e.loc); }catch{ continue; }
      if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
      if(LINK_SKIP_EXT.test(abs.pathname)) continue;
      // name-matched sitemap: keep every page URL (the sitemap IS the people directory); otherwise keep
      // only the ones the generic bio detector / a site adapter recognizes.
      if(!nameHit && !isBioOrContactUrl(abs.toString(), directoryRules, genderMap) && !findSiteApi(abs.toString())) continue;
      abs.hash = "";
      bio.push({ url: abs.toString(), lastmod: e.lastmod });
    }
    const ratio = total ? bio.length / total : 0;
    const qualifies = nameHit ? bio.length >= 1 : (bio.length >= minBioCount && ratio >= minBioRatio);
    if(qualifies){
      watches.push({
        sitemapUrl: item.url,
        parentUrl: item.parent || null,
        lastmod: item.lastmod || null,
        urlCount: total,
        bioCount: bio.length,
        bioRatio: ratio,
        byName: nameHit,
        domain: bio.length ? hostOf(bio[0].url) : (item.url ? hostOf(item.url) : ""),
        bioUrls: bio,
      });
    }
  }
  return { watches, sitemapsFetched: fetched, totalUrls };
}

// Sitemap filenames that are content-type feeds, NOT people/location directories. Excluded from
// RATIO-based classification (a people/location name-lexicon match still wins), so e.g. attachment /
// meeting / taxonomy / blog sitemaps whose URLs happen to trip the bio detector don't pollute the Library.
const NEG_SITEMAP_NAME = /(?:^|[\/_-])(attachment|attachments|media|image|images|gallery|galleries|photo|photos|blog|news|article|articles|post|posts|press|press-release|event|events|meeting|meetings|calendar|taxonomy|categor(?:y|ies)|tag|tags|product|products|video|videos|download|downloads|faq|faqs|review|reviews|course|courses|webinar|webinars)(?:[-_.]|$)/i;

// Like discoverBioSitemaps, but classifies each qualifying child sitemap as People OR Location and
// returns watches carrying `kind`. People = filename in bioSitemapNames OR bio-ratio passes; Location =
// filename in locationSitemapNames OR location-ratio passes. When both pass, the higher ratio wins.
// Powers the Sitemap Library build-out (discover-sitemaps.js). (offline-testable via _fetchDoc)
// Delay between consecutive sitemap fetches on the SAME host during an index walk (env-tunable).
const SITEMAP_FETCH_DELAY_MS = Number(process.env.SITEMAP_FETCH_DELAY_MS) || 120;
async function discoverSitemaps(opts = {}){
  const { content = "", urls = [], directoryRules = {}, genderMap = {}, _fetchDoc = fetchDoc,
          minRatio = 0.30, minCount = 3, maxSitemaps = 200, maxUrls = 500000,
          bioSitemapNames = null, locationSitemapNames = null, keywordHints = null, sourceUrl = "" } = opts;
  const fileNameOf = (u) => String(u || "").split("?")[0].split("#")[0].split("/").pop().toLowerCase();
  const seenSm = new Set();
  const queue = [];
  if(String(content || "").trim()) queue.push({ inline: content, url: sourceUrl || "(pasted sitemap)", parent: null, lastmod: null });
  for(const u of urls){ if(u) queue.push({ url: String(u).trim(), parent: null, lastmod: null }); }
  const watches = [];
  let fetched = 0, totalUrls = 0;

  while(queue.length && fetched < maxSitemaps && totalUrls < maxUrls){
    const item = queue.shift();
    let xml = "";
    if(item.inline != null){ xml = item.inline; }
    else {
      if(seenSm.has(item.url)) continue;
      seenSm.add(item.url);
      // Space consecutive fetches on this host, but only BETWEEN them. This used to sleep after every
      // fetch, so a leaf sitemap — one fetch, no children, which is what the expander asks for 100k+
      // times — paid the full delay for nothing. Spacing within an index walk is unchanged.
      if(fetched > 0) await sleep(SITEMAP_FETCH_DELAY_MS);
      xml = await _fetchDoc(item.url);
      fetched++;
    }
    if(!xml) continue;

    const { isIndex, entries } = extractSitemapLocs(xml);
    if(isIndex){                                        // index -> queue each child, carrying its <lastmod>
      for(const e of entries){
        if(seenSm.has(e.loc)) continue;
        if(seenSm.size + queue.length >= maxSitemaps * 4) break;
        queue.push({ url: e.loc, parent: item.url || null, lastmod: e.lastmod });
      }
      continue;
    }
    const fname = fileNameOf(item.url);
    let bioName = !!(bioSitemapNames && bioSitemapNames.has(fname));
    const locName = !!(locationSitemapNames && locationSitemapNames.has(fname));
    // keyword second-pass: a child/leaf whose filename CONTAINS a trusted keyword token is treated as a
    // People directory (keep all its URLs), as long as it isn't a known non-directory feed. Lets the
    // monitor rescue People sitemaps the strict filename/ratio pass missed (e.g. numeric-id agent URLs).
    if (!bioName && !locName && keywordHints && keywordHints.size && !NEG_SITEMAP_NAME.test(fname)) {
      for (const k of keywordHints) { if (k && k.length >= 3 && fname.includes(k)) { bioName = true; break; } }
    }
    if (!bioName && !locName && NEG_SITEMAP_NAME.test(fname)) continue;   // content-type feed, not a people/location directory
    let total = 0; const bio = [], loc = [], all = [];
    for(const e of entries){
      total++; totalUrls++;
      let abs; try{ abs = new URL(e.loc); }catch{ continue; }
      if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
      if(LINK_SKIP_EXT.test(abs.pathname)) continue;
      abs.hash = "";
      const rec = { url: abs.toString(), lastmod: e.lastmod };
      all.push(rec);
      if(isBioOrContactUrl(rec.url, directoryRules, genderMap) || findSiteApi(rec.url)) bio.push(rec);
      if(isLocationUrl(rec.url)) loc.push(rec);
    }
    if(!total) continue;
    const bioRatio = bio.length / total, locRatio = loc.length / total;
    // name match keeps EVERY page URL (the sitemap IS the directory); else keep the matching subset.
    let kind = null, items = null, ratio = 0, byName = false, keyword = "";
    if(bioName){ kind = "People"; items = all; byName = true; ratio = bioRatio; keyword = fname; }
    else if(locName){ kind = "Location"; items = all; byName = true; ratio = locRatio; keyword = fname; }
    else {
      const bioQ = bio.length >= minCount && bioRatio >= minRatio;
      const locQ = loc.length >= minCount && locRatio >= minRatio;
      if(bioQ && bioRatio >= locRatio){ kind = "People"; items = bio; ratio = bioRatio; }
      else if(locQ){ kind = "Location"; items = loc; ratio = locRatio; }
      else if(bioQ){ kind = "People"; items = bio; ratio = bioRatio; }
    }
    if(kind){
      watches.push({
        sitemapUrl: item.url, parentUrl: item.parent || null, lastmod: item.lastmod || null,
        kind, keyword, urlCount: total, itemCount: items.length, ratio, byName,
        domain: items.length ? hostOf(items[0].url) : (item.url ? hostOf(item.url) : ""),
        urls: items,
      });
    }
  }
  return { watches, sitemapsFetched: fetched, totalUrls };
}

// Discover bio/contact page URLs for one or more DOMAINS straight from the Common Crawl index —
// no sitemap needed. For each domain it does ONE paginated CDX query (loadDomainIndex) to list
// every archived 200/text-html capture, then keeps the URLs that pass isBioOrContactUrl. Lets a
// user paste bare domains and have the system auto-find archived people-pages in the background.
// Returns { bioUrls, totalUrls, domainsScanned, perDomain } — bioUrls run as a normal 'webpage'
// job (which then reads each from CC via the bulk index cache). (offline-testable via opts._loadDomainIndex)
async function discoverBioUrlsFromCC(opts = {}){
  const { domains = [], directoryRules = {}, genderMap = {}, crawl = CRAWL,
          maxPages = 100, maxBioPerDomain = 5000, _loadDomainIndex = loadDomainIndex,
          onProgress = () => {} } = opts;
  const hosts = [...new Set(domains.map(d => hostOf(String(d || "").trim())).filter(Boolean))];
  const bio = new Set();
  const perDomain = {};
  let totalUrls = 0, scanned = 0;
  for(const host of hosts){
    let m;
    try{ m = await _loadDomainIndex(host, { crawl, maxPages }); }
    catch(e){ perDomain[host] = { error: e.message, bio: 0, urls: 0 }; scanned++; onProgress({ host, scanned, total: hosts.length, error: e.message }); continue; }
    let kept = 0;
    for(const rec of m.values()){
      totalUrls++;
      const u = rec && rec.url; if(!u) continue;
      let abs; try{ abs = new URL(u); }catch{ continue; }
      if(abs.protocol !== "http:" && abs.protocol !== "https:") continue;
      if(LINK_SKIP_EXT.test(abs.pathname)) continue;
      if(!isBioOrContactUrl(abs.toString(), directoryRules, genderMap)) continue;
      if(kept >= maxBioPerDomain) break;
      abs.hash = ""; abs.search = "";
      if(!bio.has(abs.toString())){ bio.add(abs.toString()); kept++; }
    }
    perDomain[host] = { bio: kept, urls: m.size };
    scanned++;
    onProgress({ host, scanned, total: hosts.length, bio: kept, urls: m.size });
  }
  return { bioUrls: [...bio], totalUrls, domainsScanned: scanned, perDomain };
}

// Fetch a page over plain HTTP/1.1 using Node's built-in http(s). We deliberately
// avoid global fetch here: live crawling hits thousands of arbitrary servers, and
// fetch's HTTP/2 path can emit an UNCATCHABLE socket 'error' event when a server
// drops the connection, which kills the whole run. http(s) lets us handle every
// failure locally and just return "" for a bad page. Returns "" on any problem.
// Returns the body string, OR (with opts.returnMeta) { status, body }. status 0 = network error.
function httpGetRaw(url, opts = {}){
  const { redirectsLeft = 4, accept = /html/, maxBytes = 4 * 1024 * 1024, returnMeta = false } = opts;
  return new Promise((resolve) => {
    let settled = false;
    const finish = (status, body) => { if(!settled){ settled = true; resolve(returnMeta ? { status, body } : body); } };
    let u;
    try{ u = new URL(url); }catch{ return finish(0, ""); }
    const lib = u.protocol === "http:" ? http : https;
    const isHttp = u.protocol === "http:";
    // opts.proxyTier: 'fallback' -> residential gateway; else primary rotating proxy / direct.
    // (The website unblocker is an HTTP API, not a proxy — see unblockerFetch.)
    const agent = (opts.proxyTier === "fallback" && PROXY_FALLBACK_URL)
      ? (isHttp ? proxyAgentHttpFb : proxyAgentHttpsFb)              // residential fallback
      : PROXY_URL
        ? (isHttp ? proxyAgentHttp : proxyAgentHttps)               // primary rotating proxy
        : (lib === http ? keepAliveHttp : keepAliveHttps);          // direct

    const reqOpts = {
      method: "GET",
      agent,
      headers: {
        "User-Agent": opts.userAgent || UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
      },
      timeout: opts.timeout || 15000,
    };
    const req = lib.request(u, reqOpts, (res) => {
      const status = res.statusCode || 0;

      // follow redirects (propagate returnMeta)
      if(status >= 300 && status < 400 && res.headers.location && redirectsLeft > 0){
        res.resume();
        let next;
        try{ next = new URL(res.headers.location, u).toString(); }catch{ return finish(status, ""); }
        return resolve(httpGetRaw(next, { ...opts, redirectsLeft: redirectsLeft - 1 }));
      }

      const ct = (res.headers["content-type"] || "").toLowerCase();
      if(status !== 200 || (accept && ct && !accept.test(ct))){ res.resume(); return finish(status, ""); }

      const enc = (res.headers["content-encoding"] || "").toLowerCase();
      let stream = res;
      try{
        if(enc === "gzip") stream = res.pipe(zlib.createGunzip());
        else if(enc === "deflate") stream = res.pipe(zlib.createInflate());
        else if(enc === "br") stream = res.pipe(zlib.createBrotliDecompress());
      }catch{ res.resume(); return finish(status, ""); }

      const chunks = []; let bytes = 0;
      stream.on("data", (c) => { bytes += c.length; if(bytes <= maxBytes) chunks.push(c); else { req.destroy(); } });
      stream.on("end", () => {
        let buf = Buffer.concat(chunks);
        if(buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b){ try{ buf = zlib.gunzipSync(buf); }catch{ /* keep raw */ } }
        finish(200, buf.toString("utf8"));
      });
      stream.on("error", () => finish(status, ""));
      res.on("error", () => finish(status, ""));
    });

    req.on("error", () => finish(0, ""));               // DNS failure, reset, TLS error, etc.
    req.on("timeout", () => { req.destroy(); finish(0, ""); });
    req.end();
  });
}

// Website-unblocker API (NetNut): POST {url, format:"html"} to the configured endpoint, which
// renders the page in a real browser (solving Akamai/Cloudflare JS+TLS challenges) and returns
// the final HTML. Slow (~60-90s) + billed per request, so liveFetchPage only calls it as a last
// resort. Returns the HTML on success, "" otherwise. Endpoint + Basic-auth creds come from
// UNBLOCKER_API_URL (https://USER:PASS@host/path).
function unblockerFetch(target, timeoutMs = 120000){
  return new Promise((resolve) => {
    if(!UNBLOCKER_API_URL) return resolve("");
    let api; try{ api = new URL(UNBLOCKER_API_URL); }catch{ return resolve(""); }
    const lib = api.protocol === "http:" ? http : https;
    const auth = (api.username || api.password)
      ? `${decodeURIComponent(api.username)}:${decodeURIComponent(api.password)}` : undefined;
    const body = JSON.stringify({ url: target, format: "html" });
    let settled = false;
    const done = (s) => { if(!settled){ settled = true; resolve(s); } };
    const req = lib.request({
      protocol: api.protocol, hostname: api.hostname, port: api.port || undefined,
      path: api.pathname + api.search, method: "POST", auth,
      headers: {
        "Content-Type": "application/json", "Accept": "text/html, application/json",
        "Accept-Encoding": "gzip, deflate, br", "Content-Length": Buffer.byteLength(body),
      },
      timeout: timeoutMs,
    }, (res) => {
      const status = res.statusCode || 0;
      const enc = (res.headers["content-encoding"] || "").toLowerCase();
      let stream = res;
      try{
        if(enc === "gzip") stream = res.pipe(zlib.createGunzip());
        else if(enc === "deflate") stream = res.pipe(zlib.createInflate());
        else if(enc === "br") stream = res.pipe(zlib.createBrotliDecompress());
      }catch{ res.resume(); return done(""); }
      const chunks = []; let bytes = 0;
      stream.on("data", (c) => { bytes += c.length; if(bytes <= HTML_MAX_BYTES) chunks.push(c); else req.destroy(); });
      stream.on("end", () => {
        if(status !== 200) return done("");
        const text = Buffer.concat(chunks).toString("utf8");
        // format:html returns raw HTML, but tolerate a JSON envelope ({html|content|data|body})
        if(text.trimStart().startsWith("{")){
          try{ const j = JSON.parse(text); return done(j.html || j.content || j.data || j.body || ""); }
          catch{ return done(""); }
        }
        done(text);
      });
      stream.on("error", () => done(""));
      res.on("error", () => done(""));
    });
    req.on("error", () => done(""));
    req.on("timeout", () => { req.destroy(); done(""); });
    req.write(body); req.end();
  });
}

async function liveFetchPage(url){
  // honor an explicit proxy via undici when one is configured; otherwise use the
  // crash-proof built-in http(s) path. Counts blocks (403/429/503) and backs off once.
  if(proxyEnv && ProxyAgent && undiciFetch){
    try{
      const res = await fetchImpl(url, {
        headers:{ "User-Agent":UA, "Accept":"text/html,application/xhtml+xml" },
        redirect:"follow",
        signal: AbortSignal.timeout(LIVE_TIMEOUT),
      });
      if(res.status === 429 || res.status === 503 || res.status === 403){ _net.blocked++; return ""; }
      if(!res.ok) { _net.fetched++; return ""; }
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      _net.fetched++;
      if(ct && !ct.includes("html")) return "";
      return await res.text();
    }catch{ return ""; }
  }
  // Escalate cheapest-first across proxy tiers, then the unblocker API as a last resort. A
  // tier is tried only if the previous one was BLOCKED (403/429/503), never on a 404/network
  // error. With a rotating gateway a block means that exit IP was flagged, so we retry to roll
  // a fresh IP.
  const TIERS = [
    { name: "primary",  attempts: PROXY_URL ? 4 : 2, proxied: !!PROXY_URL },
    { name: "fallback", attempts: 4,                 proxied: true, on: !!PROXY_FALLBACK_URL },
  ].filter(t => t.on !== false);
  for(const tier of TIERS){
    const rotates = tier.proxied;                            // a gateway is in front -> a fresh IP per attempt
    for(let attempt = 0; attempt < tier.attempts; attempt++){
      const r = await httpGetRaw(url, { accept: /html/, maxBytes: HTML_MAX_BYTES, returnMeta: true,
        proxyTier: tier.name, timeout: LIVE_TIMEOUT, userAgent: tier.proxied ? BROWSER_UA : UA });
      if(r.status === 200){ _net.fetched++; return r.body; }
      if(r.status === 403 || r.status === 429 || r.status === 503){                            // blocked
        _net.blocked++;
        const canRetry = attempt < tier.attempts - 1 && (rotates || r.status === 429 || r.status === 503);
        if(canRetry){ await sleep(rotates ? 400 : 800); continue; }
        break;                                                                                 // tier exhausted -> escalate
      }
      _net.fetched++;                                                                          // 404 / other -> definitive, don't escalate
      return "";
    }
  }
  // Last resort: the website-unblocker API (real browser; slow + per-request cost).
  if(UNBLOCKER_API_URL){
    const html = await unblockerFetch(url);
    if(html){ _net.fetched++; return html; }
    _net.blocked++;
  }
  return "";
}

// Fetch robots.txt / sitemaps — XML, plain text, or gzipped, possibly large.
// opts.timeout       — per-request timeout (default 15s)
// opts.fallbackStatus — which primary statuses justify a second, residential attempt. Default null keeps
//   the original "retry anything that wasn't a 200" behaviour. That default is right for a handful of
//   important documents and wrong for a 237,018-sitemap sweep: a dead host times out on the primary path
//   AND again on the residential one, so every dead sitemap cost up to 30s and dead sitemaps are the
//   majority. Passing [403,429,503] keeps the Cloudflare escalation that the fallback exists for while
//   not spending residential bandwidth on hosts that are simply gone.
async function fetchDoc(url, opts = {}){
  const accept = /xml|text|gzip|octet-stream|html|rss|plain/;
  const timeout = opts.timeout || 15000;
  // 30MB per document is right for a deliberate fetch of one big sitemap. It is not right for hundreds of
  // concurrent ones: 320 in flight against a 6.6GB heap is 9.6GB of worst case, which is what aborted the
  // app process (exit 134) during the first nightly sweep. Sweeps pass a much smaller ceiling.
  const maxBytes = opts.maxBytes || 30 * 1024 * 1024;
  let r = await httpGetRaw(url, { accept, maxBytes, returnMeta: true, timeout });
  if(r.status === 200 && r.body) return r.body;
  const mayFallback = !opts.fallbackStatus || opts.fallbackStatus.includes(r.status);
  // escalate to the residential gateway for docs blocked on the primary path (e.g. a Cloudflare-
  // fronted sitemap) so big agent sitemaps still come through.
  if(PROXY_FALLBACK_URL && mayFallback){
    r = await httpGetRaw(url, { accept, maxBytes, returnMeta: true, proxyTier: "fallback", timeout });
    if(r.status === 200 && r.body) return r.body;
  }
  return "";
}

async function liveCrawl(domain, opts = {}){
  const { wireless, genderMap = {}, directoryRules = {} } = opts;
  const liveFetch = opts._liveFetch || liveFetchPage;     // fetch HTML pages
  const docFetch  = opts._fetchDoc  || fetchDoc;          // fetch robots.txt / sitemaps
  const maxPages = opts.maxPages || Number(process.env.LIVE_MAX_PAGES) || 150;   // raise to crawl more per site
  const perDomainCap = opts.perDomainCap || maxPages;
  const today = new Date().toISOString().slice(0, 10);
  const records = [];
  const seen = new Set();

  // 0) robots.txt — gives us sitemap locations (to find every page) AND Disallow rules (to be polite)
  let rules = [], sitemaps = [];
  for(const ru of [`https://${domain}/robots.txt`, `https://www.${domain}/robots.txt`]){
    const txt = await docFetch(ru);
    if(txt){ const parsed = parseRobots(txt, UA); rules = parsed.rules; sitemaps = parsed.sitemaps; break; }
  }
  if(!sitemaps.length) sitemaps = [`https://${domain}/sitemap.xml`, `https://${domain}/sitemap_index.xml`];
  const allowed = (urlStr) => { try{ return robotsAllows(new URL(urlStr).pathname, rules); }catch{ return true; } };

  // 1) homepage — try the bare host, then www (also our link-discovery seed)
  let homeHtml = "", homeUrl = "";
  for(const u of [`https://${domain}/`, `https://www.${domain}/`]){
    if(!allowed(u)) continue;
    seen.add(u);
    homeHtml = await liveFetch(u);
    if(homeHtml){ homeUrl = u; break; }
    await sleep(250);
  }
  if(homeHtml){
    const homeRec = extractRecord(homeHtml, homeUrl, { wireless, genderMap, directoryRules, source:"Live Crawl", timestamp: today });
    if(homeRec) records.push(homeRec);
  }

  // 2) build the candidate queue (respecting robots Disallow)
  const queue = [];
  const enqueue = (url) => {
    if(seen.has(url) || queue.length >= maxPages * 4) return;
    if(!allowed(url)) return;
    seen.add(url); queue.push(url);
  };

  // 2a) PRIMARY: every bio/contact URL listed in the site's sitemaps
  const fromSitemaps = await collectSitemapCandidates(domain,
    { directoryRules, genderMap, _fetchDoc: docFetch,
      maxSitemaps: opts.maxSitemaps, maxUrls: opts.maxSitemapUrls, candidateCap: maxPages * 4 },
    sitemaps, rules);
  for(const u of fromSitemaps) enqueue(u);
  const sitemapMatches = fromSitemaps.length;

  // 2b) plus anything the homepage links to, and common probe paths (catches pages absent from sitemaps)
  if(homeHtml){
    for(const u of extractSameDomainLinks(homeHtml, homeUrl || `https://${domain}/`, domain)){
      if(isBioOrContactUrl(u, directoryRules, genderMap)) enqueue(u);
    }
  }
  const probeSet = new Set();                                    // static probe paths often 404 legitimately —
  for(const p of LIVE_PROBE_PATHS){ const pu = `https://${domain}${p}`; probeSet.add(pu); enqueue(pu); }  // don't let them trip the block-streak

  if(sitemapMatches > maxPages){
    console.log(`  ${domain}: ${sitemapMatches} matching pages in sitemaps — fetching first ${maxPages} (raise LIVE_MAX_PAGES to get all)`);
  }

  // 3) crawl the queue with small in-site concurrency: a few pages from THIS site at
  //    once (still one site), each followed by a polite pause. The queue grows as we
  //    discover deeper bio links (e.g. /attorneys/ -> /attorneys/jane-doe/).
  const inSite = Math.max(1, opts.inSiteConcurrency || Number(process.env.IN_SITE_CONCURRENCY) || 3);
  const perHostDelay = opts.perHostDelay != null ? opts.perHostDelay : 200;
  const shouldStop = opts.shouldStop || (() => false);
  let qi = 0, active = 0, fetchedPages = 0, blockStreak = 0, bailed = false;

  await new Promise((resolve) => {
    const tick = () => {
      if(active === 0 && (qi >= queue.length || records.length >= perDomainCap || fetchedPages >= maxPages || bailed || shouldStop())){
        return resolve();
      }
      while(active < inSite && qi < queue.length && records.length < perDomainCap && fetchedPages < maxPages && !bailed && !shouldStop()){
        const url = queue[qi++]; active++; fetchedPages++;
        (async () => {
          const html = await liveFetch(url);
          await sleep(perHostDelay);              // polite pause per fetch (with ~inSite in flight)
          if(html){
            blockStreak = 0;                      // reachable -> reset the fast-fail streak
            const out = extractRecord(html, url, { wireless, genderMap, directoryRules, source:"Live Crawl", timestamp: today });
            if(out) records.push(out);
            for(const sub of extractSameDomainLinks(html, url, domain)){
              if(isBioOrContactUrl(sub, directoryRules, genderMap)) enqueue(sub);
            }
          } else if(!probeSet.has(url) && ++blockStreak >= LIVE_BLOCK_LIMIT && !bailed){
            bailed = true;                        // N real bio/sitemap pages in a row unreachable -> stop grinding this host
            console.log(`  ${domain}: ${blockStreak} pages in a row unreachable — skipping the rest (host appears to be blocking)`);
          }
        })().catch(() => {}).finally(() => { active--; tick(); });
      }
    };
    tick();
  });

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
  // Fetch Common Crawl's PUBLIC data store (data.commoncrawl.org, S3/CloudFront) DIRECTLY — it serves
  // bulk WARC ranges to anyone and scales, so the slow/metered live-fetch proxy is neither needed nor
  // wanted here (proxying CC data was ~12x slower on prod). The live-fetch + index paths still proxy.
  // WARC data store scales (S3/CloudFront) — fetch in parallel via its own pool, NOT the index lane.
  return warcLimit(async () => {
    const res = await fetchImpl(url, opts);
    if(!res.ok && res.status !== 206) throw new Error(`warc ${res.status} for ${rec.url}`);
    const gz = Buffer.from(await res.arrayBuffer());
    return warcToHtml(zlib.gunzipSync(gz));
  });
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
    // email-less records (webpage mode, kept for email modelling) de-dupe by source URL so
    // they don't all collapse onto one row; rows with neither email nor URL are dropped.
    const key = email || ("url:" + String(r["Web Source URL"] || "").trim().toLowerCase());
    if(key === "url:") continue;
    const current = best.get(key);
    if(!current || scoreRecord(r) > scoreRecord(current)){
      best.set(key, r);
    }
  }
  return [...best.values()];
}

// ---------------------------------------------------------------- orchestration
async function run(csvPath, opts = {}){
  const {
    wirelessPath = (__dirname + "/phone-blocks.csv"),
    genderMap = {}, directoryRules = {}, outPath = "cc-results.csv",
    // injectable for testing; default to the real network functions
    _queryIndex = queryIndex, _queryIndexUrl = queryIndexUrl, _fetchWarc = fetchWarc, _liveCrawl = liveCrawl,
    _findSiteApi = findSiteApi,  // per-site JSON-API adapters (site-apis.js); injectable for tests
    liveFallback = true,        // when CC has nothing / 504s, crawl the live site
    shouldStop = () => false,   // cooperative cancel: when true, stop taking new domains
    onRecord = () => {}, onProgress = () => {},
  } = opts;

  // mode: 'domain' (crawl whole domain, default) | 'webpage' (only the exact URLs given)
  const mode = opts.mode === 'webpage' ? 'webpage' : 'domain';
  // Webpage/sitemap jobs: bulk-load each domain's CC index once + serve every URL from memory
  // (one index query per domain instead of one per URL). WARC fetches then run in parallel.
  if(mode === 'webpage' && !opts._domainIndexCache) opts._domainIndexCache = new Map();
  const fetchPage = opts._liveFetch || liveFetchPage;
  const today = new Date().toISOString().slice(0, 10);
  resetNetStats();
  // cap simultaneous requests to any one host even when total concurrency is high
  const hostGate = makeKeyedLimiter(Math.max(1, Number(process.env.HOST_CONCURRENCY) || 3));

  const lines = Array.isArray(opts._items) ? opts._items : fs.readFileSync(csvPath, "utf8").split(/\r?\n/);
  const domains = mode === 'webpage' ? normalizeUrlList(lines) : normalizeDomainList(lines);
  const wireless = loadWirelessBlocks(wirelessPath);
  console.log(`${mode === 'webpage' ? 'Webpages' : 'Domains'}: ${domains.length}   Wireless blocks: ${wireless.size.toLocaleString()}   Crawl: ${CRAWL}\n`);

  const all = [];
  const seenEmails = new Map();  // de-dupe live onRecord callbacks: best record per email
  const coverage = { found:0, live:0, empty:0, errored:0 };

  // push a record into the result set, emitting the best-per-email to onRecord
  const ingest = (out) => {
    all.push(out);
    const email = String(out["Email Address"] || "").trim().toLowerCase();
    // de-dupe by email; email-less records (webpage mode, kept for email modelling) would
    // ALL collapse onto the "" key, so key those by their source URL instead.
    const key = email || ("url:" + String(out["Web Source URL"] || "").trim().toLowerCase());
    const current = seenEmails.get(key);
    if(!current || scoreRecord(out) > scoreRecord(current)){
      seenEmails.set(key, out);
      onRecord(out);
    }
  };

  const liveOnly = opts.liveOnly === true || process.env.LIVE_ONLY === 'true';
  // webpage/sitemap URLs are cheap (cached CC lookup + parallel WARC), so run many at once;
  // whole-site domain crawls are heavier, so keep those modest.
  const domainConcurrency = Math.max(1, opts.concurrency
    || Number(process.env.DOMAIN_CONCURRENCY)
    || (mode === 'webpage' ? (Number(process.env.WEBPAGE_CONCURRENCY) || 24) : 6));
  let ccFailStreak = 0, ccDisabled = liveOnly;   // circuit breaker; liveOnly skips CC entirely
  if(liveOnly) console.log("(live-only mode: skipping Common Crawl)");
  console.log(`Crawling up to ${domainConcurrency} domain(s) at once...\n`);

  // process ONE domain: Common Crawl first (unless disabled), then live-crawl fallback
  async function processDomain(domain, index){
    const domainNumber = index + 1;
    onProgress({ status: 'domain-start', domain, index: domainNumber, total: domains.length });

    // ---- WEBPAGE mode: this exact URL only. Common Crawl archive first (bypasses live
    //      blocks like Cloudflare), then live-fetch fallback. No domain crawl. ----
    if(mode === 'webpage'){
      let kept = 0, wnote = "", fromCC = false, fromUrl = false, fromApi = false;
      // 0) Site API: large directories (century21, …) render their bio pages with JavaScript, so
      //    the crawler only sees an empty shell — CC and live fetch both come back blank. When a
      //    registered adapter handles this domain, pull the record straight from the site's own
      //    JSON API (cheap, exact: name/email/phone incl. cell => Mobile). See site-apis.js.
      const siteApi = _findSiteApi(domain);
      if(siteApi){
        try{
          const out = await siteApi.fetchRecord(domain, { wireless, genderMap, directoryRules, source: "Site API", timestamp: today, allowNoEmail: true, _getText: fetchPage });
          if(out){
            ingest(out); kept++; fromApi = true;
            _siteApiSeen++;
            if(_siteApiSeen <= 5 || _siteApiSeen % 2000 === 0){   // sample so the logs show fields populating
              console.log(`Site API sample #${_siteApiSeen}: ${(out["First"]||"")} ${(out["Last"]||"")} | email=${out["Email Address"]||"-"} | phone=${out["Phone"]||"-"} [${out["Phone Type"]||"-"}] | loc=${out["Phone Location"]||"-"}`);
            }
          }
        }catch(e){ /* adapter failed -> fall through to Common Crawl / live */ }
      }
      // 1) Common Crawl: read the archived snapshot of this exact URL. Prefer a PRE-RESOLVED WARC
      //    pointer (from cc-domain-miner --warc-out): fetch the archived record DIRECTLY, skipping the
      //    per-domain index lookup — the bottleneck for many-small-domain batches — and works even if
      //    the index is down. Otherwise do an exact-URL index lookup.
      if(kept === 0){
        const ptr = opts._warcByUrl && opts._warcByUrl.get(domain);   // { url, filename, offset, length, timestamp? }
        let rec = ptr || null;
        if(!rec && !ccDisabled){
          try{ rec = await _queryIndexUrl(domain, opts); ccFailStreak = 0; }   // index responded → it's up
          catch(e){ if(++ccFailStreak >= 3){ ccDisabled = true; console.log("(Common Crawl unreachable — webpage mode falling back to live only)"); } }
        }
        if(rec){
          let html = ""; try{ html = await _fetchWarc(rec, opts); }catch{ html = ""; }
          if(html){
            const ts = (rec.timestamp||"").slice(0,8).replace(/(\d{4})(\d{2})(\d{2})/,"$1-$2-$3");
            const out = extractRecord(html, domain, { wireless, genderMap, directoryRules, source:"Common Crawl", timestamp: ts, allowNoEmail: true });
            if(out){ ingest(out); kept++; fromCC = true; }
          }
        }
      }
      // 2) live fetch fallback (the original webpage behavior) when CC had nothing
      if(kept === 0){
        try{
          const html = await hostGate(hostOf(domain), () => fetchPage(domain));   // ≤HOST_CONCURRENCY per host
          if(html){
            const out = extractRecord(html, domain, { wireless, genderMap, directoryRules, source:"Webpage", timestamp: today, allowNoEmail: true });
            if(out){ ingest(out); kept++; }
          } else { wnote = "page not reachable"; }
        }catch(e){ wnote = e.message; }
      }
      // 3) URL-only fallback: the page couldn't be fetched (blocked/404) and isn't in Common
      //    Crawl, but the URL itself names a person under a known directory (e.g. an agent
      //    profile behind bot protection). Build a record from the URL alone — name + role +
      //    gender; the email is modelled later if the company's pattern is known.
      if(kept === 0){
        const out = extractRecord("", domain, { wireless, genderMap, directoryRules, source:"URL", timestamp: today, allowNoEmail: true });
        if(out){ ingest(out); kept++; fromUrl = true; }
      }
      if(kept > 0){
        if(fromCC || fromApi) coverage.found++; else coverage.live++;
        const via = fromApi ? 'Site API' : fromCC ? 'Common Crawl' : fromUrl ? `URL only${wnote ? ` (${wnote})` : ''}` : 'webpage';
        console.log(`◆ ${domain.slice(0,48).padEnd(48)} ${kept} record(s) via ${via}`);
        onProgress({ status:'domain-done', domain, index: domainNumber, total: domains.length, source: fromApi ? 'Site API' : fromCC ? 'Common Crawl' : fromUrl ? 'URL' : 'Webpage', kept });
      }else{
        coverage.empty++;
        console.log(`· ${domain.slice(0,48).padEnd(48)} no contacts found${wnote ? `  (${wnote})` : ""}`);
        onProgress({ status:'no-candidates', domain, index: domainNumber, total: domains.length });
      }
      return;
    }

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
            }
          }
        }
      }
    }catch(e){                                   // 504 / outage → treat as "not in crawl", fall through to live
      note = e.message;
      ccFailStreak++;
      if(ccFailStreak >= 3 && !ccDisabled){
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

  // worker pool: crawl several DIFFERENT domains at once (each domain stays polite
  // internally; Common Crawl stays globally rate-limited via ccLimit).
  // Webpage/sitemap work uses the lighter, higher webpage cap; whole-domain crawls
  // use the modest heavy cap so they can't OOM the box.
  const crawlLimit = mode === 'webpage' ? globalWebpageLimit : globalCrawlLimit;
  let cursor = 0;
  let stopped = false;
  const worker = async () => {
    while(true){
      if(shouldStop()){ stopped = true; return; }   // cancel: don't pick up new domains
      const index = cursor++;
      if(index >= domains.length) return;
      // run the actual work through the GLOBAL limiter so total concurrency is bounded
      // across every job in the process (not just within this one).
      try{ await crawlLimit(() => processDomain(domains[index], index)); }
      catch(e){ coverage.empty++; console.log(`! ${domains[index].padEnd(28)} ${e.message}`); }
    }
  };
  await Promise.all(Array.from({ length: Math.min(domainConcurrency, domains.length) }, worker));
  if(stopped) console.log("Run stopped early by request.");

  let unique = uniqueByEmail(all);
  if(unique.length < all.length){
    console.log(`\nDropped ${all.length - unique.length} duplicate email record(s) to enforce one email per record`);
  }
  unique = analyzePhones(unique);   // dedupe Phone 2, relabel recurring Direct numbers as Office
  await geocodeRecords(unique);     // fill Phone Location (City, Region, Country) via libphonenumber
  writeCsv(unique, outPath);
  const net = getNetStats();
  console.log(`\nCoverage: ${coverage.found} via Common Crawl · ${coverage.live} via live crawl · ${coverage.empty} no contacts`);
  console.log(`People:   ${unique.length} unique email records → ${outPath}`);
  console.log(`Network:  ${net.fetched} fetched · ${net.blocked} blocked (403/429/503)`);
  onProgress({ status: 'done', totalRecords: unique.length, coverage, netStats: net });
  return unique;
}

module.exports = { run, runDomains, readDomains, selectCandidates, warcToHtml, queryIndex, queryIndexUrl, fetchWarc,
  liveCrawl, liveFetchPage, extractSameDomainLinks, isBioOrContactUrl, COLUMNS, fetchDoc,
  parseRobots, robotsAllows, extractSitemapLocs, extractBioUrlsFromSitemaps, extractBioUrlGroups, discoverBioSitemaps, discoverBioUrlsFromCC,
  isLocationUrl, discoverSitemaps,
  resolveLatestCrawl, currentCrawl };

// ---------------------------------------------------------------- offline self-tests
if(require.main === module){
  const parseArgs = argv => {
    const opts = { csvPath: "", genderPath: "", directoryRulesPath: "", selftest:false, proxyTest:false };
    for(let i = 2; i < argv.length; i++){
      const a = argv[i];
      if(a === "--selftest") { opts.selftest = true; continue; }
      if(a === "--proxy-test") { opts.proxyTest = true; continue; }
      if(a === "--gender" || a === "--gender-file") { opts.genderPath = argv[++i] || ""; continue; }
      if(a === "--directory-rules" || a === "--dir-rules" || a === "--dirs") { opts.directoryRulesPath = argv[++i] || ""; continue; }
      if(!opts.csvPath) opts.csvPath = a;
    }
    return opts;
  };

  const args = parseArgs(process.argv);

  // `node cc-engine.js --proxy-test [url]` — verify the configured proxies + unblocker reach a
  // target (default: a bot-protected Howard Hanna agent page). Shows each proxy tier's exit IP
  // across 3 calls (should vary on a rotating gateway), then fetches the page via the full
  // liveFetchPage chain (primary -> residential -> unblocker API).
  if(args.proxyTest){
    (async () => {
      const realPage = (html) => html.length > 5000 &&
        !/just a moment|access denied|reference #\d|akamai|attention required|verify you are human|enable javascript and cookies/i.test(html.slice(0, 4000));
      console.log(`PROXY_URL (primary):          ${PROXY_URL ? mask(PROXY_URL) : "(not set)"}`);
      console.log(`PROXY_FALLBACK_URL (resi):    ${PROXY_FALLBACK_URL ? mask(PROXY_FALLBACK_URL) : "(not set)"}`);
      console.log(`UNBLOCKER_API_URL:            ${UNBLOCKER_API_URL ? mask(UNBLOCKER_API_URL) : "(not set)"}`);
      console.log(`HTTPS_PROXY (undici):         ${proxyEnv ? mask(proxyEnv) + (ProxyAgent ? "" : "  — undici NOT installed, this won't work") : "(not set)"}`);
      if(!PROXY_URL && !PROXY_FALLBACK_URL && !UNBLOCKER_API_URL && !proxyEnv){ console.log("\nNothing configured. Set PROXY_URL / PROXY_FALLBACK_URL / UNBLOCKER_API_URL and re-run."); return; }

      const showExitIps = async (label, tier) => {
        console.log(`\n${label} exit IP (3 samples — should vary on a rotating gateway):`);
        for(let i = 0; i < 3; i++){
          const r = await httpGetRaw("https://ipinfo.io/json", { accept: /json|text|html/, maxBytes: 64 * 1024, returnMeta: true, proxyTier: tier });
          let info = (r.body || "").trim();
          try{ const j = JSON.parse(r.body); info = `${j.ip}   ${j.org || ""}   ${[j.city, j.region, j.country].filter(Boolean).join(", ")}`; }catch{}
          console.log(`  [${r.status}] ${info || "(no response — proxy unreachable / bad credentials?)"}`);
        }
      };
      if(PROXY_URL) await showExitIps("PRIMARY", "primary");
      if(PROXY_FALLBACK_URL) await showExitIps("RESIDENTIAL FALLBACK", "fallback");

      const target = args.csvPath || "https://www.howardhanna.com/Agent/Detail/Aaron-Foster/72909";

      // direct unblocker-API check (isolates it from the proxy chain; ~60-90s)
      if(UNBLOCKER_API_URL){
        console.log(`\nUnblocker API (direct POST) for: ${target}`);
        const t = Date.now();
        const html = await unblockerFetch(target);
        console.log(`  bytes: ${html.length}   time: ${Date.now() - t}ms   ${html.length ? (realPage(html) ? "real page ✅" : "returned a block/challenge page ⚠️") : "no response ❌ (check endpoint/credentials)"}`);
      }

      console.log(`\nFull chain via liveFetchPage: ${target}`);
      resetNetStats();
      const t0 = Date.now();
      const html = await liveFetchPage(target);
      console.log(`  bytes: ${html.length}   time: ${Date.now() - t0}ms   net: ${JSON.stringify(_net)}`);
      console.log(`  result: ${realPage(html) ? "OK — looks like a real page ✅"
        : html.length ? `got HTML but it looks like a block/challenge page ⚠️${UNBLOCKER_API_URL ? " (even via the unblocker — verify UNBLOCKER_API_URL)" : " — set UNBLOCKER_API_URL for JS-challenge sites"}`
        : "BLOCKED / empty ❌"}`);
    })().catch(e => { console.error(e); process.exit(1); });
    return;
  }

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
      const wireless = loadWirelessBlocks((__dirname + "/phone-blocks.csv"));
      const pages = {
        "https://acme.com/team/marcus-patel":
          `<h1>Marcus Patel</h1><meta property="og:description" content="VP of Marketing at Acme.">`
          + `<a href="mailto:marcus.patel@acme.com">e</a><a href="tel:+12012042888">c</a>`,   // 2012042 = PCS/Mobile block
        "https://acme.com/contact": `<h1>Contact</h1><p>123 Main St.</p>`,   // dropped by gate
      };
      const tmp = os.tmpdir();
      fs.writeFileSync(`${tmp}/domains.csv`, "domain\nacme.com\nuncrawled-xyz.com\n");
      const recs = await run(`${tmp}/domains.csv`, {
        wirelessPath:(__dirname + "/phone-blocks.csv"),
        genderMap:{ marcus:"M" }, outPath:`${tmp}/cc-results.csv`,
        liveFallback:false,                              // keep the self-test fully offline
        _queryIndex: async (domain) => domain === "acme.com" ? Object.keys(pages).map(url =>
          ({ url, filename:"f", offset:0, length:1, timestamp:"20260201" })) : [],
        _fetchWarc: async (rec) => pages[rec.url] || "",
      });
      ok("pipeline keeps the bio record, drops the empty contact page", recs.length === 1);
      ok("pipeline classified the PCS number as Mobile via the block table", recs[0]["Phone Type"] === "Mobile");
      ok("pipeline filled block-level Phone Location (City, ST)", /,\s*NJ/.test(recs[0]["Phone Location"] || ""));
      // bug-fix regression: a WIRE/Office (landline) block must classify as Direct, NOT Mobile
      const { classifyLineType: _clt } = require("./wireless-block-classifier");
      ok("landline (WIRE) block classifies as Direct, not Mobile", _clt("+12012012345", wireless).type === "Direct");
      ok("pipeline tagged source = Common Crawl", recs[0]["Source"] === "Common Crawl");
      ok("results CSV was written", fs.existsSync(`${tmp}/cc-results.csv`));

      // 3b) webpage mode: Common Crawl first (bypasses live blocks like Cloudflare), live as fallback
      let wpLive = 0;
      const wpRecs = await run("", {
        mode: "webpage",
        _items: ["https://blocked.com/team/jane-smith/", "https://blocked.com/team/bob-uncrawled/"],
        wirelessPath:(__dirname + "/phone-blocks.csv"),
        genderMap:{ jane:"F" }, outPath:`${tmp}/wp-results.csv`,
        _queryIndexUrl: async (u) => u.includes("jane-smith")
          ? ({ url:u, filename:"f", offset:0, length:1, timestamp:"20260201000000" }) : null,   // only jane is archived
        _fetchWarc: async () => `<h1>Jane Smith</h1><a href="mailto:jane.smith@blocked.com">e</a>`,
        _liveFetch: async () => { wpLive++; return ""; },                                        // live blocked -> empty
      });
      ok("webpage mode reads an archived URL from Common Crawl",
        wpRecs.some(r => String(r["Email Address"]).toLowerCase() === "jane.smith@blocked.com" && r["Source"] === "Common Crawl"));
      ok("webpage mode live-fetches only the URL CC didn't have", wpLive === 1);

      // 3b-warc) WARC FAST PATH: a pre-resolved pointer (opts._warcByUrl) fetches the archived record
      //          DIRECTLY — the index lookup must NOT be called for that URL.
      let idxCalls = 0;
      const warcRecs = await run("", {
        mode: "webpage",
        _items: ["https://acme.com/agent/jane-roe"],
        wirelessPath:(__dirname + "/phone-blocks.csv"),
        genderMap:{ jane:"F" }, outPath:`${tmp}/wp-warc.csv`,
        _warcByUrl: new Map([["https://acme.com/agent/jane-roe", { url:"https://acme.com/agent/jane-roe", filename:"f", offset:0, length:1, timestamp:"20260201000000" }]]),
        _queryIndexUrl: async () => { idxCalls++; return null; },     // must stay 0
        _fetchWarc: async (rec) => rec.filename === "f" ? `<h1>Jane Roe</h1><a href="mailto:jane.roe@acme.com">e</a>` : "",
        _liveFetch: async () => "",
      });
      ok("WARC fast path extracts via the pointer (Common Crawl)",
        warcRecs.some(r => String(r["Email Address"]).toLowerCase() === "jane.roe@acme.com" && r["Source"] === "Common Crawl"));
      ok("WARC fast path skips the index lookup", idxCalls === 0);

      // 3b-2) Site API adapter: when a registered adapter handles the domain (e.g. century21, a
      //       JS-rendered site), the record comes straight from its JSON API and Common Crawl +
      //       live fetch are NOT consulted for that URL.
      let apiCC = 0, apiLive = 0;
      const apiRecs = await run("", {
        mode: "webpage",
        _items: ["https://www.century21.com/agent/detail/nj/x/agents/agnes-aaron/aid-P00200000ABCdefGhij"],
        wirelessPath:(__dirname + "/phone-blocks.csv"),
        genderMap:{ agnes:"F" }, outPath:`${tmp}/wp-api.csv`,
        _findSiteApi: () => ({ name:"fake", fetchRecord: async (u, deps) =>
          extractRecord(`<h1>Agnes Aaron</h1><a href="mailto:a@c21.com">e</a><a href="sms:+16094136297">t</a>`, u, { ...deps, source:"Site API" }) }),
        _queryIndexUrl: async () => { apiCC++; return null; },
        _liveFetch: async () => { apiLive++; return ""; },
      });
      ok("webpage mode uses the Site API adapter when one matches",
        apiRecs.some(r => r["Source"] === "Site API" && r["Phone Type"] === "Mobile" && r["Last Path"] === "Agnes Aaron"));
      ok("Site API record skips Common Crawl + live fetch", apiCC === 0 && apiLive === 0);

      // 3c) URL-only fallback: page blocked (403) + not in Common Crawl, but the URL itself
      //     names a person under a known directory (e.g. an agent profile behind bot protection)
      const urlOnly = await run("", {
        mode: "webpage",
        _items: ["https://blocked.com/Agent/Detail/Jane-Smith/71955"],
        wirelessPath:(__dirname + "/phone-blocks.csv"),
        genderMap:{ jane:"F" }, outPath:`${tmp}/wp-url.csv`,
        _queryIndexUrl: async () => null,                                                        // not archived
        _liveFetch: async () => "",                                                              // blocked
      });
      ok("URL-only record built when page blocked + not in Common Crawl",
        urlOnly.length === 1 && urlOnly[0]["First"] === "Jane" && urlOnly[0]["Last"] === "Smith"
        && urlOnly[0]["Source"] === "URL" && urlOnly[0]["Title"] === "Agent");

      const duplicateRows = [
        { "Email Address": "test@xyz.com", "Phone": "", "Email Type": "Role-Based" },
        { "Email Address": "TEST@xyz.com", "Phone": "+12025550123", "Email Type": "Professional", "First": "Test", "Last": "User" },
        { "Email Address": "other@xyz.com", "Phone": "", "Email Type": "Professional" },
      ];
      const uniqueRows = uniqueByEmail(duplicateRows);
      ok("uniqueByEmail dedupes by lowercase email", uniqueRows.length === 2 && uniqueRows.some(r => r["Email Address"].toLowerCase() === "test@xyz.com") && uniqueRows.some(r => r["Email Address"] === "other@xyz.com"));
      ok("uniqueByEmail keeps the richer record for duplicate emails", uniqueRows.find(r => r["Email Address"].toLowerCase() === "test@xyz.com").Phone === "+12025550123");

      // 3b) ccUrlKey: a sitemap URL and its CC capture must collapse to the SAME key
      //     (ignore scheme/www/trailing-slash/query) or the bulk-index cache silently misses.
      ok("ccUrlKey strips scheme, www and query, normalizes trailing slash",
        ccUrlKey("https://www.Acme.com/Team/Jane/?ref=x") === ccUrlKey("http://acme.com/Team/Jane")
        && ccUrlKey("https://acme.com/team/jane/") === "acme.com/team/jane");
      ok("ccUrlKey maps a bare host to root path",
        ccUrlKey("https://www.acme.com") === "acme.com/" && ccUrlKey("https://www.acme.com/") === "acme.com/");

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
      // dir term in the SUBDOMAIN + a person leaf (agents.farmers.com/ca/calabasas/alex-sayeri)
      ok("isBioOrContactUrl flags a person under a bio-dir subdomain",
        isBioOrContactUrl("https://agents.farmers.com/ca/calabasas/alex-sayeri"));
      ok("isBioOrContactUrl ignores a bio-dir subdomain listing page (no person leaf)",
        !isBioOrContactUrl("https://agents.farmers.com/ca/calabasas"));
      // singular "agent." subdomain too (agent.travelers.com); bare root is not a bio
      ok("isBioOrContactUrl flags a person under the singular agent. subdomain",
        isBioOrContactUrl("https://agent.travelers.com/ca/los-angeles/john-smith")
        && isBioOrContactUrl("https://agent.travelers.com/john-smith"));
      ok("isBioOrContactUrl ignores a bio-dir subdomain root (no person leaf)",
        !isBioOrContactUrl("https://agent.travelers.com/"));
      // profile URLs that TRAIL an opaque id after the name (century21 /agent/detail/.../First-Last/aid-…):
      // the id segment must be skipped so the person-name leaf is found
      ok("isBioOrContactUrl flags a /First-Last/aid-<blob> agent profile",
        isBioOrContactUrl("https://www.century21.com/agent/detail/fl/jacksonville/agents/dragan-spiridonovic/aid-P00200000000033dyujCKdEsXQx08N4pMQLeFT62")
        && !isBioOrContactUrl("https://www.century21.com/agent/detail/fl/jacksonville/agents/aid-P00200000000033dyujCKdEsXQx08N4pMQLeFT62"));
      // team-page terms must work whether in the SUBDOMAIN or the PATH (with a person leaf)
      const teamTerms = ["insurance-agents","staff","bio","contacts","advisor","advisors","broker","brokers","realtor","realtors","financialprofessionals","pathologists"];
      ok("team-page terms classify as bio in both subdomain and path positions",
        teamTerms.every((t) =>
          isBioOrContactUrl(`https://example.com/${t}/john-smith`)
          && isBioOrContactUrl(`https://${t}.example.com/john-smith`)));

      // 5) liveCrawl follows bio links and extracts via the real extractor (mocked network)
      const livePages = {
        "https://demo-firm.com/": `<a href="/attorneys/">Attorneys</a><a href="/blog/x">Blog</a>`,
        "https://demo-firm.com/attorneys/": `<a href="/attorneys/jane-doe/">Jane Doe</a>`,
        "https://demo-firm.com/attorneys/jane-doe/":
          `<h1>Jane Doe</h1><meta property="og:description" content="Partner at Demo Firm.">`
          + `<a href="mailto:jane.doe@demo-firm.com">e</a><a href="tel:+12012012345">c</a>`,
      };
      const liveRecs = await liveCrawl("demo-firm.com", { wireless,
        _liveFetch: async (u) => livePages[u] || "", _fetchDoc: async () => "" });   // no robots/sitemap → offline
      const jane = liveRecs.find(r => String(r["Email Address"]).toLowerCase() === "jane.doe@demo-firm.com");
      ok("liveCrawl follows bio links and extracts a record", !!jane);
      ok("liveCrawl tags source = Live Crawl", jane && jane["Source"] === "Live Crawl");

      // 6) robots.txt parsing: sitemaps + agent-specific rules
      const robots = parseRobots(
        "Sitemap: https://x.com/sitemap.xml\nUser-agent: *\nDisallow: /private/\nAllow: /private/ok\n", "RampedUp-CC-Engine/0.1");
      ok("parseRobots extracts sitemap urls", robots.sitemaps[0] === "https://x.com/sitemap.xml");
      ok("robotsAllows blocks a disallowed path", robotsAllows("/private/secret", robots.rules) === false);
      ok("robotsAllows permits a normal path", robotsAllows("/attorneys/jane/", robots.rules) === true);
      ok("robotsAllows: longer Allow overrides Disallow", robotsAllows("/private/ok", robots.rules) === true);

      // 7) sitemap parsing: index vs urlset
      const idx2 = extractSitemapLocs(`<sitemapindex><sitemap><loc>https://x.com/sm1.xml</loc></sitemap></sitemapindex>`);
      ok("extractSitemapLocs detects a sitemap index", idx2.isIndex === true && idx2.locs[0] === "https://x.com/sm1.xml");
      const set2 = extractSitemapLocs(`<urlset><url><loc>https://x.com/attorneys/jane/</loc></url><url><loc>https://x.com/blog/p</loc></url></urlset>`);
      ok("extractSitemapLocs lists page urls", set2.isIndex === false && set2.locs.length === 2);

      // 7b) extractBioUrlsFromSitemaps: inline index -> child urlset -> bio-only, deduped
      const smHost = "smx.com";
      const smDocs = {
        [`https://${smHost}/child.xml`]:
          `<urlset><url><loc>https://${smHost}/attorneys/jane-doe/</loc></url>` +
          `<url><loc>https://${smHost}/attorneys/jane-doe/</loc></url>` +     // dup -> collapsed
          `<url><loc>https://${smHost}/blog/post-1/</loc></url>` +            // not a bio -> skipped
          `<url><loc>https://${smHost}/team/john-roe.pdf</loc></url></urlset>`, // skipped ext
      };
      const smOut = await extractBioUrlsFromSitemaps({
        content: `<sitemapindex><sitemap><loc>https://${smHost}/child.xml</loc></sitemap></sitemapindex>`,
        _fetchDoc: async (u) => smDocs[u] || "",
      });
      ok("extractBioUrlsFromSitemaps recurses index + keeps bio urls only",
        smOut.bioUrls.length === 1 && smOut.bioUrls[0] === `https://${smHost}/attorneys/jane-doe/`);
      const smOut2 = await extractBioUrlsFromSitemaps({
        urls: [`https://${smHost}/child.xml`],
        _fetchDoc: async (u) => smDocs[u] || "",
      });
      ok("extractBioUrlsFromSitemaps works from a fetched sitemap URL too",
        smOut2.bioUrls.length === 1 && smOut2.sitemapsFetched === 1);

      // 7b-2) extractBioUrlGroups: index -> ONE group per child sitemap, deduped across sitemaps
      const grpDocs = {
        [`https://${smHost}/index.xml`]:
          `<sitemapindex><sitemap><loc>https://${smHost}/a.xml</loc></sitemap>` +
          `<sitemap><loc>https://${smHost}/b.xml</loc></sitemap></sitemapindex>`,
        [`https://${smHost}/a.xml`]:
          `<urlset><url><loc>https://${smHost}/team/jane-doe/</loc></url>` +
          `<url><loc>https://${smHost}/blog/x/</loc></url></urlset>`,             // non-bio -> skipped
        [`https://${smHost}/b.xml`]:
          `<urlset><url><loc>https://${smHost}/team/john-roe/</loc></url>` +
          `<url><loc>https://${smHost}/team/jane-doe/</loc></url></urlset>`,       // jane dup across sitemaps
      };
      const grp = [];
      const grpOut = await extractBioUrlGroups({
        urls: [`https://${smHost}/index.xml`],
        _fetchDoc: async (u) => grpDocs[u] || "",
        onGroup: (g) => grp.push(g),
      });
      ok("extractBioUrlGroups yields one group per leaf sitemap",
        grp.length === 2 && grpOut.totalGroups === 2);
      ok("extractBioUrlGroups groups hold each sitemap's bio urls, deduped across sitemaps",
        grp[0].bioUrls.length === 1 && grp[0].bioUrls[0] === `https://${smHost}/team/jane-doe/` &&
        grp[1].bioUrls.length === 1 && grp[1].bioUrls[0] === `https://${smHost}/team/john-roe/` &&
        grpOut.totalBioUrls === 2);

      // 7b-3) extractSitemapLocs pairs <loc> with its sibling <lastmod> (entries[]), back-compat locs[]
      const lmSet = extractSitemapLocs(
        `<urlset><url><loc>https://x.com/attorneys/jane/</loc><lastmod>2026-06-20</lastmod></url>` +
        `<url><loc>https://x.com/attorneys/john/</loc></url></urlset>`);   // 2nd has no lastmod
      ok("extractSitemapLocs binds lastmod per block",
        lmSet.entries.length === 2 && lmSet.locs.length === 2 &&
        lmSet.entries[0].loc === "https://x.com/attorneys/jane/" && lmSet.entries[0].lastmod === "2026-06-20" &&
        lmSet.entries[1].lastmod === null);
      const lmIdx = extractSitemapLocs(
        `<sitemapindex><sitemap><loc>https://x.com/agents.xml</loc><lastmod>2026-06-24T00:00:00Z</lastmod></sitemap></sitemapindex>`);
      ok("extractSitemapLocs carries child-sitemap lastmod from an index",
        lmIdx.isIndex === true && lmIdx.entries[0].lastmod === "2026-06-24T00:00:00Z");

      // 7b-4) discoverBioSitemaps: keep only children that are DEDICATED to bio pages
      const dbsHost = "agency.com";
      const dbsDocs = {
        [`https://${dbsHost}/sitemap_index.xml`]:
          `<sitemapindex>` +
          `<sitemap><loc>https://${dbsHost}/agents.xml</loc><lastmod>2026-06-24</lastmod></sitemap>` +
          `<sitemap><loc>https://${dbsHost}/blog.xml</loc><lastmod>2026-06-24</lastmod></sitemap>` +
          `</sitemapindex>`,
        // agents.xml: 3/3 bio -> dedicated (qualifies)
        [`https://${dbsHost}/agents.xml`]:
          `<urlset>` +
          `<url><loc>https://${dbsHost}/agents/jane-doe/</loc><lastmod>2026-06-20</lastmod></url>` +
          `<url><loc>https://${dbsHost}/agents/john-roe/</loc></url>` +
          `<url><loc>https://${dbsHost}/agents/amy-poe/</loc></url></urlset>`,
        // blog.xml: 0/3 bio -> NOT dedicated (filtered out)
        [`https://${dbsHost}/blog.xml`]:
          `<urlset><url><loc>https://${dbsHost}/blog/a/</loc></url>` +
          `<url><loc>https://${dbsHost}/blog/b/</loc></url>` +
          `<url><loc>https://${dbsHost}/news/c/</loc></url></urlset>`,
      };
      const dbsOut = await discoverBioSitemaps({
        urls: [`https://${dbsHost}/sitemap_index.xml`],
        _fetchDoc: async (u) => dbsDocs[u] || "",
      });
      ok("discoverBioSitemaps keeps only the bio-dedicated child sitemap",
        dbsOut.watches.length === 1 && dbsOut.watches[0].sitemapUrl === `https://${dbsHost}/agents.xml`);
      ok("discoverBioSitemaps reports the child's lastmod (from the parent index) + bio-ratio",
        dbsOut.watches[0].lastmod === "2026-06-24" && dbsOut.watches[0].bioRatio === 1 &&
        dbsOut.watches[0].bioCount === 3 && dbsOut.watches[0].parentUrl === `https://${dbsHost}/sitemap_index.xml`);
      ok("discoverBioSitemaps pairs each bio url with its own lastmod",
        dbsOut.watches[0].bioUrls[0].url === `https://${dbsHost}/agents/jane-doe/` &&
        dbsOut.watches[0].bioUrls[0].lastmod === "2026-06-20" &&
        dbsOut.watches[0].bioUrls[1].lastmod === null);

      // 7b-5) filename fast-path: a known-bio sitemap name qualifies + captures ALL its URLs even when
      // the generic detector wouldn't recognize the slugs.
      const nmHost = "lender.com";
      const nmDocs = {
        [`https://${nmHost}/loan-officer-sitemap.xml`]:
          `<urlset><url><loc>https://${nmHost}/lo/jsmith-90210</loc></url>` +    // opaque slug: detector misses it
          `<url><loc>https://${nmHost}/lo/bjones-30303</loc></url></urlset>`,
      };
      const nmFetch = async (u) => nmDocs[u] || "";
      const noName = await discoverBioSitemaps({ urls: [`https://${nmHost}/loan-officer-sitemap.xml`], _fetchDoc: nmFetch });
      ok("without the name list, an opaque-slug sitemap does NOT qualify", noName.watches.length === 0);
      const byName = await discoverBioSitemaps({
        urls: [`https://${nmHost}/loan-officer-sitemap.xml`], _fetchDoc: nmFetch,
        bioSitemapNames: new Set(["loan-officer-sitemap.xml"]),
      });
      ok("a known-bio sitemap filename qualifies + captures all its URLs",
        byName.watches.length === 1 && byName.watches[0].byName === true && byName.watches[0].bioUrls.length === 2);

      // 7b-6) sourceUrl lets inline content be name-matched (used by the monitor's per-pass extraction)
      const inlineByName = await discoverBioSitemaps({
        content: nmDocs[`https://${nmHost}/loan-officer-sitemap.xml`], sourceUrl: `https://${nmHost}/loan-officer-sitemap.xml`,
        bioSitemapNames: new Set(["loan-officer-sitemap.xml"]),
      });
      ok("sourceUrl makes inline content name-matchable", inlineByName.watches.length === 1 && inlineByName.watches[0].bioUrls.length === 2);

      // 7b-7) isLocationUrl: a place under a location container qualifies; the container/index page does not
      ok("isLocationUrl accepts a leaf store page", isLocationUrl("https://x.com/locations/austin-tx") === true);
      ok("isLocationUrl accepts a dealer leaf", isLocationUrl("https://x.com/dealers/ca/los-angeles") === true);
      ok("isLocationUrl accepts a locations.<domain> leaf", isLocationUrl("https://locations.x.com/austin-tx") === true);
      ok("isLocationUrl rejects the container/index page", isLocationUrl("https://x.com/locations/") === false);
      ok("isLocationUrl rejects an unrelated page", isLocationUrl("https://x.com/blog/hello") === false);

      // 7b-8) discoverSitemaps: classify People vs Location children from one index
      const dsHost = "brand.com";
      const dsDocs = {
        [`https://${dsHost}/sitemap_index.xml`]:
          `<sitemapindex>` +
          `<sitemap><loc>https://${dsHost}/agents.xml</loc><lastmod>2026-06-24</lastmod></sitemap>` +
          `<sitemap><loc>https://${dsHost}/stores.xml</loc></sitemap>` +
          `<sitemap><loc>https://${dsHost}/blog.xml</loc></sitemap>` +
          `</sitemapindex>`,
        [`https://${dsHost}/agents.xml`]:
          `<urlset><url><loc>https://${dsHost}/agents/jane-doe/</loc></url>` +
          `<url><loc>https://${dsHost}/agents/john-roe/</loc></url>` +
          `<url><loc>https://${dsHost}/agents/amy-poe/</loc></url></urlset>`,
        [`https://${dsHost}/stores.xml`]:
          `<urlset><url><loc>https://${dsHost}/locations/austin-tx</loc></url>` +
          `<url><loc>https://${dsHost}/locations/dallas-tx</loc></url>` +
          `<url><loc>https://${dsHost}/locations/houston-tx</loc></url></urlset>`,
        [`https://${dsHost}/blog.xml`]:
          `<urlset><url><loc>https://${dsHost}/blog/a</loc></url><url><loc>https://${dsHost}/blog/b</loc></url><url><loc>https://${dsHost}/blog/c</loc></url></urlset>`,
      };
      const dsOut = await discoverSitemaps({ urls: [`https://${dsHost}/sitemap_index.xml`], _fetchDoc: async (u) => dsDocs[u] || "" });
      const dsPeople = dsOut.watches.find((w) => w.sitemapUrl.endsWith("/agents.xml"));
      const dsLoc = dsOut.watches.find((w) => w.sitemapUrl.endsWith("/stores.xml"));
      const dsBlog = dsOut.watches.find((w) => w.sitemapUrl.endsWith("/blog.xml"));
      ok("discoverSitemaps classifies the agents child as People", !!dsPeople && dsPeople.kind === "People" && dsPeople.itemCount === 3);
      ok("discoverSitemaps classifies the stores child as Location", !!dsLoc && dsLoc.kind === "Location" && dsLoc.itemCount === 3);
      ok("discoverSitemaps drops the blog child", !dsBlog);
      // location filename fast-path keeps all urls even with opaque slugs
      const dsLocName = await discoverSitemaps({
        urls: [`https://loc.com/stores-sitemap.xml`],
        locationSitemapNames: new Set(["stores-sitemap.xml"]),
        _fetchDoc: async () => `<urlset><url><loc>https://loc.com/s/8842</loc></url><url><loc>https://loc.com/s/9931</loc></url></urlset>`,
      });
      ok("discoverSitemaps location filename fast-path keeps all urls", dsLocName.watches.length === 1 && dsLocName.watches[0].kind === "Location" && dsLocName.watches[0].byName === true && dsLocName.watches[0].itemCount === 2);

      // 7b-9) precision: a content-type sitemap (attachment/meeting/…) with bio-looking URLs is NOT classified
      const negHost = "town.gov";
      const negBios = `<urlset>` +
        `<url><loc>https://${negHost}/staff/jane-doe/</loc></url>` +
        `<url><loc>https://${negHost}/staff/john-roe/</loc></url>` +
        `<url><loc>https://${negHost}/staff/amy-poe/</loc></url></urlset>`;
      const negName = await discoverSitemaps({ urls: [`https://${negHost}/attachment-sitemap.xml`], _fetchDoc: async () => negBios });
      ok("discoverSitemaps drops a negative-name (attachment) sitemap despite bio-looking urls", negName.watches.length === 0);
      const staffName = await discoverSitemaps({ urls: [`https://${negHost}/staff-sitemap.xml`], _fetchDoc: async () => negBios });
      ok("discoverSitemaps still keeps a real staff sitemap (same urls)", staffName.watches.length === 1 && staffName.watches[0].kind === "People");

      // 7b-10) keyword second-pass: a People sitemap the strict pass can't detect (opaque numeric-id URLs)
      // is rescued when a trusted keyword token matches its filename — but never overrides a negative feed.
      const kwHost = "kw.com";
      const kwOpaque = `<urlset><url><loc>https://${kwHost}/x/48213</loc></url>` +
        `<url><loc>https://${kwHost}/x/48214</loc></url><url><loc>https://${kwHost}/x/48215</loc></url></urlset>`;
      const kwStrict = await discoverSitemaps({ urls: [`https://${kwHost}/roster-2026.xml`], _fetchDoc: async () => kwOpaque });
      ok("discoverSitemaps strict pass drops an opaque numeric-id sitemap", kwStrict.watches.length === 0);
      const kwHint = await discoverSitemaps({ urls: [`https://${kwHost}/roster-2026.xml`], keywordHints: new Set(["roster"]), _fetchDoc: async () => kwOpaque });
      ok("discoverSitemaps keyword pass rescues it as People (keeps all urls)", kwHint.watches.length === 1 && kwHint.watches[0].kind === "People" && kwHint.watches[0].byName === true && kwHint.watches[0].itemCount === 3);
      const kwNeg = await discoverSitemaps({ urls: [`https://${kwHost}/attachment-sitemap.xml`], keywordHints: new Set(["attachment"]), _fetchDoc: async () => kwOpaque });
      ok("discoverSitemaps keyword pass does not override a negative-name feed", kwNeg.watches.length === 0);

      // 7c) discoverBioUrlsFromCC: a domain's CC index -> keep only bio-looking captures
      const ccHost = "ccfirm.com";
      const fakeIndex = new Map([
        ["a", { url: `https://${ccHost}/attorneys/jane-doe/`, timestamp: "20260101" }],
        ["b", { url: `https://www.${ccHost}/attorneys/john-roe?ref=x`, timestamp: "20260101" }],
        ["c", { url: `https://${ccHost}/blog/hello/`, timestamp: "20260101" }],
        ["d", { url: `https://${ccHost}/team/jane.pdf`, timestamp: "20260101" }],
      ]);
      const ccOut = await discoverBioUrlsFromCC({
        domains: [`https://www.${ccHost}/`],
        _loadDomainIndex: async () => fakeIndex,
      });
      ok("discoverBioUrlsFromCC keeps bio captures, drops blog/file, strips query",
        ccOut.bioUrls.length === 2
        && ccOut.bioUrls.includes(`https://${ccHost}/attorneys/jane-doe/`)
        && ccOut.bioUrls.includes(`https://www.${ccHost}/attorneys/john-roe`)
        && !ccOut.bioUrls.some(u => /blog|\.pdf/.test(u)));

      // 8) liveCrawl discovers bios from a sitemap (not linked on the homepage), offline
      const smDomain = "smfirm.com";
      const smPages = {
        [`https://${smDomain}/robots.txt`]: `Sitemap: https://${smDomain}/sitemap.xml\nUser-agent: *\nDisallow: /hidden/`,
        [`https://${smDomain}/sitemap.xml`]:
          `<urlset><url><loc>https://${smDomain}/attorneys/amy-tran/</loc></url>` +
          `<url><loc>https://${smDomain}/attorneys/ben-roe/</loc></url>` +
          `<url><loc>https://${smDomain}/hidden/attorneys/secret-one/</loc></url>` + // disallowed → skipped
          `<url><loc>https://${smDomain}/blog/post/</loc></url></urlset>`,          // not a bio → skipped
        [`https://${smDomain}/attorneys/amy-tran/`]: `<h1>Amy Tran</h1><a href="mailto:atran@${smDomain}">e</a>`,
        [`https://${smDomain}/attorneys/ben-roe/`]: `<h1>Ben Roe</h1><a href="mailto:broe@${smDomain}">e</a>`,
      };
      const smRecs = await liveCrawl(smDomain, { wireless,
        _liveFetch: async (u) => smPages[u] || "",
        _fetchDoc: async (u) => smPages[u] || "" });
      const emails = smRecs.map(r => String(r["Email Address"]).toLowerCase());
      ok("liveCrawl pulls bios listed only in the sitemap", emails.includes(`atran@${smDomain}`) && emails.includes(`broe@${smDomain}`));
      ok("liveCrawl respects robots Disallow (no hidden/blog records)", !emails.some(e => /secret|post/.test(e)) && smRecs.length === 2);

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
