/**
 * serper.js — Google search via serper.dev, for the Site Search feature.
 *
 * Given a webpage/path, run `site:<host+path>` and paginate the organic results to discover
 * bio pages (URL + title + snippet). The caller filters to bio URLs and extracts contact hints.
 * Key comes from SERPER_API_KEY (or opts.apiKey). Each page = 1 serper credit.
 */
const https = require("https");
const SERPER_URL = "https://google.serper.dev/search";

function serperSearch(query, { page = 1, num = 100, apiKey } = {}){
  return new Promise((resolve) => {
    const key = apiKey || process.env.SERPER_API_KEY || "";
    if(!key) return resolve({ error: "no SERPER_API_KEY", organic: [] });
    const body = JSON.stringify({ q: query, num, page });
    let u; try{ u = new URL(SERPER_URL); }catch{ return resolve({ error: "bad url", organic: [] }); }
    const req = https.request(u, {
      method: "POST", timeout: 30000,
      headers: { "X-API-KEY": key, "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
    }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        let j = {}; try{ j = JSON.parse(Buffer.concat(chunks).toString("utf8")); }catch{}
        if(res.statusCode !== 200) return resolve({ error: `serper ${res.statusCode}: ${(j && (j.message || j.error)) || ""}`, organic: [] });
        resolve(j);
      });
      res.on("error", () => resolve({ error: "stream", organic: [] }));
    });
    req.on("error", () => resolve({ error: "request", organic: [] }));
    req.on("timeout", () => { req.destroy(); resolve({ error: "timeout", organic: [] }); });
    req.write(body); req.end();
  });
}

// site: target from a URL/domain input — strip the scheme + trailing slash, keep host (+ path)
// so a path like "jll.com/en-us/people/bio-broker" narrows the search to that section.
function siteTarget(input){
  return String(input || "").trim().replace(/^https?:\/\//i, "").replace(/\/+$/, "").trim();
}

// Paginate `site:<target>` and return deduped organic results [{ link, title, snippet }].
async function siteSearch(input, { apiKey, maxPages = 20, perPage = 100, onPage } = {}){
  const target = siteTarget(input);
  if(!target) return { target: "", results: [], pages: 0, credits: 0, error: "no target" };
  const seen = new Set(); const results = [];
  let pages = 0, credits = 0, error = "";
  for(let page = 1; page <= maxPages; page++){
    const j = await serperSearch(`site:${target}`, { page, num: perPage, apiKey });
    pages = page;
    if(j.error){ if(!results.length) error = j.error; break; }
    credits += (j.credits || 0);
    const organic = j.organic || [];
    if(!organic.length) break;
    let added = 0;
    for(const o of organic){
      const link = String(o.link || "").split("#")[0];
      if(!link || seen.has(link)) continue;
      seen.add(link); results.push({ link, title: o.title || "", snippet: o.snippet || "" }); added++;
    }
    if(onPage) try{ onPage({ page, total: results.length }); }catch{}
    if(added === 0) break;                                  // no new links on this page -> stop
    await new Promise((r) => setTimeout(r, 200));           // be gentle on the API
  }
  return { target, results, pages, credits, error };
}

module.exports = { serperSearch, siteSearch, siteTarget };

// ---- CLI: dry-run a site search (no DB). SERPER_API_KEY=... node serper.js <urlOrDomain> ----
if(require.main === module){
  (async () => {
    const input = process.argv[2];
    if(!input){ console.error("usage: SERPER_API_KEY=... node serper.js <urlOrDomain> [maxPages]"); process.exit(1); }
    const maxPages = Number(process.argv[3]) || 5;
    const { isBioOrContactUrl } = require("./cc-engine");
    const { findPosition, cleanEmail } = require("./extractor");
    const t0 = Date.now();
    const sr = await siteSearch(input, { maxPages, onPage: ({ page, total }) => console.log(`  page ${page}: ${total} results so far`) });
    console.log(`\ntarget: site:${sr.target}   pages: ${sr.pages}   results: ${sr.results.length}   credits: ${sr.credits}   ${sr.error ? "error: " + sr.error : ""}`);
    const EMAIL_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
    const bio = sr.results.filter((r) => isBioOrContactUrl(r.link));
    console.log(`bio URLs: ${bio.length} / ${sr.results.length}  (${Date.now() - t0}ms)\n`);
    for(const r of bio.slice(0, 8)){
      const emails = [...new Set((`${r.title} ${r.snippet}`.match(EMAIL_RE) || []).map((e) => cleanEmail(e)).filter(Boolean))];
      console.log(`  ${r.link}\n    pos: ${findPosition(r.title, r.snippet) || "-"}   email: ${emails.join(",") || "-"}\n    ${r.snippet.slice(0, 100)}`);
    }
  })().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
}
