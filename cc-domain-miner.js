#!/usr/bin/env node
/**
 * cc-domain-miner.js — find company DOMAINS from Common Crawl by BIO-PAGE url paths.
 *
 * Phase 0 (domain acquisition) of SCALING-ROADMAP.md. The app takes a CSV of domains as input;
 * this generates that CSV at scale. It streams CC's CDX index shards (cdx-NNNNN.gz — predictable
 * names, public, no AWS/listing needed), keeps 200 / text-html captures whose URL PATH matches a
 * bio-page pattern (/our-people, /team, /agents, /attorneys, …), and aggregates to REGISTRABLE
 * domains with a bio-page count + a sample path. Output: cc-domains.csv → feed the crawler.
 *
 * Pure Node (https + zlib), so it runs anywhere with no dependencies. NOTE: a CDX scan reads the
 * whole shard (no path index); for a FULL crawl (~300 shards) the columnar index via Athena reads
 * far fewer bytes and is cheaper (see SCALING-ROADMAP.md). This tool is the no-AWS / proof-of-value
 * path and is ideal for sampling (--max-mb) or scanning a subset of shards.
 *
 *   node cc-domain-miner.js --crawl CC-MAIN-2026-25 --shards 0-9 --min-pages 2 --out cc-domains.csv
 *   node cc-domain-miner.js --shards 100 --max-mb 8            # quick sample of one shard
 *   node cc-domain-miner.js --paths "agents,attorneys,advisors" --shards 0-299   # full, targeted
 *   node cc-domain-miner.js --selftest                          # offline tests
 */
const https = require("https");
const zlib = require("zlib");
const fs = require("fs");

// ---- bio-page path patterns (defaults tuned to professional-services verticals) ----
const DEFAULT_BIO = [
  "our-people", "our-team", "meet-the-team", "meet-our-team", "team", "staff", "our-staff",
  "people", "agents", "agent", "real-estate-agents", "find-an-agent", "attorneys", "attorney",
  "lawyers", "our-attorneys", "advisors", "advisor", "financial-advisor", "find-an-advisor",
  "professionals", "our-professionals", "leadership", "physicians", "doctors", "providers", "bios",
];

// registrable-domain heuristic: strip www; last 2 labels, with a small multi-level public-suffix set.
// (Good enough for domain acquisition; swap in a full PSL later if precision matters.)
const MULTI_TLD = new Set([
  "co.uk", "org.uk", "gov.uk", "ac.uk", "me.uk", "co.nz", "com.au", "net.au", "org.au",
  "co.za", "com.br", "co.jp", "co.in", "com.mx", "com.sg", "com.hk",
]);
function regDomain(host) {
  host = String(host || "").toLowerCase().replace(/^www\./, "").replace(/\.$/, "");
  const p = host.split(".");
  if (p.length <= 2) return host;
  return MULTI_TLD.has(p.slice(-2).join(".")) ? p.slice(-3).join(".") : p.slice(-2).join(".");
}

function bioRegex(terms) {
  const esc = terms.map((t) => t.trim().toLowerCase()).filter(Boolean)
    .map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  return new RegExp("/(" + esc.join("|") + ")(/|$)", "i");
}

// Drop content paths that often contain a bio term but aren't a people directory (blog posts,
// news/press, tag/category archives, attachments). Keeps real bios under /about-us/team etc.
const EXCLUDE_RE = /\/(blog|news|press|article|articles|category|categories|tag|tags|attachment|event|events|podcast)\//i;

// Parse one CDX line; return { domain, path } if it's a bio page (200 + html + bio path), else null.
function classify(line, bioRe) {
  const sp = line.indexOf(" {");
  if (sp < 0) return null;
  let j; try { j = JSON.parse(line.slice(sp + 1)); } catch { return null; }
  if (j.status !== "200") return null;
  if (!/html/.test(j.mime || j["mime-detected"] || "")) return null;
  let u; try { u = new URL(j.url); } catch { return null; }
  if (!bioRe.test(u.pathname) || EXCLUDE_RE.test(u.pathname)) return null;
  return { domain: regDomain(u.hostname), path: u.pathname };
}

function shardUrl(crawl, n) {
  return `https://data.commoncrawl.org/cc-index/collections/${crawl}/indexes/cdx-${String(n).padStart(5, "0")}.gz`;
}

// Stream one shard, updating the domains map. Resolves { scanned, kept }.
function mineShard(crawl, n, bioRe, domains, maxMb, stats) {
  return new Promise((resolve) => {
    const headers = { "User-Agent": "rampedup-cc-domain-miner" };
    if (maxMb) headers.Range = `bytes=0-${Math.round(maxMb * 1024 * 1024)}`;
    const req = https.get(shardUrl(crawl, n), { headers }, (res) => {
      if (res.statusCode !== 200 && res.statusCode !== 206) {
        console.error(`  shard ${n}: HTTP ${res.statusCode}`); res.resume(); return resolve();
      }
      const gun = zlib.createGunzip();
      let buf = "";
      const flush = (last) => {
        let nl;
        while ((nl = buf.indexOf("\n")) >= 0) { absorb(buf.slice(0, nl)); buf = buf.slice(nl + 1); }
        if (last && buf) { absorb(buf); buf = ""; }
      };
      const absorb = (line) => {
        if (!line) return; stats.scanned++;
        const hit = classify(line, bioRe);
        if (!hit) return; stats.kept++;
        const e = domains.get(hit.domain) || { count: 0, sample: "" };
        e.count++; if (!e.sample) e.sample = hit.path; domains.set(hit.domain, e);
      };
      res.pipe(gun);
      gun.on("data", (d) => { buf += d.toString("latin1"); flush(false); });
      gun.on("error", () => resolve());          // truncated gzip from a Range request ends cleanly here
      gun.on("end", () => { flush(true); resolve(); });
    });
    req.on("error", (e) => { console.error(`  shard ${n}: ${e.message}`); resolve(); });
  });
}

function parseShards(spec) {
  const out = [];
  for (const s of String(spec).split(",")) {
    const m = s.trim().match(/^(\d+)-(\d+)$/);
    if (m) { for (let i = +m[1]; i <= +m[2]; i++) out.push(i); }
    else if (/^\d+$/.test(s.trim())) out.push(+s.trim());
  }
  return out;
}

async function main() {
  const arg = (name, def) => { const i = process.argv.indexOf("--" + name); return i > 0 ? process.argv[i + 1] : def; };
  const crawl = arg("crawl", "CC-MAIN-2026-25");
  const shards = parseShards(arg("shards", "0"));
  const maxMb = Number(arg("max-mb", "0")) || 0;
  const minPages = Number(arg("min-pages", "1"));
  const out = arg("out", "cc-domains.csv");
  const conc = Math.max(1, Number(arg("concurrency", "2")));
  const paths = arg("paths", "");
  const terms = paths ? paths.split(",") : DEFAULT_BIO;
  const bioRe = bioRegex(terms);

  console.error(`Mining ${shards.length} shard(s) of ${crawl}${maxMb ? ` (~${maxMb}MB each)` : ""} for bio paths.`);
  const domains = new Map();
  const stats = { scanned: 0, kept: 0 };
  let idx = 0;
  const worker = async () => {
    while (idx < shards.length) {
      const n = shards[idx++];
      await mineShard(crawl, n, bioRe, domains, maxMb, stats);
      console.error(`  shard ${n} done — ${domains.size} domains so far`);
    }
  };
  await Promise.all(Array.from({ length: Math.min(conc, shards.length) }, worker));

  const rows = [...domains.entries()].filter(([, e]) => e.count >= minPages).sort((a, b) => b[1].count - a[1].count);
  fs.writeFileSync(out, "domain,bio_pages,sample_path\n" +
    rows.map(([d, e]) => `${d},${e.count},"${e.sample.replace(/"/g, '""')}"`).join("\n") + "\n");
  console.error(`\nScanned ${stats.scanned.toLocaleString()} captures, kept ${stats.kept.toLocaleString()} bio pages -> ${rows.length.toLocaleString()} domains (min-pages ${minPages}) -> ${out}`);
  console.error("Top 15 by bio-page count:");
  console.error(rows.slice(0, 15).map(([d, e]) => `  ${String(e.count).padStart(5)}  ${d}  (${e.sample})`).join("\n"));
}

module.exports = { regDomain, bioRegex, classify, parseShards, DEFAULT_BIO };

// ---- offline self-test: node cc-domain-miner.js --selftest ----
if (require.main === module && process.argv.includes("--selftest")) {
  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? "✓" : "✗") + " " + l); c ? p++ : f++; };
  ok("regDomain strips www + subdomain", regDomain("www.advisor.morganstanley.com") === "morganstanley.com");
  ok("regDomain keeps co.uk", regDomain("people.rsmuk.co.uk") === "rsmuk.co.uk");
  ok("regDomain bare apex", regDomain("acme.com") === "acme.com");
  ok("parseShards range + list", JSON.stringify(parseShards("0-2,5")) === "[0,1,2,5]");
  const re = bioRegex(DEFAULT_BIO);
  const L = (url, status, mime) => `com,x)/p 20260101 ${JSON.stringify({ url, status, mime })}`;
  ok("keeps /our-people 200 html", (classify(L("https://x.com/our-people/jane-doe", "200", "text/html"), re) || {}).domain === "x.com");
  ok("keeps /agents/ html", !!classify(L("https://realty.com/agents/john-smith", "200", "text/html"), re));
  ok("drops a blog path", classify(L("https://x.com/blog/our-team-wins", "200", "text/html"), re) === null);
  ok("drops /news/category/people archive", classify(L("https://x.com/news/category/people/", "200", "text/html"), re) === null);
  ok("keeps /about-us/team (no excluded seg)", (classify(L("https://x.com/about-us/team/jane", "200", "text/html"), re) || {}).domain === "x.com");
  ok("drops non-200", classify(L("https://x.com/team/jane", "301", "text/html"), re) === null);
  ok("drops non-html", classify(L("https://x.com/team/jane.pdf", "200", "application/pdf"), re) === null);
  ok("custom paths only", classify(L("https://x.com/team/jane", "200", "text/html"), bioRegex(["agents"])) === null);
  console.log(`\ncc-domain-miner self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
} else if (require.main === module) {
  main().catch((e) => { console.error(e); process.exit(1); });
}
