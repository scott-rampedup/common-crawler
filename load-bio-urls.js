#!/usr/bin/env node
/**
 * load-bio-urls.js — feed a bio-URL file into the app as chunked webpage jobs (Phase 0 loader).
 *
 * Reads bio URLs (one per line, e.g. from `cc-domain-miner --urls-out`), de-dupes, and submits them
 * to the running app as `webpage` jobs via POST /api/jobs. The server extracts each URL CC-first and
 * upserts contacts to the Master DB (email-keyed dedup + normalize.js). Chunks run ONE AT A TIME —
 * the loader waits for each job to finish before starting the next, so it never starts many big jobs
 * at once (the single-machine memory guard).
 *
 *   node load-bio-urls.js --server https://common-crawler.fly.dev --cookie "sid=XXX" --file urls.txt
 *   node load-bio-urls.js --server http://localhost:3000 --login user:pass --file urls.txt --chunk 50 --limit 200
 *
 * args: --server (req) --file (req) --chunk (default 5000) --limit (cap total; 0=all) --start (skip N)
 *       --name --type --cookie "sid=…"  OR  --login user:pass   --poll-secs (default 5) --dry-run
 */
const http = require("http");
const https = require("https");
const fs = require("fs");
const { URL } = require("url");

const arg = (n, d) => { const i = process.argv.indexOf("--" + n); return i > 0 ? process.argv[i + 1] : d; };
const flag = (n) => process.argv.includes("--" + n);

function req(method, urlStr, { cookie, json } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const lib = u.protocol === "http:" ? http : https;
    const body = json != null ? JSON.stringify(json) : null;
    const headers = { "User-Agent": "rampedup-bio-loader", "Accept": "application/json" };
    if (cookie) headers.Cookie = cookie;
    if (body) { headers["Content-Type"] = "application/json"; headers["Content-Length"] = Buffer.byteLength(body); }
    const r = lib.request(u, { method, headers, timeout: 600000 }, (res) => {
      const chunks = []; res.on("data", (d) => chunks.push(d));
      res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString("utf8") }));
    });
    r.on("error", reject); r.on("timeout", () => { r.destroy(new Error("timeout")); });
    if (body) r.write(body); r.end();
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const chunk = (arr, n) => { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; };

async function login(server, userPass) {
  const [username, ...rest] = userPass.split(":"); const password = rest.join(":");
  // the app's account login (sets the `sid` cookie); tries username then email key
  for (const payload of [{ username, password }, { email: username, password }]) {
    const r = await req("POST", server + "/api/auth/login", { json: payload });
    const sc = (r.headers["set-cookie"] || []).find((c) => /^sid=/.test(c));
    if (r.status < 300 && sc) return sc.split(";")[0];
  }
  throw new Error("login failed — check --login user:pass (or use --cookie)");
}

async function main() {
  const server = (arg("server", "") || "").replace(/\/$/, "");
  const file = arg("file", "");
  if (!server || !file) { console.error("need --server and --file"); process.exit(2); }
  const size = Math.max(1, Number(arg("chunk", "5000")));
  const limit = Number(arg("limit", "0")) || 0;
  const start = Number(arg("start", "0")) || 0;
  const name = arg("name", "CC bio-path load");
  const type = arg("type", "CC Bio-Path");
  const pollMs = (Number(arg("poll-secs", "5")) || 5) * 1000;

  let urls = fs.readFileSync(file, "utf8").split(/\r?\n/).map((s) => s.trim()).filter((s) => /^https?:\/\//i.test(s));
  urls = [...new Set(urls)];
  if (start) urls = urls.slice(start);
  if (limit) urls = urls.slice(0, limit);
  const batches = chunk(urls, size);
  console.error(`${urls.length} URLs -> ${batches.length} chunk(s) of <=${size} into ${server}`);
  if (flag("dry-run")) { console.error("(dry-run) first chunk sample:", batches[0]?.slice(0, 3)); return; }

  const loginCreds = arg("login", "");
  let cookie = arg("cookie", "");
  if (!cookie && loginCreds) cookie = await login(server, loginCreds);
  if (!cookie) { console.error("need --cookie \"sid=…\" or --login user:pass"); process.exit(2); }
  if (!/^sid=/.test(cookie)) cookie = "sid=" + cookie;   // accept bare token
  const AUTH_MSG = "auth lost (session ended) — re-run with --login for auto re-auth, or a fresh --cookie. Jobs already started finish server-side.";

  // Re-authenticate on a mid-run 401 (a 14-day session can still be ended by sign-out). Needs --login.
  async function relogin() { if (!loginCreds) return false; try { cookie = await login(server, loginCreds); console.error("  (re-authenticated)"); return true; } catch { return false; } }
  async function authed(method, u, json) {
    let r = await req(method, u, { cookie, json });
    if (r.status === 401 && await relogin()) r = await req(method, u, { cookie, json });
    return r;
  }

  let totalAdded = 0, authLost = false;
  for (let i = 0; i < batches.length && !authLost; i++) {
    const r = await authed("POST", server + "/api/jobs", { domains: batches[i], mode: "webpage", type, name: `${name} [${i + 1}/${batches.length}]` });
    if (r.status === 401) { console.error(`chunk ${i + 1}: ${AUTH_MSG}`); authLost = true; break; }
    if (r.status >= 300) { console.error(`chunk ${i + 1}: POST /api/jobs -> ${r.status} ${r.body.slice(0, 200)}`); break; }
    let job; try { job = JSON.parse(r.body); } catch { console.error(`chunk ${i + 1}: bad response`); break; }
    process.stderr.write(`chunk ${i + 1}/${batches.length} job ${job.id} (${batches[i].length} urls) `);
    let last;
    for (;;) {
      await sleep(pollMs);
      const s = await authed("GET", `${server}/api/jobs/${job.id}`);
      if (s.status === 401) { console.error(`\n  ${AUTH_MSG}`); authLost = true; break; }
      let j; try { j = JSON.parse(s.body); } catch { continue; }
      if (!j.id) continue;                 // ignore transient error bodies
      last = j;
      if (j.status !== "running" && j.status !== "queued" && j.status !== "starting") break;
      process.stderr.write(".");
    }
    if (last) { const added = last.recordCount != null ? last.recordCount : 0; totalAdded += added; console.error(` ${last.status} — ${added} record(s) [${last.done || 0}/${last.total || batches[i].length} urls]`); }
  }
  console.error(`\n${authLost ? "Stopped early (auth)." : "Done."} ~${totalAdded} record(s) across the completed chunk(s). (DB dedups by email; check the Search tab for totals.)`);
}

module.exports = { chunk };

if (require.main === module && flag("selftest")) {
  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? "✓" : "✗") + " " + l); c ? p++ : f++; };
  ok("chunk splits evenly", JSON.stringify(chunk([1, 2, 3, 4, 5], 2)) === "[[1,2],[3,4],[5]]");
  ok("chunk single", JSON.stringify(chunk([1, 2], 5)) === "[[1,2]]");
  ok("chunk empty", JSON.stringify(chunk([], 5)) === "[]");
  console.log(`\nload-bio-urls self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
} else if (require.main === module) {
  main().catch((e) => { console.error(e.message || e); process.exit(1); });
}
