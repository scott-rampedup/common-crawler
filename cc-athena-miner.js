#!/usr/bin/env node
/**
 * cc-athena-miner.js — discover bio-page WARC pointers across a FULL Common Crawl corpus using the
 * columnar (Parquet) index on AWS Athena, instead of streaming the CDX shards (cc-domain-miner.js).
 *
 * Athena scans the index SERVER-SIDE with predicate pushdown, so there's no 166 GB download and no
 * 30 MB-per-shard cap: the TLD filter, bio-path match, and <=N-per-domain cap all run in SQL, and only
 * the matched pointer rows come back. Output is {url,filename,offset,length,timestamp} JSONL — the exact
 * shape load-bio-urls.js --warc-file already feeds into the app's WARC fast path.
 *
 * Needs AWS creds in ~/.aws (or env) for the `cc-athena` user: Athena + Glue + S3 read on `commoncrawl`
 * (AmazonAthenaFullAccess + AmazonS3ReadOnlyAccess). Reads of the index in us-east-1 are free; a filtered
 * single-crawl query scans ~14 GB => ~$0.07. The db/table/partition are created on demand (idempotent).
 *
 *   node cc-athena-miner.js --count                                   # how many match in the latest crawl
 *   node cc-athena-miner.js --warc-out ptr.jsonl                      # full discovery -> JSONL pointers
 *   node cc-athena-miner.js --crawl CC-MAIN-2026-25 --per-domain 10 --limit 50000 --warc-out p.jsonl
 *   node cc-athena-miner.js --tlds com,org,us,edu,gov --warc-out p.jsonl
 *   node cc-athena-miner.js --selftest                                # offline tests (no AWS)
 */
const https = require("https");
const fs = require("fs");
const readline = require("readline");
const { DEFAULT_BIO } = require("./cc-domain-miner.js");

const REGION = process.env.AWS_REGION || "us-east-1";
const DB = process.env.ATHENA_DB || "ccindex_db";
const WORKGROUP = process.env.ATHENA_WORKGROUP || "primary";
// US-centric default per Scott: English-speaking markets + US institutions (.edu/.gov). url_host_tld is
// the public-suffix TLD (e.g. co.uk -> 'uk'), so these cover .co.uk/.com.au/etc. too.
const DEFAULT_TLDS = ["com", "org", "net", "us", "edu", "gov", "uk", "ca", "au", "nz", "ie"];
// Content paths that carry a bio term but aren't a people directory — mirrors cc-domain-miner EXCLUDE_RE.
const EXCLUDE_TERMS = ["blog", "news", "press", "article", "articles", "category", "categories", "tag", "tags", "attachment", "event", "events", "podcast"];

// ---- SQL builders (pure; covered by --selftest) ----
const escAlt = (t) => String(t).toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");   // regex-escape one alternation term
const sqlStrList = (arr) => arr.map((t) => `'${String(t).toLowerCase().replace(/'/g, "''")}'`).join(",");
const bioRegexSql = (terms) => "(?i)/(" + terms.map(escAlt).join("|") + ")(/|$)";
const excludeRegexSql = (terms) => "(?i)/(" + terms.map(escAlt).join("|") + ")/";

function buildDiscoverySql({ crawl, tlds, perDomain, limit, bioTerms }) {
  const bio = bioRegexSql(bioTerms && bioTerms.length ? bioTerms : DEFAULT_BIO).replace(/'/g, "''");
  const exc = excludeRegexSql(EXCLUDE_TERMS).replace(/'/g, "''");
  const capCol = perDomain > 0 ? `,\n         row_number() OVER (PARTITION BY url_host_registered_domain ORDER BY url_path) AS rn` : "";
  const inner =
`  SELECT url,
         warc_filename AS filename,
         warc_record_offset AS "offset",
         warc_record_length AS length,
         date_format(fetch_time, '%Y%m%d%H%i%s') AS "timestamp"${capCol}
  FROM ${DB}.ccindex
  WHERE crawl='${crawl}' AND subset='warc'
    AND fetch_status=200
    AND content_mime_detected IN ('text/html','application/xhtml+xml')
    AND url_host_tld IN (${sqlStrList(tlds)})
    AND regexp_like(url_path, '${bio}')
    AND NOT regexp_like(url_path, '${exc}')`;
  let sql = perDomain > 0
    ? `SELECT url, filename, "offset", length, "timestamp"\nFROM (\n${inner}\n)\nWHERE rn <= ${perDomain}`
    : inner;
  if (limit > 0) sql += `\nLIMIT ${limit}`;
  return sql;
}

function buildCountSql({ crawl, tlds, bioTerms }) {
  const bio = bioRegexSql(bioTerms && bioTerms.length ? bioTerms : DEFAULT_BIO).replace(/'/g, "''");
  const exc = excludeRegexSql(EXCLUDE_TERMS).replace(/'/g, "''");
  return `SELECT count(*) AS pages, count(DISTINCT url_host_registered_domain) AS domains
FROM ${DB}.ccindex
WHERE crawl='${crawl}' AND subset='warc'
  AND fetch_status=200
  AND content_mime_detected IN ('text/html','application/xhtml+xml')
  AND url_host_tld IN (${sqlStrList(tlds)})
  AND regexp_like(url_path, '${bio}')
  AND NOT regexp_like(url_path, '${exc}')`;
}

// --- exact-URL resolution mode (resolve a GIVEN bio-URL list -> WARC pointers) ---
// Normalize a URL to the JOIN key used on both sides: lowercased host (leading "www." stripped) + "|" +
// lowercased path with trailing slashes trimmed. MUST stay byte-identical to the SQL expression in
// buildResolveSql, or the JOIN silently misses. Query string + fragment are dropped (CC's url_path is
// the path only). Returns null for anything that isn't an http(s) URL.
function normalizeKey(urlStr) {
  let u;
  try { u = new URL(String(urlStr).trim()); } catch { return null; }
  if (u.protocol !== "http:" && u.protocol !== "https:") return null;
  const host = u.hostname.toLowerCase().replace(/^www\./, "");
  const path = (u.pathname || "").toLowerCase().replace(/\/+$/, "");   // "/" and "" both -> ""
  return host + "|" + path;
}

// External table over the uploaded one-column keys file (one normalized key per line). The "|" in keys
// is data, not a delimiter, so a tab field-terminator keeps each whole line as the single column k.
function buildKeysTableSql({ keysTable, bucket, prefix, db = DB }) {
  return `CREATE EXTERNAL TABLE IF NOT EXISTS ${db}.${keysTable} (k STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
LOCATION 's3://${bucket}/${prefix}'`;
}

// JOIN the keys table against ccindex on the normalized key, across one or more crawls, and keep the
// FRESHEST capture per key (row_number over fetch_time). Output is the load-queue/--warc-file shape.
function buildResolveSql({ crawls, keysTable, db = DB }) {
  const crawlList = crawls.map((c) => `'${String(c).replace(/'/g, "''")}'`).join(",");
  return `SELECT url, filename, "offset", length, "timestamp"
FROM (
  SELECT i.url AS url,
         i.warc_filename AS filename,
         i.warc_record_offset AS "offset",
         i.warc_record_length AS length,
         date_format(i.fetch_time, '%Y%m%d%H%i%s') AS "timestamp",
         row_number() OVER (PARTITION BY r.k ORDER BY i.fetch_time DESC) AS rn
  FROM ${db}.ccindex i
  JOIN ${db}.${keysTable} r
    ON r.k = concat(regexp_replace(lower(i.url_host_name), '^www\\.', ''), '|', lower(rtrim(i.url_path, '/')))
  WHERE i.crawl IN (${crawlList}) AND i.subset='warc'
    AND i.fetch_status=200
    AND i.content_mime_detected IN ('text/html','application/xhtml+xml')
)
WHERE rn = 1`;
}

// --- bio/people SITEMAP finder ---
// Match the url's FINAL path segment against a list of well-known people-directory sitemap filenames
// (agents-sitemap.xml, attorneys-sitemap.xml, team-sitemap.xml, loan-officer-sitemap.xml …). No mime
// filter — sitemaps are served as xml/text/octet-stream; fetch_status=200 keeps live captures.
const sitemapRegexSql = (names) => "(?i)/(" + names.map(escAlt).join("|") + ")(\\?|$)";
function buildSitemapCountSql({ crawl, tlds, names }) {
  const re = sitemapRegexSql(names).replace(/'/g, "''");
  return `SELECT count(DISTINCT url) AS sitemaps, count(DISTINCT url_host_registered_domain) AS domains
FROM ${DB}.ccindex
WHERE crawl='${crawl}' AND subset='warc'
  AND fetch_status=200
  AND url_host_tld IN (${sqlStrList(tlds)})
  AND regexp_like(url_path, '${re}')`;
}
function buildSitemapSql({ crawl, tlds, names, limit }) {
  const re = sitemapRegexSql(names).replace(/'/g, "''");
  let sql = `SELECT DISTINCT url_host_registered_domain AS domain, url
FROM ${DB}.ccindex
WHERE crawl='${crawl}' AND subset='warc'
  AND fetch_status=200
  AND url_host_tld IN (${sqlStrList(tlds)})
  AND regexp_like(url_path, '${re}')`;
  if (limit > 0) sql += `\nLIMIT ${limit}`;
  return sql;
}
// Read the sitemap-filename list from a one-column CSV (skips a leading 'name' header).
function readSitemapNames(file) {
  return require("fs").readFileSync(file, "utf8").split(/\r?\n/).map((s) => s.trim())
    .filter((l, i) => l && !(i === 0 && /^name$/i.test(l)));
}

// Parse Athena's CSV result (every field quoted, "" escapes a literal "). State machine -> rows of fields.
// Handles embedded commas/quotes; emits each row to onRow so we never hold the whole result twice.
function parseCsv(text, onRow) {
  let field = "", row = [], inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; }
      else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field); onRow(row); row = []; field = ""; }
    else if (c === "\r") { /* skip */ }
    else field += c;
  }
  if (field.length || row.length) { row.push(field); onRow(row); }
}

// ---- AWS plumbing (lazy-required so --selftest needs no SDK/creds) ----
function aws() {
  const { STSClient, GetCallerIdentityCommand } = require("@aws-sdk/client-sts");
  const { S3Client, HeadBucketCommand, CreateBucketCommand, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
  const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } = require("@aws-sdk/client-athena");
  return { STSClient, GetCallerIdentityCommand, S3Client, HeadBucketCommand, CreateBucketCommand, GetObjectCommand, PutObjectCommand,
    AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand,
    sts: new STSClient({ region: REGION }), s3: new S3Client({ region: REGION }), athena: new AthenaClient({ region: REGION }) };
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Latest CC corpus id (collinfo.json is newest-first); same source the engine/CDX miner use.
function latestCrawl(fallback = "CC-MAIN-2026-25") {
  return new Promise((resolve) => {
    const req = https.get("https://index.commoncrawl.org/collinfo.json", { headers: { "User-Agent": "cc-athena-miner" }, timeout: 8000 }, (res) => {
      let b = ""; res.on("data", (d) => b += d); res.on("end", () => {
        try { const a = JSON.parse(b); const id = a && a[0] && a[0].id; resolve(/^CC-MAIN-\d{4}-\d+$/.test(id) ? id : fallback); } catch { resolve(fallback); }
      });
    });
    req.on("error", () => resolve(fallback)); req.on("timeout", () => { req.destroy(); resolve(fallback); });
  });
}

// The latest N CC corpus ids (collinfo.json is newest-first) — for scanning several recent crawls when
// resolving a URL list (a bio page may only be in an older crawl).
function latestCrawls(n = 1, fallback = ["CC-MAIN-2026-25"]) {
  return new Promise((resolve) => {
    const req = https.get("https://index.commoncrawl.org/collinfo.json", { headers: { "User-Agent": "cc-athena-miner" }, timeout: 8000 }, (res) => {
      let b = ""; res.on("data", (d) => b += d); res.on("end", () => {
        try {
          const ids = JSON.parse(b).map((x) => x && x.id).filter((id) => /^CC-MAIN-\d{4}-\d+$/.test(id)).slice(0, Math.max(1, n));
          resolve(ids.length ? ids : fallback);
        } catch { resolve(fallback); }
      });
    });
    req.on("error", () => resolve(fallback)); req.on("timeout", () => { req.destroy(); resolve(fallback); });
  });
}

async function ensureBucket(A, bucket) {
  try { await A.s3.send(new A.HeadBucketCommand({ Bucket: bucket })); return; }
  catch (e) { /* create */ }
  await A.s3.send(new A.CreateBucketCommand({ Bucket: bucket }));   // us-east-1: no LocationConstraint
  console.error("created Athena results bucket:", bucket);
}

// Run a query; resolve { id, scanned, output } when SUCCEEDED, throw on FAILED/CANCELLED.
async function runAthena(A, sql, output, label) {
  const start = await A.athena.send(new A.StartQueryExecutionCommand({
    QueryString: sql, QueryExecutionContext: { Database: DB }, ResultConfiguration: { OutputLocation: output }, WorkGroup: WORKGROUP }));
  const id = start.QueryExecutionId;
  for (;;) {
    await sleep(1500);
    const q = await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }));
    const st = q.QueryExecution.Status.State;
    if (st === "SUCCEEDED") {
      const scanned = Number(q.QueryExecution.Statistics?.DataScannedInBytes || 0);
      if (label) console.error(`  [${label}] ok — scanned ${(scanned / 1048576).toFixed(0)} MB (~$${(scanned / 1e12 * 5).toFixed(4)})`);
      return { id, scanned, output: q.QueryExecution.ResultConfiguration.OutputLocation };
    }
    if (st === "FAILED" || st === "CANCELLED") throw new Error(`[${label || "query"}] ${st}: ${q.QueryExecution.Status.StateChangeReason || ""}`);
  }
}

async function ensureTable(A, crawl, output) {
  await runAthena(A, `CREATE DATABASE IF NOT EXISTS ${DB}`, output, "create db");
  await runAthena(A, CREATE_TABLE, output, "create table");
  await runAthena(A, `ALTER TABLE ${DB}.ccindex ADD IF NOT EXISTS PARTITION (crawl='${crawl}', subset='warc') LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/crawl=${crawl}/subset=warc/'`, output, "add partition");
}

async function s3Text(A, s3url) {
  const m = s3url.match(/^s3:\/\/([^/]+)\/(.+)$/); if (!m) throw new Error("bad s3 url: " + s3url);
  const obj = await A.s3.send(new A.GetObjectCommand({ Bucket: m[1], Key: m[2] }));
  const chunks = []; for await (const c of obj.Body) chunks.push(c); return Buffer.concat(chunks).toString("utf8");
}

// Parse ONE CSV line into fields (reuses the tested parseCsv; our rows never contain raw newlines).
function parseCsvLine(line) { let row = null; parseCsv(line + "\n", (r) => { if (!row) row = r; }); return row || []; }

// STREAM an S3 result object row-by-row (the full columnar harvest result is ~1GB — too big to buffer
// into one Node string, which caps at ~512MB). readline over the S3 Body, parse each CSV line, await
// onRow (so the caller can apply write backpressure).
async function s3StreamRows(A, s3url, onRow) {
  const m = s3url.match(/^s3:\/\/([^/]+)\/(.+)$/); if (!m) throw new Error("bad s3 url: " + s3url);
  const obj = await A.s3.send(new A.GetObjectCommand({ Bucket: m[1], Key: m[2] }));
  const rl = readline.createInterface({ input: obj.Body, crlfDelay: Infinity });
  for await (const line of rl) { if (line.length) await onRow(parseCsvLine(line)); }
}

const CREATE_TABLE = `
CREATE EXTERNAL TABLE IF NOT EXISTS ${DB}.ccindex (
  url_surtkey STRING, url STRING, url_host_name STRING, url_host_tld STRING,
  url_host_2nd_last_part STRING, url_host_3rd_last_part STRING, url_host_4th_last_part STRING,
  url_host_5th_last_part STRING, url_host_registry_suffix STRING, url_host_registered_domain STRING,
  url_host_private_suffix STRING, url_host_private_domain STRING, url_host_name_reversed STRING,
  url_protocol STRING, url_port INT, url_path STRING, url_query STRING,
  fetch_time TIMESTAMP, fetch_status SMALLINT, fetch_redirect STRING, content_digest STRING,
  content_mime_type STRING, content_mime_detected STRING, content_charset STRING,
  content_languages STRING, content_truncated STRING,
  warc_filename STRING, warc_record_offset INT, warc_record_length INT, warc_segment STRING)
PARTITIONED BY (crawl STRING, subset STRING)
STORED AS parquet
LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/'`;

async function main() {
  const arg = (n, d) => { const i = process.argv.indexOf("--" + n); return i > 0 ? process.argv[i + 1] : d; };
  const flag = (n) => process.argv.includes("--" + n);
  const A = aws();
  const acct = (await A.sts.send(new A.GetCallerIdentityCommand({}))).Account;
  const bucket = process.env.ATHENA_RESULTS_BUCKET || `aws-athena-query-results-${acct}-${REGION}`;
  const output = `s3://${bucket}/`;

  let crawl = arg("crawl", "");
  if (!crawl) { crawl = await latestCrawl(); console.error(`(no --crawl) latest corpus: ${crawl}`); }
  const tlds = (arg("tlds", "") ? arg("tlds", "").split(",").map((s) => s.trim()).filter(Boolean) : DEFAULT_TLDS);
  const perDomain = Number(arg("per-domain", "30")) || 0;
  const limit = Number(arg("limit", "0")) || 0;
  const warcOut = arg("warc-out", "");
  // --bio-terms-file: override DEFAULT_BIO with a bigger path-pattern set (one term/line), e.g. the
  // People+Company Path IDs from Directories.xlsx — widens bio-URL discovery.
  const bioTermsFile = arg("bio-terms-file", "");
  const bioTerms = bioTermsFile
    ? require("fs").readFileSync(bioTermsFile, "utf8").split(/\r?\n/).map((s) => s.trim()).filter(Boolean)
    : null;
  if (bioTerms) console.error(`bio terms: ${bioTerms.length} from ${bioTermsFile} (vs ${DEFAULT_BIO.length} default)`);

  await ensureBucket(A, bucket);
  await ensureTable(A, crawl, output);

  if (flag("setup")) { console.error("setup complete (db/table/partition ready)."); return; }

  // --- exact-URL resolution mode: resolve a given bio-URL list -> WARC pointers for the worker fleet ---
  const resolveFile = arg("resolve-urls", "");
  if (resolveFile) {
    if (!warcOut) { console.error("--resolve-urls requires --warc-out <file>"); return; }
    const lastN = Number(arg("last-n", "1")) || 1;
    // --crawls c1,c2,... scans an EXACT crawl set (e.g. only older crawls, to widen coverage without
    // re-scanning ones already resolved). Else --crawl (single) or the latest N.
    const crawlsArg = arg("crawls", "");
    const crawls = crawlsArg ? crawlsArg.split(",").map((s) => s.trim()).filter(Boolean)
      : (arg("crawl", "") ? [crawl] : await latestCrawls(lastN));
    console.error(`Resolve mode: ${resolveFile} -> ${warcOut}; crawls: ${crawls.join(", ")}`);

    // 1) Build the normalized, de-duplicated key set from the URL file (streamed; the file is ~95MB).
    const keys = new Set();
    let read = 0, skipped = 0;
    await new Promise((res, rej) => {
      const rl = readline.createInterface({ input: fs.createReadStream(resolveFile), crlfDelay: Infinity });
      rl.on("line", (line) => { const t = line.trim(); if (!t) return; read++; const k = normalizeKey(t); if (k) keys.add(k); else skipped++; });
      rl.on("close", res); rl.on("error", rej);
    });
    console.error(`  ${read.toLocaleString()} URL(s) read -> ${keys.size.toLocaleString()} distinct key(s) (${skipped} non-URL skipped)`);
    if (!keys.size) { console.error("no usable URLs — nothing to resolve."); return; }

    // 2) Upload the keys as a single S3 object under a per-run prefix, then define an external table over it.
    const tag = (arg("resolve-tag", "") || String(Date.now())).replace(/[^a-z0-9]/gi, "").slice(0, 24);
    const keysTable = "bio_resolve_keys_" + tag;
    const prefix = `bio-resolve/${tag}/`;
    const body = Buffer.from([...keys].join("\n") + "\n", "utf8");
    console.error(`  uploading keys (${(body.length / 1048576).toFixed(1)} MB) -> s3://${bucket}/${prefix}keys.txt ...`);
    await A.s3.send(new A.PutObjectCommand({ Bucket: bucket, Key: prefix + "keys.txt", Body: body }));
    for (const c of crawls) await ensureTable(A, c, output);   // make sure every scanned crawl's partition exists
    await runAthena(A, `DROP TABLE IF EXISTS ${DB}.${keysTable}`, output, "drop keys tbl");
    await runAthena(A, buildKeysTableSql({ keysTable, bucket, prefix }), output, "create keys tbl");

    // 3) JOIN -> stream the matched WARC pointers to the JSONL the fleet's load-queue consumes.
    const { id } = await runAthena(A, buildResolveSql({ crawls, keysTable }), output, "resolve");
    const loc = (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation;
    // A large S3 result read can drop mid-body (transient socket reset). The Athena result is immutable,
    // so on failure retry the whole stream from the start (createWriteStream truncates warcOut each try).
    let seen = 0, written = 0;
    for (let attempt = 1; ; attempt++) {
      try {
        seen = 0; written = 0;
        const ws = fs.createWriteStream(warcOut);
        await s3StreamRows(A, loc, async (r) => {
          if (seen++ === 0 && r[0] === "url") return;            // header
          const [url, filename, offset, length, timestamp] = r;
          if (!url || !filename) return;
          written++;
          if (!ws.write(JSON.stringify({ url, filename, offset, length, timestamp }) + "\n"))
            await new Promise((resolve) => ws.once("drain", resolve));
        });
        await new Promise((resolve) => ws.end(resolve));
        break;
      } catch (e) {
        if (attempt >= 3) throw e;
        console.error(`  result stream attempt ${attempt} failed (${(e && e.message) || e}); retrying…`);
      }
    }
    const pct = keys.size ? ((written / keys.size) * 100).toFixed(1) : "0";
    console.error(`\nResolved ${written.toLocaleString()} / ${keys.size.toLocaleString()} URL(s) in CC (${pct}% hit) -> ${warcOut}`);
    console.error(`Next: DATABASE_URL=… node load-queue.js ${warcOut}   (the 12-region fleet then drains it)`);
    return;
  }

  // --- sitemap-finder mode: find CC-archived sitemaps whose filename is a known people/bio sitemap ---
  const sitemapNamesFile = arg("sitemap-names", "");
  if (sitemapNamesFile) {
    const names = readSitemapNames(sitemapNamesFile);
    const sitemapsOut = arg("sitemaps-out", "");
    console.error(`Sitemap finder: ${names.length} filename pattern(s); tlds: ${tlds.join(",")}`);
    if (flag("count") || !sitemapsOut) {
      const { id } = await runAthena(A, buildSitemapCountSql({ crawl, tlds, names }), output, "sitemap-count");
      const csv = await s3Text(A, (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation);
      const rows = []; parseCsv(csv, (r) => rows.push(r));
      const [sm, dom] = rows[1] || [];
      console.error(`\n${crawl}: ${Number(sm).toLocaleString()} matching sitemap URL(s) across ${Number(dom).toLocaleString()} domain(s).`);
      if (!sitemapsOut) { console.error("(no --sitemaps-out given; count only.)"); return; }
    }
    console.error(`\nListing sitemap URLs -> ${sitemapsOut} ...`);
    const { id } = await runAthena(A, buildSitemapSql({ crawl, tlds, names, limit }), output, "sitemaps");
    const loc = (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation;
    const ws = fs.createWriteStream(sitemapsOut);
    let seen = 0, written = 0;
    await s3StreamRows(A, loc, async (r) => {
      if (seen++ === 0 && r[0] === "domain") return;       // header
      const url = r[1];
      if (!url) return;
      written++;
      if (!ws.write(url + "\n")) await new Promise((res) => ws.once("drain", res));
    });
    await new Promise((res) => ws.end(res));
    console.error(`Wrote ${written.toLocaleString()} sitemap URL(s) -> ${sitemapsOut}`);
    console.error(`Next: feed to the monitor via POST /api/monitor/watch  { sitemaps: [...] }.`);
    return;
  }

  if (flag("count") || !warcOut) {
    const { id } = await runAthena(A, buildCountSql({ crawl, tlds, bioTerms }), output, "count");
    const csv = await s3Text(A, (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation);
    const rows = []; parseCsv(csv, (r) => rows.push(r));
    const [pages, domains] = rows[1] || [];
    console.error(`\n${crawl}: ${Number(pages).toLocaleString()} bio pages across ${Number(domains).toLocaleString()} domains (tlds: ${tlds.join(",")}).`);
    if (!warcOut) { console.error("(no --warc-out given; count only.)"); return; }
  }

  console.error(`\nDiscovering pointers -> ${warcOut} (per-domain<=${perDomain || "∞"}${limit ? `, limit ${limit}` : ""}) ...`);
  const { id } = await runAthena(A, buildDiscoverySql({ crawl, tlds, perDomain, limit, bioTerms }), output, "discover");
  const loc = (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation;
  const ws = fs.createWriteStream(warcOut);
  let seen = 0, written = 0;
  await s3StreamRows(A, loc, async (r) => {
    if (seen++ === 0 && r[0] === "url") return;           // header
    const [url, filename, offset, length, timestamp] = r;
    if (!url || !filename) return;
    written++;
    if (!ws.write(JSON.stringify({ url, filename, offset, length, timestamp }) + "\n"))
      await new Promise((res) => ws.once("drain", res));  // backpressure: don't outrun the disk
  });
  await new Promise((res) => ws.end(res));
  console.error(`Wrote ${written.toLocaleString()} WARC pointers -> ${warcOut}`);
  console.error(`Next: DATABASE_URL=… node load-queue.js ${warcOut}`);
}

module.exports = { buildDiscoverySql, buildCountSql, bioRegexSql, excludeRegexSql, sitemapRegexSql, buildSitemapSql, buildSitemapCountSql, parseCsv, parseCsvLine, normalizeKey, buildKeysTableSql, buildResolveSql, DEFAULT_TLDS };

// ---- offline self-test: node cc-athena-miner.js --selftest ----
if (require.main === module && process.argv.includes("--selftest")) {
  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? "✓" : "✗") + " " + l); c ? p++ : f++; };
  ok("bio regex anchors term between / and (/|$)", bioRegexSql(["team"]) === "(?i)/(team)(/|$)");
  ok("exclude regex wraps in slashes", excludeRegexSql(["blog", "news"]) === "(?i)/(blog|news)/");
  const sql = buildDiscoverySql({ crawl: "CC-MAIN-2026-25", tlds: DEFAULT_TLDS, perDomain: 30, limit: 0 });
  ok("discovery sql filters the crawl", /crawl='CC-MAIN-2026-25'/.test(sql));
  ok("discovery sql has the <=30/domain cap", /row_number\(\) OVER \(PARTITION BY url_host_registered_domain/.test(sql) && /WHERE rn <= 30/.test(sql));
  ok("discovery sql lists tlds incl edu/gov", /'edu','gov'/.test(sql));
  ok("discovery sql emits CC 14-digit timestamp", /date_format\(fetch_time, '%Y%m%d%H%i%s'\)/.test(sql));
  ok("no-cap sql omits row_number", !/row_number/.test(buildDiscoverySql({ crawl: "X", tlds: ["com"], perDomain: 0, limit: 10 })));
  ok("limit appended", /LIMIT 10$/.test(buildDiscoverySql({ crawl: "X", tlds: ["com"], perDomain: 0, limit: 10 })));
  const rows = []; parseCsv(`"url","filename"\n"https://x.com/a,b","f.warc.gz"\n"he said ""hi""","g"\n`, (r) => rows.push(r));
  ok("csv parses quoted comma field", rows[1][0] === "https://x.com/a,b" && rows[1][1] === "f.warc.gz");
  ok("csv unescapes doubled quotes", rows[2][0] === 'he said "hi"');
  const pl = parseCsvLine('"https://x.com/a,b","f.warc.gz","10","20","20260601000000"');
  ok("parseCsvLine streams one row, quoted comma intact", pl[0] === "https://x.com/a,b" && pl[1] === "f.warc.gz" && pl[4] === "20260601000000");
  ok("count sql has no row_number/cap", !/row_number/.test(buildCountSql({ crawl: "X", tlds: ["com"] })));
  // sitemap finder
  ok("sitemap regex matches the final path segment, escaping dots",
    sitemapRegexSql(["agents-sitemap.xml", "People.xml"]) === "(?i)/(agents-sitemap\\.xml|people\\.xml)(\\?|$)");
  const smSql = buildSitemapSql({ crawl: "CC-MAIN-2026-25", tlds: ["com", "org"], names: ["team-sitemap.xml"], limit: 0 });
  ok("sitemap sql selects distinct domain+url, no html-mime filter",
    /SELECT DISTINCT url_host_registered_domain AS domain, url/.test(smSql) && !/content_mime_detected/.test(smSql));
  ok("sitemap sql matches url_path on the filename + keeps fetch_status=200",
    /regexp_like\(url_path, '\(\?i\)\/\(team-sitemap\\\.xml\)\(\\\?\|\$\)'\)/.test(smSql) && /fetch_status=200/.test(smSql));
  ok("sitemap count sql counts distinct sitemaps + domains",
    /count\(DISTINCT url\) AS sitemaps/.test(buildSitemapCountSql({ crawl: "X", tlds: ["com"], names: ["x.xml"] })));
  // exact-URL resolution
  ok("normalizeKey strips www + trailing slash, lowercases",
    normalizeKey("https://www.11kbw.com/Barristers/Aileen-McColgan/") === "11kbw.com|/barristers/aileen-mccolgan");
  ok("normalizeKey: root path -> empty path component",
    normalizeKey("https://www.x.com/") === "x.com|" && normalizeKey("https://x.com") === "x.com|");
  ok("normalizeKey: keeps non-www subdomains, drops query/fragment",
    normalizeKey("http://agents.farmers.com/ca/jane-doe?ref=1#bio") === "agents.farmers.com|/ca/jane-doe");
  ok("normalizeKey: rejects non-http(s) and junk", normalizeKey("ftp://x.com/a") === null && normalizeKey("not a url") === null);
  const rsql = buildResolveSql({ crawls: ["CC-MAIN-2026-25", "CC-MAIN-2026-21"], keysTable: "bio_resolve_keys_t1" });
  ok("resolve sql joins keys table to ccindex on the normalized key",
    /JOIN ccindex_db\.bio_resolve_keys_t1 r/.test(rsql) &&
    /r\.k = concat\(regexp_replace\(lower\(i\.url_host_name\), '\^www\\\.', ''\), '\|', lower\(rtrim\(i\.url_path, '\/'\)\)\)/.test(rsql));
  ok("resolve sql scans the given crawls and keeps the freshest capture per key",
    /i\.crawl IN \('CC-MAIN-2026-25','CC-MAIN-2026-21'\)/.test(rsql) &&
    /row_number\(\) OVER \(PARTITION BY r\.k ORDER BY i\.fetch_time DESC\)/.test(rsql) && /WHERE rn = 1$/.test(rsql));
  ok("resolve sql keeps 200 + html only", /i\.fetch_status=200/.test(rsql) && /content_mime_detected IN \('text\/html'/.test(rsql));
  ok("keys table sql defines a one-column external table at the run prefix",
    /CREATE EXTERNAL TABLE IF NOT EXISTS ccindex_db\.kt \(k STRING\)/.test(buildKeysTableSql({ keysTable: "kt", bucket: "b", prefix: "bio-resolve/t/" })) &&
    /LOCATION 's3:\/\/b\/bio-resolve\/t\/'/.test(buildKeysTableSql({ keysTable: "kt", bucket: "b", prefix: "bio-resolve/t/" })));
  console.log(`\ncc-athena-miner self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
} else if (require.main === module) {
  main().catch((e) => { console.error("ERROR:", (e && e.stack) || e); process.exit(1); });
}
