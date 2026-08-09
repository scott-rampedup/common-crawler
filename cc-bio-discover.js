/**
 * cc-bio-discover.js — BACKUP bio-URL discovery, independent of sitemaps. Runs (ideally on a second
 * machine) a Common Crawl / Athena search for bio-path pages (/agents/jane, /attorneys/john, /team/…)
 * ON the Company-Database domains, keeps the NET-NEW ones (no contact yet), and feeds them into the app's
 * extraction queue (POST /api/jobs, WARC fast path) — so they're crawled CC-first and become contacts.
 *
 * This complements the sitemap path: even when a site has no usable sitemap, CC's URL index still reveals
 * its bio pages by directory/keyword. Discovery is server-side Athena (no proxy), so it won't compete with
 * the sitemap monitor's live fetches.
 *
 *   OPENSEARCH_ENDPOINT=… LOADER_TOKEN=… node cc-bio-discover.js \
 *     [--industry "real estate,law practice,…"] [--country "…"] [--limit-domains N] [--last-n 2]
 *     [--chunk 2000] [--max-urls 0] [--app https://common-crawler.fly.dev] [--in-flight 2] [--dry]
 *
 * Run as an independent machine, e.g.:  fly machine run <image> --  node cc-bio-discover.js …
 */
const companies = require('./companies');
const openSearch = require('./opensearch');
const miner = require('./cc-athena-miner');
const https = require('https');
const http = require('http');
const { URL } = require('url');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function apiReq(method, urlStr, token, jsonBody) {
  return new Promise((resolve) => {
    const u = new URL(urlStr); const lib = u.protocol === 'http:' ? http : https;
    const body = jsonBody ? JSON.stringify(jsonBody) : null;
    const req = lib.request({ hostname: u.hostname, port: u.port || (u.protocol === 'http:' ? 80 : 443), path: u.pathname + u.search, method,
      headers: { 'x-loader-token': token, 'content-type': 'application/json', ...(body ? { 'content-length': Buffer.byteLength(body) } : {}) }, timeout: 60000 },
      (res) => { let s = ''; res.on('data', (d) => (s += d)); res.on('end', () => { let j = null; try { j = JSON.parse(s); } catch (e) { /* */ } resolve({ status: res.statusCode, json: j, body: s }); }); });
    req.on('error', () => resolve({ status: 0 })); req.on('timeout', () => { req.destroy(); resolve({ status: 0 }); });
    if (body) req.write(body); req.end();
  });
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const token = arg('token', '') || process.env.LOADER_TOKEN;
  const app = arg('app', 'https://common-crawler.fly.dev').replace(/\/$/, '');
  const industry = arg('industry', 'real estate,law practice,legal services,insurance,financial services,investment management,investment banking,banking,accounting,medical practice,hospital & health care');
  const country = arg('country', 'united states,united kingdom,canada,australia,new zealand,ireland');
  const limitDomains = Number(arg('limit-domains', '0')) || 0;
  const lastN = Number(arg('last-n', '2')) || 2;
  const chunk = Number(arg('chunk', '2000')) || 2000;
  const maxUrls = Number(arg('max-urls', '0')) || 0;
  const inFlight = Math.max(1, Number(arg('in-flight', '2')) || 2);
  const dry = has('dry');
  const tag = (arg('tag', '') || String(process.pid)).replace(/[^a-z0-9_]/gi, '').slice(0, 32).toLowerCase();
  if (!dry && !token) { console.error('need LOADER_TOKEN (or --token) to post jobs'); process.exit(1); }

  const coClient = companies.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const osc = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const CONTACTS = process.env.OPENSEARCH_INDEX || 'contacts';

  // Which of these URLs do we already have a contact for (web_source_url)? -> filter to net-new.
  async function haveSet(urls) {
    const have = new Set();
    for (let i = 0; i < urls.length; i += 1024) {
      const c = urls.slice(i, i + 1024);
      try { const r = await osc.search({ index: CONTACTS, body: { size: 0, query: { terms: { web_source_url: c } }, aggs: { u: { terms: { field: 'web_source_url', size: c.length } } } } }); for (const b of (((r.body || r).aggregations.u.buckets) || [])) have.add(b.key); } catch (e) { /* */ }
    }
    return have;
  }
  // Wait until fewer than `inFlight` of OUR jobs are still running (single-machine memory guard).
  async function waitForSlot(myIds) {
    for (;;) {
      const r = await apiReq('GET', `${app}/api/jobs`, token);
      const jobs = (r.json && (r.json.jobs || r.json)) || [];
      const running = jobs.filter((j) => myIds.has(j.id) && j.status === 'running').length;
      if (running < inFlight) return;
      await sleep(5000);
    }
  }

  // 1) Company domains -> S3 domains table.
  console.error(`Domains: industry="${industry}", country="${country}"…`);
  const domains = new Set();
  await companies.each(coClient, { industry, country }, (row) => { const d = String(row.domain || '').toLowerCase().trim().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, ''); if (d && d.includes('.') && !d.includes(' ')) domains.add(d); }, limitDomains || 5000000);
  console.error(`  ${domains.size.toLocaleString()} distinct domain(s).`);
  if (!domains.size) { console.error('No domains.'); process.exit(0); }

  const A = miner.aws();
  const acct = (await A.sts.send(new A.GetCallerIdentityCommand({}))).Account;
  const bucket = process.env.ATHENA_RESULTS_BUCKET || `aws-athena-query-results-${acct}-${miner.REGION}`;
  const output = `s3://${bucket}/`;
  const crawls = await miner.latestCrawls(lastN);
  console.error(`Crawls: ${crawls.join(', ')}`);
  await miner.ensureBucket(A, bucket);
  for (const c of crawls) await miner.ensureTable(A, c, output);
  const domainsTable = 'bio_disc_' + tag;
  const prefix = `cc-bio-discover/${tag}/`;
  await A.s3.send(new A.PutObjectCommand({ Bucket: bucket, Key: prefix + 'domains.txt', Body: Buffer.from([...domains].join('\n') + '\n', 'utf8') }));
  await miner.runAthena(A, `DROP TABLE IF EXISTS ${miner.DB}.${domainsTable}`, output, 'drop domains tbl');
  await miner.runAthena(A, miner.buildKeysTableSql({ keysTable: domainsTable, bucket, prefix }), output, 'create domains tbl');

  // 2) Athena bio-page discovery, freshest capture per page.
  const { id } = await miner.runAthena(A, miner.buildDomainDiscoverySql({ crawls, domainsTable, tlds: miner.DEFAULT_TLDS }), output, 'bio discover');
  const loc = (await A.athena.send(new A.GetQueryExecutionCommand({ QueryExecutionId: id }))).QueryExecution.ResultConfiguration.OutputLocation;

  // 3) Stream results -> net-new filter -> feed /api/jobs (WARC fast path), paced by in-flight cap.
  const totals = { discovered: 0, netNew: 0, queued: 0, jobs: 0 };
  const myIds = new Set();
  let buf = [];   // { url, filename, offset, length, timestamp }
  let first = true, stop = false;

  async function flush() {
    if (!buf.length) return;
    const rows = buf; buf = [];
    const have = await haveSet(rows.map((r) => r.url));
    const fresh = rows.filter((r) => !have.has(r.url));
    totals.netNew += fresh.length;
    if (!fresh.length) return;
    if (dry) { totals.queued += fresh.length; return; }
    await waitForSlot(myIds);
    const r = await apiReq('POST', `${app}/api/jobs`, token, {
      mode: 'webpage', type: 'CC Discovery', name: `CC Bio Discovery ${new Date().toISOString().slice(0, 10)}`,
      domains: fresh.map((x) => x.url), warc: fresh,
    });
    if (r.status === 200 && r.json && r.json.id) { myIds.add(r.json.id); totals.jobs++; totals.queued += fresh.length; }
    else console.error(`  POST /api/jobs -> ${r.status} ${(r.body || '').slice(0, 120)}`);
  }

  await miner.s3StreamRows(A, loc, async (rr) => {
    if (first) { first = false; if (rr[0] === 'url') return; }
    first = false;
    if (stop) return;
    const [url, filename, offset, length, timestamp] = rr;
    if (!url || !filename) return;
    totals.discovered++;
    buf.push({ url, filename, offset, length, timestamp });
    if (buf.length >= chunk) { await flush(); if (maxUrls && totals.queued >= maxUrls) { stop = true; } }
  });
  await flush();

  console.error(`\nDiscovered ${totals.discovered.toLocaleString()} bio page(s) | net-new ${totals.netNew.toLocaleString()} | queued ${totals.queued.toLocaleString()} in ${totals.jobs} job(s).`);
  if (dry) console.error('--dry: nothing posted.');
  process.exit(0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
