/**
 * load-child-sitemaps.js — load a list of CHILD sitemap URLs into the Sitemap Library, find the BIO URLs
 * they contain, put the productive ones under nightly monitoring, and queue the bios for extraction.
 *
 *   OPENSEARCH_ENDPOINT=… node load-child-sitemaps.js --in s3://bucket/key.txt [--conc 256] [--limit N]
 *                                                     [--recheck] [--no-queue] [--dry-run]
 *
 * The input is one sitemap URL per line. This is deliberately NOT the Data Ingest path
 * (sitemap-lib-ingest.js): that one classifies and upserts but never returns the page URLs it saw, and the
 * whole point here is to come away with the BIO URLs as well as the Library rows.
 *
 * Runs on its own machine for the same reason the nightly sweep does — a quarter of a million sitemap
 * fetches has no business sharing a process with the UI.
 *
 * Already-known URLs are skipped by default. The Library holds ~248k sitemaps and an imported list will
 * overlap it heavily; re-fetching what we already classified is the expensive half of the job.
 */
const fs = require('fs');
const path = require('path');
const sitemaps = require('./sitemaps');
const openSearch = require('./opensearch');
const ccEngine = require('./cc-engine');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const num = (f, d) => Number(arg(f, '')) || d;
const IN = arg('--in', '');
const CONC = num('--conc', Number(process.env.LOAD_CONC) || 256);
const LIMIT = num('--limit', 0);
const RECHECK = process.argv.includes('--recheck');       // re-fetch sitemaps already in the Library
const NO_QUEUE = process.argv.includes('--no-queue');
const QUEUE_ALL = process.argv.includes('--queue-all');   // skip the have-check (re-queue everything)
const DRY = process.argv.includes('--dry-run');
const FETCH_TIMEOUT = num('--timeout', 8000);
const QUEUE_PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';

function loadNames(file) {
  try {
    const csv = fs.readFileSync(path.join(__dirname, file), 'utf8');
    return new Set(csv.split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name')));
  } catch (e) { return new Set(); }
}

async function readList(src) {
  if (/^s3:\/\//i.test(src)) {
    const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(src);
    if (!m) throw new Error('bad --in s3 uri');
    const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
    const r = await new S3Client({ region: process.env.AWS_REGION || 'us-east-1' })
      .send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }));
    const chunks = [];
    for await (const c of r.Body) chunks.push(c);
    return Buffer.concat(chunks).toString('utf8');
  }
  return fs.readFileSync(src, 'utf8');
}

let s3 = null, queuedObjects = 0, queueErrors = 0;
async function queueBioUrls(urls) {
  if (NO_QUEUE || DRY || !urls.length) return;
  const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
  const region = process.env.AWS_REGION || 'us-east-1';
  if (!s3) s3 = new S3Client({ region });
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
  const key = `${QUEUE_PREFIX}${new Date().toISOString().replace(/[:.]/g, '-')}-${urls.length}.txt`;
  try {
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: urls.join('\n') + '\n', ContentType: 'text/plain' }));
    queuedObjects++;
    console.error(`[queue] ${urls.length} bio URL(s) -> s3://${bucket}/${key}`);
  } catch (e) { queueErrors++; console.error(`[queue] FAILED (${urls.length} URLs): ${e.message}`); }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!IN) { console.error('need --in <s3://… | path>'); process.exit(1); }
  const t0 = Date.now();
  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const bioNames = loadNames('Sitemap extensions.csv');
  const locNames = loadNames('Sitemap extensions - locations.csv');

  const raw = await readList(IN);
  let list = [...new Set(raw.split(/\r?\n/).map((s) => s.trim()).filter((s) => /^https?:\/\//i.test(s)))];
  console.error(`══════ LOAD CHILD SITEMAPS · ${list.length.toLocaleString()} unique URL(s) ══════`);

  // Skip what the Library already holds. _id IS the sitemap_url, so mget answers this in ~250 round trips.
  let known = 0;
  if (!RECHECK) {
    const fresh = [];
    for (let i = 0; i < list.length; i += 1000) {
      const chunk = list.slice(i, i + 1000);
      try {
        const r = await client.mget({ index: sitemaps.INDEX, body: { ids: chunk }, _source: false });
        const docs = ((r.body || r).docs) || [];
        docs.forEach((d, j) => { if (d && d.found) known++; else fresh.push(chunk[j]); });
      } catch (e) { fresh.push(...chunk); }        // unknown -> treat as new rather than silently drop
      if (i && i % 50000 === 0) console.error(`  dedupe ${i.toLocaleString()}/${list.length.toLocaleString()}…`);
    }
    list = fresh;
    console.error(`already in the Library : ${known.toLocaleString()}  (skipped)`);
    console.error(`to fetch               : ${list.length.toLocaleString()}`);
  }
  if (LIMIT && list.length > LIMIT) { list = list.slice(0, LIMIT); console.error(`--limit ${LIMIT} applied`); }
  if (!list.length) { console.error('nothing new to load.'); return; }

  // Same fetch tuning the nightly sweep uses: most of a list like this is dead hosts, and the default
  // fetchDoc spends 15s on the primary path then another 15s on the residential gateway for each one.
  const swFetch = (u) => ccEngine.fetchDoc(u, { timeout: FETCH_TIMEOUT, fallbackStatus: [403, 429, 503], maxBytes: 8 * 1024 * 1024 });

  // Only queue bio URLs we do NOT already hold a contact for -- the same gate the nightly sweep applies.
  // Without it this queues every bio URL on every productive sitemap: a 400-URL sample produced 51,901
  // bios, which extrapolates to ~23M for the full list, most of them people already in the database.
  const contacts = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);
  async function unseen(urls) {
    if (QUEUE_ALL || !urls.length) return urls;
    const have = new Set();
    for (let i = 0; i < urls.length; i += 1024) {
      const chunk = urls.slice(i, i + 1024);
      try {
        const r = await contacts.search({ index: openSearch.INDEX, body: { size: 0,
          query: { terms: { web_source_url: chunk } },
          aggs: { u: { terms: { field: 'web_source_url', size: chunk.length } } } } });
        for (const b of (((r.body || r).aggregations.u.buckets) || [])) have.add(b.key);
      } catch (e) { /* on error keep the URL: re-queueing is recoverable, dropping it is not */ }
    }
    return urls.filter((u) => !have.has(u));
  }

  const sum = { fetched: 0, people: 0, location: 0, unknown: 0, errors: 0, bioUrls: 0, newBios: 0, alreadyHave: 0, withBios: 0, upserted: 0, upsertErrors: 0 };
  const now = new Date().toISOString();
  let docBuf = [], urlBuf = [];

  const flushDocs = async () => {
    if (!docBuf.length || DRY) { docBuf = []; return; }
    const batch = docBuf; docBuf = [];
    try { const r = await sitemaps.bulkUpsert(client, batch, now); sum.upserted += r.upserted; sum.upsertErrors += r.errors; }
    catch (e) { sum.upsertErrors += batch.length; console.error('[library] bulk failed:', e.message); }
  };
  const flushUrls = async () => {
    if (!urlBuf.length) return;
    const batch = urlBuf; urlBuf = [];
    const fresh = await unseen(batch);
    sum.newBios += fresh.length; sum.alreadyHave += batch.length - fresh.length;
    await queueBioUrls(fresh);
  };

  const one = async (url) => {
    try {
      const { watches } = await ccEngine.discoverSitemaps({
        urls: [url], directoryRules: {}, genderMap: {},
        bioSitemapNames: bioNames, locationSitemapNames: locNames, _fetchDoc: swFetch,
      });
      sum.fetched++;
      if (!watches || !watches.length) { sum.unknown++; docBuf.push(sitemaps.docFromUrl(url, { source: 'child-sitemaps-2' })); return; }
      for (const w of watches) {
        const doc = sitemaps.docFromWatch(w, { source: 'child-sitemaps-2' });
        const bios = (w.kind === 'People' ? (w.urls || []) : []).map((x) => x.url).filter(Boolean);
        if (w.kind === 'People') sum.people++; else if (w.kind === 'Location') sum.location++; else sum.unknown++;
        // The ask: sitemaps that PRODUCE bio URLs go under nightly monitoring. Absent means monitored, but
        // set it explicitly so the intent survives anyone later bulk-editing the Library.
        if (bios.length) { doc.monitored = true; sum.withBios++; sum.bioUrls += bios.length; urlBuf.push(...bios); }
        docBuf.push(doc);
      }
      if (docBuf.length >= 500) await flushDocs();
      if (urlBuf.length >= 3000) await flushUrls();
    } catch (e) {
      sum.errors++;
      if (sum.errors <= 3) console.error(`  fetch error ${url}: ${String(e.message || e).slice(0, 90)}`);
    }
  };

  console.error(`fetching at conc ${CONC}${DRY ? ' [dry-run: no writes]' : ''}…`);
  let done = 0, lastLog = 0;
  for (let i = 0; i < list.length; i += CONC) {
    await Promise.all(list.slice(i, i + CONC).map(one));
    done = Math.min(i + CONC, list.length);
    if (done - lastLog >= 10000) {
      lastLog = done;
      const rate = done / Math.max(1, (Date.now() - t0) / 1000);
      console.error(`  ${done.toLocaleString()}/${list.length.toLocaleString()} | People ${sum.people.toLocaleString()} | ${sum.bioUrls.toLocaleString()} bios seen, ${sum.newBios.toLocaleString()} new | ${rate.toFixed(1)}/s | ETA ${Math.round((list.length - done) / rate / 60)}m`);
    }
  }
  await flushDocs();
  await flushUrls();
  try { await client.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* */ }

  const secs = Math.round((Date.now() - t0) / 1000);
  console.error(`\n══════ DONE · ${secs}s ══════`);
  console.error(`  already known (skipped) : ${known.toLocaleString()}`);
  console.error(`  fetched                 : ${sum.fetched.toLocaleString()}  (${sum.errors.toLocaleString()} error(s))`);
  console.error(`  classified People       : ${sum.people.toLocaleString()}`);
  console.error(`  classified Location     : ${sum.location.toLocaleString()}`);
  console.error(`  no classification       : ${sum.unknown.toLocaleString()}`);
  console.error(`  PRODUCED BIO URLs       : ${sum.withBios.toLocaleString()} sitemap(s) -> ${sum.bioUrls.toLocaleString()} bio URL(s)  [monitored=true]`);
  console.error(`    already have a contact: ${sum.alreadyHave.toLocaleString()}`);
  console.error(`    NEW, queued to process: ${sum.newBios.toLocaleString()}`);
  console.error(`  Library rows upserted   : ${sum.upserted.toLocaleString()}  (${sum.upsertErrors.toLocaleString()} error(s))`);
  console.error(`  queued for extraction   : ${queuedObjects.toLocaleString()} object(s), ${queueErrors.toLocaleString()} failed`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
