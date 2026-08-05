/**
 * sitemap-lib-backfill-ingest.js — one-time backfill: take the sitemaps previously submitted through the
 * Data Ingest UI and add them to the Sitemap Library, classified with our ingest logic (source='imported').
 *
 * Source of the "Data Ingest files": the monitor's SQLite `watched_sitemaps` (where Data-Ingest submissions
 * that were kept for monitoring are persisted) PLUS any files passed as args (one sitemap URL per line, or
 * the first column of a CSV). Idempotent — dedupe is by sitemap_url (_id); a re-sight just refreshes.
 *
 *   OPENSEARCH_ENDPOINT=… DATA_DIR=/data node sitemap-lib-backfill-ingest.js [more-urls.txt ...]
 *   flags: --no-db (skip watched_sitemaps)   --chunk=N (URLs classified per discovery batch, default 40)
 */
const fs = require('fs');
const path = require('path');
const sitemaps = require('./sitemaps');
const ccEngine = require('./cc-engine');
const { ingestSitemapsToLibrary } = require('./sitemap-lib-ingest');
const { loadGenderMap } = require('./extractor');

function loadNameSet(file) {
  try {
    const csv = fs.readFileSync(path.join(__dirname, file), 'utf8');
    return new Set(csv.split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name')));
  } catch (e) { return new Set(); }
}
function urlsFromFile(file) {
  const out = [];
  try {
    for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
      const first = String(line).split(',')[0].trim();               // URL or first CSV column
      if (/^https?:\/\//i.test(first)) out.push(first);
    }
  } catch (e) { console.error('  could not read', file, '-', e.message); }
  return out;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const args = process.argv.slice(2);
  const noDb = args.includes('--no-db');
  const chunk = Math.max(1, Number((args.find((a) => a.startsWith('--chunk=')) || '').split('=')[1]) || 40);
  const files = args.filter((a) => !a.startsWith('--'));

  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(client);
  const genderMap = loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const bioSitemapNames = loadNameSet('Sitemap extensions.csv');
  const locationSitemapNames = loadNameSet('Sitemap extensions - locations.csv');
  console.log(`Lexicons: ${bioSitemapNames.size} people / ${locationSitemapNames.size} location names, ${Object.keys(genderMap).length.toLocaleString()} first-name genders`);

  // Gather the submitted sitemap URLs.
  const urls = new Set();
  if (!noDb) {
    try {
      const { makeDb } = require('./db');
      const db = makeDb(process.env.DATA_DIR || __dirname);
      for (const w of db.listWatches()) if (w.sitemap_url) urls.add(String(w.sitemap_url).trim());
      console.log(`watched_sitemaps: ${urls.size} sitemap(s) from Data Ingest history`);
    } catch (e) { console.error('watched_sitemaps read failed (continuing):', e.message); }
  }
  for (const f of files) { const u = urlsFromFile(f); console.log(`${f}: ${u.length} url(s)`); for (const x of u) urls.add(x); }

  const list = [...urls];
  if (!list.length) { console.error('No sitemap URLs to backfill.'); process.exit(0); }
  console.log(`Backfilling ${list.length.toLocaleString()} sitemap(s) into the Library (chunk ${chunk})…`);

  const totals = { submitted: 0, classified: 0, unknown: 0, upserted: 0, errors: 0 };
  for (let i = 0; i < list.length; i += chunk) {
    const batch = list.slice(i, i + chunk);
    try {
      const r = await ingestSitemapsToLibrary({ sitemaps, sitemapsClient: client, ccEngine, urls: batch,
        genderMap, bioSitemapNames, locationSitemapNames, source: 'imported' });
      for (const k in totals) totals[k] += (r[k] || 0);
    } catch (e) { console.error('  batch error:', e.message); totals.errors += batch.length; }
    if ((i / chunk) % 10 === 0 || i + chunk >= list.length) {
      console.log(`  ${Math.min(i + chunk, list.length).toLocaleString()}/${list.length.toLocaleString()} | classified ${totals.classified.toLocaleString()} · unknown ${totals.unknown.toLocaleString()} · upserted ${totals.upserted.toLocaleString()} · err ${totals.errors}`);
    }
  }
  try { await client.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* */ }
  console.log('DONE:', JSON.stringify(totals));
  try { console.log('Library now:', JSON.stringify(await sitemaps.stats(client))); } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
