/**
 * corporate-places-ingest.js — fan out over the ATP brand catalog and populate the Corporate Places index.
 * For each brand row (from the CSV) that has a "Most Recent Link", fetch its GeoJSON, turn every feature
 * into a place doc via corporate-places.docFromFeature, ENRICH each with the spreadsheet BRAND (Name) +
 * TYPE, and bulk-index into corporate_places. Also stamps atp_library with how many places each brand loaded.
 *
 *   OPENSEARCH_ENDPOINT=… node corporate-places-ingest.js [csv] [--limit=N] [--concurrency=6] [--skip-loaded]
 *
 * Idempotent: place _id = spider|ref, so re-runs upsert. --skip-loaded skips brands already stamped loaded>0.
 */
const fs = require('fs');
const path = require('path');
const cp = require('./corporate-places');
const atp = require('./atp');

function parseCsv(text) {
  const rows = []; let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; } else field += c; }
    else if (c === '"') inQ = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c === '\r') { /* skip */ }
    else field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows;
}

async function fetchGeojson(url, tries = 3) {
  for (let t = 0; t < tries; t++) {
    try {
      const ctrl = new AbortController(); const to = setTimeout(() => ctrl.abort(), 60000);
      const res = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': 'CommonCrawler/1.0 (+places ingest)' } });
      clearTimeout(to);
      if (!res.ok) { if (res.status === 404) return { features: [] }; throw new Error('HTTP ' + res.status); }
      const j = await res.json();
      return j && Array.isArray(j.features) ? j : { features: [] };
    } catch (e) { if (t === tries - 1) throw e; await new Promise((r) => setTimeout(r, 800 * (t + 1))); }
  }
  return { features: [] };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const args = process.argv.slice(2);
  const flag = (n, d) => { const a = args.find((x) => x.startsWith('--' + n + '=')); return a ? a.split('=')[1] : d; };
  const csv = args.find((a) => !a.startsWith('--')) || path.join(__dirname, 'All the places Appended with Websites.csv');
  const limit = Number(flag('limit', 0)) || 0;
  const concurrency = Math.max(1, Number(flag('concurrency', 6)) || 6);
  const skipLoaded = args.includes('--skip-loaded');

  const client = cp.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const atpClient = atp.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await cp.ensureIndex(client);

  const rows = parseCsv(fs.readFileSync(csv, 'utf8'));
  const brands = [];
  for (let i = 1; i < rows.length; i++) {
    const c = rows[i]; if (!c) continue;
    const [name, website, country, spider, source, count, type, link] = c;
    if (!/^https?:\/\//i.test(String(link || '').trim())) continue;   // only rows with a geojson link
    brands.push({ name: (name || '').trim(), website: (website || '').trim().toLowerCase(), country: (country || '').trim(),
      spider: (spider || '').trim(), type: (type || '').trim(), link: link.trim(), id: atp.idFor({ spider, website, name }) });
  }
  const work = limit ? brands.slice(0, limit) : brands;
  console.error(`${brands.length} brand(s) with a link; processing ${work.length} (concurrency ${concurrency})`);

  const totals = { brands: 0, indexed: 0, errors: 0, empty: 0, failed: 0 };
  let cursor = 0;
  async function worker(wid) {
    while (cursor < work.length) {
      const b = work[cursor++];
      const n = cursor;
      try {
        const geo = await fetchGeojson(b.link);
        const feats = geo.features || [];
        if (!feats.length) { totals.empty++; await atp.setIngestState(atpClient, b.id, { loaded: 0, last_ingest: new Date().toISOString() }); continue; }
        const meta = { brand: b.name, type: b.type, spider: b.spider, brand_website: b.website, country: b.country };
        let brandIndexed = 0;
        for (let i = 0; i < feats.length; i += 1000) {
          const docs = feats.slice(i, i + 1000).map((f) => cp.docFromFeature(f, meta));
          const r = await cp.bulkIndex(client, docs);
          brandIndexed += r.indexed; totals.indexed += r.indexed; totals.errors += r.errors;
        }
        totals.brands++;
        await atp.setIngestState(atpClient, b.id, { loaded: brandIndexed, last_ingest: new Date().toISOString() });
        if (n % 25 === 0 || n === work.length) console.error(`  [${n}/${work.length}] ${b.name}: +${brandIndexed} | total indexed ${totals.indexed.toLocaleString()} | empty ${totals.empty} | failed ${totals.failed}`);
      } catch (e) { totals.failed++; console.error(`  FAIL ${b.name} (${b.link}): ${e.message}`); }
    }
  }
  await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
  try { await client.indices.refresh({ index: cp.INDEX }); } catch (e) { /* */ }
  console.error('DONE:', JSON.stringify(totals));
  try { console.error('Corporate Places now:', JSON.stringify(await cp.stats(client))); } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
