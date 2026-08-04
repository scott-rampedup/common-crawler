/**
 * sitemap-csv-load.js — load the curated "Sitemap Master List.csv" (hand-reviewed, known-active child
 * sitemaps) straight into the Sitemap Library OpenSearch index. Grows the Library from vetted sitemaps
 * with NO crawling. Each row -> a Library doc tagged source='reviewed'; dedup by sitemap_url (_id).
 *
 *   OPENSEARCH_ENDPOINT=… node sitemap-csv-load.js ["Sitemap Master List.csv"]
 *
 * CSV header: Industry,Website,SiteMap,Key Word,Email,Type,Count,Source,Type,CompanyId,
 *             CompanyCountry,CompanyEmployeeSizeRange,CompanyType
 */
const fs = require('fs');
const path = require('path');
const sitemaps = require('./sitemaps');
const co = require('./companies');

const CSV = process.argv[2] || path.join(__dirname, 'Sitemap Master List.csv');
// Rows whose Key Word / sitemap name signal LOCATION directories (store/branch/office/dealer); the rest
// of the curated catalog is people directories (agents/advisors/providers/attorneys/agencies).
const LOC_RE = /(?:^|[^a-z])(locations?|stores?|branch(?:es)?|offices?|dealers?|dealerships?|showrooms?)(?:[^a-z]|$)/i;

// Streaming quote-aware CSV parser -> yields field arrays (handles quotes, commas + newlines in fields).
function* parseCsv(text) {
  let field = '', row = [], q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
    else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); yield row; row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); yield row; }
}

const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return ''; } };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!fs.existsSync(CSV)) { console.error('CSV not found:', CSV); process.exit(1); }
  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(client);
  const text = fs.readFileSync(CSV, 'utf8');
  const now = new Date().toISOString();
  let seen = 0, kept = 0, skipped = 0, upserted = 0, errors = 0, people = 0, location = 0, header = true, batch = [];
  const flush = async () => { if (!batch.length) return; const r = await sitemaps.bulkUpsert(client, batch, now); upserted += r.upserted; errors += r.errors; batch = []; };

  for (const cols of parseCsv(text)) {
    if (header) { header = false; continue; }
    seen++;
    const smUrl = String(cols[2] || '').trim();
    if (!/^https?:\/\/\S+/i.test(smUrl)) { skipped++; continue; }
    const domain = co.normDomain(String(cols[1] || '').trim()) || hostOf(smUrl);
    if (!domain) { skipped++; continue; }
    const keyword = String(cols[3] || '').trim().toLowerCase();
    const kind = (LOC_RE.test(keyword) || LOC_RE.test(smUrl)) ? 'Location' : 'People';
    if (kind === 'Location') location++; else people++;
    const count = Number(String(cols[6] || '').replace(/[^0-9]/g, '')) || 0;
    batch.push({
      sitemap_url: smUrl,
      domain,
      parent_url: `https://${domain}/sitemap_index.xml`,   // curated rows are child sitemaps -> Type = Child
      kind,
      keyword,
      url_count: count,
      item_count: count,
      ratio: 1,
      by_name: false,
      industry: String(cols[0] || '').trim().toLowerCase(),
      company_id: String(cols[9] || '').trim(),
      lastmod: '',
      source: 'reviewed',
      status: 'active',
    });
    kept++;
    if (batch.length >= 1000) { await flush(); if (kept % 10000 === 0) console.error(`  kept ${kept.toLocaleString()} | upserted ${upserted.toLocaleString()}`); }
  }
  await flush();
  console.error(`DONE: seen ${seen.toLocaleString()} | kept ${kept.toLocaleString()} (People ${people.toLocaleString()} / Location ${location.toLocaleString()}) | skipped ${skipped.toLocaleString()} | upserted ${upserted.toLocaleString()} | errors ${errors}`);
  try { const st = await sitemaps.stats(client); console.error('Library now:', JSON.stringify(st)); } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
