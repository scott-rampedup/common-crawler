/**
 * atp-csv-load.js — load "All the places Appended with Websites.csv" into the All The Places Library
 * (atp_library OpenSearch index). One row per brand/spider; dedup by spider id. Mirrors sitemap-csv-load.js.
 *
 *   OPENSEARCH_ENDPOINT=… node atp-csv-load.js ["All the places Appended with Websites.csv"]
 *
 * CSV header: Name,Website,Country,Spider,Source,Count,Type,All the places Most Recent Link
 */
const fs = require('fs');
const path = require('path');
const atp = require('./atp');

const CSV = process.argv[2] || path.join(__dirname, 'All the places Appended with Websites.csv');

// Minimal RFC-4180 CSV parser (handles quotes + embedded commas/newlines).
function parseCsv(text) {
  const rows = []; let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; }
      else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c === '\r') { /* skip */ }
    else field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = atp.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await atp.ensureIndex(client);
  const rows = parseCsv(fs.readFileSync(CSV, 'utf8'));
  const now = new Date().toISOString();
  const srcFile = path.basename(CSV);

  let seen = 0, upserted = 0, errors = 0, skipped = 0, batch = [];
  const flush = async () => { if (!batch.length) return; const r = await atp.bulkUpsert(client, batch, now); upserted += r.upserted; errors += r.errors; batch = []; };

  for (let i = 1; i < rows.length; i++) {                 // skip header
    const c = rows[i]; if (!c || !c.length) continue;
    const [name, website, country, spider, source, count, type, link] = c;
    if (!(name || website || spider)) { skipped++; continue; }
    seen++;
    batch.push(atp.docFromRow({ name, website, country, spider, source, count, type, link, source_file: srcFile }));
    if (batch.length >= 1000) await flush();
    if (seen % 1000 === 0) console.error(`  ${seen} rows | upserted ${upserted}`);
  }
  await flush();
  try { await client.indices.refresh({ index: atp.INDEX }); } catch (e) { /* */ }
  console.error(`DONE: seen ${seen} | upserted ${upserted} | skipped ${skipped} | errors ${errors}`);
  try { console.error('ATP Library now:', JSON.stringify(await atp.stats(client))); } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
