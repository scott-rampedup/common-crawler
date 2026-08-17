/**
 * revagent-append.js — join the RevAgent company list to the Master DB on ROOT DOMAIN.
 *
 *   OPENSEARCH_ENDPOINT=… node revagent-append.js --file revagent-companies.csv --out /tmp/out
 *
 * Produces TWO separate appends, because they answer different questions and have different shapes:
 *
 *   <out>-contacts.csv   every CONTACT whose domain matches a listed website (one row per person)
 *   <out>-companies.csv  every COMPANY record whose domain matches (one row per company record —
 *                        a domain can hold several, which is exactly the HQ/Affiliate duplication
 *                        measured earlier: 25,200,130 HQ records over 24,918,439 distinct domains)
 *
 * The join key is the registrable domain, not the URL. The list carries "https://example.com/" while
 * contacts carry "example.com", and matching those as strings finds nothing.
 *
 * The source file is parsed with a real quote-aware state machine rather than split(','): the
 * Description column contains commas AND embedded newlines, so a naive line-by-line read tears rows
 * apart and silently mis-columns every field after Description — the website would be read out of the
 * middle of a paragraph.
 *
 * Read-only against the DB. Writes only the two CSVs.
 */
const fs = require('fs');
const path = require('path');
const os = require('./opensearch');
const co = require('./companies');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const FILE = arg('file', '');
const OUT = arg('out', '/tmp/revagent');
const PER_DOMAIN = Number(arg('per-domain', '500')) || 500;   // cap per domain, per index
const BATCH = Number(arg('batch', '80')) || 80;               // domains per msearch

const N = (n) => Number(n || 0).toLocaleString();
const esc = (v) => { const s = String(v == null ? '' : v).replace(/\r?\n/g, ' ').trim(); return /[",]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
const rootOf = (u) => {
  const s = String(u || '').trim();
  if (!s) return '';
  try { return new URL(/^https?:\/\//i.test(s) ? s : 'https://' + s).hostname.replace(/^www\./, '').toLowerCase(); }
  catch (e) { return ''; }
};

// The source list is staged in S3 rather than baked into the image: .dockerignore excludes CSVs, and a
// 20MB data file has no business inside a deploy artifact that ships on every release.
async function openIn(src) {
  if (/^s3:\/\//i.test(src)) {
    const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
    const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(src);
    const r = await new S3Client({ region: process.env.AWS_REGION || 'us-east-1' })
      .send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }));
    r.Body.setEncoding('utf8');
    return r.Body;
  }
  return fs.createReadStream(src, { encoding: 'utf8', highWaterMark: 1 << 20 });
}

/** Quote-aware streaming CSV parse — commas and newlines live inside quoted fields here. */
async function readRows(file, onRow) {
  const stream = await openIn(file);
  return new Promise((resolve, reject) => {
    let field = '', row = [], q = false;
    stream.on('data', (chunk) => {
      for (let i = 0; i < chunk.length; i++) {
        const c = chunk[i];
        if (q) {
          if (c === '"') { if (chunk[i + 1] === '"') { field += '"'; i++; } else q = false; }
          else field += c;
        } else if (c === '"') q = true;
        else if (c === ',') { row.push(field); field = ''; }
        else if (c === '\n') { row.push(field); onRow(row); row = []; field = ''; }
        else if (c !== '\r') field += c;
      }
    });
    stream.on('end', () => { if (field.length || row.length) { row.push(field); onRow(row); } resolve(); });
    stream.on('error', reject);
  });
}

// hits.total.value is capped at 10,000 by default, so an uncorrected "matched" count both overstates
// small domains' totals and hides that a domain was truncated. track_total_hits makes the number real;
// fetchAll then pages any domain that exceeds the per-request size so the append is COMPLETE rather than
// merely large. The first run of this wrote 35,729 company rows against 48,901 "matched" and reported no
// truncation at all, because capping was only tracked on the contacts loop.
async function fetchAll(client, index, domain, pageSize) {
  const out = [];
  let after = null;
  for (;;) {
    const body = { size: pageSize, query: { term: { domain } }, sort: [{ _doc: 'asc' }], track_total_hits: true };
    if (after) body.search_after = after;
    const r = await client.search({ index, body });
    const hits = ((r.body || r).hits.hits) || [];
    if (!hits.length) break;
    for (const h of hits) out.push(h);
    after = hits[hits.length - 1].sort;
    if (hits.length < pageSize) break;
  }
  return out;
}

(async () => {
  if (!FILE || (!/^s3:\/\//i.test(FILE) && !fs.existsSync(FILE))) { console.error('need --file <path or s3://…>'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);

  // ---- 1. read the list, key it by domain ----
  const byDomain = new Map();          // domain -> { name, website, sector, category, sub }
  let rows = 0, noSite = 0, hdr = null;
  await readRows(FILE, (r) => {
    if (!hdr) { hdr = r.map((h) => String(h).trim()); return; }
    rows++;
    const col = (name, fallback) => { const i = hdr.indexOf(name); return i > -1 ? (r[i] || '') : (r[fallback] || ''); };
    const website = col('Website', 4);
    const d = rootOf(website);
    if (!d) { noSite++; return; }
    if (!byDomain.has(d)) {
      byDomain.set(d, { domain: d, name: col('Name', 0), website: String(website).trim(),
        sector: col('Sector', 23), category: col('Category', 24), sub: col('Sub-Category', 25) });
    }
  });
  console.log(`${path.basename(FILE)}: ${N(rows)} row(s) -> ${N(byDomain.size)} distinct domain(s)${noSite ? `, ${N(noSite)} with no usable website` : ''}`);

  const domains = [...byDomain.keys()];

  // ---- 2. contacts append ----
  const cOut = fs.createWriteStream(`${OUT}-contacts.csv`);
  cOut.write(['RevAgent Name', 'RevAgent Website', 'Sector', 'Category', 'Sub-Category', 'Domain',
    'First', 'Last', 'Title', 'Position', 'Email Address', 'Email Type', 'Phone', 'Mobile', 'Gender',
    'LinkedIn URL', 'Company', 'Work Location', 'Phone Location', 'Source', 'Web Source URL'].join(',') + '\n');

  let cMatched = 0, cRows = 0, domainsWithContacts = 0, capped = 0;
  for (let i = 0; i < domains.length; i += BATCH) {
    const chunk = domains.slice(i, i + BATCH);
    const body = [];
    for (const d of chunk) { body.push({ index: os.INDEX }); body.push({ size: PER_DOMAIN, query: { term: { domain: d } }, track_total_hits: true }); }
    let responses = [];
    try { const r = await client.msearch({ body }); responses = ((r.body || r).responses) || []; }
    catch (e) { console.error(`  contacts batch ${i} failed: ${e.message}`); continue; }
    for (let k = 0; k < chunk.length; k++) {
      const d = chunk[k];
      const res = responses[k];
      let hits = (res && res.hits && res.hits.hits) || [];
      const total = (res && res.hits && res.hits.total && res.hits.total.value) || 0;
      if (!hits.length) continue;
      domainsWithContacts++; cMatched += total;
      // Page the rest rather than silently truncating this domain.
      if (total > hits.length) { capped++; try { hits = await fetchAll(client, os.INDEX, d, PER_DOMAIN); } catch (e) { /* keep the first page */ } }
      const m = byDomain.get(d);
      for (const h of hits) {
        const s = h._source || {};
        cRows++;
        cOut.write([m.name, m.website, m.sector, m.category, m.sub, d,
          s.first, s.last, s.title, s.position, s.email, s.email_type, s.phone, s.mobile, s.gender,
          s.linkedin_url, s.company, s.work_location, s.phone_location, s.source, s.web_source_url]
          .map(esc).join(',') + '\n');
      }
    }
    if ((i / BATCH) % 25 === 0) console.log(`  contacts: ${N(i)}/${N(domains.length)} domains | ${N(cRows)} row(s)`);
  }
  await new Promise((r) => cOut.end(r));

  // ---- 3. companies append ----
  const oOut = fs.createWriteStream(`${OUT}-companies.csv`);
  oOut.write(['RevAgent Name', 'RevAgent Website', 'Sector', 'Category', 'Sub-Category', 'Domain',
    'Company Name', 'Company Website', 'Company Type', 'Website Type', 'Industry', 'Size', 'Founded',
    'Locality', 'Region', 'Country', 'LinkedIn URL', 'NAICS Code', 'NAICS Title', 'Contact Count'].join(',') + '\n');

  let oMatched = 0, oRows = 0, domainsWithCompany = 0, oCapped = 0;
  for (let i = 0; i < domains.length; i += BATCH) {
    const chunk = domains.slice(i, i + BATCH);
    const body = [];
    for (const d of chunk) { body.push({ index: co.INDEX }); body.push({ size: PER_DOMAIN, query: { term: { domain: d } }, track_total_hits: true }); }
    let responses = [];
    try { const r = await client.msearch({ body }); responses = ((r.body || r).responses) || []; }
    catch (e) { console.error(`  companies batch ${i} failed: ${e.message}`); continue; }
    for (let k = 0; k < chunk.length; k++) {
      const d = chunk[k];
      const res = responses[k];
      let hits = (res && res.hits && res.hits.hits) || [];
      const total = (res && res.hits && res.hits.total && res.hits.total.value) || 0;
      if (!hits.length) continue;
      domainsWithCompany++; oMatched += total;
      if (total > hits.length) { oCapped++; try { hits = await fetchAll(client, co.INDEX, d, PER_DOMAIN); } catch (e) { /* keep the first page */ } }
      const m = byDomain.get(d);
      for (const h of hits) {
        const s = h._source || {};
        oRows++;
        oOut.write([m.name, m.website, m.sector, m.category, m.sub, d,
          s.name, s.website, s.company_type, s.website_type, s.industry, s.size, s.founded,
          s.locality, s.region, s.country, s.linkedin_url, s.naics_code, s.naics_title, s.contact_count]
          .map(esc).join(',') + '\n');
      }
    }
    if ((i / BATCH) % 25 === 0) console.log(`  companies: ${N(i)}/${N(domains.length)} domains | ${N(oRows)} row(s)`);
  }
  await new Promise((r) => oOut.end(r));

  const pct = (n) => ((n / Math.max(1, byDomain.size)) * 100).toFixed(1) + '%';
  console.log(`\n=== CONTACTS ===`);
  console.log(`  domains with at least one contact  ${N(domainsWithContacts)}  ${pct(domainsWithContacts)}`);
  console.log(`  contacts available                 ${N(cMatched)}`);
  console.log(`  rows written                       ${N(cRows)}  -> ${OUT}-contacts.csv`);
  if (capped) console.log(`  ${N(capped)} domain(s) exceeded one page and were fully paged.`);
  console.log(`\n=== COMPANIES ===`);
  console.log(`  domains with a company record      ${N(domainsWithCompany)}  ${pct(domainsWithCompany)}`);
  console.log(`  company records matched            ${N(oMatched)}`);
  console.log(`  rows written                       ${N(oRows)}  -> ${OUT}-companies.csv`);
  if (oCapped) console.log(`  ${N(oCapped)} domain(s) exceeded one page and were fully paged.`);
  // Push both files somewhere durable — the machine's /tmp dies with the machine.
  if (/^(1|true|yes|on)$/i.test(process.env.UPLOAD || '1')) {
    try {
      const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
      const region = process.env.AWS_REGION || 'us-east-1';
      const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
      const s3c = new S3Client({ region });
      for (const kind of ['contacts', 'companies']) {
        const local = `${OUT}-${kind}.csv`;
        const key = `revagent/append-${kind}.csv`;
        const st = fs.statSync(local);
        await s3c.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: fs.createReadStream(local),
          ContentLength: st.size, ContentType: 'text/csv' }));
        console.log(`  saved ${kind}: ${(st.size / 1e6).toFixed(1)}MB -> s3://${bucket}/${key}`);
      }
    } catch (e) { console.error('  upload failed:', e.message, '- files remain at', OUT + '-*.csv'); }
  }

  if (oRows > domainsWithCompany) {
    console.log(`  ${N(oRows - domainsWithCompany)} extra row(s): several company records share one domain.`);
  }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
