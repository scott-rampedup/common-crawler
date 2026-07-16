/**
 * lambda-enrich.js — AWS-native Common Crawl HOME-PAGE enrichment (Phase 4, the universe-scale unlock).
 * ---------------------------------------------------------------------------------------------------
 * Sibling of lambda-extract: reads s3://commoncrawl home-page WARC records DIRECT, runs the SAME
 * cc-home-enrich.enrichFromHtml the local batch uses, and writes the company field updates (+ the bio
 * URLs it discovers, + the grouped on-domain contacts) as JSONL to S3. A loader (load-enriched.js) then
 * applies the updates to the `companies` OpenSearch index and hands the bio URLs to Hop 2.
 *
 *   Event: { companies:[{url,filename,offset,length,timestamp,id,domain,website,full_address,phone}],
 *            outKey?, batchId?, crawl?, altList?[], concurrency? }
 *   Env:   OUT_BUCKET (S3 bucket for the enriched JSONL)
 *   Return:{ companies, fetched, empty, errs, updated, bioUrls, contacts, written, outKey, secs }
 *
 * Output JSONL line: { id, domain, updates, contacts:[…], bio:[…] }  (updates = the co.update doc).
 */
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const path = require('path');
const { makeCcS3 } = require('./cc-s3');
const extractor = require('./extractor');
const che = require('./cc-home-enrich');

const s3 = new S3Client({});
const fetchWarc = makeCcS3();
const genderMap = extractor.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
const OUT_BUCKET = process.env.OUT_BUCKET || '';

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const worker = async () => { for (;;) { const k = i++; if (k >= items.length) return; out[k] = await fn(items[k], k); } };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
  return out;
}

exports.handler = async (event = {}) => {
  const companies = Array.isArray(event.companies) ? event.companies : [];
  const conc = Number(event.concurrency) || 48;
  const altList = Array.isArray(event.altList) && event.altList.length ? event.altList : undefined;
  const crawl = event.crawl || '';
  const outKey = event.outKey || `cc-enriched/${event.batchId || 'batch'}-${companies.length}.jsonl`;
  const now = new Date().toISOString();
  const t0 = Date.now();
  let fetched = 0, empty = 0, errs = 0, bioUrls = 0, contactsN = 0;

  const results = await mapLimit(companies, conc, async (c) => {
    let html;
    try { html = await fetchWarc({ url: c.url, filename: c.filename, offset: c.offset, length: c.length, timestamp: c.timestamp }); }
    catch (e) { errs++; return null; }
    if (!html) { empty++; return null; }
    fetched++;
    try {
      const company = { domain: c.domain || '', website: c.website || '', full_address: c.full_address || '', phone: c.phone || '' };
      const r = che.enrichFromHtml(company, html, { genderMap, altList, now, crawl });
      const bio = String(r.updates.bio_url || '').split(';').map((s) => s.trim()).filter(Boolean);
      bioUrls += bio.length; contactsN += (r.contacts || []).length;
      return { id: c.id, domain: c.domain, updates: r.updates, contacts: r.contacts || [], bio };
    } catch (e) { return null; }
  });

  const records = results.filter(Boolean);
  let written = 0;
  if (records.length && OUT_BUCKET) {
    const body = records.map((r) => JSON.stringify(r)).join('\n') + '\n';
    await s3.send(new PutObjectCommand({ Bucket: OUT_BUCKET, Key: outKey, Body: body, ContentType: 'application/x-ndjson' }));
    written = records.length;
  }
  return { companies: companies.length, fetched, empty, errs, updated: records.length, bioUrls, contacts: contactsN, written, outKey, secs: +((Date.now() - t0) / 1000).toFixed(1) };
};
