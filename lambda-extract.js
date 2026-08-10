/**
 * lambda-extract.js — AWS-native Common Crawl contact extraction (Phase 2, the throughput unlock).
 * ---------------------------------------------------------------------------------------------------
 * Reads s3://commoncrawl WARC records DIRECT (no CloudFront throttle, no Postgres queue), runs the SAME
 * extractRecord the worker fleet uses, and writes the extracted contact records as JSONL to S3 for a
 * bulk loader to index into OpenSearch. This decouples extraction (thousands of concurrent Lambdas,
 * S3-scale output) from indexing (a controlled bulk-load) — removing the Postgres write bottleneck that
 * caps the fleet at ~12-16 workers.
 *
 *   Event: { pointers:[{url,filename,offset,length,timestamp}], outKey?, batchId?, concurrency? }
 *   Env:   OUT_BUCKET (S3 bucket for the extracted JSONL)
 *   Return:{ pointers, fetched, empty, errs, records, written, outKey, secs }
 *
 * Packaging note: phone-blocks.csv (40MB) is intentionally omitted — Phone Type/Location are backfilled
 * later by the batch geocoder. extractRecord uses `wireless` only inside `if (... && wireless)`, and the
 * geo-carrier require in extractor is lazy, so neither is loaded here. @aws-sdk/client-s3 ships in the
 * nodejs20.x runtime, so it isn't bundled.
 */
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const path = require('path');
const { makeCcS3 } = require('./cc-s3');
const extractor = require('./extractor');
const { modelMissingEmails } = require('./email-model');
let liName = null; try { liName = require('./li-name'); } catch (e) { /* optional in older deploy packages */ }

const s3 = new S3Client({});
const fetchWarc = makeCcS3();                                             // S3-direct WARC reader (byte-identical to the fleet's)
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
  const pointers = Array.isArray(event.pointers) ? event.pointers : [];
  const conc = Number(event.concurrency) || 64;
  const outKey = event.outKey || `cc-extracted/${event.batchId || 'batch'}-${pointers.length}.jsonl`;
  const t0 = Date.now();
  let fetched = 0, empty = 0, errs = 0;

  const results = await mapLimit(pointers, conc, async (p) => {
    let html;
    try { html = await fetchWarc({ url: p.url, filename: p.filename, offset: p.offset, length: p.length, timestamp: p.timestamp }); }
    catch (e) { errs++; return null; }
    if (!html) { empty++; return null; }
    fetched++;
    const ts = String(p.timestamp || '').slice(0, 8).replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
    try {
      return extractor.extractRecord(html, p.url, { genderMap, directoryRules: {}, source: 'Common Crawl', timestamp: ts, allowNoEmail: true }) || null;
    } catch (e) { return null; }
  });

  let records = results.filter(Boolean);
  // Same enrichment the fleet does, minus geocode (no phone-blocks in the package). dbQuery returns []
  // so email modelling uses same-batch company patterns only (no PG lookup available from Lambda).
  if (records.length) {
    // LinkedIn-slug name recovery before modelling (guarded require: an older deploy package may not
    // carry li-name.js, and a missing optional module must not take the whole Lambda down).
    try { if (liName) liName.applyToRecords(records); } catch (e) { /* best-effort */ }
    try { await modelMissingEmails(records, { dbQuery: async () => [] }); } catch (e) { /* best-effort */ }
    try { records = extractor.analyzePhones(records) || records; } catch (e) { /* best-effort */ }
  }

  let written = 0;
  if (records.length && OUT_BUCKET) {
    const body = records.map((r) => JSON.stringify(r)).join('\n') + '\n';
    await s3.send(new PutObjectCommand({ Bucket: OUT_BUCKET, Key: outKey, Body: body, ContentType: 'application/x-ndjson' }));
    written = records.length;
  }
  return { pointers: pointers.length, fetched, empty, errs, records: records.length, written, outKey, secs: +((Date.now() - t0) / 1000).toFixed(1) };
};
