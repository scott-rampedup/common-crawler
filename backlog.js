/**
 * backlog.js — answer "are we through the BIO URL backlog?" with measured numbers.
 *
 *   OPENSEARCH_ENDPOINT=… node backlog.js
 *
 * There is no single counter for this, because there is no single backlog. Bio URLs arrive from three
 * places with three different notions of "done", and conflating them is how the question gets answered
 * wrongly:
 *
 *   1. SITEMAP MONITOR QUEUE — a real, countable, finite queue. Each monitor pass writes one S3 object
 *      under monitor-queue/pending/ named {stamp}-{count}.txt, and `bio-etl --mode urls --drain` deletes
 *      what it consumes. So anything still under pending/ is, by definition, undrained. The URL count is
 *      recoverable from the key names alone — no GETs needed.
 *
 *   2. SITEMAPS AWAITING A PASS — sitemaps that exist but have never been checked, or are stale. These
 *      are not queued yet; they are potential queue.
 *
 *   3. COMMON CRAWL — NOT a queue and has no persistent done-state. Each `--mode discover` run is a fresh
 *      sweep of the index. "Backlog" here means the gap between the measured universe (18.1M bio pages per
 *      crawl across 11 English TLDs) and what has actually been turned into contacts. Reporting a
 *      percentage here would be false precision, so this prints both sides and lets them be compared.
 *
 * Read-only.
 */
const os = require('/app/opensearch');
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');

const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const BIO_PER_CRAWL = 18149225;      // measured, 468-term directory list, 11 English TLDs

const N = (n) => Number(n || 0).toLocaleString();
const MB = (b) => (b / 1e6).toFixed(1) + 'MB';

(async () => {
  const c = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const s3 = new S3Client({ region: REGION });
  const cnt = async (index, query) => (await c.count({ index, body: { query } })).body.count;
  const has = (f) => ({ bool: { must: [{ exists: { field: f } }], must_not: [{ term: { [f]: '' } }] } });

  // ---- 1. the monitor queue ----
  let token = null, objs = 0, bytes = 0, urls = 0, unparsed = 0, oldest = '', newest = '';
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: token }));
    for (const o of (r.Contents || [])) {
      objs++; bytes += o.Size || 0;
      // key looks like {ISO-stamp}-{count}.txt — the trailing integer is the URL count for that pass.
      const m = /-(\d+)\.txt$/.exec(o.Key || '');
      if (m) urls += Number(m[1]); else unparsed++;
      const k = (o.Key || '').slice(PREFIX.length);
      if (!oldest || k < oldest) oldest = k;
      if (!newest || k > newest) newest = k;
    }
    token = r.IsTruncated ? r.NextContinuationToken : null;
  } while (token);

  console.log('\n=== 1. SITEMAP MONITOR QUEUE (undrained) ===');
  console.log(`  objects under ${PREFIX}   ${N(objs)}   ${MB(bytes)}`);
  console.log(`  bio URLs queued                        ${N(urls)}${unparsed ? `   (+${N(unparsed)} object(s) with an unparseable name)` : ''}`);
  if (objs) {
    console.log(`  oldest pass                            ${oldest.slice(0, 19)}`);
    console.log(`  newest pass                            ${newest.slice(0, 19)}`);
  }
  console.log(`  -> drained by: bio-etl --mode urls --in s3://${BUCKET}/${PREFIX} --live --drain`);

  // ---- 2. sitemaps not yet (or not recently) checked ----
  const SM = 'sitemaps';
  const smTotal = await cnt(SM, { match_all: {} });
  let smMonitored = 0, smNeverChecked = 0, smPeople = 0;
  try { smMonitored = await cnt(SM, { term: { monitored: true } }); } catch (e) { smMonitored = -1; }
  try { smNeverChecked = await cnt(SM, { bool: { must_not: [{ exists: { field: 'last_checked' } }] } }); } catch (e) { smNeverChecked = -1; }
  try { smPeople = await cnt(SM, { term: { kind: 'People' } }); } catch (e) { smPeople = -1; }
  console.log('\n=== 2. SITEMAPS (the source that FEEDS that queue) ===');
  console.log(`  sitemaps total                         ${N(smTotal)}`);
  if (smPeople >= 0) console.log(`  of type People                         ${N(smPeople)}`);
  if (smMonitored >= 0) console.log(`  with monitoring on                     ${N(smMonitored)}`);
  if (smNeverChecked >= 0) console.log(`  never checked                          ${N(smNeverChecked)}   <- have not produced queue yet`);
  // total_new is the cumulative URL count monitoring has already handed to extraction. Comparing it to
  // what is still sitting in pending/ shows whether the monitor is out-running the drain.
  try {
    const r = await c.search({ index: SM, body: { size: 0, aggs: { tn: { sum: { field: 'total_new' } }, ln: { sum: { field: 'last_new' } } } } });
    const a = (r.body || r).aggregations;
    console.log(`  cumulative URLs handed on (total_new)  ${N(Math.round(a.tn.value))}`);
    console.log(`  from the most recent pass  (last_new)  ${N(Math.round(a.ln.value))}`);
  } catch (e) { /* optional */ }

  // ---- 3. common crawl ----
  const contacts = await cnt(os.INDEX, { match_all: {} });
  const withBio = await cnt(os.INDEX, has('web_source_url'));
  let bySource = {};
  try {
    // `source` is mapped as keyword directly — not text-with-a-.keyword-subfield — so aggregating on
    // 'source.keyword' silently resolves to nothing and prints an empty breakdown.
    const r = await c.search({ index: os.INDEX, body: { size: 0, aggs: { s: { terms: { field: 'source', size: 30 } } } } });
    for (const b of ((r.body || r).aggregations.s.buckets || [])) bySource[b.key] = b.doc_count;
  } catch (e) { console.log('  (source agg failed: ' + e.message + ')'); }

  console.log('\n=== 3. COMMON CRAWL (no done-state; a sweep, not a queue) ===');
  console.log(`  bio pages in ONE crawl (measured)      ${N(BIO_PER_CRAWL)}`);
  console.log(`  crawls available                       126`);
  console.log(`  contacts in the DB right now           ${N(contacts)}`);
  console.log(`  ...of which carry a web_source_url     ${N(withBio)}`);
  if (Object.keys(bySource).length) {
    console.log('\n  contacts by source:');
    for (const k of Object.keys(bySource).sort((a, b) => bySource[b] - bySource[a]).slice(0, 12)) {
      console.log(`    ${k.padEnd(28)} ${N(bySource[k]).padStart(12)}`);
    }
  }
  console.log('\n  A single crawl holds more bio pages than the DB holds contacts, and 125 more crawls');
  console.log('  exist. There is no "through it" for this source — only how much has been swept.');
})().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
