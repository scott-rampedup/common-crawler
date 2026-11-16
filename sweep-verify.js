#!/usr/bin/env node
//
// sweep-verify.js — did the night's sitemap sweep actually put contacts in the Contact Crawler?
// ---------------------------------------------------------------------------------------------------
// WHY THIS EXISTS. The sweep reports "814,869 new URL(s)" and exits 0, and every stage after it reports
// success too: bio-feed advances its cursor, the workers mark rows done, the merge says it wrote. None
// of that is the thing we actually care about. A contact is DELIVERED when someone searching the
// Contact Crawler UI can find it -- and every one of those green lights can be on while the record is
// absent from the index the UI reads.
//
// That is not hypothetical. On 2026-11-14 a sync watermark bug left 0.87% of merged contacts in
// Postgres and invisible in OpenSearch, and it was found by accident. This walks the chain the other
// way -- from the URLs the sweep discovered to the documents the UI can return -- so the question is
// answered with evidence instead of with four green lights.
//
//   node sweep-verify.js                      # today's sweep output, 2,000-URL sample
//   node sweep-verify.js --sample 10000
//   node sweep-verify.js --since 2026-11-15T06:00:00Z --until 2026-11-16T00:00:00Z
//   node sweep-verify.js --all                # every URL the sweep wrote (slow, exact)
//   node sweep-verify.js --fix                # index anything found in Postgres but not in OpenSearch
//
// WHAT EACH OUTCOME MEANS. The funnel narrows for legitimate reasons at every step, so a number being
// smaller than the one above it is not a fault:
//
//   not fed yet        the sweep is still running, or bio-feed has not reached that object. Mid-run
//                      this is normal and large; hours after the sweep ends it is not.
//   filtered           fed, but never became a crawl_queue row: already crawled before, already has a
//                      contact, or sits on one of the blocked domains. This is the bulk of a sweep --
//                      "new" in the sweep's output means new TO THAT SITEMAP, not new to the pipeline.
//   crawled, no email  the page was fetched and held no usable contact. Expected for ~20% of bios.
//   SEARCHABLE         a contact exists in the index the UI queries. This is delivery.
//   STRANDED           a contact exists in Postgres and NOT in OpenSearch. This is the only outcome
//                      that is always a fault, and the only one --fix repairs. os-reconcile.js does
//                      the same repair for the whole table; this finds it for one night's work.
const { Client } = require('pg');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { makeClient, rowToDoc, bulkUpsert, INDEX } = require('./opensearch');

const argv = process.argv.slice(2);
const arg = (n, d) => { const i = argv.indexOf('--' + n); return i > -1 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : d; };
const has = (f) => argv.includes('--' + f);

const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const SINCE = arg('since', new Date().toISOString().slice(0, 10) + 'T00:00:00.000Z');
// Bounded above so a run can be aimed at ONE night. Without this, checking last night's sweep also
// scoops up tonight's objects, whose URLs are legitimately uncrawled and would read as a delivery gap.
const UNTIL = arg('until', '9999');
const ALL = has('all');
const SAMPLE = ALL ? Infinity : (Number(arg('sample', 2000)) || 2000);
const FIX = has('fix');
const BATCH = 500;
// web_source_url has no index, so every lookup against it is a sequential scan of the whole contacts
// table. Chunking that at BATCH would pay for the scan once per 500 URLs; one wide chunk pays once.
const SCAN_BATCH = 20000;
const N = (n) => Number(n || 0).toLocaleString();
const pct = (a, b) => (b ? ((a / b) * 100).toFixed(1) + '%' : '—');

const s3 = new S3Client({ region: REGION });
const readBody = async (b) => { const c = []; for await (const x of b) c.push(x); return Buffer.concat(c).toString('utf8'); };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT || !process.env.DATABASE_URL) {
    console.error('need OPENSEARCH_ENDPOINT and DATABASE_URL'); process.exit(1);
  }
  const osc = makeClient(process.env.OPENSEARCH_ENDPOINT);
  const pg = new Client({ connectionString: process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/'),
    ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false });
  await pg.connect();
  await pg.query("SET statement_timeout='280s'");

  // 1) The objects this sweep wrote. S3 objects here are write-once, so LastModified identifies the run.
  console.log(`sweep-verify — queue objects written since ${SINCE}`
    + (UNTIL === '9999' ? '' : ` and before ${UNTIL}`) + '\n');
  let tok = null; const keys = [];
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
    for (const o of (r.Contents || [])) {
      const iso = new Date(o.LastModified).toISOString();
      if (iso >= SINCE && iso < UNTIL) keys.push({ key: o.Key, iso });
    }
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);
  keys.sort((a, b) => a.iso.localeCompare(b.iso));
  if (!keys.length) { console.log('no queue objects in that window — the sweep has not written anything yet'); await pg.end(); return; }
  console.log(`  ${N(keys.length)} object(s), ${keys[0].iso} .. ${keys[keys.length - 1].iso}`);

  const urls = new Set();
  for (const k of keys) {
    try {
      const txt = await readBody((await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: k.key }))).Body);
      for (const line of txt.split('\n')) { const u = line.trim(); if (/^https?:\/\/[^\s/]+\.[^\s]/i.test(u)) urls.add(u); }
    } catch (e) { console.error(`  read failed ${k.key}: ${e.message}`); }
  }
  const all = [...urls];
  console.log(`  ${N(all.length)} distinct URL(s) discovered`);

  // Sample across the whole run, not the first N -- objects are written in sitemap order, so the head
  // of the list is one slice of the web and tells you nothing about the rest.
  let pick = all;
  if (all.length > SAMPLE) {
    pick = all.slice();
    for (let i = pick.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [pick[i], pick[j]] = [pick[j], pick[i]]; }
    pick = pick.slice(0, SAMPLE);
  }
  console.log(`  checking ${N(pick.length)}${pick.length < all.length ? ' (random sample)' : ' (all)'}\n`);

  // 2) crawl_queue, keyed by url -- this is a primary-key lookup, so it stays cheap at any sample size.
  const qrow = new Map();
  for (let i = 0; i < pick.length; i += BATCH) {
    const chunk = pick.slice(i, i + BATCH);
    const r = await pg.query(`SELECT url, status, records, no_email FROM crawl_queue WHERE url = ANY($1)`, [chunk]);
    for (const row of r.rows) qrow.set(row.url, row);
  }

  // 3) For every URL that yielded a record, pull the contact row(s) Postgres keys to that URL.
  //    web_source_url is not indexed, so this is a sequential scan -- batched deliberately wide to pay
  //    for the scan as few times as possible.
  const expected = pick.filter((u) => { const q = qrow.get(u); return q && q.status === 'done' && Number(q.records) > 0; });
  const rowsByUrl = new Map();
  for (let i = 0; i < expected.length; i += SCAN_BATCH) {
    const chunk = expected.slice(i, i + SCAN_BATCH);
    const r = await pg.query(`SELECT * FROM contacts WHERE web_source_url = ANY($1)`, [chunk]);
    for (const row of r.rows) {
      if (!rowsByUrl.has(row.web_source_url)) rowsByUrl.set(row.web_source_url, []);
      rowsByUrl.get(row.web_source_url).push(row);
    }
  }

  // 4) Ask OpenSearch BY EMAIL, because the index is email-keyed. Checking by URL instead understates
  //    delivery badly: when an extracted record merges into a contact that already exists under an
  //    older source URL, nothing in the index is keyed to the new URL and the person is nonetheless
  //    perfectly searchable. The only honest question is "is this person in the index", and the only
  //    real fault is a row Postgres has that the index does not.
  const pgRows = [].concat(...rowsByUrl.values());
  const emails = [...new Set(pgRows.map((r) => String(r.email || '').toLowerCase()).filter(Boolean))];
  const inIndex = new Set();
  for (let i = 0; i < emails.length; i += BATCH) {
    const chunk = emails.slice(i, i + BATCH);
    const r = await osc.mget({ index: INDEX, body: { ids: chunk }, _source: false });
    ((r.body || r).docs || []).forEach((d, k) => { if (d.found) inIndex.add(chunk[k]); });
  }
  const strandedRows = pgRows.filter((r) => !inIndex.has(String(r.email || '').toLowerCase()));
  // A URL that yielded a record but has no contact keyed to it: the record merged into a person who
  // already existed under a different source URL. Expected, and not a loss -- the upsert only rewrites
  // a contact when the new record scores at least as high AND differs, so a re-find of a known person
  // deliberately leaves the stored row (and its original URL) alone.
  const mergedElsewhere = expected.filter((u) => !rowsByUrl.has(u));
  const deliveredUrls = expected.filter((u) => (rowsByUrl.get(u) || []).some((r) => inIndex.has(String(r.email || '').toLowerCase())));

  const st = { fed: 0, pending: 0, done: 0, noEmail: 0, withRecs: 0 };
  for (const u of pick) {
    const q = qrow.get(u); if (!q) continue;
    st.fed++;
    if (q.status !== 'done') { st.pending++; continue; }
    st.done++;
    if (Number(q.records) > 0) st.withRecs++;
    if (Number(q.no_email) === 1) st.noEmail++;
  }

  console.log('FUNNEL  (of the sampled URLs)');
  console.log(`  discovered by the sweep      ${N(pick.length)}`);
  console.log(`  not fed / filtered out       ${N(pick.length - st.fed)}   ${pct(pick.length - st.fed, pick.length)}`);
  console.log(`  reached crawl_queue          ${N(st.fed)}   ${pct(st.fed, pick.length)}`);
  console.log(`    still queued               ${N(st.pending)}`);
  console.log(`    crawled                    ${N(st.done)}`);
  console.log(`      yielded a record         ${N(st.withRecs)}`);
  console.log(`      no email on the page     ${N(st.noEmail)}`);
  console.log('');
  console.log(`  of those ${N(expected.length)} record-yielding URL(s):`);
  console.log(`    contact keyed to this URL  ${N(rowsByUrl.size)}`);
  console.log(`      SEARCHABLE in the UI     ${N(deliveredUrls.length)}   ${pct(deliveredUrls.length, rowsByUrl.size || 1)}`);
  console.log(`      STRANDED (pg, not index) ${N(strandedRows.length)} row(s)`);
  console.log(`    merged into an existing contact under another URL  ${N(mergedElsewhere.length)}`);

  if (strandedRows.length) {
    console.log('\n  examples:');
    for (const r of strandedRows.slice(0, 8)) console.log(`    ${r.email}   ${r.web_source_url}`);
    if (FIX) {
      const res = await bulkUpsert(osc, strandedRows.map(rowToDoc));
      const errs = (res && res.errors) || 0;
      try { await osc.indices.refresh({ index: INDEX }); } catch (e) { /* best-effort */ }
      console.log(`\n  --fix: indexed ${N(strandedRows.length - errs)}${errs ? `, ${N(errs)} failed` : ''}`);
    } else {
      console.log('\n  --fix indexes these into the Contact Crawler');
    }
  }

  // Scale the sample back up, so the headline is about the night's work and not about the sample.
  if (pick.length < all.length && st.withRecs) {
    const f = all.length / pick.length;
    console.log(`\nESTIMATE for all ${N(all.length)} discovered URL(s): ~${N(Math.round(st.withRecs * f))} with a record, `
      + `~${N(Math.round(deliveredUrls.length * f))} searchable under their own URL, `
      + `~${N(Math.round(strandedRows.length * f))} stranded`);
  }
  await pg.end();
  process.exit(strandedRows.length && !FIX ? 1 : 0);
})().catch((e) => { console.error('sweep-verify FAILED:', e.message, e.stack); process.exit(2); });
