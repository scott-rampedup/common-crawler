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
//   node sweep-verify.js --include-all        # count every producer's queue objects, not just the sweep's
//
// Also a module: `require('./sweep-verify').verify({ sample, fix, log })` runs the same walk and
// returns the funnel instead of printing it, which is how the QA manager's sweep_delivered check asks
// the question hourly. The CLI below is a thin wrapper over it, so both paths measure the same way --
// a separate reimplementation inside the QA manager would have been one more thing to keep honest.
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
const BATCH = 500;
// web_source_url has no index, so every lookup against it is a sequential scan of the whole contacts
// table. Chunking that at BATCH would pay for the scan once per 500 URLs; one wide chunk pays once.
const SCAN_BATCH = 20000;
const N = (n) => Number(n || 0).toLocaleString();
const pct = (a, b) => (b ? ((a / b) * 100).toFixed(1) + '%' : '—');

// The sweep is not the only writer in this prefix. queued-rescue puts objects here too, and they are
// written whenever the QA manager repairs the abandoned pool -- which is to say, at an arbitrary hour,
// into the same day's window. Counting those as "the sweep" wrecks the measurement twice: their URLs
// are mixed into the funnel, and because the newest object is then minutes old rather than hours, the
// settle test that waits for the sweep to finish resets and the check skips forever. (That is not
// hypothetical -- it is what the first live run of sweep_delivered did, 20 minutes after the rescue
// fix ran.) A sweep object is <stamp>-<count>.txt; every other producer tags itself in between.
const SWEEP_KEY = /Z-\d+\.txt$/i;

const s3 = new S3Client({ region: REGION });
const readBody = async (b) => { const c = []; for await (const x of b) c.push(x); return Buffer.concat(c).toString('utf8'); };

/**
 * Walk one sweep's URLs through to the index. Returns the funnel; printing is the caller's business,
 * so the QA manager can have the numbers without the report.
 *
 * @param opts.since/.until  ISO bounds on queue-object LastModified (one night)
 * @param opts.sample        how many discovered URLs to check (Infinity for all)
 * @param opts.fix           index any row Postgres has and the index does not
 * @param opts.log           where the human-readable report goes; pass () => {} for silence
 */
async function verify(opts = {}) {
  const SINCE = opts.since || new Date().toISOString().slice(0, 10) + 'T00:00:00.000Z';
  // Bounded above so a run can be aimed at ONE night. Without this, checking last night's sweep also
  // scoops up tonight's objects, whose URLs are legitimately uncrawled and would read as a delivery gap.
  const UNTIL = opts.until || '9999';
  const ALL = !!opts.all;
  const SAMPLE = ALL ? Infinity : (Number(opts.sample) || 2000);
  const FIX = !!opts.fix;
  const log = opts.log || console.log;
  // Default to the sweep's own objects, because that is the question this file asks. --include-all
  // widens it back to every producer in the prefix.
  const INCLUDE_ALL = !!opts.includeAll;

  if (!process.env.OPENSEARCH_ENDPOINT || !process.env.DATABASE_URL) {
    throw new Error('need OPENSEARCH_ENDPOINT and DATABASE_URL');
  }
  const osc = makeClient(process.env.OPENSEARCH_ENDPOINT);
  const pg = new Client({ connectionString: process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/'),
    ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false });
  await pg.connect();
  try {
    await pg.query("SET statement_timeout='280s'");

    // 1) The objects this sweep wrote. S3 objects here are write-once, so LastModified identifies the run.
    log(`sweep-verify — queue objects written since ${SINCE}`
      + (UNTIL === '9999' ? '' : ` and before ${UNTIL}`) + '\n');
    let tok = null; const keys = []; let skippedKeys = 0;
    do {
      const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
      for (const o of (r.Contents || [])) {
        const iso = new Date(o.LastModified).toISOString();
        if (iso < SINCE || iso >= UNTIL) continue;
        if (!INCLUDE_ALL && !SWEEP_KEY.test(o.Key)) { skippedKeys++; continue; }
        keys.push({ key: o.Key, iso });
      }
      tok = r.IsTruncated ? r.NextContinuationToken : null;
    } while (tok);
    keys.sort((a, b) => a.iso.localeCompare(b.iso));
    if (!keys.length) {
      log('no queue objects in that window — the sweep has not written anything yet');
      return { objects: 0, discovered: 0, checked: 0, fed: 0, pending: 0, done: 0, withRecs: 0,
               noEmail: 0, keyed: 0, searchable: 0, stranded: 0, mergedElsewhere: 0,
               first: null, last: null, fixed: 0, strandedExamples: [] };
    }
    log(`  ${N(keys.length)} sweep object(s), ${keys[0].iso} .. ${keys[keys.length - 1].iso}`
      + (skippedKeys ? `  (ignored ${N(skippedKeys)} object(s) from other producers)` : ''));

    const urls = new Set();
    for (const k of keys) {
      try {
        const txt = await readBody((await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: k.key }))).Body);
        for (const line of txt.split('\n')) { const u = line.trim(); if (/^https?:\/\/[^\s/]+\.[^\s]/i.test(u)) urls.add(u); }
      } catch (e) { log(`  read failed ${k.key}: ${e.message}`); }
    }
    const all = [...urls];
    log(`  ${N(all.length)} distinct URL(s) discovered`);

    // Sample across the whole run, not the first N -- objects are written in sitemap order, so the head
    // of the list is one slice of the web and tells you nothing about the rest.
    let pick = all;
    if (all.length > SAMPLE) {
      pick = all.slice();
      for (let i = pick.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [pick[i], pick[j]] = [pick[j], pick[i]]; }
      pick = pick.slice(0, SAMPLE);
    }
    log(`  checking ${N(pick.length)}${pick.length < all.length ? ' (random sample)' : ' (all)'}\n`);

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

    log('FUNNEL  (of the sampled URLs)');
    log(`  discovered by the sweep      ${N(pick.length)}`);
    log(`  not fed / filtered out       ${N(pick.length - st.fed)}   ${pct(pick.length - st.fed, pick.length)}`);
    log(`  reached crawl_queue          ${N(st.fed)}   ${pct(st.fed, pick.length)}`);
    log(`    still queued               ${N(st.pending)}`);
    log(`    crawled                    ${N(st.done)}`);
    log(`      yielded a record         ${N(st.withRecs)}`);
    log(`      no email on the page     ${N(st.noEmail)}`);
    log('');
    log(`  of those ${N(expected.length)} record-yielding URL(s):`);
    log(`    contact keyed to this URL  ${N(rowsByUrl.size)}`);
    log(`      SEARCHABLE in the UI     ${N(deliveredUrls.length)}   ${pct(deliveredUrls.length, rowsByUrl.size || 1)}`);
    log(`      STRANDED (pg, not index) ${N(strandedRows.length)} row(s)`);
    log(`    merged into an existing contact under another URL  ${N(mergedElsewhere.length)}`);

    let fixed = 0;
    if (strandedRows.length) {
      log('\n  examples:');
      for (const r of strandedRows.slice(0, 8)) log(`    ${r.email}   ${r.web_source_url}`);
      if (FIX) {
        const res = await bulkUpsert(osc, strandedRows.map(rowToDoc));
        const errs = (res && res.errors) || 0;
        try { await osc.indices.refresh({ index: INDEX }); } catch (e) { /* best-effort */ }
        fixed = strandedRows.length - errs;
        log(`\n  --fix: indexed ${N(fixed)}${errs ? `, ${N(errs)} failed` : ''}`);
      } else {
        log('\n  --fix indexes these into the Contact Crawler');
      }
    }

    // Scale the sample back up, so the headline is about the night's work and not about the sample.
    const factor = pick.length < all.length && pick.length ? all.length / pick.length : 1;
    if (pick.length < all.length && st.withRecs) {
      log(`\nESTIMATE for all ${N(all.length)} discovered URL(s): ~${N(Math.round(st.withRecs * factor))} with a record, `
        + `~${N(Math.round(deliveredUrls.length * factor))} searchable under their own URL, `
        + `~${N(Math.round(strandedRows.length * factor))} stranded`);
    }

    return {
      objects: keys.length, first: keys[0].iso, last: keys[keys.length - 1].iso,
      discovered: all.length, checked: pick.length,
      fed: st.fed, pending: st.pending, done: st.done, withRecs: st.withRecs, noEmail: st.noEmail,
      keyed: rowsByUrl.size, searchable: deliveredUrls.length, stranded: strandedRows.length,
      mergedElsewhere: mergedElsewhere.length, fixed, factor,
      strandedExamples: strandedRows.slice(0, 5).map((r) => `${r.email} (${r.web_source_url})`),
    };
  } finally {
    await pg.end();
  }
}

module.exports = { verify };

if (require.main === module) {
  const ALL = has('all');
  verify({
    since: arg('since', undefined),
    until: arg('until', undefined),
    all: ALL,
    sample: ALL ? Infinity : (Number(arg('sample', 2000)) || 2000),
    fix: has('fix'),
    includeAll: has('include-all'),
  }).then((r) => {
    process.exit(r.stranded && !has('fix') ? 1 : 0);
  }).catch((e) => { console.error('sweep-verify FAILED:', e.message, e.stack); process.exit(2); });
}
