/**
 * queue-bio-urls.js — put a file of bio URLs into the monitor queue, where bio-pump collects them.
 *
 *   const { queueBioUrls } = require('./queue-bio-urls');
 *   await queueBioUrls('/data/overture/overture-bio-urls.txt', { source: 'Overture', tag: 'ov' });
 *
 * WHY THIS IS SHARED AND NOT COPIED. gm-run.js carried this logic privately, and its own comment
 * records why it was written: "gm-load writes gm-bio-urls.txt and nothing ever read it. Two runs
 * produced bio URLs -- 14,084 from the 17-08 export and ~14,000 more from 24-08 -- and both sets died
 * with their machine's /tmp." overture-run.js then grew the same shape -- it extracts bio URLs, prints
 * the count and the path, and queues nothing -- so 168,154 Overture bio URLs sat on a volume for a day
 * doing nothing, for the identical reason. A producer that writes a URL file and does not hand it on is
 * the recurring bug here, so the hand-off lives in one place that every producer calls.
 *
 * WHERE THEY GO. bio-pump.js loops continuously and spawns bio-feed.js whenever the queue has objects
 * newer than its cursor; bio-feed resolves them against Common Crawl into the Postgres crawl_queue, and
 * the worker fleet claims from there. So writing the object IS the hand-off -- nothing has to be
 * scheduled or switched on afterwards, and a URL queued now is normally picked up within seconds.
 *
 * TWO ROUTES, DELIBERATELY. Pushing through the app gives the Data Importer a named receipt, so the
 * list is visible as something that arrived. The direct S3 write is the fallback, because the URLs
 * reaching the queue must not depend on the app being up: a missing receipt is a far smaller loss than
 * a missing batch.
 */
const fs = require('fs');

const QUEUE_PREFIX = () => process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';

// The endpoint caps a single request at BIO_URL_INGEST_MAX (20,000) URLs and SILENTLY DROPS the rest --
// it reports `capped` and still returns 200. Chunking by bytes alone ran to ~23,800 URLs per chunk, so
// every chunk lost its tail: 100,810 of 863,965 URLs vanished while the push reported success. BOTH
// limits have to hold, which is why this is a named function with tests rather than an inline loop.
const MAX_BODY = 1500000;     // headroom under the server's body cap
const MAX_URLS = 20000;       // the endpoint's own cap; exceeding it loses the remainder silently

function chunkByBytes(lines, maxBody = MAX_BODY, maxUrls = MAX_URLS) {
  const chunks = [];
  let cur = [], curBytes = 0;
  for (const u of lines) {
    const cost = String(u).length + 3;
    if (cur.length && (curBytes + cost > maxBody || cur.length >= maxUrls)) { chunks.push(cur); cur = []; curBytes = 0; }
    cur.push(u); curBytes += cost;
  }
  if (cur.length) chunks.push(cur);
  return chunks;
}

// Name the list after the export that produced it, when the producer left a sidecar saying so.
function sourceNameFor(file, fallback) {
  try {
    const meta = JSON.parse(fs.readFileSync(String(file).replace(/\.txt$/, '') + '.meta.json', 'utf8'));
    if (meta && Array.isArray(meta.sourceFiles) && meta.sourceFiles.length) {
      return meta.sourceFiles.length === 1 ? meta.sourceFiles[0] : `${meta.sourceFiles[0]} +${meta.sourceFiles.length - 1} more`;
    }
  } catch { /* no sidecar: the caller's name stands */ }
  return fallback;
}

async function queueViaApp(lines, sourceName, source, log) {
  const base = String(process.env.APP_URL || process.env.COMMON_CRAWLER_URL || '').replace(/\/+$/, '');
  const token = process.env.LOADER_TOKEN || '';
  if (!base || !token) return false;
  const chunks = chunkByBytes(lines);
  let sent = 0, listId = '';                 // one receipt per export; later chunks append to it
  for (const chunk of chunks) {
    const body = JSON.stringify({ urls: chunk, sourceUrl: sourceName, source, label: `${source}: ${sourceName}`, listId });
    const r = await fetch(base + '/api/bio-urls', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-loader-token': token }, body,
    }).catch((e) => ({ ok: false, status: 0, _err: e.message }));
    if (!r || !r.ok) { log(`  app push failed (${(r && (r.status || r._err)) || '?'}) — falling back to a direct queue write`); return false; }
    if (!listId) { try { listId = (await r.json()).listId || ''; } catch { /* continue without appending */ } }
    sent += chunk.length;
  }
  log(`  queued ${sent.toLocaleString()} bio URL(s) via the app, recorded as "${sourceName}"`);
  return true;
}

async function queueBioUrls(file, opts = {}) {
  const { source = 'Bio URLs', tag = 'bio', noQueue = false, log = console.error } = opts;
  if (noQueue) { log('  --no-queue: not queueing the bio URLs'); return { queued: 0, objects: 0, skipped: true }; }
  if (!file || !fs.existsSync(file)) { log(`  no bio URL file at ${file}`); return { queued: 0, objects: 0 }; }

  const lines = [...new Set(fs.readFileSync(file, 'utf8').split(/\r?\n/).map((x) => x.trim()).filter(Boolean))];
  if (!lines.length) { log('  no bio URLs to queue'); return { queued: 0, objects: 0 }; }

  const sourceName = sourceNameFor(file, opts.sourceName || source);
  if (await queueViaApp(lines, sourceName, source, log)) return { queued: lines.length, objects: 0, viaApp: true };

  const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
  const region = process.env.AWS_REGION || 'us-east-1';
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
  const s3 = new S3Client({ region });
  let objects = 0, failed = 0;
  for (let i = 0; i < lines.length; i += 3000) {
    const chunk = lines.slice(i, i + 3000);
    // The trailing count is load-bearing: it is how the queue can be measured without reading every
    // object, and bio-feed reads a -p{N}- marker from this same key for priority.
    const key = `${QUEUE_PREFIX()}${new Date().toISOString().replace(/[:.]/g, '-')}-${tag}-${chunk.length}.txt`;
    try {
      await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: chunk.join('\n') + '\n', ContentType: 'text/plain' }));
      objects++;
    } catch (e) { failed++; log(`  queue write FAILED: ${e.message}`); }
    await new Promise((r) => setTimeout(r, 5));      // distinct timestamps in the key
  }
  log(`  queued ${lines.length.toLocaleString()} bio URL(s) -> ${objects} object(s) under ${QUEUE_PREFIX()}${failed ? ` (${failed} FAILED)` : ''}`);
  return { queued: lines.length, objects, failed };
}

// May a reader stop before taking `next`? Lives here, with the writer, because the hazard is created
// by how this module writes: chunkByBytes emits many objects back to back, and S3's LastModified has
// only ONE-SECOND granularity, so a burst shares timestamps. On 2026-11-21 one hand-off wrote 57
// objects across four seconds -- 10, 20, 20 and 7 to a second.
//
// bio-feed may only advance its cursor to a timestamp strictly BELOW the earliest object it left
// behind (otherwise an unread object is skipped for good). So a pass that stops PART WAY through a
// same-second group cannot advance the cursor at all: next pass re-reads exactly those objects, finds
// their URLs already queued, does nothing, and never reaches the rest. A livelock -- and it reads as a
// run of healthy passes, because the cursor doc updates, `objects` is non-zero and nothing errors.
//
// Reachable only once BIO_FEED_MAX_URLS is small enough that one group no longer fits; the 250,000
// default swallowed every group whole, which is why lowering it to 50,000 is what exposed this.
// Finishing the group overruns the cap by at most one group and guarantees forward progress.
function truncateHere(urlCount, maxUrls, next, last) {
  if (urlCount < maxUrls) return false;
  if (last && next && next.iso === last.iso && next.prio === last.prio) return false;  // finish the group
  return true;
}

module.exports = { queueBioUrls, chunkByBytes, sourceNameFor, truncateHere, MAX_BODY, MAX_URLS };
