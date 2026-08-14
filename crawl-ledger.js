/**
 * crawl-ledger.js — remember every URL we ATTEMPTED, and what came of it.
 *
 * THE GAP THIS CLOSES: the only record of a fetch was `web_source_url` on the contact it produced. A page
 * that was fetched successfully and simply had no contact on it left no trace at all, so the next drain
 * treated it as new and fetched it again. Measured on the 2026-08-14 cycle: skip-known removed 304,831 of
 * 3,730,274 URLs (8.2%), while the previous night's fleet had actually attempted 2,563,533 of them —
 * roughly 2.1M pages queued to be re-crawled to learn exactly what they taught us the night before, about
 * 12 fleet-hours. Subtracting the previous run's miss list rescued that once, but only because the list
 * happened to still be in S3 and the crawl happened to have completed. This makes it structural.
 *
 * Outcomes are recorded at FETCH time, where they are actually known:
 *
 *   extracted  — a record came off the page. It may still be dropped downstream for having no usable
 *                email; that is a different question from whether the page was worth fetching.
 *   no-record  — fetched fine, the extractor found no person. The big silent bucket.
 *   empty      — no HTML came back (dead link, hard block, redirect to nothing).
 *   error      — the fetch threw. TRANSIENT, and deliberately retried: proxies fail, sites rate-limit,
 *                and a blanket "never again" on an error would quietly shrink the universe every run.
 *
 * The retry policy follows from that: 'error' is always retried, everything else is suppressed for
 * LEDGER_RETRY_DAYS (default 30) and then allowed again, because staff pages do change.
 *
 * SIZE: one small doc per attempted URL, keyed by a hash of the URL (URLs exceed the 512-byte _id limit).
 * At ~3.7M attempts a cycle this grows quickly, so `--prune` drops entries past a cutoff. That is a
 * deliberate operational knob, not a background process — silently discarding crawl history would
 * reintroduce the exact bug this file exists to fix.
 */
const crypto = require('crypto');

const INDEX = process.env.CRAWL_LEDGER_INDEX || 'crawl_log';
const RETRY_DAYS = Number(process.env.LEDGER_RETRY_DAYS) || 30;

const MAPPING = {
  settings: { number_of_shards: 2, number_of_replicas: 0, refresh_interval: '30s' },
  mappings: {
    properties: {
      url:          { type: 'keyword', ignore_above: 2048 },
      domain:       { type: 'keyword' },
      outcome:      { type: 'keyword' },   // extracted | no-record | empty | error
      attempts:     { type: 'integer' },
      last_attempt: { type: 'date' },
      source:       { type: 'keyword' },   // Live Crawl | Common Crawl | …
    },
  },
};

const idOf = (url) => crypto.createHash('sha1').update(String(url)).digest('hex');
const domainOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return ''; } };

async function ensureIndex(client) {
  try {
    const ex = await client.indices.exists({ index: INDEX });
    if (ex.body === true || ex === true) return;
  } catch (e) { /* fall through to create */ }
  try { await client.indices.create({ index: INDEX, body: MAPPING }); }
  catch (e) { if (!/resource_already_exists/i.test(String(e && e.message))) throw e; }
}

/**
 * Record attempts. Upserts by URL hash and increments `attempts`, so re-crawls accumulate a count rather
 * than overwriting history.
 * @param {Array<{url:string, outcome:string, source?:string}>} entries
 */
async function record(client, entries) {
  if (!Array.isArray(entries) || !entries.length) return { indexed: 0, errors: 0 };
  const at = new Date().toISOString();
  const body = [];
  for (const e of entries) {
    if (!e || !e.url) continue;
    body.push({ update: { _index: INDEX, _id: idOf(e.url) } });
    body.push({
      scripted_upsert: true,
      script: {
        lang: 'painless',
        source: 'if (ctx._source.attempts == null) { ctx._source.attempts = 0 } ctx._source.attempts += 1;'
              + ' ctx._source.url = params.url; ctx._source.domain = params.domain;'
              + ' ctx._source.outcome = params.outcome; ctx._source.last_attempt = params.at;'
              + ' ctx._source.source = params.source;',
        params: { url: e.url, domain: domainOf(e.url), outcome: e.outcome || 'unknown', at, source: e.source || '' },
      },
      upsert: {},
    });
  }
  if (!body.length) return { indexed: 0, errors: 0 };
  let errors = 0;
  try {
    const r = await client.bulk({ body });
    const b = r.body || r;
    if (b && b.items) for (const it of b.items) if (it.update && it.update.error) errors++;
  } catch (e) { errors += body.length / 2; }
  return { indexed: body.length / 2, errors };
}

/**
 * Which of these URLs should be crawled? Returns a Set of the ones to SKIP.
 * A lookup failure returns an empty set — an unfiltered URL costs one fetch, a wrongly-skipped one is
 * lost data, and this filter must never be the reason a page is missed.
 */
async function skipSet(client, urls, opts = {}) {
  const skip = new Set();
  const list = [...new Set((urls || []).filter(Boolean))];
  if (!list.length) return skip;
  const retryDays = Number(opts.retryDays) || RETRY_DAYS;
  const cutoff = Date.now() - retryDays * 86400000;
  const byId = new Map();
  for (const u of list) byId.set(idOf(u), u);
  const ids = [...byId.keys()];
  for (let i = 0; i < ids.length; i += 1000) {
    const chunk = ids.slice(i, i + 1000);
    try {
      const r = await client.mget({ index: INDEX, body: { ids: chunk }, _source: ['outcome', 'last_attempt'] });
      for (const d of ((r.body || r).docs || [])) {
        if (!d || !d.found || !d._source) continue;
        const o = d._source.outcome;
        if (o === 'error') continue;                                  // transient — always retry
        const when = Date.parse(d._source.last_attempt || '') || 0;
        if (when && when < cutoff) continue;                          // stale enough to be worth re-checking
        const u = byId.get(d._id);
        if (u) skip.add(u);
      }
    } catch (e) { /* never skip on a lookup failure */ }
  }
  return skip;
}

module.exports = { INDEX, MAPPING, ensureIndex, record, skipSet, idOf, RETRY_DAYS };

// ---- CLI: stats and pruning ----
if (require.main === module) {
  const os = require('./opensearch');
  const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
  const N = (n) => Number(n || 0).toLocaleString();
  (async () => {
    const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
    await ensureIndex(client);
    // Seed from a list of URLs already crawled in an earlier run. Without this the ledger starts blind and
    // spends a full cycle re-learning what past runs already established — the miss lists from previous
    // drains ARE the attempted lists, and they are still in S3, so the history is recoverable exactly once.
    // Only seed lists whose crawl COMPLETED: a shard that died mid-list never attempted its tail, and
    // seeding it would permanently suppress URLs that were never actually tried.
    if (process.argv.includes('--seed')) {
      const src = arg('in', '');
      const outcome = arg('outcome', 'no-record');
      if (!src) { console.error('need --in <path|s3://…> with --seed'); process.exit(1); }
      const readline = require('readline');
      let stream;
      if (/^s3:\/\//i.test(src)) {
        const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
        const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(src);
        stream = (await new S3Client({ region: process.env.AWS_REGION || 'us-east-1' })
          .send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }))).Body;
      } else { stream = require('fs').createReadStream(src); }
      console.log(`seeding ledger from ${src} as outcome="${outcome}" …`);
      let buf = [], n = 0, errs = 0;
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      for await (const line of rl) {
        const t = line.trim(); if (!t) continue;
        let u = t; if (t.startsWith('{')) { try { u = JSON.parse(t).url || ''; } catch (e) { u = ''; } }
        if (!u) continue;
        buf.push({ url: u, outcome, source: 'seed' });
        if (buf.length >= 5000) { const r = await record(client, buf); n += r.indexed; errs += r.errors; buf = [];
          if (n % 250000 < 5000) console.log(`  ${N(n)} recorded`); }
      }
      if (buf.length) { const r = await record(client, buf); n += r.indexed; errs += r.errors; }
      console.log(`seeded ${N(n)} URL(s)${errs ? `, ${N(errs)} error(s)` : ''}`);
      return;
    }
    if (process.argv.includes('--prune')) {
      const days = Number(arg('older-than', '90')) || 90;
      const cutoff = new Date(Date.now() - days * 86400000).toISOString();
      console.log(`pruning ledger entries last attempted before ${cutoff} …`);
      const r = await client.deleteByQuery({ index: INDEX, refresh: false, conflicts: 'proceed', slices: 'auto',
        body: { query: { range: { last_attempt: { lt: cutoff } } } } });
      console.log(`deleted ${N(((r.body || r).deleted))}`);
      return;
    }
    const total = (await client.count({ index: INDEX, body: { query: { match_all: {} } } })).body.count;
    const agg = await client.search({ index: INDEX, body: { size: 0, aggs: {
      byOutcome: { terms: { field: 'outcome', size: 10 } },
      oldest: { min: { field: 'last_attempt' } }, newest: { max: { field: 'last_attempt' } } } } });
    const a = (agg.body || agg).aggregations;
    console.log(`crawl ledger: ${N(total)} URL(s)`);
    console.log(`  first attempt ${a.oldest.value_as_string || '—'}`);
    console.log(`  last  attempt ${a.newest.value_as_string || '—'}`);
    for (const b of (a.byOutcome.buckets || [])) console.log(`  ${String(b.key).padEnd(12)} ${N(b.doc_count).padStart(12)}`);
    console.log(`\n  retry policy: 'error' always; others after ${RETRY_DAYS} day(s)`);
  })().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
}
