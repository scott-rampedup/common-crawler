#!/usr/bin/env node
//
// queued-rescue.js — re-queue the bio URLs that were discovered and then never crawled.
// ---------------------------------------------------------------------------------------------------
// WHY THIS EXISTS. The sitemap sweep writes a crawl_log entry with outcome 'queued' for every URL it
// hands to the monitor queue, and that entry only becomes a real outcome -- extracted, no-record, empty
// -- when something actually fetches the page. A URL whose queue object was consumed while the feed was
// failing, or dropped for a reason that has since changed, keeps that 'queued' forever: discovered,
// counted in the sweep's "outstanding" line every night, and never fetched by anyone.
//
// On 2026-11-16 that line read 253,400. It is NOT 253,400 lost pages, and the distinction matters
// enough to measure rather than assume -- a first sample said 100% were on blocked domains, but it was
// a plain size:5000 search, which returns documents in index order and had landed entirely inside one
// day. A random_score sample of the same population split very differently:
//
//   56.6%  on a blocked domain        — correctly never crawled, and re-queueing them achieves nothing
//   41.1%  already in crawl_queue     — already handled; only the ledger entry was never updated
//    6.2%  genuinely uncrawled        — ~15,660 URLs, on real bio hosts: hospital.uillinois.edu,
//                                       med.umn.edu, uhhospitals.org, cliffordchance.com
//
// So this filters before it writes. Handing all 253,400 to bio-feed would work -- it applies the same
// domain gate and skip-known itself -- but it would pay for a resolve that is 94% waste, and an
// oversized resolve is exactly what throttles s3://commoncrawl and stalls the night.
//
// The output is ordinary monitor-queue objects, so nothing downstream needs to know this ran: bio-feed
// picks them up on its next pass, resolves them, and the misses go to the live fleet as usual.
//
//   node queued-rescue.js                 # measure and report, write nothing
//   node queued-rescue.js --write
//   node queued-rescue.js --write --max 50000
const { Client } = require('pg');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { makeClient } = require('./opensearch');

const argv = process.argv.slice(2);
const arg = (n, d) => { const i = argv.indexOf('--' + n); return i > -1 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : d; };
const has = (f) => argv.includes('--' + f);

const WRITE = has('write');
const MAX = Number(arg('max', 0)) || 0;
const PER_OBJECT = Number(arg('per-object', 20000)) || 20000;
const LEDGER = process.env.CRAWL_LEDGER_INDEX || 'crawl_log';
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const N = (n) => Number(n || 0).toLocaleString();
const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./i, '').toLowerCase(); } catch (e) { return ''; } };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT || !process.env.DATABASE_URL) {
    console.error('need OPENSEARCH_ENDPOINT and DATABASE_URL'); process.exit(1);
  }
  const osc = makeClient(process.env.OPENSEARCH_ENDPOINT);
  const pg = new Client({ connectionString: process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/'),
    ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false });
  await pg.connect();
  await pg.query("SET statement_timeout='280s'");

  const blocked = await require('./domain-gate').loadSet(osc);
  const cRes = await osc.count({ index: LEDGER, body: { query: { term: { outcome: 'queued' } } } });
  const total = (cRes.body || cRes).count;
  console.log(`${WRITE ? 'WRITE' : 'DRY RUN'} — ${N(total)} ledger entr(y/ies) stuck at outcome=queued`);
  console.log(`  domain gate holds ${N(blocked.size)} blocked domain(s)\n`);

  // Scroll the whole population. search_after on _doc would be lighter, but a scroll is the one form
  // that is stable while the sweep is writing new 'queued' entries underneath us.
  const st = { seen: 0, blocked: 0, inQueue: 0, actionable: 0, written: 0, objects: 0 };
  let batch = [];
  const pending = [];

  const flushToS3 = async (urls) => {
    if (!urls.length) return;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const key = `${PREFIX}${stamp}-rescue-${urls.length}.txt`;
    if (WRITE) {
      const s3 = new S3Client({ region: REGION });
      await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: Buffer.from(urls.join('\n') + '\n', 'utf8') }));
    }
    st.objects++; st.written += urls.length;
    console.log(`  ${WRITE ? 'wrote' : 'would write'} ${N(urls.length)} URL(s) -> s3://${BUCKET}/${key}`);
  };

  // Keep only what is worth paying an Athena resolve for: not on a blocked domain, and not already a
  // crawl_queue row. crawl_queue is keyed by url, so the membership test is a primary-key lookup.
  const sift = async (urls) => {
    const keep = [];
    const notBlocked = [];
    for (const u of urls) {
      st.seen++;
      if (blocked.has(hostOf(u))) { st.blocked++; continue; }
      notBlocked.push(u);
    }
    for (let i = 0; i < notBlocked.length; i += 500) {
      const chunk = notBlocked.slice(i, i + 500);
      const r = await pg.query('SELECT url FROM crawl_queue WHERE url = ANY($1)', [chunk]);
      const have = new Set(r.rows.map((x) => x.url));
      for (const u of chunk) { if (have.has(u)) st.inQueue++; else keep.push(u); }
    }
    st.actionable += keep.length;
    return keep;
  };

  let res = await osc.search({ index: LEDGER, scroll: '5m', body: {
    size: 2000, _source: ['url'], query: { term: { outcome: 'queued' } } } });
  for (;;) {
    const body = res.body || res;
    const hits = (body.hits || {}).hits || [];
    if (!hits.length) break;
    batch.push(...hits.map((h) => h._source.url).filter(Boolean));
    if (batch.length >= 10000) {
      pending.push(...await sift(batch)); batch = [];
      while (pending.length >= PER_OBJECT) await flushToS3(pending.splice(0, PER_OBJECT));
      if (st.seen % 50000 < 10000) console.log(`  ${N(st.seen)} / ${N(total)} scanned · ${N(st.actionable)} actionable`);
      if (MAX && st.actionable >= MAX) break;
    }
    res = await osc.scroll({ scroll_id: body._scroll_id, scroll: '5m' });
  }
  if (batch.length) pending.push(...await sift(batch));
  while (pending.length) await flushToS3(pending.splice(0, PER_OBJECT));

  console.log(`\nscanned                 ${N(st.seen)}`);
  console.log(`  on a blocked domain   ${N(st.blocked)}`);
  console.log(`  already in crawl_queue ${N(st.inQueue)}`);
  console.log(`  ACTIONABLE            ${N(st.actionable)}`);
  console.log(`  ${WRITE ? 'queued for the feed' : 'would queue'}   ${N(st.written)} URL(s) in ${N(st.objects)} object(s)`);
  if (!WRITE) console.log('\n(dry run — pass --write to put them in the queue)');
  else console.log('\nbio-pump picks these up on its next pass; nothing else to do.');
  await pg.end();
})().catch((e) => { console.error('queued-rescue FAILED:', e.message, e.stack); process.exit(1); });
