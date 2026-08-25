/**
 * sitemap-sweep.js — the nightly Sitemap Monitor sweep, as its own process on its own machine.
 *
 *   OPENSEARCH_ENDPOINT=… node sitemap-sweep.js [--conc 64] [--live-cap 25000] [--max-sitemaps N] [--no-email]
 *
 * Why this is not in the web app any more. The first nightly run went 320-way concurrent inside ui-server
 * and took the process down with a V8 heap abort (exit_code=134) 26 minutes in, twice. The UI, the live
 * crawler, the Sheet sync and the bio-ETL drain all live in that process; a four-hour memory-heavy sweep
 * has no business sharing it, and "the sweep failed" should never mean "the site went down".
 *
 * On its own machine the sweep gets its own heap, its own crash blast radius, and a restart policy of
 * 'no' so a crash is visible as a dead machine rather than an endless restart loop.
 *
 * The pass itself is uncapped by design: every monitored People sitemap is re-fetched and compared against
 * the contacts already held, whatever the extraction backlog looks like. See sitemap-lib-monitor.js.
 */
const fs = require('fs');
const path = require('path');
const sitemaps = require('./sitemaps');
const openSearch = require('./opensearch');
const ccEngine = require('./cc-engine');
const { makeLibMonitor } = require('./sitemap-lib-monitor');
const report = require('./sitemap-monitor-report');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const num = (f, d) => Number(arg(f, '')) || d;

const CONC = num('--conc', Number(process.env.SWEEP_CONC) || 64);
const LIVE_CAP = num('--live-cap', Number(process.env.SWEEP_LIVE_CAP) || 0);
const MAX_SITEMAPS = num('--max-sitemaps', 0);
const KIND = arg('--kind', 'People');
const TYPE = arg('--type', process.env.SWEEP_TYPE || '');
const NO_EMAIL = process.argv.includes('--no-email');
const DRAIN_AFTER = !/^(0|false|no|off)$/i.test(process.env.SWEEP_DRAIN_AFTER || '1');
const QUEUE_PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';

function loadNames(file) {
  try {
    const csv = fs.readFileSync(path.join(__dirname, file), 'utf8');
    return new Set(csv.split(/\r?\n/).map((s) => s.trim().toLowerCase()).filter((l, i) => l && !(i === 0 && l === 'name')));
  } catch (e) { return new Set(); }
}

// The durable side of extraction. S3 has no append, so each flush is its own object and the bio-ETL merges
// them. This is the ONLY thing the sweep does with what it finds -- it deliberately starts no live crawl
// jobs, because those hold their queue in memory and lose it on restart (0.2% conversion over 10.2M URLs),
// and because this machine exits as soon as the sweep is done.
let s3 = null, queuedObjects = 0, queueErrors = 0;
async function queueBioUrls(urls, label) {
  if (!urls || !urls.length) return;
  const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
  const region = process.env.AWS_REGION || 'us-east-1';
  if (!s3) s3 = new S3Client({ region });
  const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
  const key = `${QUEUE_PREFIX}${new Date().toISOString().replace(/[:.]/g, '-')}-${urls.length}.txt`;
  try {
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: urls.join('\n') + '\n', ContentType: 'text/plain' }));
    queuedObjects++;
    console.error(`[queue] ${urls.length} URL(s) -> s3://${bucket}/${key}${label ? ` (${label})` : ''}`);
  } catch (e) {
    // Count it. A queue write that fails silently means bios were found and then thrown away.
    queueErrors++;
    console.error(`[queue] FAILED (${urls.length} URLs): ${e.message}`);
  }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const t0 = Date.now();
  const sitemapsClient = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const contactsClient = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);

  const pending = [];
  const lm = makeLibMonitor({
    sitemaps, sitemapsClient, contactsClient, contactsIndex: openSearch.INDEX, ccEngine,
    bioSitemapNames: loadNames('Sitemap extensions.csv'),
    locationSitemapNames: loadNames('Sitemap extensions - locations.csv'),
    genderMap: {}, directoryRules: {},
    extract: (urls, label) => { pending.push(queueBioUrls(urls.slice(), label)); },
    log: (m) => console.error('[sweep] ' + m),
  });

  const v8 = require('v8');
  const limitGb = v8.getHeapStatistics().heap_size_limit / 1e9;
  console.error(`══════ SITEMAP SWEEP · conc ${CONC} · heap limit ${limitGb.toFixed(1)}GB ══════`);
  // Concurrency was tuned once on a 4-minute sample and the resulting settings aborted the app process at
  // 26 minutes. Measure the thing that actually failed, continuously, and carry the peak into the report so
  // the next tuning decision is made on evidence instead of on a short happy path.
  let peakHeap = 0, peakRss = 0;
  const heapTimer = setInterval(() => {
    const m = process.memoryUsage();
    peakHeap = Math.max(peakHeap, m.heapUsed); peakRss = Math.max(peakRss, m.rss);
    console.error(`  [mem] heap ${(m.heapUsed / 1e9).toFixed(2)}GB / ${limitGb.toFixed(1)}GB · rss ${(m.rss / 1e9).toFixed(2)}GB`);
  }, 60000);
  heapTimer.unref();
  const summary = await lm.runPass({ conc: CONC, liveCap: LIVE_CAP, maxSitemaps: MAX_SITEMAPS, kind: KIND, type: TYPE });
  await Promise.all(pending);                       // never exit with queue writes still in flight
  clearInterval(heapTimer);
  summary.peakHeapGb = Number((peakHeap / 1e9).toFixed(2));
  summary.peakRssGb = Number((peakRss / 1e9).toFixed(2));
  summary.heapLimitGb = Number(limitGb.toFixed(1));
  summary.conc = CONC;
  summary.queuedObjects = queuedObjects;
  summary.queueErrors = queueErrors;
  if (queueErrors) summary.ok = false;

  console.error(`\n══════ SWEEP ${summary.ok ? 'OK' : 'INCOMPLETE'} · ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  console.error(`  compared ${summary.scanned.toLocaleString()}/${summary.total.toLocaleString()} sitemaps`);
  console.error(`  ${summary.withGap.toLocaleString()} with new bios -> ${summary.newUrls.toLocaleString()} new URL(s) in ${queuedObjects} queue object(s)`);
  console.error(`  state writes: ${summary.stateOk.toLocaleString()} ok, ${summary.stateErrors.toLocaleString()} failed`);
  console.error(`  peak heap ${summary.peakHeapGb}GB / ${summary.heapLimitGb}GB · peak rss ${summary.peakRssGb}GB · conc ${CONC}`);

  if (!NO_EMAIL) {
    try { await report.sendSweepReport(summary, { client: contactsClient }); }
    catch (e) { console.error('[sweep] report failed:', e.message); }
  }

  // Hand the night's finds to Common Crawl immediately rather than waiting for the drain's 6-hourly tick.
  // CC resolves at ~3,872 pages/s through the Lambda fan-out and covered 77% of the last work list; the
  // live crawl manages ~14 URLs/s per shard. Every hour a newly-found URL sits in the queue is an hour of
  // the cheap, fast path going unused -- and the sweep is what knows the work exists.
  if (!DRAIN_AFTER || !summary.newUrls) {
    if (summary.newUrls && !DRAIN_AFTER) console.error('[sweep] SWEEP_DRAIN_AFTER=0 — not launching a drain');
  } else if (!process.env.FLY_API_TOKEN) {
    console.error('[sweep] FLY_API_TOKEN not set — cannot launch the drain; the next scheduled tick will pick it up');
  } else {
    try {
      const { launchFleet, reapFleet } = require('./fleet-launch');
      const region = process.env.AWS_REGION || 'us-east-1';
      const bucket = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${region}`;
      await reapFleet({ namePrefix: 'sweep-drain', log: (m) => console.error(m) });
      const started = await launchFleet({
        shards: 1, namePrefix: 'sweep-drain', memoryMb: 16384, cpus: 8,
        cmd: () => ['node', '/app/bio-etl.js', '--mode', 'urls', '--in', `s3://${bucket}/${QUEUE_PREFIX}`],
        log: (m) => console.error(m),
      });
      console.error(`[sweep] drain launched for ${summary.newUrls.toLocaleString()} new URL(s): ${started.map((m) => m.id).join(', ') || 'NONE'}`);
    } catch (e) { console.error('[sweep] could not launch the drain:', e.message, '- the scheduled tick will pick it up'); }
  }
  process.exit(summary.ok ? 0 : 1);                 // non-zero so a failed sweep is visible to the launcher
})().catch((e) => { console.error('SWEEP CRASHED', e && e.stack || e); process.exit(2); });
