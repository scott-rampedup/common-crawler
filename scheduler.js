/**
 * scheduler.js — the one process that decides WHEN work runs. Nothing else schedules.
 * -----------------------------------------------------------------------------------
 * Step 2 of the rebuild. ui-server.js is 3,200 lines that serve HTTP *and* host six schedulers; it has
 * aborted twice with a V8 heap error while running the jobs it hosts, and searches time out when a heavy
 * job is in flight. This process takes the jobs. The web app keeps only its read paths.
 *
 * Two rules, both of them corrections to bugs this repo has already paid for:
 *
 *   1. NO BOOT-ANCHORED TIMERS. Due-ness is computed from a PERSISTED last-run time, re-checked on a
 *      short tick. `setInterval(job, 6h)` restarts from zero on every deploy, and this app deploys often
 *      -- so a 6h job effectively never reached 6h, and a `setTimeout` to the next 06:00 that restarted
 *      at 06:26 silently armed for TOMORROW and lost the day. ui-server.js documents that trap in a
 *      comment and then falls into it again two blocks later for the firmographic sweep. Measured: the
 *      firmo sweep ran 21 minutes after a deploy, not on any 6-hour boundary.
 *
 *   2. STAMP last_run ONLY WHEN THE JOB ACTUALLY RAN. Stamping on *attempt* records a refusal as a
 *      success and parks the job for another full interval. That exact bug left bio_etl_drain_state
 *      reading a fresh timestamp while nothing had run.
 *
 *   node scheduler.js              # run forever
 *   node scheduler.js --once       # evaluate every job once and exit (what CI / a smoke test wants)
 *   node scheduler.js --dry        # report what is due, run nothing
 *   node scheduler.js --only firmo # just one job
 *
 * Env: OPENSEARCH_ENDPOINT (required). Per-job knobs documented on each JOBS entry.
 *      SCHED_TICK_MS       how often due-ness is re-evaluated (default 300000 = 5 min)
 *      CC_CONFIG_INDEX     where last-run state lives (default cc_config)
 */
const os = require('./opensearch');
const companies = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);

const ONCE = has('once');
const DRY = has('dry');
const ONLY = arg('only', '');
const TICK_MS = Number(process.env.SCHED_TICK_MS) || 300000;
const CFG = process.env.CC_CONFIG_INDEX || 'cc_config';

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('scheduler: OPENSEARCH_ENDPOINT required'); process.exit(1); }

const hours = (n) => n * 3600 * 1000;
const nowIso = () => new Date().toISOString();

// ---------------------------------------------------------------- the job registry
//
// Each job owns: a stable id (its state doc), how often it is due, and a run() that RESOLVES TRUTHFULLY --
// returning false means "did not run", which leaves last_run alone so the next tick retries.
const JOBS = [
  {
    id: 'firmo_sweep',
    label: 'firmographic enrichment',
    everyMs: hours(Math.max(1, Number(process.env.FIRMO_SWEEP_HOURS) || 6)),
    // Scheduler-scoped flag, NOT the app's FIRMO_SWEEP. FIRMO_SWEEP=0 is what silences the copy inside
    // ui-server; if this job read the same variable, handing ownership over would disable the work in
    // both places at once and the sweep would just stop -- the precise failure this refactor exists to
    // end. Ownership moves, so the switch moves with it.
    enabled: () => process.env.SCHED_FIRMO !== '0',
    // Enriches contacts that have a domain we hold company data for. Partial-doc update
    // ({update, doc}), so it is additive and cannot blank a field it does not set -- unlike the
    // full-document upsert that once wiped firmographics off 28,028 contacts.
    async run(ctx) {
      const limit = Number(process.env.FIRMO_SWEEP_MAX) || 300000;
      const r = await require('./enrich-firmographics').enrichMissing({
        client: ctx.contacts, coClient: ctx.companies, endpoint: process.env.OPENSEARCH_ENDPOINT,
        limit, newestFirst: true,
      });
      ctx.log(`enriched ${(r.updated || 0).toLocaleString()} contact(s) (scanned ${(r.scanned || 0).toLocaleString()})`);
      return { updated: r.updated || 0, scanned: r.scanned || 0 };
    },
  },
];

// ---------------------------------------------------------------- launching a job onto its own machine
//
// The heavy jobs do not run HERE either. This process decides when; the work goes to a machine that is
// created for it and destroyed after. Returning false when nothing started matters: the caller leaves
// last_run alone, so a failed launch retries on the next tick instead of parking the job for its whole
// interval.
async function launchOwnMachine({ namePrefix, cmd, env, memoryMb, cpus, log }) {
  if (!process.env.FLY_API_TOKEN) { log('FLY_API_TOKEN not set — cannot launch a machine'); return false; }
  const { launchFleet, reapFleet } = require('./fleet-launch');
  await reapFleet({ namePrefix, log: () => {} });              // free the names from the previous run
  const started = await launchFleet({
    shards: 1, namePrefix, memoryMb, cpus, env, cmd: () => cmd, log: () => {},
  });
  if (!started.length) { log('no machine started'); return false; }
  log(`launched on ${started.map((m) => `${m.id} (${m.name})`).join(', ')}`);
  return { machines: started.map((m) => m.id) };
}

const OUT_BUCKET = process.env.OUT_BUCKET
  || `aws-athena-query-results-475987770186-${process.env.AWS_REGION || 'us-east-1'}`;

// The four jobs below are REGISTERED but default to disabled, exactly as they are in the web app today
// (BIO_ETL_DRAIN / BACKLOG_COUNT / SITEMAP_LIB_MONITOR / FLEET_HEALTH are all '0'). Registering them here
// changes no behaviour -- it gives them an owner, so that whenever one is switched on it starts in this
// process instead of inside the one serving the UI. Each has its own SCHED_* flag for the same reason
// firmo does: the app-side variable silences the app's copy, not the scheduler's.
JOBS.push(
  {
    id: 'backlog_count',
    label: 'true bio-URL backlog recount',
    everyMs: hours(Math.max(1, Number(process.env.BACKLOG_COUNT_HOURS) || 1)),
    enabled: () => process.env.SCHED_BACKLOG === '1',
    // This is the number the Data Importer shows. It has been stale since 2026-08-27 because the job that
    // computes it was switched off -- see the audit. It reads ~13,000 S3 objects and have-checks every URL,
    // which is why it needs its own 16GB box and must never run in the web process (it aborted the app
    // with exit_code=134 twice).
    run(ctx) {
      return launchOwnMachine({
        namePrefix: 'backlog-count', memoryMb: 16384, cpus: 4,
        env: { NODE_OPTIONS: '--max-old-space-size=12288' },
        cmd: ['node', '/app/bio-backlog-count.js', '--conc', '24', '--quiet'],
        log: ctx.log,
      });
    },
  },
  {
    id: 'bio_feed',
    label: 'feed new bio URLs into the work queue',
    // MINUTES, not hours. bio-feed reads only the queue objects written since its cursor, so a pass
    // costs seconds rather than the hour bio-etl spent re-scanning all 13,297 objects to find what was
    // new. That scan was the only reason for a 6-hour interval -- with it gone there is nothing to
    // amortise, and a URL sent from the browser is resolved and claimable within one tick.
    everyMs: 60 * 1000 * Math.max(5, Number(process.env.BIO_FEED_MINUTES) || 15),
    enabled: () => process.env.SCHED_BIO_FEED !== '0',
    run(ctx) {
      return launchOwnMachine({
        namePrefix: 'bio-feed', memoryMb: 8192, cpus: 4,
        env: { CRAWLS_N: String(Number(process.env.CRAWLS_N) || 4) },
        cmd: ['node', '/app/bio-feed.js'],
        log: ctx.log,
      });
    },
  },
  {
    id: 'cc_refresh',
    label: 'monthly Common Crawl refresh of company records (+ bio URLs on the same cadence)',
    // Monthly, because that is roughly the Common Crawl release cadence — refreshing more often re-reads
    // the same corpus and pays the Athena scan again for identical pointers. CC_REFRESH_DAYS controls
    // which records are considered due; this interval controls how often we go looking.
    everyMs: hours(24 * Math.max(1, Number(process.env.CC_REFRESH_EVERY_DAYS) || 30)),
    // OFF until the first full run has been watched end to end. cc_crawl is currently set on 14 of
    // 30.7M records, so the first pass is the one that matters and it should not start by itself at
    // whatever hour the scheduler happens to come due.
    enabled: () => process.env.SCHED_CC_REFRESH === '1',
    run(ctx) {
      return launchOwnMachine({
        namePrefix: 'cc-refresh', memoryMb: 16384, cpus: 8,
        env: {
          CC_REFRESH_MAX: String(Number(process.env.CC_REFRESH_MAX) || 100000),
          CC_REFRESH_DAYS: String(Number(process.env.CC_REFRESH_DAYS) || 30),
          CC_REFRESH_CONC: String(Number(process.env.CC_REFRESH_CONC) || 8),
        },
        cmd: ['node', '/app/cc-monthly-refresh.js'],
        log: ctx.log,
      });
    },
  },
  {
    id: 'bio_drain',
    label: 'LEGACY full-queue drain (superseded by bio_feed)',
    everyMs: hours(Math.max(1, Number(process.env.BIO_ETL_DRAIN_HOURS) || 6)),
    // Off by default now. bio_feed + the continuous worker fleet do this incrementally; running the
    // old whole-queue drain alongside them would re-scan everything and duplicate the resolve spend.
    // Kept registered because it is still the way to force a full re-sweep after a ledger change.
    enabled: () => process.env.SCHED_BIO_DRAIN === '1',
    // NOTE: deliberately WITHOUT --drain. That flag deletes the consumed queue objects, the IAM user has no
    // s3:DeleteObject, and the refusal path then marks objects consumed in a ledger that never expires --
    // which is what currently locks ~6.8M URLs behind 13,174 retired objects. Until the permission and the
    // ledger expiry are sorted, the per-URL crawl_log is the only retirement we want.
    run(ctx) {
      const args = ['--mode', 'urls', '--in', `s3://${OUT_BUCKET}/monitor-queue/pending/`];
      if (!/^(0|false|no|off)$/i.test(process.env.BIO_ETL_LIVE || '1')) args.push('--live');
      return launchOwnMachine({
        namePrefix: 'bio-etl-run', memoryMb: Number(process.env.BIO_ETL_MEMORY_MB) || 16384,
        cpus: Number(process.env.BIO_ETL_CPUS) || 8,
        env: { FILTER_CONC: String(Number(process.env.FILTER_CONC) || 24),
               FLEET_SHARDS: String(Number(process.env.FLEET_SHARDS) || 24) },
        cmd: ['node', '/app/bio-etl.js', ...args],
        log: ctx.log,
      });
    },
  },
  {
    id: 'bio_live',
    label: 'live-crawl the bio URLs Common Crawl lacks',
    // Half of every bio-feed batch resolves to a WARC pointer and flows to the continuous workers; the
    // other half exists nowhere in the archive and can only be fetched live. bio-feed writes those to
    // bio-miss/ and stops, because worker.js fetches BY POINTER and cannot take a pointerless row. Under
    // the old batch drain bio-etl handed that remainder to a fleet at the end of each run; the move to a
    // continuous feed left the hand-off behind and 7,982 URLs quietly piled up. This is that hand-off.
    everyMs: 60 * 1000 * Math.max(10, Number(process.env.BIO_LIVE_MINUTES) || 60),
    enabled: () => process.env.SCHED_BIO_LIVE !== '0',
    // bio-live launches its own fleet and manages its own cursor, so it reports false when it decided
    // not to run (backlog too small) -- which leaves last_run alone and lets the next tick reconsider.
    run(ctx) {
      return launchOwnMachine({
        namePrefix: 'bio-live', memoryMb: 4096, cpus: 2,
        env: { BIO_LIVE_MIN_URLS: String(Number(process.env.BIO_LIVE_MIN_URLS) || 2000),
               BIO_LIVE_SHARDS: String(Number(process.env.BIO_LIVE_SHARDS) || 12) },
        cmd: ['node', '/app/bio-live.js'],
        log: ctx.log,
      });
    },
  },
  {
    id: 'sitemap_sweep',
    label: 'full sweep of every monitored People sitemap',
    everyMs: hours(24),
    enabled: () => process.env.SCHED_SITEMAP === '1',
    // Once per UTC day, inside an off-peak window. The window is enforced by returning false rather than
    // by sleeping: false leaves last_run untouched, so the next tick inside the window picks it up. The
    // original used setTimeout to the next 06:00 and silently skipped a whole day whenever the process
    // restarted after that hour.
    run(ctx) {
      const start = Number(process.env.SITEMAP_SWEEP_UTC_START || 6);
      const end = Number(process.env.SITEMAP_SWEEP_UTC_END || 12);
      const h = new Date().getUTCHours();
      if (h < start || h >= end) { ctx.log(`outside the ${start}-${end}h UTC window (now ${h}h) — not starting`); return false; }
      return launchOwnMachine({
        namePrefix: 'sitemap-sweep',
        memoryMb: Number(process.env.SITEMAP_SWEEP_MEMORY_MB) || 16384,
        cpus: Number(process.env.SITEMAP_SWEEP_CPUS) || 8,
        env: { SWEEP_CONC: String(Number(process.env.SITEMAP_LIB_MONITOR_CONC) || 64),
               CC_MAX_SOCKETS: String(Number(process.env.CC_MAX_SOCKETS) || 1024) },
        cmd: ['node', '/app/sitemap-sweep.js'],
        log: ctx.log,
      });
    },
  },
  {
    id: 'fleet_health',
    label: 'report fleet machine health',
    everyMs: 60 * 1000 * Math.max(2, Number(process.env.FLEET_HEALTH_MINUTES) || 10),
    enabled: () => process.env.SCHED_FLEET_HEALTH === '1',
    // Read-only: asks Fly for machine states and logs failures. Light enough to run in-process here.
    async run(ctx) {
      if (!process.env.FLY_API_TOKEN) { ctx.log('FLY_API_TOKEN not set'); return false; }
      const { fleetStatus } = require('./fleet-launch');
      const out = [];
      for (const prefix of ['live-fleet', 'cq-worker', 'cq-w2', 'cq-w3', 'sitemap-sweep', 'bio-etl-run', 'backlog-count']) {
        const st = await fleetStatus({ namePrefix: prefix });
        if (!st.ok || !st.total) continue;
        const line = `${prefix}: ${st.running} running, ${st.done} finished, ${st.failed.length} FAILED`;
        if (st.failed.length) {
          ctx.log(`${line} — ${st.failed.map((f) => `${f.name}(exit ${f.exitCode}${f.oom ? ', oom' : ''})`).join(', ')}`);
        } else { ctx.log(line); }
        out.push(line);
      }
      if (!out.length) ctx.log('no fleets present');
      return { fleets: out.length };
    },
  },
);

// ---------------------------------------------------------------- persisted due-ness
async function lastRun(client, id) {
  try {
    const g = await client.get({ index: CFG, id: 'sched_' + id });
    return Date.parse(((g.body || g)._source || {}).last_run || '') || 0;
  } catch (e) {
    const code = e && (e.statusCode || (e.meta && e.meta.statusCode));
    if (code === 404 || /404|not_found|index_not_found/i.test(String(e && e.message))) return 0;
    throw e;                                    // a real failure must not read as "never run"
  }
}

async function stampRun(client, id, result) {
  await client.index({
    index: CFG, id: 'sched_' + id, refresh: true,
    body: { last_run: nowIso(), by: process.env.FLY_MACHINE_ID || 'local', result: result || null },
  });
}

async function evaluate(ctx) {
  for (const job of JOBS) {
    if (ONLY && job.id !== ONLY) continue;
    const log = (m) => console.log(`[${job.id}] ${m}`);
    if (!job.enabled()) { if (DRY) log('disabled by env — skipped'); continue; }

    let last;
    try { last = await lastRun(ctx.contacts, job.id); }
    catch (e) { console.error(`[${job.id}] state read FAILED (${e.message}) — not running, will retry next tick`); continue; }

    const dueIn = last ? (last + job.everyMs) - Date.now() : 0;
    if (dueIn > 0) {
      if (DRY) log(`not due for ${Math.round(dueIn / 60000)}m (last ran ${new Date(last).toISOString()})`);
      continue;
    }
    if (DRY) { log(`DUE (last ran ${last ? new Date(last).toISOString() : 'never'})`); continue; }

    log(`starting — ${job.label}${last ? `, last ran ${new Date(last).toISOString()}` : ', never run before'}`);
    const t0 = Date.now();
    try {
      const result = await job.run({ ...ctx, log });
      if (result === false) { log('did not run; leaving last_run alone so the next tick retries'); continue; }
      await stampRun(ctx.contacts, job.id, result);
      log(`done in ${Math.round((Date.now() - t0) / 1000)}s`);
    } catch (e) {
      // Deliberately NOT stamped: a failed run is not a run.
      console.error(`[${job.id}] FAILED after ${Math.round((Date.now() - t0) / 1000)}s: ${e.message}`);
    }
  }
}

(async () => {
  const ctx = {
    contacts: os.makeClient(process.env.OPENSEARCH_ENDPOINT),
    companies: companies.makeClient(process.env.OPENSEARCH_ENDPOINT),
  };
  console.log(`scheduler: ${JOBS.length} job(s) registered${ONLY ? ` (--only ${ONLY})` : ''}`
    + `${DRY ? ', DRY RUN' : ''}; tick every ${Math.round(TICK_MS / 1000)}s`);
  // Say "10m", not "0h": an interval printed as zero reads like a misconfiguration.
  const every = (ms) => (ms >= 3600000 ? `${+(ms / 3600000).toFixed(1)}h` : `${Math.max(1, Math.round(ms / 60000))}m`);
  for (const j of JOBS) console.log(`  ${j.id.padEnd(16)} every ${every(j.everyMs).padEnd(5)} ${j.enabled() ? 'enabled' : 'DISABLED by env'}`);

  await evaluate(ctx);
  if (ONCE || DRY) { process.exit(0); }

  const timer = setInterval(() => { evaluate(ctx).catch((e) => console.error('scheduler tick error:', e.message)); }, TICK_MS);
  const bye = (sig) => { console.log(`scheduler: ${sig} — stopping`); clearInterval(timer); setTimeout(() => process.exit(0), 1000); };
  process.on('SIGTERM', () => bye('SIGTERM'));
  process.on('SIGINT', () => bye('SIGINT'));
})().catch((e) => { console.error('scheduler: fatal', e && e.stack || e); process.exit(1); });
