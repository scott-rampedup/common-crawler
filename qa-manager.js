#!/usr/bin/env node
//
// qa-manager.js — ask "where are we, and did every BIO URL make it into the Contact Crawler?" on a
// schedule, so nobody has to ask it in the morning.
// ---------------------------------------------------------------------------------------------------
// THE DIRECTIVE. Maximise the number of BIO URLs found by the sitemap scan, the Google Maps upload and
// the data importer that end up PRESENT IN THE CONTACT CRAWLER FOR SEARCH. Not fetched. Not merged.
// Searchable. Every check below is judged against that and nothing else.
//
// WHY THIS EXISTS. The pipeline fails in a way that looks exactly like success. A stale image, a hidden
// env var, a throttled bucket, a cursor that stops, a fleet shard that hangs, a read-only database, a
// rotated token -- in every one of these the machines stay `started`, the jobs report done, the logs go
// quiet, and the only signal is that nothing arrived. Every incident in this repo's history was found
// the same way: a person asked "why didn't anything process?" hours after it stopped. Examples, all real:
//
//   2026-11-16  CRAWLS_N=4 left in one machine's config made every Athena resolve scan ~7.8GB instead of
//               ~0.7GB. Every resolve failed on S3 throttling for 6h18m. The feed cursor never moved,
//               bio-pump logged "idle", and the day lost two thirds of its delivery.
//   2026-11-14  opensearch-sync shipped a fix to the machine that does not run opensearch-sync, so the
//               bug went on stranding contacts in Postgres where the UI could not see them.
//   2026-11-14  `fly machine restart` does not re-inject secrets, so seven workers kept a superseded AWS
//               key after a rotation and would have lost S3 entirely had the old key been deleted.
//   earlier     Postgres went read-only at 90% disk and the writer silently fell back to SQLite. Jobs
//               reported success; the Contact Crawler stayed empty.
//
// What they share is that each was cheap to DETECT and only expensive to notice. So this measures the
// delivery chain end to end, every hour, fixes what is safe to fix without asking, re-measures to prove
// the fix worked, and remembers what it saw so a problem that keeps coming back is visible as a pattern
// rather than as a fresh surprise.
//
//   node qa-manager.js              # measure, fix, verify, report
//   node qa-manager.js --dry        # measure and report; change nothing
//   node qa-manager.js --json
//
// WHAT IT WILL AND WILL NOT DO ON ITS OWN. It fixes things that are idempotent, reversible and already
// part of normal operation: rolling a machine onto the deployed image, indexing a contact that is in
// Postgres and missing from the index, handing stuck queue rows back, reaping a hung shard, re-queueing
// discovered-but-never-crawled URLs. It does NOT enable a disabled scheduler job, change a tuning knob,
// delete anything, or destroy a machine. Those change what the system is meant to do, and that remains
// a human decision -- it reports them instead, with the evidence, which is the part that was missing.
const { Client } = require('pg');

const argv = process.argv.slice(2);
const has = (f) => argv.includes('--' + f);
const arg = (n, d) => { const i = argv.indexOf('--' + n); return i > -1 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : d; };

const DRY = has('dry') || process.env.QA_FIX === '0';
const JSON_OUT = has('json');
const ONLY = arg('only', '');
const CFG = process.env.CC_CONFIG_INDEX || 'cc_config';
const STATE_ID = 'qa_manager_state';
const LEDGER = process.env.CRAWL_LEDGER_INDEX || 'crawl_log';
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';

const N = (n) => Number(n || 0).toLocaleString();
const hrs = (ms) => (ms / 3600000).toFixed(1) + 'h';
const agoMs = (iso) => (iso ? Date.now() - Date.parse(iso) : Infinity);

// ---------------------------------------------------------------- the checks
//
// Each check measures one link in the chain from "a URL was discovered" to "a person can find the
// contact". `measure` returns { ok, detail, metrics }. `fix` is optional and only ever does something
// listed in the header above. `fixEveryMs` throttles a fix that is too expensive to repeat hourly.
const CHECKS = [

  {
    id: 'deploy_parity',
    what: 'every long-running worker is on the image that was deployed',
    // First because it invalidates everything else: a check that passes against stale code has told you
    // nothing. `fly deploy` only updates the `app` process group, so this drifts on every single deploy.
    async measure(ctx) {
      if (!process.env.FLY_API_TOKEN) return { ok: true, skipped: true, detail: 'no FLY_API_TOKEN' };
      const m = await require('./deploy-parity').measure({ token: process.env.FLY_API_TOKEN });
      const bad = [...m.drifted.map((r) => r.name + ' (stale)'), ...m.down.map((r) => r.name + ' (' + r.state + ')')];
      return { ok: m.ok, detail: m.ok ? `all ${m.rows.length} on ${m.want.tag}` : bad.join(', '), metrics: { drifted: m.drifted.length, down: m.down.length } };
    },
    async fix() {
      const r = await require('./deploy-parity').run({ token: process.env.FLY_API_TOKEN, fix: true, log: () => {} });
      return { detail: `rolled ${r.fixed.filter((x) => x.updated).length} machine(s)` };
    },
  },

  {
    id: 'db_writable',
    what: 'Postgres accepts writes (not read-only, no silent SQLite fallback)',
    // Postgres going read-only at high disk is the quietest failure here: every job reports success
    // while the writer falls back to SQLite and the Contact Crawler stays empty. A write is the only
    // honest test of a writable database; a size query would pass throughout.
    async measure(ctx) {
      try {
        await ctx.pg.query('CREATE TABLE IF NOT EXISTS qa_heartbeat (id int primary key, at timestamptz)');
        await ctx.pg.query(`INSERT INTO qa_heartbeat (id, at) VALUES (1, now())
                            ON CONFLICT (id) DO UPDATE SET at = now()`);
        const sz = (await ctx.pg.query('SELECT pg_size_pretty(pg_database_size(current_database())) s')).rows[0].s;
        return { ok: true, detail: `writable, database ${sz}`, metrics: { size: sz } };
      } catch (e) {
        return { ok: false, detail: 'WRITE FAILED: ' + e.message, severity: 'fail' };
      }
    },
  },

  {
    id: 'sweep_ran',
    what: 'the sitemap scan completed within the last 26h',
    async measure(ctx) {
      const doc = await ctx.getCfg('sched_sitemap_sweep');
      if (!doc) return { ok: false, detail: 'never run — SCHED_SITEMAP may be off', severity: 'fail' };
      const age = agoMs(doc.last_run);
      return { ok: age < 26 * 3600000, detail: `last run ${doc.last_run} (${hrs(age)} ago)`, metrics: { ageHours: age / 3600000 } };
    },
    // Deliberately no fix: starting a sweep off-schedule, or enabling a job someone turned off, changes
    // what the system is meant to do. Report it.
  },

  {
    id: 'feed_cursor',
    what: 'the feed is consuming the queue (the cursor is moving)',
    // The single most valuable check here. A stuck cursor is what every ingestion stall looks like from
    // the outside, and it is invisible: bio-pump logs "idle", every machine is healthy, nothing arrives.
    async measure(ctx) {
      const cur = await ctx.getCfg('bio_feed_cursor');
      if (!cur) return { ok: false, detail: 'no bio_feed_cursor doc', severity: 'fail' };
      const { objects, urls, oldest } = await ctx.queueAhead(cur.cursor);
      const stuckFor = agoMs(oldest);
      // Work waiting is normal; work waiting while the cursor has not moved past it for hours is not.
      const ok = objects === 0 || stuckFor < Number(process.env.QA_FEED_STALL_HOURS || 3) * 3600000;
      return {
        ok,
        detail: objects === 0
          ? `caught up (cursor ${cur.cursor})`
          : `${N(objects)} object(s) / ${N(urls)} URL(s) unread, oldest ${hrs(stuckFor)} old, cursor ${cur.cursor}`,
        metrics: { objectsAhead: objects, urlsAhead: urls, oldestHours: stuckFor / 3600000 },
        severity: 'fail',
      };
    },
  },

  {
    id: 'archive_resolve',
    what: 'Common Crawl lookups are succeeding',
    // The cause behind most stuck cursors. Reported separately so the report says WHY the feed stalled
    // rather than only that it did -- on 2026-11-16 that distinction was the whole diagnosis.
    async measure(ctx) {
      const { ok: okN, failed } = await ctx.athenaToday();
      if (okN + failed === 0) return { ok: true, skipped: true, detail: 'no resolves attempted today' };
      const rate = failed / (okN + failed);
      return {
        ok: rate < 0.5,
        detail: `${N(okN)} ok / ${N(failed)} failed today (${(rate * 100).toFixed(0)}% failing)`,
        metrics: { ok: okN, failed, failRate: rate },
      };
    },
  },

  {
    id: 'crawl_queue_stuck',
    what: 'no crawl_queue rows are wedged in error or claimed',
    async measure(ctx) {
      const r = await ctx.pg.query(`SELECT coalesce(status,'?') s, count(*)::bigint n FROM crawl_queue WHERE status <> 'done' GROUP BY 1`);
      const by = {}; for (const row of r.rows) by[row.s] = Number(row.n);
      const bad = (by.error || 0) + (by.claimed || 0);
      return {
        ok: bad === 0,
        detail: Object.keys(by).length ? Object.entries(by).map(([k, v]) => `${k}=${N(v)}`).join('  ') : 'queue empty',
        metrics: by,
      };
    },
    async fix(ctx) {
      const { makeQueue } = require('./crawl-queue');
      const q = await makeQueue(ctx.pool);
      const e = await q.requeueErrors({});
      const s = q.requeueStale ? await q.requeueStale(30) : { requeued: 0 };
      return { detail: `requeued ${N(e.requeued || 0)} error + ${N(s.requeued || 0)} stale row(s)` };
    },
  },

  {
    id: 'contacts_searchable',
    what: 'contacts written recently are findable in the Contact Crawler',
    // The directive's actual bar. Everything upstream can be green while the record the customer needs
    // is in Postgres and absent from the index the UI reads -- which is exactly what happened on
    // 2026-11-14, and what a stray 2026-11-14 row was still doing two days later.
    async measure(ctx) {
      const since = new Date(Date.now() - 36 * 3600000).toISOString();
      const rows = (await ctx.pg.query(
        `SELECT email FROM contacts WHERE updated_at >= $1 ORDER BY updated_at DESC LIMIT 4000`, [since])).rows;
      if (!rows.length) return { ok: true, skipped: true, detail: 'no contacts written in 36h' };
      const missing = await ctx.missingFromIndex(rows.map((r) => String(r.email).toLowerCase()));
      return {
        ok: missing.length === 0,
        detail: missing.length
          ? `${N(missing.length)} of ${N(rows.length)} recent contact(s) NOT searchable`
          : `all ${N(rows.length)} recent contact(s) searchable`,
        metrics: { checked: rows.length, missing: missing.length },
        severity: 'fail',
      };
    },
    async fix(ctx) {
      const since = new Date(Date.now() - 36 * 3600000).toISOString();
      const rows = (await ctx.pg.query(
        `SELECT * FROM contacts WHERE updated_at >= $1 ORDER BY updated_at DESC LIMIT 4000`, [since])).rows;
      const missing = new Set(await ctx.missingFromIndex(rows.map((r) => String(r.email).toLowerCase())));
      const todo = rows.filter((r) => missing.has(String(r.email).toLowerCase()));
      if (!todo.length) return { detail: 'nothing to index' };
      const { rowToDoc, bulkUpsert, INDEX } = require('./opensearch');
      const res = await bulkUpsert(ctx.osc, todo.map(rowToDoc));
      try { await ctx.osc.indices.refresh({ index: INDEX }); } catch (e) { /* best-effort */ }
      return { detail: `indexed ${N(todo.length - ((res && res.errors) || 0))} contact(s)` };
    },
  },

  {
    id: 'os_sync_lag',
    what: 'the syncer is keeping the index close to the database',
    async measure(ctx) {
      // Columns are k/v, both TEXT -- the watermark is a JSON string, not a jsonb column.
      const r = await ctx.pg.query(`SELECT v FROM sync_state WHERE k = 'os_sync'`).catch(() => ({ rows: [] }));
      let wm = null;
      try { wm = r.rows.length ? JSON.parse(r.rows[0].v) : null; } catch (e) { wm = null; }
      if (!wm) return { ok: true, skipped: true, detail: 'no watermark yet' };
      // Lag only counts when there is something newer to have synced. An old watermark on an idle
      // pipeline is correct, not late.
      const newer = Number((await ctx.pg.query(
        `SELECT count(*)::bigint n FROM contacts WHERE updated_at > $1`, [wm.updated_at])).rows[0].n);
      const age = agoMs(wm.updated_at);
      return {
        ok: newer === 0 || age < Number(process.env.QA_SYNC_LAG_HOURS || 1) * 3600000,
        detail: `watermark ${wm.updated_at} (${hrs(age)} ago), ${N(newer)} row(s) newer`,
        metrics: { lagHours: age / 3600000, newer },
      };
    },
  },

  {
    id: 'hung_shards',
    what: 'no live-crawl shard is stuck pretending to work',
    // A shard that finishes its slice and fails to exit stays `started` forever, and the pump treats the
    // fleet as busy and stops feeding it. That once cost ten hours and 1,802,166 banked URLs.
    async measure(ctx) {
      if (!process.env.FLY_API_TOKEN) return { ok: true, skipped: true, detail: 'no FLY_API_TOKEN' };
      const { fleetStatus } = require('./fleet-launch');
      const st = await fleetStatus({ namePrefix: 'live-fleet' });
      const stale = (st && st.stale) || [];
      return {
        ok: stale.length === 0,
        detail: stale.length ? stale.map((s) => `${s.name} ${hrs(s.ageMs || 0)}`).join(', ') : 'none hung',
        metrics: { stale: stale.length, running: (st && st.running) || 0 },
      };
    },
    async fix(ctx) {
      const { reapFleet } = require('./fleet-launch');
      const removed = await reapFleet({ namePrefix: 'live-fleet', log: () => {} });   // returns a count
      return { detail: `reaped ${N(removed)} machine(s)` };
    },
  },

  {
    id: 'abandoned_urls',
    what: 'URLs discovered but never crawled are back in the queue',
    // The sweep records 'queued' for everything it hands over, and only a fetch turns that into a real
    // outcome. Anything still 'queued' days later was dropped on the floor by a stall. Most of the pool
    // is legitimately blocked or already queued, so the count that matters is the actionable remainder.
    fixEveryMs: 20 * 3600000,                    // at most once a day; it scrolls the whole ledger
    async measure(ctx) {
      const c = await ctx.osc.count({ index: LEDGER, body: { query: { term: { outcome: 'queued' } } } });
      const total = ((c.body || c)).count;
      const est = await ctx.actionableQueued(total);
      return {
        ok: est < Number(process.env.QA_ABANDONED_MAX || 5000),
        detail: `${N(total)} ledger entr(y/ies) queued, ~${N(est)} genuinely uncrawled`,
        metrics: { queued: total, actionable: est },
        severity: 'warn',
      };
    },
    async fix(ctx) {
      const { spawnSync } = require('child_process');
      const r = spawnSync(process.execPath, [require('path').join(__dirname, 'queued-rescue.js'), '--write'],
        { cwd: __dirname, env: process.env, encoding: 'utf8' });
      const m = /queued for the feed\s+([\d,]+)/.exec(r.stdout || '');
      return { detail: m ? `re-queued ${m[1]} URL(s)` : `queued-rescue exit ${r.status}` };
    },
  },

  {
    id: 'sources_24h',
    what: 'each ingestion source delivered something in the last 24h',
    // The directive names three sources, and they land under these `source` values: the Google Maps
    // upload tags 'Google Maps' (gm-bio-scan sets it), the data importer tags 'Sheet Import', and the
    // sitemap scan's URLs arrive as whatever path fetched them ('Common Crawl' from the archive,
    // 'Webpage' from the live fleet) -- which is why the sitemap channel is judged by sweep_ran and
    // feed_cursor above rather than by a source tag that would not distinguish it.
    //
    // A quiet day for one channel is normal, so this fails only when NOTHING arrived at all. A channel
    // that produced within the week and then went silent is called out separately: that is the shape of
    // an upload that stopped being run, or a token that expired, and it is exactly what goes unnoticed.
    async measure(ctx) {
      const since = (h) => new Date(Date.now() - h * 3600000).toISOString();
      const grab = async (h) => {
        const r = await ctx.pg.query(
          `SELECT coalesce(nullif(source,''),'(none)') s, count(*)::bigint n
           FROM contacts WHERE updated_at >= $1 GROUP BY 1`, [since(h)]);
        const m = new Map(); for (const x of r.rows) m.set(x.s, Number(x.n));
        return m;
      };
      const d1 = await grab(24), d7 = await grab(24 * 7);
      const total = [...d1.values()].reduce((a, b) => a + b, 0);
      const silent = [...d7.keys()].filter((s) => !d1.get(s) && d7.get(s) > 0);
      const by = [...d1.entries()].sort((a, b) => b[1] - a[1]).map(([s, n]) => `${s}=${N(n)}`).join('  ');
      return {
        ok: total > 0,
        detail: (total ? `24h: ${by}` : 'NO contacts in 24h')
          + (silent.length ? `  |  silent 24h but active this week: ${silent.join(', ')}` : ''),
        severity: 'fail',
        metrics: { total, silent },
      };
    },
  },
];

// ---------------------------------------------------------------- shared measurement helpers
async function buildCtx() {
  const { makeClient, INDEX } = require('./opensearch');
  const osc = makeClient(process.env.OPENSEARCH_ENDPOINT);
  const { Pool } = require('pg');
  const conn = process.env.DATABASE_URL.replace(/:(5432|5433)\//, ':5433/');
  const ssl = process.env.PGSSL ? { rejectUnauthorized: false } : false;
  const pool = new Pool({ connectionString: conn, ssl, max: 3 });
  const pg = new Client({ connectionString: conn, ssl });
  await pg.connect();
  await pg.query("SET statement_timeout='120s'");

  const ctx = { osc, pg, pool, INDEX };

  ctx.getCfg = async (id) => {
    try { const g = await osc.get({ index: CFG, id }); return ((g.body || g)._source) || null; }
    catch (e) { return null; }
  };

  // Objects sitting in the queue that the cursor has not passed. Key names carry the URL count, so the
  // size of the backlog is readable without fetching a single object.
  ctx.queueAhead = async (cursor) => {
    const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');
    const s3 = new S3Client({ region: REGION });
    let tok = null, objects = 0, urls = 0, oldest = null;
    do {
      const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
      for (const o of (r.Contents || [])) {
        const iso = new Date(o.LastModified).toISOString();
        if (cursor && iso <= cursor) continue;
        objects++;
        if (!oldest || iso < oldest) oldest = iso;
        const m = /-(\d+)\.txt$/.exec(o.Key);
        if (m) urls += Number(m[1]);
      }
      tok = r.IsTruncated ? r.NextContinuationToken : null;
    } while (tok);
    return { objects, urls, oldest };
  };

  ctx.athenaToday = async () => {
    const { AthenaClient, ListQueryExecutionsCommand, BatchGetQueryExecutionCommand } = require('@aws-sdk/client-athena');
    const a = new AthenaClient({ region: REGION });
    const wg = process.env.ATHENA_WORKGROUP || 'primary';
    const today = new Date().toISOString().slice(0, 10);
    let tok = null, ok = 0, failed = 0;
    for (let page = 0; page < 6; page++) {
      const l = await a.send(new ListQueryExecutionsCommand({ WorkGroup: wg, MaxResults: 50, NextToken: tok || undefined }));
      const ids = l.QueryExecutionIds || [];
      if (!ids.length) break;
      const b = await a.send(new BatchGetQueryExecutionCommand({ QueryExecutionIds: ids }));
      for (const q of (b.QueryExecutions || [])) {
        if (!/^SELECT url, filename/.test((q.Query || '').trim())) continue;   // the resolve query only
        const s = q.Status || {};
        const at = s.SubmissionDateTime ? new Date(s.SubmissionDateTime).toISOString() : '';
        if (at.slice(0, 10) !== today) continue;
        if (s.State === 'SUCCEEDED') ok++; else if (s.State === 'FAILED' || s.State === 'CANCELLED') failed++;
      }
      tok = l.NextToken; if (!tok) break;
    }
    return { ok, failed };
  };

  ctx.missingFromIndex = async (emails) => {
    const miss = [];
    for (let i = 0; i < emails.length; i += 500) {
      const chunk = emails.slice(i, i + 500);
      const r = await osc.mget({ index: INDEX, body: { ids: chunk }, _source: false });
      ((r.body || r).docs || []).forEach((d, k) => { if (!d.found) miss.push(chunk[k]); });
    }
    return miss;
  };

  // Estimate how much of the 'queued' ledger is genuinely uncrawled, from a RANDOM sample -- a plain
  // search returns index order, and one taken that way read 100% blocked when the true figure was 57%.
  ctx.actionableQueued = async (total) => {
    if (!total) return 0;
    const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./i, '').toLowerCase(); } catch (e) { return ''; } };
    const r = await osc.search({ index: LEDGER, body: {
      size: 1000, _source: ['url'],
      query: { function_score: { query: { term: { outcome: 'queued' } },
        random_score: { seed: Date.now(), field: '_seq_no' }, boost_mode: 'replace' } } } });
    const urls = (((r.body || r).hits || {}).hits || []).map((h) => h._source.url).filter(Boolean);
    if (!urls.length) return 0;
    const blocked = await require('./domain-gate').loadSet(osc);
    const candidates = urls.filter((u) => !blocked.has(hostOf(u)));
    let known = 0;
    for (let i = 0; i < candidates.length; i += 500) {
      const chunk = candidates.slice(i, i + 500);
      const q = await pg.query('SELECT url FROM crawl_queue WHERE url = ANY($1)', [chunk]);
      known += q.rows.length;
    }
    const rate = (candidates.length - known) / urls.length;
    return Math.round(total * rate);
  };

  return ctx;
}

// ---------------------------------------------------------------- run
async function run() {
  if (!process.env.OPENSEARCH_ENDPOINT || !process.env.DATABASE_URL) {
    console.error('qa-manager: need OPENSEARCH_ENDPOINT and DATABASE_URL'); process.exit(2);
  }
  const started = new Date().toISOString();
  const ctx = await buildCtx();
  const prev = (await ctx.getCfg(STATE_ID)) || { checks: {} };
  const results = [];

  for (const chk of CHECKS) {
    if (ONLY && chk.id !== ONLY) continue;
    const row = { id: chk.id, what: chk.what };
    try {
      const m = await chk.measure(ctx);
      Object.assign(row, m);
    } catch (e) {
      Object.assign(row, { ok: false, detail: 'CHECK ERRORED: ' + e.message, severity: 'fail' });
    }

    // Fix, then MEASURE AGAIN. A fix that reports success is not the same as a problem that went away,
    // and the difference is the whole reason this exists rather than a cron that runs repairs blindly.
    if (!row.ok && !row.skipped && chk.fix && !DRY) {
      const last = ((prev.checks || {})[chk.id] || {}).lastFixAt;
      const throttled = chk.fixEveryMs && last && agoMs(last) < chk.fixEveryMs;
      if (throttled) {
        row.fix = { skipped: `last fixed ${hrs(agoMs(last))} ago` };
      } else {
        try {
          const f = await chk.fix(ctx);
          row.fix = { ...f, at: new Date().toISOString() };
          row.lastFixAt = row.fix.at;
          const after = await chk.measure(ctx);
          row.fix.verified = !!after.ok;
          row.fix.after = after.detail;
          row.ok = after.ok;
        } catch (e) {
          row.fix = { error: e.message, verified: false };
        }
      }
    }

    // A problem that keeps coming back is a different problem from one that just appeared.
    const before = (prev.checks || {})[chk.id];
    if (before && before.ok === false && row.ok === false) row.recurring = (before.recurring || 1) + 1;
    results.push(row);
  }

  const failed = results.filter((r) => !r.ok && !r.skipped && (r.severity || 'fail') === 'fail');
  const warned = results.filter((r) => !r.ok && !r.skipped && r.severity === 'warn');
  const fixed = results.filter((r) => r.fix && r.fix.verified);

  const state = { at: started, finished: new Date().toISOString(), checks: {} };
  for (const r of results) state.checks[r.id] = { ok: r.ok, detail: r.detail, recurring: r.recurring || 0, lastFixAt: r.lastFixAt || ((prev.checks || {})[r.id] || {}).lastFixAt };
  try { await ctx.osc.index({ index: CFG, id: STATE_ID, refresh: true, body: state }); } catch (e) { /* reporting only */ }

  const lines = [];
  lines.push(`QA  ${failed.length ? 'PROBLEMS' : warned.length ? 'OK (warnings)' : 'ALL CLEAR'}  — ${started}`);
  for (const r of results) {
    const mark = r.skipped ? '  --  ' : r.ok ? '  ok  ' : (r.severity === 'warn' ? ' warn ' : ' FAIL ');
    lines.push(`${mark}${r.id.padEnd(20)}${r.detail || ''}`);
    if (r.fix) {
      if (r.fix.skipped) lines.push(`        fix skipped: ${r.fix.skipped}`);
      else if (r.fix.error) lines.push(`        FIX ERRORED: ${r.fix.error}`);
      else lines.push(`        fixed: ${r.fix.detail} -> ${r.fix.verified ? 'VERIFIED' : 'DID NOT CLEAR'} (${r.fix.after})`);
    }
    if (r.recurring > 1) lines.push(`        recurring: ${r.recurring} runs in a row`);
  }
  const report = lines.join('\n');
  if (JSON_OUT) console.log(JSON.stringify({ state, results }, null, 2));
  else console.log(report);

  // Mail only when a person needs to do something: something failed and could not be fixed, or a fix
  // had to be applied. A green run every hour in the inbox is how an alert stops being read.
  const unresolved = failed.filter((r) => !(r.fix && r.fix.verified));
  if (!DRY && (unresolved.length || fixed.length)) {
    try {
      const mailer = require('./mailer');
      if (mailer.mailEnabled()) {
        await mailer.sendMail({
          to: mailer.adminEmail(),
          subject: unresolved.length
            ? `Contact Crawler QA — ${unresolved.length} unresolved: ${unresolved.map((r) => r.id).join(', ')}`
            : `Contact Crawler QA — fixed ${fixed.map((r) => r.id).join(', ')}`,
          text: report,
        });
      }
    } catch (e) { console.error('qa-manager: report mail failed: ' + e.message); }
  }

  await ctx.pg.end();
  try { await ctx.pool.end(); } catch (e) { /* already closed */ }
  return { ok: unresolved.length === 0, results, unresolved };
}

module.exports = { CHECKS, run };

if (require.main === module) {
  run().then((r) => process.exit(r.ok ? 0 : 1))
    .catch((e) => { console.error('qa-manager FAILED:', (e && e.stack) || e); process.exit(2); });
}
