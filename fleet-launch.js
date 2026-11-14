/**
 * fleet-launch.js — start an N-machine live-crawl fleet over a URL list. Module + CLI.
 *
 *   FLY_API_TOKEN=… node fleet-launch.js --in s3://…/miss-todo.txt --shards 8 --tag drain-2026-08-14
 *
 * The drain became a scheduled job, but the expensive half of it did not: every fleet so far was eight
 * `flyctl machine run` commands typed by hand. That is the remaining reason this pipeline is not actually
 * unattended — the monitor produces continuously, the scheduled drain resolves and Lambdas the ~17% that
 * Common Crawl already has, and then the ~83% remainder sits in S3 until a human starts the fleet.
 *
 * Uses the Machines REST API directly: there is no flyctl in the runtime image (node:24-slim) and adding
 * one to shell out from a web server would be worse than an HTTP call.
 *
 * Deliberate choices:
 *   - restart policy "no". A shard that dies must STAY dead and be visible to fleet-health, which computes
 *     a resume offset. Auto-restart would silently begin the shard again from zero and re-crawl everything
 *     it had already done.
 *   - NODE_OPTIONS=--max-old-space-size on every machine. Node defaults to ~2GB regardless of the VM, which
 *     is what killed four of eight shards on the first fleet — and a dead shard looks exactly like a
 *     finished one to Fly.
 *   - the machines inherit the app's secrets automatically, so OPENSEARCH_ENDPOINT / AWS / proxy
 *     credentials do not have to be passed (and must not be logged).
 */
const APP = process.env.FLY_APP_NAME || 'common-crawler';
const API = process.env.FLY_API_HOST || 'https://api.machines.dev/v1';

async function flyApi(path, method, body, token) {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null; try { json = text ? JSON.parse(text) : null; } catch (e) { /* non-JSON error body */ }
  if (!res.ok) throw new Error(`fly ${method} ${path} -> ${res.status} ${text.slice(0, 300)}`);
  return json;
}

/**
 * The image the app itself is running — a fleet must never lag the deployed code.
 *
 * Picking the newest machine by created_at is WRONG and shipped that way: a deploy updates an app
 * machine's image in place while its created_at stays the day it was first created, so any long-running
 * worker machine started since then looks "newer" while carrying a months-old image. On 2026-08-15 that
 * chose deployment-01KZMTGWW2 from a stale live-shard machine, and all 24 fleet shards died instantly
 * with "Cannot find module '/app/live-fleet-shard.js'".
 *
 * Two corrections, either of which would have prevented it:
 *   - prefer machines in the 'app' process group (the deployed web app), not arbitrary workers;
 *   - rank by the deployment tag itself. Fly deployment IDs are ULIDs, which sort lexicographically in
 *     time order, so the greatest tag IS the most recent release regardless of machine age.
 */
async function currentImage(token, app = APP) {
  const machines = await flyApi(`/apps/${app}/machines`, 'GET', null, token);
  const withImage = (machines || []).filter((m) => m.config && m.config.image);
  if (!withImage.length) throw new Error('no machine to take an image from');
  const appGroup = withImage.filter((m) => ((m.config.metadata || {}).fly_process_group === 'app'));
  const pool = appGroup.length ? appGroup : withImage;
  const tagOf = (m) => String(m.config.image).split(':').pop() || '';
  pool.sort((a, b) => tagOf(b).localeCompare(tagOf(a)));
  return pool[0].config.image;
}

/**
 * @param {object} o
 * @param {string} o.in       s3:// or local path of the URL list
 * @param {number} o.shards   fleet size
 * @param {string} o.tag      run tag (also the S3 output prefix for extracted JSONL)
 * @returns {Promise<Array<{shard:number,id:string,name:string}>>}
 */
// Crawl tuning a launched machine must carry EXPLICITLY.
//
// Machines created through the Machines API inherit the app's SECRETS but NOT fly.toml's [env]. Every
// value tuned there therefore reverted to its code default on a shard, and the defaults are an order of
// magnitude lower: DOMAIN_CONCURRENCY 48 -> 6, CC_MAX_SOCKETS 512 -> 64. The fleet asked each shard for
// LIVE_CONC=96 while the socket pool capped it at 64, so shards measured ~14 URLs/s and 24 of them totalled
// 336/s. That is not a resource limit, it is a config that never arrived.
//
// This gap has now cost three separate things: it throttled the nightly sweep, it made the scheduler
// request an 8-cpu/8GB guest that Fly rejects outright, and it held the crawl fleet at an eighth of its
// intended concurrency. So the values live here, in the launcher, and travel with every machine.
//
// HOST_CONCURRENCY stays at 3 deliberately — throughput comes from many hosts at once, not from hammering
// one. Raising it would make us rude, not fast.
const TUNING_DEFAULTS = {
  DOMAIN_CONCURRENCY: '48',     // total domains/URLs in flight per machine (code default: 6)
  IN_SITE_CONCURRENCY: '4',
  HOST_CONCURRENCY: '3',        // politeness — do not raise
  CC_CONCURRENCY: '1',
  HTML_MAX_KB: '1024',
  LIVE_MAX_PAGES: '300',
  CC_MAX_SOCKETS: '1024',       // must exceed LIVE_CONC or it silently caps it (code default: 64)
};
// The launcher's own env wins when set, so a machine that DOES have fly.toml values passes its own through.
function TUNING() {
  const out = {};
  for (const k of Object.keys(TUNING_DEFAULTS)) out[k] = String(process.env[k] || TUNING_DEFAULTS[k]);
  return out;
}

async function launchFleet(o) {
  const token = o.token || process.env.FLY_API_TOKEN;
  if (!token) throw new Error('no FLY_API_TOKEN — cannot launch a fleet');
  if (!o.in && !o.cmd) throw new Error('no --in list to crawl');
  const app = o.app || APP;
  const shards = Math.max(1, Number(o.shards) || 8);
  const tag = o.tag || 'fleet';
  const region = o.region || process.env.FLEET_REGION || 'ewr';
  const memMb = Number(o.memoryMb || process.env.FLEET_MEMORY_MB) || 8192;
  const cpus = Number(o.cpus || process.env.FLEET_CPUS) || 4;
  // Leave headroom below the VM's RAM: the heap is not the process's only allocation, and an OOM-kill by
  // the kernel is harder to diagnose than a clean V8 heap error.
  const heapMb = Number(o.heapMb || process.env.FLEET_HEAP_MB) || Math.max(1024, Math.floor(memMb * 0.75));
  const image = o.image || await currentImage(token, app);
  const log = o.log || ((m) => console.error(m));

  log(`[fleet] launching ${shards} shard(s) on ${image.split(':').pop()} (${cpus}x${memMb}MB, heap ${heapMb}MB)`);
  const out = [];
  for (let i = 0; i < shards; i++) {
    const name = `${o.namePrefix || 'live-fleet'}-${i}`;
    const config = {
      image,
      // 'no' is right for a BATCH shard: it finishes its slice and must stay finished, or a completed
      // crawl would restart forever. It is wrong for a long-lived SERVICE (the delta syncer, the
      // scheduler) -- those should come back after a crash. The syncer proved it: an unhandled idle-pg
      // error exited it at 19:49Z and restart:no left it dead for 18 hours while Postgres drifted ahead
      // of OpenSearch. Pass o.restart for a service, e.g. { policy: 'always' }.
      restart: o.restart || { policy: 'no' },
      guest: { cpu_kind: 'performance', cpus, memory_mb: memMb },
      env: Object.assign({}, TUNING(), {
        LIVE_CONC: String(o.liveConc || process.env.FLEET_LIVE_CONC || 96),
        CONC: String(o.conc || process.env.FLEET_CONC || 64),
        NODE_OPTIONS: `--max-old-space-size=${heapMb}`,
      }, o.env || {}),
      // o.cmd lets a caller run a different entrypoint on the same fleet plumbing (image resolution,
      // restart:no, explicit heap). It receives (shardIndex, shardCount) so it can partition its own work.
      init: { cmd: o.cmd ? o.cmd(i, shards)
        : ['node', '/app/live-fleet-shard.js', '--in', o.in, '--shard', `${i}/${shards}`, '--tag', tag] },
    };
    try {
      const m = await flyApi(`/apps/${app}/machines`, 'POST', { name, region, config }, token);
      out.push({ shard: i, id: m.id, name: m.name });
      log(`[fleet]   shard ${i}/${shards} -> ${m.id} (${m.name})`);
    } catch (e) {
      // One shard failing to start must not abandon the rest — the list is partitioned, so the others
      // still cover their own slices and the gap is a single re-launchable shard.
      log(`[fleet]   shard ${i}/${shards} FAILED to start: ${e.message}`);
    }
  }
  log(`[fleet] ${out.length}/${shards} shard(s) started`);
  return out;
}

// How long a shard may sit in `started` before it is presumed hung rather than working.
//
// On 2026-11-13 live-fleet-11 finished its slice at 10:26:47 -- it logged DONE and wrote its crawl
// ledger -- and then never exited; Fly kept reporting it `started` for the next ten hours. Because
// `busy` was read straight off that state, bio-pump skipped the live leg on every pass and 1,802,166
// URLs banked up behind one machine that had nothing left to do. Worse, the reaper that would have
// cleared it only runs inside bio-live, which is exactly what the machine was blocking.
//
// This was first set to three hours on the reasoning that a fleet takes at most BIO_LIVE_MAX_URLS
// (400k) over 12 shards, so ~33k each, an hour or two at the observed rate. That reasoning was wrong,
// and the same day proved it: sharding is BY DOMAIN, so a domain bigger than an even slice cannot be
// split and lands whole on one machine. biolive-20261113213650 put 318,718 roomvu.com URLs -- 52.7%
// of a 604,724-URL batch -- on shard 0 while the other eleven took 26,000 each. At the 16/s that host
// throttled us to, that shard needs five and a half hours of entirely honest work, and a three-hour
// guard would have deleted it mid-crawl.
//
// So eight hours, which is slack enough for a mega-domain while still bounding a hang to one night
// rather than forever. It is a stopgap: an age cannot distinguish working from hung, which is the one
// distinction this is for. The real fix is for the shard to heartbeat its progress and for staleness
// to be measured on THAT, so a shard is hung when it stops advancing, not when it takes a long time.
//
// Per-prefix, and OFF by default everywhere else. Only live-fleet has a runtime worth bounding at all;
// a bio-etl drain or a sitemap sweep can honestly run half a day, and declaring one of those hung
// would invent an outage rather than cure one. FLEET_STALE_MS forces a threshold on every fleet if
// that is ever wanted.
const STALE_DEFAULTS = { 'live-fleet': 8 * 60 * 60 * 1000 };
const staleFor = (prefix, override) =>
  Number(override || 0) || Number(process.env.FLEET_STALE_MS || 0) || STALE_DEFAULTS[prefix] || 0;

/** Epoch ms of the machine's most recent lifecycle event, or 0 when Fly reported none. */
function lastEventAt(m) {
  let t = 0;
  for (const ev of (m.events || [])) { const v = Number(ev.timestamp) || 0; if (v > t) t = v; }
  if (!t && m.updated_at) t = Date.parse(m.updated_at) || 0;
  return t;
}

/** A shard still `started` long past any plausible runtime is hung, not busy. staleMs 0 = no guard. */
function isStale(m, staleMs = 0) {
  if (m.state !== 'started' || !(staleMs > 0)) return false;
  const t = lastEventAt(m);
  return !!t && (Date.now() - t) > staleMs;
}

/**
 * Remove finished machines named with the fleet prefix, so the next launch can reuse the names.
 *
 * Hung shards are removed too. They hold a name that launchFleet needs, so leaving one in place turns
 * a stalled fleet into a fleet that cannot start at all.
 */
async function reapFleet(o = {}) {
  const token = o.token || process.env.FLY_API_TOKEN;
  if (!token) return 0;
  const app = o.app || APP;
  const prefix = o.namePrefix || 'live-fleet';
  const log = o.log || ((m) => console.error(m));
  const staleMs = staleFor(prefix, o.staleMs);
  const machines = await flyApi(`/apps/${app}/machines`, 'GET', null, token);
  let n = 0, hung = 0;
  for (const m of (machines || [])) {
    if (!String(m.name || '').startsWith(prefix)) continue;
    const stale = isStale(m, staleMs);
    if (m.state === 'started' && !stale) continue;             // never reap a shard that is still working
    if (stale) { hung++; log(`[fleet] ${m.name} hung since ${new Date(lastEventAt(m)).toISOString()} — removing`); }
    try { await flyApi(`/apps/${app}/machines/${m.id}?force=true`, 'DELETE', null, token); n++; }
    catch (e) { log(`[fleet] could not remove ${m.name}: ${e.message}`); }
  }
  if (n) log(`[fleet] removed ${n} machine(s)` + (hung ? ` (${hung} hung)` : ''));
  return n;
}

/**
 * Health of a running fleet, over the Machines API rather than by shelling out to flyctl.
 *
 * fleet-health.js exists but is CLI-only (no exports) and shells to flyctl, which is not in the app image
 * — so nothing has ever watched a fleet automatically. That is not a cosmetic gap: Fly reports a machine
 * that died from a V8 heap abort as `stopped`, which is indistinguishable from a shard that finished its
 * slice. Four of eight shards were lost that way on an earlier run and the drain reported success.
 *
 * A shard is FAILED if it stopped with a non-zero exit code (134 = V8 abort, 137 = OOM-kill) or was
 * oom_killed. Everything else stopped is treated as finished.
 *
 * A shard is STALE if Fly still calls it `started` long past any plausible runtime (see STALE_DEFAULTS).
 * Stale shards are deliberately kept OUT of `running`, because the only consumer of `running` is a
 * gate asking "may the next batch start yet?" and a hung machine must never be allowed to answer no
 * forever. They are returned separately so the caller can say so out loud rather than silently
 * discounting them.
 */
async function fleetStatus(o = {}) {
  const token = o.token || process.env.FLY_API_TOKEN;
  if (!token) return { ok: false, reason: 'no FLY_API_TOKEN', shards: [] };
  const app = o.app || APP;
  const prefix = o.namePrefix || 'live-fleet';
  const staleMs = staleFor(prefix, o.staleMs);
  const machines = await flyApi(`/apps/${app}/machines`, 'GET', null, token);
  const shards = [];
  for (const m of (machines || [])) {
    if (!String(m.name || '').startsWith(prefix)) continue;
    let exitCode = null, oom = false;
    for (const ev of (m.events || [])) {
      if (ev.type === 'exit' && ev.request && ev.request.exit_event) {
        exitCode = ev.request.exit_event.exit_code;
        oom = !!ev.request.exit_event.oom_killed;
        break;
      }
    }
    const state = m.state;
    const failed = state !== 'started' && (oom || (exitCode !== null && exitCode !== 0));
    const startedAt = lastEventAt(m);
    const stale = isStale(m, staleMs);
    shards.push({ id: m.id, name: m.name, state, exitCode, oom, failed, stale,
      ageMs: startedAt ? Date.now() - startedAt : null });
  }
  const running = shards.filter((s) => s.state === 'started' && !s.stale).length;
  const failed = shards.filter((s) => s.failed);
  const stale = shards.filter((s) => s.stale);
  return { ok: true, total: shards.length, running, stale,
    done: shards.length - running - failed.length - stale.length, failed, shards };
}

module.exports = { launchFleet, reapFleet, currentImage, fleetStatus, isStale, staleFor };

if (require.main === module) {
  const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
  (async () => {
    if (process.argv.includes('--reap')) { await reapFleet({ namePrefix: arg('prefix', 'live-fleet') }); return; }
    const r = await launchFleet({
      in: arg('in', ''), shards: Number(arg('shards', '8')), tag: arg('tag', ''),
      region: arg('region', ''), namePrefix: arg('prefix', 'live-fleet'),
    });
    console.log(JSON.stringify(r, null, 2));
  })().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
}
