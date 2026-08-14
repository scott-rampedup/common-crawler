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

/** The image the app itself is running — a fleet must never lag the deployed code. */
async function currentImage(token, app = APP) {
  const machines = await flyApi(`/apps/${app}/machines`, 'GET', null, token);
  const running = (machines || []).filter((m) => m.state === 'started' && m.config && m.config.image);
  if (!running.length) throw new Error('no started machine to take an image from');
  // Newest by created_at: after a deploy the older app machine may still be on the previous release.
  running.sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));
  return running[0].config.image;
}

/**
 * @param {object} o
 * @param {string} o.in       s3:// or local path of the URL list
 * @param {number} o.shards   fleet size
 * @param {string} o.tag      run tag (also the S3 output prefix for extracted JSONL)
 * @returns {Promise<Array<{shard:number,id:string,name:string}>>}
 */
async function launchFleet(o) {
  const token = o.token || process.env.FLY_API_TOKEN;
  if (!token) throw new Error('no FLY_API_TOKEN — cannot launch a fleet');
  if (!o.in) throw new Error('no --in list to crawl');
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
      restart: { policy: 'no' },
      guest: { cpu_kind: 'performance', cpus, memory_mb: memMb },
      env: {
        LIVE_CONC: String(o.liveConc || process.env.FLEET_LIVE_CONC || 96),
        CONC: String(o.conc || process.env.FLEET_CONC || 64),
        NODE_OPTIONS: `--max-old-space-size=${heapMb}`,
      },
      init: { cmd: ['node', '/app/live-fleet-shard.js', '--in', o.in, '--shard', `${i}/${shards}`, '--tag', tag] },
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

/** Remove stopped machines named with the fleet prefix, so the next launch can reuse the names. */
async function reapFleet(o = {}) {
  const token = o.token || process.env.FLY_API_TOKEN;
  if (!token) return 0;
  const app = o.app || APP;
  const prefix = o.namePrefix || 'live-fleet';
  const log = o.log || ((m) => console.error(m));
  const machines = await flyApi(`/apps/${app}/machines`, 'GET', null, token);
  let n = 0;
  for (const m of (machines || [])) {
    if (!String(m.name || '').startsWith(prefix)) continue;
    if (m.state === 'started') continue;                       // never reap a live shard
    try { await flyApi(`/apps/${app}/machines/${m.id}?force=true`, 'DELETE', null, token); n++; }
    catch (e) { log(`[fleet] could not remove ${m.name}: ${e.message}`); }
  }
  if (n) log(`[fleet] removed ${n} finished machine(s)`);
  return n;
}

module.exports = { launchFleet, reapFleet, currentImage };

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
