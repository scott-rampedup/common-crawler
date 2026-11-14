/**
 * deploy-parity.js — are the machines that are RUNNING actually running the code that was deployed?
 * ---------------------------------------------------------------------------------------------------
 * WHY THIS EXISTS. `fly deploy` updates the `app` process group and nothing else. Every long-running
 * worker in this app -- scheduler-0, bio-pump-0, os-sync-0, cq-live-0..3 -- is a standalone machine
 * outside that group, so a deploy leaves them on whatever image they were last given, indefinitely and
 * without a word. The deploy reports success because the deploy DID succeed; the fix just is not running
 * anywhere that matters.
 *
 * This is not hypothetical and it is not rare. pipeline-health.js already records "bio-pump ran 25,680
 * passes on an image seven days old, because it sits outside fly.toml". On 2026-11-14 it happened again
 * and cost a day's work: a commit-lag fix for opensearch-sync.js shipped while os-sync-0 -- the one
 * machine that runs opensearch-sync.js -- stayed five days behind, so the bug it fixed went on stranding
 * contacts; and a URL-truncation fix in cc-engine.js shipped while all four cq-live workers, which are
 * the only things that crawl, kept truncating. Both were found by accident.
 *
 * So the check is: for every machine that is supposed to be up, does its image match the app's?
 *
 *   FLY_API_TOKEN=... node deploy-parity.js            # report; exit 1 if anything has drifted
 *   FLY_API_TOKEN=... node deploy-parity.js --fix      # roll the drifted machines onto the app image
 *   FLY_API_TOKEN=... node deploy-parity.js --roll-all # roll ALL of them, drifted or not (see secrets)
 *   FLY_API_TOKEN=... node deploy-parity.js --json
 *
 * SECRETS HAVE THE SAME BLIND SPOT, AND IT CANNOT BE DETECTED HERE. `fly secrets set` also updates only
 * the `app` process group, so every persistent worker keeps the OLD value until its config is rewritten.
 * Worse, `fly machine restart` does NOT re-inject secrets -- it restarts the process with the environment
 * it already had. Only an update (what --fix and --roll-all do) replaces it. Verified on 2026-11-14
 * during an AWS key rotation: after restarting all seven workers, every one still held the superseded
 * key, and deleting the old key at that point would have cut the entire pipeline off from AWS.
 *
 * The Machines API exposes no per-machine secrets version, so there is nothing to compare and this tool
 * cannot tell you that a secret has drifted. That is why --roll-all exists: after `fly secrets set`, run
 * it, because the alternative is reasoning about which machines were updated when.
 *
 * COMPARE DIGESTS, NOT THE IMAGE STRING. `fly deploy` records an image by tag
 * ("...:deployment-01M7DD5E...") and `fly machine update` records the same image by digest
 * ("...@sha256:5ea7e88d..."). They are the same bytes written two ways, so a string comparison of
 * config.image reports DRIFT on machines that are perfectly up to date -- and a check that cries wolf on
 * every run is one nobody reads, which is how the real drift stayed invisible. image_ref carries tag AND
 * digest separately; the digest is the identity.
 *
 * SCOPE IS RESTART POLICY, NOT A NAME LIST. A machine with restart policy `always` is one Fly is told to
 * keep up forever -- that is exactly the set of persistent workers, and it stays correct when someone
 * adds a worker without editing this file. Everything else is a job machine: fleet shards, sweeps, ETL
 * runs. Those are launched FROM the current image by fleet-launch.currentImage() and exit, so they
 * cannot be stale in a way that lasts, and restarting one mid-run would destroy the work it is doing.
 */
const APP = process.env.FLY_APP_NAME || 'common-crawler';
const API = process.env.FLY_API_HOST || 'https://api.machines.dev/v1';

const argv = process.argv.slice(2);
const has = (f) => argv.includes('--' + f);
const ROLL_ALL = has('roll-all');
const FIX = has('fix') || ROLL_ALL;
const JSON_OUT = has('json');

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

const groupOf = (m) => ((m.config || {}).metadata || {}).fly_process_group || '';
const policyOf = (m) => (((m.config || {}).restart) || {}).policy || '';
const refOf = (m) => m.image_ref || {};

/**
 * What the app is running. Deliberately the SAME rule fleet-launch.currentImage uses -- prefer the `app`
 * process group, then rank by the deployment tag, because Fly deployment ids are ULIDs and sort
 * lexicographically in time order. Ranking by machine created_at instead once picked a months-old image
 * off a long-running worker and killed 24 fleet shards on boot.
 */
function appImage(machines) {
  const withImage = (machines || []).filter((m) => refOf(m).digest);
  const appGroup = withImage.filter((m) => groupOf(m) === 'app');
  const pool = appGroup.length ? appGroup : withImage;
  if (!pool.length) return null;
  pool.sort((a, b) => String(refOf(b).tag || '').localeCompare(String(refOf(a).tag || '')));
  return refOf(pool[0]);
}

async function measure({ token = process.env.FLY_API_TOKEN, app = APP } = {}) {
  if (!token) throw new Error('no FLY_API_TOKEN — cannot read machine images');
  const machines = await flyApi(`/apps/${app}/machines`, 'GET', null, token);
  const want = appImage(machines);
  if (!want) throw new Error('no machine carries an image_ref — cannot establish the deployed image');

  const rows = [];
  for (const m of (machines || [])) {
    const policy = policyOf(m);
    const group = groupOf(m);
    // In scope: the app itself, plus anything Fly is told to keep running forever. See the header.
    const persistent = group === 'app' || policy === 'always';
    if (!persistent) continue;
    const ref = refOf(m);
    rows.push({
      id: m.id, name: m.name, state: m.state, group: group || '-', policy: policy || '-',
      tag: ref.tag || '(none)', digest: ref.digest || '',
      drift: (ref.digest || '') !== want.digest,
    });
  }
  rows.sort((a, b) => a.name.localeCompare(b.name));
  // A machine Fly is told to keep up but which is NOT up is its own problem, and a different one: it is
  // not running stale code, it is not running at all. Report it separately rather than folding it into
  // the drift count, because "restart it" and "redeploy it" are different actions.
  const down = rows.filter((r) => r.policy === 'always' && r.state !== 'started');
  const drifted = rows.filter((r) => r.drift);
  return { want, rows, drifted, down, ok: drifted.length === 0 && down.length === 0 };
}

/**
 * Roll a machine onto the app image. The Machines API replaces the whole config, so this reads the
 * machine first and changes only the image -- posting a hand-built config would silently drop the
 * machine's env, mounts and guest size.
 *
 * Safe to do unattended, which is why --fix exists at all: every persistent worker here recovers its own
 * interrupted work on boot. worker.js calls requeueStale(30) at startup so a cq-live restart hands back
 * the rows it had claimed; bio-pump re-feeds error rows every pass; os-sync resumes from its watermark.
 * A worker running the wrong code is worse than a worker that restarts.
 */
async function roll({ token = process.env.FLY_API_TOKEN, app = APP, id, image }) {
  const m = await flyApi(`/apps/${app}/machines/${id}`, 'GET', null, token);
  const config = { ...(m.config || {}), image };
  await flyApi(`/apps/${app}/machines/${id}`, 'POST', { config }, token);
  // Wait for it to come back, so a --fix that reports success means the machine is actually up on the
  // new image rather than merely accepted for update.
  try {
    await flyApi(`/apps/${app}/machines/${id}/wait?state=started&timeout=60`, 'GET', null, token);
  } catch (e) { return { ok: false, error: e.message }; }
  return { ok: true };
}

async function run({ token = process.env.FLY_API_TOKEN, app = APP, fix = FIX, rollAll = ROLL_ALL, log = console.log } = {}) {
  const m = await measure({ token, app });
  // --roll-all ignores the drift test entirely: it is for the secrets case, where there is nothing to
  // compare and the only safe assumption is that every worker is behind.
  //
  // Never roll the machine this is running on. The first --roll-all did, and killed the ssh session it
  // was being run through half way down the list, leaving two workers unrolled and the operator with no
  // output saying which. Self is excluded rather than ordered last: finishing the list matters more than
  // updating the one machine that `fly deploy` already handles, and which is the reference image here
  // anyway -- rolling it onto its own image is a no-op bought with an outage.
  const self = process.env.FLY_MACHINE_ID || '';
  const targets = (rollAll ? m.rows.filter((r) => r.state === 'started') : m.drifted)
    .filter((r) => r.id !== self && r.group !== 'app');
  const imageStr = `${m.want.registry}/${m.want.repository}:${m.want.tag}`;

  if (!JSON_OUT) {
    log(`deployed image  ${m.want.tag}`);
    log(`                ${m.want.digest}`);
    for (const r of m.rows) {
      const mark = r.drift ? 'STALE  ' : (r.state !== 'started' && r.policy === 'always' ? 'DOWN   ' : 'ok     ');
      log(`  ${mark}${r.name.padEnd(22)}${r.state.padEnd(10)}${r.drift ? r.tag : ''}`);
    }
  }

  const fixed = [];
  if (fix && targets.length) {
    log(`rolling ${targets.length} machine(s) onto ${m.want.tag}${rollAll ? ' (--roll-all)' : ''}`);
    for (const r of targets) {
      const res = await roll({ token, app, id: r.id, image: imageStr });
      log(`  ${r.name} ${res.ok ? 'updated' : 'FAILED: ' + res.error}`);
      fixed.push({ ...r, updated: res.ok, error: res.error });
    }
  }

  const after = (fix && fixed.length) ? await measure({ token, app }) : m;
  if (JSON_OUT) console.log(JSON.stringify({ ...after, fixed }, null, 2));
  else if (after.ok) log('every persistent machine is running the deployed image');
  else {
    if (after.drifted.length) log(`${after.drifted.length} machine(s) NOT running the deployed image: `
      + after.drifted.map((r) => r.name).join(', '));
    if (after.down.length) log(`${after.down.length} machine(s) meant to be up but are not: `
      + after.down.map((r) => r.name).join(', '));
    if (!fix && after.drifted.length) log('  --fix rolls them onto the deployed image');
  }
  return { ...after, fixed };
}

module.exports = { run, measure, roll, appImage };

if (require.main === module) {
  run().then((r) => process.exit(r.ok ? 0 : 1))
    .catch((e) => { console.error('deploy-parity ERR', (e && e.stack) || e); process.exit(1); });
}
