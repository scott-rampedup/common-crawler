/**
 * fleet-health.js — one-screen health for a live-crawl fleet: who is working, who stalled, who died.
 *
 *   FLY_API_TOKEN=… node fleet-health.js --app common-crawler --prefix live-fleet
 *   … --watch 60          re-check every 60s until every shard is finished or dead
 *
 * WHY: the 2026-08-13 fleet lost four of eight shards and nothing said so. Fly reported them "stopped",
 * which is also what a SUCCESSFUL shard looks like, and the run summary lives in a log line hundreds of
 * lines back. They had died of "Ineffective mark-compacts near heap limit" — Node's default ~2GB cap on an
 * 8GB machine — at 4,500 to 265,000 URLs of 315,613, and the fleet appeared to be progressing normally the
 * whole time.
 *
 * So a shard is classified against its OWN declared total, not against machine state:
 *
 *   DEAD      — stopped, no completion line, progress short of total. Work was lost; relaunch with --skip.
 *   OOM       — dead AND the log carries a heap-limit abort. Distinguished because the fix differs:
 *               more heap, not a retry.
 *   STALLED   — running, but its progress counter has not moved since the previous check.
 *   RUNNING   — running and advancing.
 *   DONE      — printed its completion line.
 *
 * Read-only: reads machine state and logs, changes nothing. It prints the exact relaunch command for every
 * shard that needs one, rather than acting on its own.
 */
const { spawnSync } = require('child_process');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const APP = arg('app', 'common-crawler');
const PREFIX = arg('prefix', 'live-fleet');
const WATCH = Number(arg('watch', '0')) || 0;
const FLY = process.env.FLYCTL || 'C:\\Users\\scott\\.fly\\bin\\flyctl.exe';

const N = (n) => Number(n || 0).toLocaleString();
const strip = (s) => String(s || '').replace(/\x1b\[[0-9;]*m/g, '');

function fly(args) {
  const r = spawnSync(FLY, args, { encoding: 'utf8', maxBuffer: 96 * 1024 * 1024, env: process.env });
  return strip((r.stdout || '') + (r.stderr || ''));
}

function machines() {
  const out = fly(['machine', 'list', '-a', APP]);
  const rows = [];
  for (const line of out.split('\n')) {
    if (!/^\s*[0-9a-f]{12,}/.test(line)) continue;
    const cols = line.split('│').map((c) => c.trim());
    if (cols.length < 3) continue;
    if (PREFIX && !cols[1].startsWith(PREFIX)) continue;
    rows.push({ id: cols[0], name: cols[1], state: cols[2] });
  }
  return rows.sort((a, b) => (a.name < b.name ? -1 : 1));
}

function inspect(id) {
  const log = fly(['logs', '-a', APP, '-i', id, '--no-tail']);
  const s = { done: false, oom: false, at: 0, total: 0, fetched: 0, extracted: 0, err: 0, rate: 0, planned: 0 };
  // "shard 3/8: 315,613 of 2,563,533 URL(s)" — the shard's own declared workload.
  const plan = /shard \d+\/\d+: ([\d,]+) of ([\d,]+) URL/.exec(log);
  if (plan) s.planned = Number(plan[1].replace(/,/g, ''));
  // last progress line
  let m, last = null;
  const re = /\[live\] ([\d,]+)\/([\d,]+) \| fetched ([\d,]+) \| extracted ([\d,]+) \| ([\d,]+) err \| ([\d.]+)\/s/g;
  while ((m = re.exec(log))) last = m;
  if (last) {
    s.at = Number(last[1].replace(/,/g, '')); s.total = Number(last[2].replace(/,/g, ''));
    s.fetched = Number(last[3].replace(/,/g, '')); s.extracted = Number(last[4].replace(/,/g, ''));
    s.err = Number(last[5].replace(/,/g, '')); s.rate = Number(last[6]);
  }
  const fin = /\[live\] done: ([\d,]+) fetched, ([\d,]+) extracted/.exec(log);
  if (fin) { s.done = true; s.fetched = Number(fin[1].replace(/,/g, '')); s.extracted = Number(fin[2].replace(/,/g, '')); }
  if (/Ineffective mark-compacts|JavaScript heap out of memory|OOMErrorHandler/i.test(log)) s.oom = true;
  return s;
}

const prev = new Map();

function pass() {
  const rows = machines();
  if (!rows.length) { console.log(`no machines matching "${PREFIX}" in ${APP}`); return true; }

  console.log(`\n${new Date().toISOString().slice(11, 19)}  ${APP} / ${PREFIX}*`);
  console.log('  shard              state    progress                 rate    extracted    err  health');
  console.log('  ---------------------------------------------------------------------------------------');

  let live = 0, dead = 0, doneN = 0, totalExtracted = 0, remaining = 0, aggRate = 0;
  const relaunch = [];
  for (const m of rows) {
    const s = inspect(m.id);
    const started = m.state === 'started';
    const target = s.total || s.planned || 0;
    const pct = target ? ((s.at / target) * 100).toFixed(0) + '%' : '—';

    let health;
    if (s.done) { health = 'DONE'; doneN++; }
    else if (!started && s.oom) { health = 'OOM'; dead++; }
    else if (!started) { health = 'DEAD'; dead++; }
    else if (prev.has(m.id) && prev.get(m.id) === s.at && s.at > 0) { health = 'STALLED'; live++; }
    else { health = 'RUNNING'; live++; aggRate += s.rate; }
    prev.set(m.id, s.at);

    totalExtracted += s.extracted;
    if (!s.done && target) remaining += Math.max(0, target - s.at);
    if ((health === 'DEAD' || health === 'OOM' || health === 'STALLED')) relaunch.push({ m, s });

    console.log(`  ${m.name.padEnd(16)} ${String(m.state).padEnd(8)} ${(`${N(s.at)}/${N(target)}`).padEnd(20)} ${pct.padStart(4)} ${String(s.rate).padStart(6)}/s ${N(s.extracted).padStart(10)} ${N(s.err).padStart(6)}  ${health}`);
  }

  console.log(`  ---------------------------------------------------------------------------------------`);
  console.log(`  ${live} live · ${doneN} done · ${dead} dead   |   extracted ${N(totalExtracted)}   |   ${N(remaining)} URL(s) left`);
  if (aggRate > 0 && remaining > 0) {
    // Deliberately computed off the CURRENT live rate: as shards finish, that rate falls and the estimate
    // rises. An estimate from the peak aggregate would flatter a fleet that has no work-stealing.
    console.log(`  at the current ${aggRate.toFixed(0)}/s aggregate: ~${(remaining / aggRate / 3600).toFixed(1)}h (falls as shards finish — no work-stealing)`);
  }
  if (relaunch.length) {
    console.log(`\n  needs attention:`);
    for (const { m, s } of relaunch) {
      const shard = /(\d+)$/.exec(m.name);
      console.log(`    ${m.name}: lost at ${N(s.at)}/${N(s.total || s.planned)}${s.oom ? '  (heap limit — needs --max-old-space-size)' : ''}`);
      if (shard) console.log(`      relaunch: --shard ${shard[1]}/8 --skip ${s.at}`);
    }
  }
  return live === 0;
}

(async () => {
  for (;;) {
    const finished = pass();
    if (!WATCH || finished) break;
    await new Promise((r) => setTimeout(r, WATCH * 1000));
  }
})().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
