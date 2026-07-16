/**
 * universe-refresh.js — the scheduled two-hop Common Crawl refresh routine.
 *
 *   HOP 1  dump company domains -> waterfall-resolve their home pages across a stack of recent crawls
 *          (freshest capture wins) -> enrich the companies + emit every discovered BIO URL.
 *   HOP 2  waterfall-resolve those BIO URLs in CC -> extract PERSON contacts into the Master DB,
 *          live-crawling (proxy) the bio pages that aren't in the archive.
 *
 * Usage:
 *   OPENSEARCH_ENDPOINT=… node universe-refresh.js --filter '{"industry":"law practice","country":"ireland"}' \
 *        --cap 3000 --crawls CC-MAIN-2026-25,CC-MAIN-2026-21,CC-MAIN-2026-17 --tag lawie --live
 *
 *   --filter <json>   company filter (same shape as the Company Crawler / dump-company-urls). '' = whole universe.
 *   --cap <n>         max companies to dump this run (default 2,000,000).
 *   --crawls <list>   the waterfall stack, newest-first, comma-separated CC crawl ids.
 *   --tag <t>         names the scratch artifacts + Athena resolve tags.
 *   --scratch <dir>   working dir for the intermediate files (default ./_universe).
 *   --live            after CC, live-crawl the bio URLs not found in the archive (Hop 2 fallback).
 *   --skip-dump       reuse an existing <tag>-urls.txt / <tag>-targets.ndjson (re-run from resolve).
 */
const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const arg = (f, d = '') => { const i = process.argv.indexOf(f); return i > -1 ? (process.argv[i + 1] !== undefined && !String(process.argv[i + 1]).startsWith('--') ? process.argv[i + 1] : true) : d; };
const has = (f) => process.argv.includes(f);

const FILTER = arg('--filter', '');
const CAP = arg('--cap', '2000000');
const CRAWLS = arg('--crawls', 'CC-MAIN-2026-25,CC-MAIN-2026-21,CC-MAIN-2026-17,CC-MAIN-2026-12,CC-MAIN-2026-08,CC-MAIN-2026-04');
const TAG = arg('--tag', 'universe');
const SCRATCH = arg('--scratch', path.join(__dirname, '_universe'));
const LIVE = has('--live');
const SKIP_DUMP = has('--skip-dump');

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
fs.mkdirSync(SCRATCH, { recursive: true });
const P = (s) => path.join(SCRATCH, `${TAG}-${s}`);
const F = { urls: P('urls.txt'), targets: P('targets.ndjson'), homeptr: P('homeptr.jsonl'), bioUrls: P('bio-urls.txt'), bioptr: P('bioptr.jsonl'), bioMiss: P('bio-miss.txt') };

function step(name, cmd, args, extraEnv) {
  const banner = `\n══════ ${name} ${new Date().toISOString().slice(11, 19)} ══════`;
  console.error(banner);
  const r = spawnSync(cmd, args, { stdio: 'inherit', env: { ...process.env, ...(extraEnv || {}) }, cwd: __dirname });
  if (r.status !== 0) { console.error(`✗ ${name} exited ${r.status}`); process.exit(r.status || 1); }
}
const node = (script, args, env) => step(script, process.execPath, [script, ...args], env);

async function countLines(f) { if (!fs.existsSync(f)) return 0; let n = 0; const rl = readline.createInterface({ input: fs.createReadStream(f), crlfDelay: Infinity }); for await (const l of rl) if (l.trim()) n++; return n; }
const keyOf = (u) => { try { const x = new URL(/^https?:/i.test(u) ? u : 'https://' + u); return x.hostname.toLowerCase().replace(/^www\./, '') + '|' + x.pathname.replace(/\/+$/, ''); } catch { return ''; } };

async function computeBioMisses() {
  const resolved = new Set();
  if (fs.existsSync(F.bioptr)) { const rl = readline.createInterface({ input: fs.createReadStream(F.bioptr), crlfDelay: Infinity }); for await (const l of rl) { if (!l.trim()) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.url) resolved.add(keyOf(o.url)); } }
  const miss = [];
  const rl = readline.createInterface({ input: fs.createReadStream(F.bioUrls), crlfDelay: Infinity });
  for await (const l of rl) { const u = l.trim(); if (!u) continue; if (!resolved.has(keyOf(u))) miss.push(u); }
  fs.writeFileSync(F.bioMiss, miss.join('\n') + '\n');
  return { resolved: resolved.size, miss: miss.length };
}

(async () => {
  const t0 = Date.now();
  console.error(`UNIVERSE REFRESH  tag=${TAG}  live=${LIVE}\n  filter=${FILTER || '(whole universe)'}\n  waterfall crawls=${CRAWLS}\n  scratch=${SCRATCH}`);

  // ---- HOP 1: dump -> waterfall-resolve homes -> enrich + emit bio URLs ----
  if (!SKIP_DUMP) node('dump-company-urls.js', [FILTER || '{}', CAP, F.urls, F.targets]);
  const nUrls = await countLines(F.urls), nTargets = await countLines(F.targets);
  console.error(`  dumped ${nTargets.toLocaleString()} companies / ${nUrls.toLocaleString()} home URLs`);

  node('cc-athena-miner.js', ['--resolve-urls', F.urls, '--warc-out', F.homeptr, '--crawls', CRAWLS, '--resolve-tag', `${TAG}h`]);
  const nHome = await countLines(F.homeptr);
  console.error(`  waterfall-resolved ${nHome.toLocaleString()} home pages (${nTargets ? (100 * nHome / nTargets).toFixed(1) : 0}% of dumped)`);

  node('cc-enrich-from-pointers.js', [F.homeptr, F.targets], { BIO_OUT: F.bioUrls });
  const nBio = await countLines(F.bioUrls);
  console.error(`  discovered ${nBio.toLocaleString()} bio URLs`);

  // ---- HOP 2: waterfall-resolve bio URLs -> extract contacts (+ live fallback) ----
  let missInfo = { resolved: 0, miss: 0 };
  if (nBio) {
    node('cc-athena-miner.js', ['--resolve-urls', F.bioUrls, '--warc-out', F.bioptr, '--crawls', CRAWLS, '--resolve-tag', `${TAG}b`]);
    missInfo = await computeBioMisses();
    console.error(`  bio pages in CC: ${missInfo.resolved.toLocaleString()} | not in CC: ${missInfo.miss.toLocaleString()}${LIVE ? ' (live fallback)' : ' (skipped — no --live)'}`);
    const exArgs = ['--ptr', F.bioptr, '--tag', TAG];
    if (LIVE && missInfo.miss) exArgs.push('--live', F.bioMiss);
    node('extract-from-pointers.js', exArgs);
  } else {
    console.error('  no bio URLs discovered — skipping Hop 2');
  }

  console.error(`\n══════ UNIVERSE REFRESH DONE ${Math.round((Date.now() - t0) / 1000)}s ══════`);
  console.error(`  companies: ${nTargets.toLocaleString()} dumped -> ${nHome.toLocaleString()} home pages refreshed`);
  console.error(`  bio: ${nBio.toLocaleString()} URLs -> ${missInfo.resolved.toLocaleString()} via CC${LIVE ? ` + ${missInfo.miss.toLocaleString()} via live` : ''} -> extracted to Master DB (see Hop 2 DONE line above)`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
