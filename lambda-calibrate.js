/**
 * lambda-calibrate.js — find the concurrency that S3 will actually serve, by measuring instead of guessing.
 *
 *   node lambda-calibrate.js --ptr /tmp/cal.jsonl [--slice 40000] [--target 90]
 *   node lambda-calibrate.js --discover 40000            (build the sample itself, then calibrate)
 *
 * THE PROBLEM THIS SOLVES: the real limit on this pipeline is not Lambda concurrency, it is how many
 * simultaneous GETs s3://commoncrawl will serve. Fleet size and per-Lambda fetch concurrency MULTIPLY:
 *
 *     5,000 Lambdas x 64 internal =  320,000 concurrent GETs -> 32.8% of pointers actually fetched
 *     2,500 Lambdas x 12 internal =   30,000 concurrent GETs -> 52.7%
 *
 * Everything above the ceiling is wasted twice: it burns Lambda time AND loses the page. Each full-sweep
 * attempt costs ~30 minutes, so bisecting by re-running the sweep is the expensive way to learn this.
 * This runs the SAME pointer slice at a ladder of settings and reports delivery for each, so the knee
 * shows up in minutes.
 *
 * Re-fetching the same slice each round is deliberate: identical work per round is what makes the
 * delivery percentages comparable. Nothing is indexed — the extracted JSONL is written to a throwaway
 * prefix and ignored.
 */
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };

const PTR = arg('ptr', '/tmp/cal-ptr.jsonl');
const DISCOVER = Number(arg('discover', '0')) || 0;
const SLICE = Number(arg('slice', '40000')) || 40000;
const TARGET = Number(arg('target', '90')) || 90;
const CRAWL = arg('crawl', 'CC-MAIN-2026-30');

// fleet x per-Lambda -> concurrent GETs. Ordered most-aggressive first, so the run can stop as soon as a
// setting clears the target: everything below it is gentler and will only do better.
const LADDER = [
  { conc: 2000, fetch: 10 },   // 20,000
  { conc: 1200, fetch: 10 },   // 12,000
  { conc: 800,  fetch: 10 },   //  8,000
  { conc: 600,  fetch: 8  },   //  4,800
  { conc: 400,  fetch: 8  },   //  3,200
  { conc: 250,  fetch: 8  },   //  2,000
];

function run(script, args, env) {
  const r = spawnSync(process.execPath, [path.join(__dirname, script), ...args], {
    cwd: __dirname, env: { ...process.env, ...env }, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024,
  });
  return (r.stderr || '') + (r.stdout || '');
}

(async () => {
  if (DISCOVER) {
    console.error(`building a ${DISCOVER.toLocaleString()}-pointer sample from ${CRAWL}…`);
    const out = run('cc-athena-miner.js', ['--bio-terms-file', path.join(__dirname, 'data', 'bio-path-terms.txt'),
      '--crawl', CRAWL, '--per-domain', '3', '--limit', String(DISCOVER), '--warc-out', PTR]);
    const m = /Wrote ([\d,]+) WARC pointers/.exec(out);
    console.error('  ' + (m ? m[1] + ' pointers' : 'discovery output unparsed'));
  }
  if (!fs.existsSync(PTR)) { console.error(`no pointer file at ${PTR} — pass --ptr or --discover N`); process.exit(1); }

  // One fixed slice, reused every round. The name MUST still end in .jsonl: lambda-drive locates its
  // input with argv.find(a => /\.jsonl?$/i.test(a)), so a ".slice" suffix makes it print usage and exit —
  // which this harness then dutifully reported as 0.0% delivery on every rung.
  const slicePath = PTR.replace(/\.jsonl?$/i, '') + '-slice.jsonl';
  let sliceCount = 0;
  {
    const w = fs.createWriteStream(slicePath);
    const rl = require('readline').createInterface({ input: fs.createReadStream(PTR), crlfDelay: Infinity });
    for await (const l of rl) {
      if (!l.trim()) continue;
      if (!w.write(l + '\n')) await new Promise((r) => w.once('drain', r));
      if (++sliceCount >= SLICE) break;
    }
    await new Promise((r) => w.end(r));
    console.error(`\ncalibrating on ${sliceCount.toLocaleString()} pointer(s), target >=${TARGET}% delivery\n`);
  }

  // A rung can only be exercised if the slice contains enough BATCHes to fill the fleet AND keep it full.
  // With BATCH=200, a 40,000-pointer slice is 200 batches — so at most 200 Lambdas ever run, and a rung
  // configured for 2,000 actually measures a tenth of that. The first run of this harness reported
  // "100% delivery at 20,000 GETs" when it had really tested ~2,000. Skip what we cannot honestly test.
  const BATCH_N = Number(process.env.BATCH) || 200;
  const batchesInSlice = Math.floor(sliceCount / BATCH_N);
  const SUSTAIN = 3;                                    // want the fleet filled ~3x over, not just once
  console.error(`  slice = ${sliceCount.toLocaleString()} pointers = ${batchesInSlice.toLocaleString()} batches of ${BATCH_N}`);
  console.error(`  a rung is only testable up to ~${Math.floor(batchesInSlice / SUSTAIN).toLocaleString()} concurrent Lambdas\n`);

  console.error('  fleet x fetch =  concurrent |  delivery | fetch/s | ptr-err');
  console.error('  ------------------------------------------------------------');
  const results = [];
  let winner = null;
  for (const step of LADDER) {
    if (step.conc > Math.floor(batchesInSlice / SUSTAIN)) {
      console.error(`  ${String(step.conc).padStart(5)} x ${String(step.fetch).padStart(2)} = ${String(step.conc * step.fetch).padStart(8)} | SKIPPED — slice too small to fill this fleet`);
      continue;
    }
    const out = run('lambda-drive.js', [slicePath], {
      CONCURRENCY: String(step.conc), LAMBDA_FETCH_CONC: String(step.fetch),
      BATCH: process.env.BATCH || '200', RUN: `calib/${step.conc}x${step.fetch}`,
    });
    const m = /delivery\s+([\d.]+)%/.exec(out);
    if (!m) {
      // A round that produced no summary DID NOT deliver 0% — it failed to run. Reporting it as 0%
      // silently turns a broken harness into a plausible-looking measurement, so say so and stop.
      console.error(`\n  round ${step.conc}x${step.fetch} produced no summary — lambda-drive did not run.`);
      console.error('  its output was:\n' + out.split('\n').slice(0, 12).map((l) => '    ' + l).join('\n'));
      process.exit(1);
    }
    const pct = Number(m[1] || 0);
    const rate = Number(((/fetched\s+[\d,]+\s+\(([\d,]+)\/s/.exec(out) || [])[1] || '0').replace(/,/g, ''));
    const perr = Number(((/ptr-err\s+([\d,]+)/.exec(out) || [])[1] || '0').replace(/,/g, ''));
    const gets = step.conc * step.fetch;
    results.push({ ...step, gets, pct, rate, perr });
    console.error(`  ${String(step.conc).padStart(5)} x ${String(step.fetch).padStart(2)} = ${String(gets).padStart(8)} | ${String(pct.toFixed(1)).padStart(7)}% | ${String(rate).padStart(7)} | ${perr.toLocaleString()}`);
    if (pct >= TARGET) { winner = { ...step, gets, pct, rate }; break; }   // gentler settings can only be better
  }

  console.error('');
  if (winner) {
    console.error(`RECOMMENDED: CONCURRENCY=${winner.conc} LAMBDA_FETCH_CONC=${winner.fetch}`);
    console.error(`  ${winner.gets.toLocaleString()} concurrent GETs -> ${winner.pct.toFixed(1)}% delivery at ${winner.rate.toLocaleString()} pages/s`);
  } else {
    const best = results.slice().sort((a, b) => b.pct - a.pct)[0];
    console.error(`No rung cleared ${TARGET}%. Best was ${best.conc}x${best.fetch} (${best.gets.toLocaleString()} GETs) at ${best.pct.toFixed(1)}%.`);
    console.error('Extend the ladder downward — S3, not Lambda, is the binding constraint.');
  }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
