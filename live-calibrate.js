/**
 * live-calibrate.js — find the live-crawl concurrency the proxy will actually serve.
 *
 *   node live-calibrate.js --in s3://bucket/bio-resolve/drain20260813/miss.txt [--per-rung 400]
 *   node live-calibrate.js --in /tmp/miss.txt --rungs 4,16,48,128,256
 *
 * WHY: extract-from-pointers defaults LIVE_CONC to 4, with the comment "it goes through the Evomi proxy at
 * real sites, so it stays low." Measured on the 2026-08-13 drain that yields 2 pages/s — which turns the
 * 2,563,533-URL live remainder into 15 DAYS. That default has never been tested against the proxy; it is a
 * guess that became a constant, and it is currently the binding constraint on the whole pipeline.
 *
 * The Lambda path was calibrated the same way and the answer was counter-intuitive (LOWER concurrency was
 * both faster and more complete, because S3 was the ceiling). So this measures rather than assumes, and it
 * is built to avoid the two ways the Lambda harness lied to me:
 *
 *   - Every rung gets DISTINCT URLs. Re-fetching the same slice would let DNS/proxy caching flatter the
 *     later rungs, and the rungs are ordered low-to-high, so the flattery would land exactly where the
 *     conclusion is drawn.
 *   - A rung that returns no successful fetches is reported as FAILED, not as "0 pages/s". A broken rung
 *     that reads as a slow rung is how a harness turns a structural failure into a plausible number.
 *
 * Fetches only. Extracts nothing, indexes nothing, writes nothing.
 */
const fs = require('fs');
const readline = require('readline');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('in', '');
const PER_RUNG = Number(arg('per-rung', '400')) || 400;
const RUNGS = String(arg('rungs', '4,16,48,128,256')).split(',').map((s) => Number(s.trim())).filter(Boolean);
const REGION = process.env.AWS_REGION || 'us-east-1';

const ccEngine = require('./cc-engine');

async function readUrls(src, need) {
  const urls = [];
  let stream;
  if (/^s3:\/\//i.test(src)) {
    const m = /^s3:\/\/([^/]+)\/(.+)$/i.exec(src);
    const r = await new S3Client({ region: REGION }).send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }));
    stream = r.Body;
  } else {
    stream = fs.createReadStream(src);
  }
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    // The miss list is one URL per line, but tolerate JSONL in case it is pointed at a pointer file.
    let u = t;
    if (t.startsWith('{')) { try { u = JSON.parse(t).url || ''; } catch (e) { u = ''; } }
    if (u) urls.push(u);
    if (urls.length >= need) break;
  }
  return urls;
}

async function pump(items, conc, fn) {
  let i = 0;
  const workers = Array.from({ length: Math.min(conc, items.length) }, async () => {
    for (;;) { const k = i++; if (k >= items.length) return; await fn(items[k]); }
  });
  await Promise.all(workers);
}

(async () => {
  if (!IN) { console.error('need --in <s3://… or path>'); process.exit(1); }
  const need = PER_RUNG * RUNGS.length;
  console.error(`reading ${need.toLocaleString()} URL(s) from ${IN}…`);
  const all = await readUrls(IN, need);
  if (all.length < need) console.error(`  only ${all.length.toLocaleString()} available — rungs will be shortened`);

  console.error(`\n  conc |   pages/s |  ok  | empty |  err | mean ms | slice`);
  console.error('  ---------------------------------------------------------');
  const results = [];
  let offset = 0;
  for (const conc of RUNGS) {
    const slice = all.slice(offset, offset + PER_RUNG);
    offset += slice.length;
    if (!slice.length) { console.error(`  ${String(conc).padStart(5)} | SKIPPED — no URLs left`); continue; }

    let ok = 0, empty = 0, err = 0, totalMs = 0;
    const t0 = Date.now();
    await pump(slice, conc, async (u) => {
      const s = Date.now();
      try {
        const html = await ccEngine.liveFetchPage(u);
        if (html && String(html).length > 200) ok++; else empty++;
      } catch (e) { err++; }
      totalMs += Date.now() - s;
    });
    const secs = (Date.now() - t0) / 1000;
    const rate = slice.length / Math.max(0.001, secs);
    const meanMs = Math.round(totalMs / Math.max(1, slice.length));

    if (!ok) {
      // Distinguish "the proxy refused everything at this concurrency" from "this rung is slow". They look
      // identical in a rate column and mean opposite things.
      console.error(`  ${String(conc).padStart(5)} | FAILED — 0 successful fetches (${empty} empty, ${err} err) in ${secs.toFixed(0)}s`);
      results.push({ conc, rate: 0, ok, empty, err, meanMs, failed: true });
      continue;
    }
    results.push({ conc, rate, ok, empty, err, meanMs, failed: false });
    console.error(`  ${String(conc).padStart(5)} | ${rate.toFixed(1).padStart(9)} | ${String(ok).padStart(4)} | ${String(empty).padStart(5)} | ${String(err).padStart(4)} | ${String(meanMs).padStart(7)} | ${slice.length}`);
  }

  const good = results.filter((r) => !r.failed && r.ok / Math.max(1, r.ok + r.empty + r.err) >= 0.5);
  console.error('');
  if (!good.length) { console.error('No rung achieved a 50% success rate — the proxy, not concurrency, is the constraint.'); return; }
  const best = good.slice().sort((a, b) => b.rate - a.rate)[0];
  const base = results.find((r) => r.conc === RUNGS[0]);
  console.error(`BEST: LIVE_CONC=${best.conc} at ${best.rate.toFixed(1)} pages/s (${((best.ok / (best.ok + best.empty + best.err)) * 100).toFixed(0)}% usable)`);
  if (base && base.rate > 0) console.error(`  ${(best.rate / base.rate).toFixed(1)}x the rate of LIVE_CONC=${base.conc}`);
  const REMAINING = 2563533;
  console.error(`\n  ${REMAINING.toLocaleString()} URLs at ${best.rate.toFixed(1)}/s on ONE machine: ${(REMAINING / best.rate / 3600).toFixed(1)}h`);
  for (const n of [4, 8, 16]) {
    console.error(`    across ${String(n).padStart(2)} machines: ${(REMAINING / (best.rate * n) / 3600).toFixed(1)}h`);
  }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
