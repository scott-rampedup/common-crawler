/**
 * lambda-drive.js — fan a WARC-pointer JSONL out across cc-extract Lambdas (the extraction driver).
 * -------------------------------------------------------------------------------------------------
 * Streams a pointer file (cc-athena-miner discovery output), batches it, and invokes the cc-extract
 * Lambda concurrently. Each Lambda reads s3://commoncrawl direct and writes extracted contact JSONL to
 * S3 under cc-extracted/<run>/. This is the throughput unlock — bounded by Lambda account concurrency,
 * not Postgres. Then `load-extracted.js cc-extracted/<run>/` indexes the run into OpenSearch.
 *
 *   BATCH=200 CONCURRENCY=100 RUN=2026-12 node lambda-drive.js pointers.jsonl
 */
const fs = require('fs');
const https = require('https');
const readline = require('readline');
const { LambdaClient, InvokeCommand, GetAccountSettingsCommand } = require('@aws-sdk/client-lambda');

(async () => {
  const file = process.argv.find((a) => /\.jsonl?$/i.test(a));
  if (!file) { console.error('usage: node lambda-drive.js <pointers.jsonl>'); process.exit(1); }
  const BATCH = Number(process.env.BATCH) || 200;
  const RUN = process.env.RUN || ('run' + Date.now());
  const region = process.env.AWS_REGION || 'us-east-1';
  // The SDK's default HTTP handler caps at 50 sockets — the real ceiling on invoke concurrency. Give it
  // a big keep-alive pool so we can actually saturate the Lambda quota.
  let requestHandler;
  try {
    const { NodeHttpHandler } = require('@smithy/node-http-handler');
    requestHandler = new NodeHttpHandler({ httpsAgent: new https.Agent({ maxSockets: 4000, keepAlive: true }) });
  } catch (e) { /* fall back to SDK default handler */ }
  const lambda = new LambdaClient({ region, maxAttempts: 6, requestHandler });   // more retries — ride out throttling near the cap

  // Auto-cap concurrency to the account's Lambda quota (leave headroom) so we don't self-throttle.
  // Raising that quota (AWS Service Quotas -> Lambda 'Concurrent executions') is what unlocks full scale.
  let limit = 10;
  try { limit = (await lambda.send(new GetAccountSettingsCommand({}))).AccountLimit.UnreservedConcurrentExecutions || 10; } catch (e) { /* default */ }
  const want = Number(process.env.CONCURRENCY) || 100;
  const CONC = Math.max(1, Math.min(want, limit - 2));
  console.error(`Lambda concurrency limit ${limit} -> driving at ${CONC} concurrent (raise the quota to go faster)`);

  // Per-Lambda fetch concurrency. This is a MULTIPLIER on Lambda concurrency against one S3 bucket:
  // 5,000 Lambdas x 64 = 320,000 simultaneous GETs on s3://commoncrawl, far past what it will serve. A
  // full sweep at those settings fetched 1.08M of 3.3M pointers — the rest failed inside the Lambdas and
  // were counted nowhere. Let the FLEET provide parallelism and keep each function polite.
  const FETCH_CONC = Number(process.env.LAMBDA_FETCH_CONC) || 12;
  const INVOKE_TRIES = Number(process.env.INVOKE_TRIES) || 5;
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  const t0 = Date.now();
  let sent = 0, records = 0, fetched = 0, empty = 0, ptrErrs = 0, invErrs = 0, lost = 0, retried = 0, batchNo = 0;
  const inflight = new Set();

  async function fire(batch, n) {
    const payload = Buffer.from(JSON.stringify({ pointers: batch, outKey: `cc-extracted/${RUN}/b${n}.jsonl`, concurrency: FETCH_CONC }));
    // Throttling during ramp is EXPECTED, not exceptional: Lambda scales +1,000 concurrency per 10s, so
    // driving at 5,000 guarantees TooManyRequestsException early on. Dropping those batches silently is
    // how a run loses six figures of pages, so retry with backoff and only then count them lost.
    for (let attempt = 1; attempt <= INVOKE_TRIES; attempt++) {
      try {
        const res = await lambda.send(new InvokeCommand({ FunctionName: 'cc-extract', Payload: payload }));
        if (res.FunctionError) {                       // the function itself threw — a retry won't help
          invErrs++; lost += batch.length; break;
        }
        const o = JSON.parse(Buffer.from(res.Payload).toString());
        records += o.written || 0; fetched += o.fetched || 0; empty += o.empty || 0;
        ptrErrs += o.errs || 0;                        // per-pointer fetch failures INSIDE the Lambda
        break;
      } catch (e) {
        const throttled = /TooManyRequests|Throttl|Rate exceeded|ServiceException|502|503/i.test(String(e && (e.name + ' ' + e.message)));
        if (attempt < INVOKE_TRIES && throttled) {
          retried++;
          await sleep(Math.min(8000, 250 * 2 ** attempt) + Math.floor(Math.random() * 250));
          continue;
        }
        invErrs++; lost += batch.length; break;
      }
    }
    sent += batch.length;
    if (n % 50 === 0) {
      const s = (Date.now() - t0) / 1000;
      console.error(`  ${sent.toLocaleString()} sent | ${fetched.toLocaleString()} fetched | ${records.toLocaleString()} records`
        + ` | ${Math.round(fetched / s)} fetch/s | inv-err ${invErrs} (${lost.toLocaleString()} lost) | ptr-err ${ptrErrs.toLocaleString()} | retried ${retried}`);
    }
  }
  async function pump(batch) {
    const n = batchNo++;
    const p = fire(batch, n).finally(() => inflight.delete(p));
    inflight.add(p);
    if (inflight.size >= CONC) await Promise.race(inflight);
  }

  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  let buf = [];
  for await (const line of rl) {
    const t = line.trim(); if (!t) continue;
    let o; try { o = JSON.parse(t); } catch (e) { continue; }
    if (!o || !o.url) continue;
    buf.push(o);
    if (buf.length >= BATCH) { await pump(buf); buf = []; }
  }
  if (buf.length) await pump(buf);
  await Promise.all(inflight);

  const s = (Date.now() - t0) / 1000;
  // Report DELIVERY, not dispatch. The old summary printed ptr/s from `sent`, which made a run that
  // fetched a third of its pointers look like it was flying — 73,296 "ptr/s" against 5,187 pages/s of
  // real work. Every pointer is now accounted for: fetched, lost to a dead invoke, or failed in-Lambda.
  const accounted = fetched + empty + ptrErrs + lost;
  console.error(`\nDONE: ${sent.toLocaleString()} pointers sent in ${Math.round(s)}s`);
  console.error(`  fetched        ${fetched.toLocaleString()}  (${Math.round(fetched / s).toLocaleString()}/s effective)`);
  console.error(`  records        ${records.toLocaleString()}`);
  console.error(`  empty          ${empty.toLocaleString()}`);
  console.error(`  ptr-err        ${ptrErrs.toLocaleString()}  (fetch failures inside the Lambdas — usually S3 throttling)`);
  console.error(`  invoke-err     ${invErrs.toLocaleString()}  -> ${lost.toLocaleString()} pointer(s) lost${retried ? `, after ${retried.toLocaleString()} retry(ies)` : ''}`);
  const unacc = sent - accounted;
  if (Math.abs(unacc) > BATCH) console.error(`  UNACCOUNTED    ${unacc.toLocaleString()}  <- investigate; every pointer should land in a bucket above`);
  const pct = sent ? ((fetched / sent) * 100).toFixed(1) : '0';
  console.error(`  delivery       ${pct}% of pointers actually fetched`);
  if (Number(pct) < 90) console.error(`  NOTE: below 90% — lower LAMBDA_FETCH_CONC (now ${FETCH_CONC}) or CONCURRENCY (now ${CONC}); S3 is the limit, not Lambda.`);
  console.error(`Next: OPENSEARCH_ENDPOINT=… node load-extracted.js cc-extracted/${RUN}/`);
})().catch((e) => { console.error('drive error:', e.message); process.exit(1); });
