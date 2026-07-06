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

  const t0 = Date.now();
  let sent = 0, records = 0, fetched = 0, empty = 0, invErrs = 0, batchNo = 0;
  const inflight = new Set();

  async function fire(batch, n) {
    const payload = Buffer.from(JSON.stringify({ pointers: batch, outKey: `cc-extracted/${RUN}/b${n}.jsonl`, concurrency: 64 }));
    try {
      const res = await lambda.send(new InvokeCommand({ FunctionName: 'cc-extract', Payload: payload }));
      if (res.FunctionError) { invErrs++; }
      else { const o = JSON.parse(Buffer.from(res.Payload).toString()); records += o.written || 0; fetched += o.fetched || 0; empty += o.empty || 0; }
    } catch (e) { invErrs++; }
    sent += batch.length;
    if (n % 50 === 0) { const s = (Date.now() - t0) / 1000; console.error(`  ${sent.toLocaleString()} ptrs | ${records.toLocaleString()} records | ${Math.round(sent / s)} ptr/s | ${invErrs} inv-err`); }
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
  console.error(`DONE: ${sent.toLocaleString()} pointers -> ${records.toLocaleString()} records | ${fetched.toLocaleString()} fetched, ${empty} empty | ${invErrs} invoke-err | ${Math.round(s)}s | ${Math.round(sent / s)} ptr/s`);
  console.error(`Next: OPENSEARCH_ENDPOINT=… node load-extracted.js cc-extracted/${RUN}/`);
})().catch((e) => { console.error('drive error:', e.message); process.exit(1); });
