/**
 * enrich-drive.js — fan resolved home-page pointers out across cc-enrich Lambdas (the Hop-1 driver).
 * -------------------------------------------------------------------------------------------------
 * Joins the Athena-resolved home pointers with the dumped company targets (id / website / address /
 * phone), batches them, and invokes the `cc-enrich` Lambda concurrently. Each Lambda reads
 * s3://commoncrawl direct, enriches, and writes company-update JSONL to S3 under cc-enriched/<run>/.
 * Then `load-enriched.js cc-enriched/<run>/` applies it to the companies index + emits the bio URLs.
 *
 *   BATCH=200 CONCURRENCY=100 RUN=2026-25 CRAWL=CC-MAIN-2026-25 \
 *     node enrich-drive.js homeptr.jsonl targets.ndjson
 */
const fs = require('fs');
const https = require('https');
const readline = require('readline');
const { LambdaClient, InvokeCommand, GetAccountSettingsCommand } = require('@aws-sdk/client-lambda');
const co = require('./companies');

(async () => {
  const ptrFile = process.argv[2], targetsFile = process.argv[3];
  if (!ptrFile || !targetsFile) { console.error('usage: node enrich-drive.js <homeptr.jsonl> <targets.ndjson>'); process.exit(1); }
  const BATCH = Number(process.env.BATCH) || 200;
  const RUN = process.env.RUN || ('run' + Date.now());
  const CRAWL = process.env.CRAWL || '';
  const region = process.env.AWS_REGION || 'us-east-1';
  let requestHandler;
  try { const { NodeHttpHandler } = require('@smithy/node-http-handler'); requestHandler = new NodeHttpHandler({ httpsAgent: new https.Agent({ maxSockets: 4000, keepAlive: true }) }); } catch (e) { /* default */ }
  const lambda = new LambdaClient({ region, maxAttempts: 6, requestHandler });

  let limit = 10;
  try { limit = (await lambda.send(new GetAccountSettingsCommand({}))).AccountLimit.UnreservedConcurrentExecutions || 10; } catch (e) { /* default */ }
  const CONC = Math.max(1, Math.min(Number(process.env.CONCURRENCY) || 100, limit - 2));
  console.error(`Lambda concurrency limit ${limit} -> driving at ${CONC} concurrent`);

  // the alternate-website admin list flows into every Lambda so reclassification matches the app
  let altList; try { altList = await co.getAltWebsites(co.makeClient(process.env.OPENSEARCH_ENDPOINT)); } catch (e) { altList = undefined; }

  // load the domain -> {id,website,full_address,phone} map (targets.ndjson from dump-company-urls)
  const targets = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(targetsFile), crlfDelay: Infinity });
    for await (const l of rl) { if (!l.trim()) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.domain) targets.set(o.domain, o); } }
  console.error(`targets: ${targets.size.toLocaleString()}`);
  const hostOf = (u) => { try { return new URL(u).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; } };

  const t0 = Date.now();
  let sent = 0, updated = 0, fetched = 0, bio = 0, invErrs = 0, batchNo = 0;
  const inflight = new Set();
  async function fire(batch, n) {
    const payload = Buffer.from(JSON.stringify({ companies: batch, outKey: `cc-enriched/${RUN}/b${n}.jsonl`, crawl: CRAWL, altList, concurrency: 48 }));
    try {
      const res = await lambda.send(new InvokeCommand({ FunctionName: 'cc-enrich', Payload: payload }));
      if (res.FunctionError) invErrs++;
      else { const o = JSON.parse(Buffer.from(res.Payload).toString()); updated += o.updated || 0; fetched += o.fetched || 0; bio += o.bioUrls || 0; }
    } catch (e) { invErrs++; }
    sent += batch.length;
    if (n % 50 === 0) { const s = (Date.now() - t0) / 1000; console.error(`  ${sent.toLocaleString()} sent | ${updated.toLocaleString()} enriched | ${bio.toLocaleString()} bio | ${Math.round(sent / s)}/s | ${invErrs} inv-err`); }
  }
  async function pump(batch) { const n = batchNo++; const p = fire(batch, n).finally(() => inflight.delete(p)); inflight.add(p); if (inflight.size >= CONC) await Promise.race(inflight); }

  const rl = readline.createInterface({ input: fs.createReadStream(ptrFile), crlfDelay: Infinity });
  let buf = [];
  for await (const line of rl) {
    const t = line.trim(); if (!t) continue;
    let o; try { o = JSON.parse(t); } catch { continue; }
    if (!o || !o.url || !o.filename) continue;
    const c = targets.get(hostOf(o.url)); if (!c) continue;                 // no matching company -> skip
    buf.push({ url: o.url, filename: o.filename, offset: o.offset, length: o.length, timestamp: o.timestamp, id: c.id, domain: c.domain, website: c.website || '', full_address: c.full_address || '', phone: c.phone || '' });
    if (buf.length >= BATCH) { await pump(buf); buf = []; }
  }
  if (buf.length) await pump(buf);
  await Promise.all(inflight);

  const s = (Date.now() - t0) / 1000;
  console.error(`DONE: ${sent.toLocaleString()} companies -> ${updated.toLocaleString()} enriched | ${bio.toLocaleString()} bio URLs | ${invErrs} invoke-err | ${Math.round(s)}s | ${Math.round(sent / s)}/s`);
  console.error(`Next: OPENSEARCH_ENDPOINT=… OUT_BUCKET=… BIO_OUT=${RUN}-bio.txt node load-enriched.js cc-enriched/${RUN}/`);
})().catch((e) => { console.error('drive error:', e.message); process.exit(1); });
