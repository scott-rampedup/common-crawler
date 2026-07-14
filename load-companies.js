/**
 * load-companies.js — stream the gzip company NDJSON into the `companies` OpenSearch index.
 *   OPENSEARCH_ENDPOINT=… node load-companies.js "../free_company_dataset.json.zip"
 * Idempotent (index by id). Run locally; the Fly app only queries the index.
 */
const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const co = require('./companies');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const file = process.argv[2];
  if (!file) { console.error('usage: node load-companies.js <company.json.gz>'); process.exit(1); }
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await co.ensureIndex(client);
  const CONC = Number(process.env.LOAD_CONC) || 8;
  const CHUNK = Number(process.env.CHUNK) || 4000;
  console.error(`loading ${file} -> ${co.INDEX} (conc ${CONC}, chunk ${CHUNK})`);

  async function upsertRetry(docs) {
    for (let a = 0; ; a++) {
      try { return (await co.bulkIndex(client, docs)).errors || 0; }
      catch (e) { if (a >= 6) throw e; await sleep(Math.min(16000, 500 * 2 ** a)); }
    }
  }
  let total = 0, errs = 0, batch = [];
  const inflight = new Set();
  const t0 = Date.now();
  async function flush(docs) {
    const p = upsertRetry(docs).then((e) => { errs += e; }).finally(() => inflight.delete(p));
    inflight.add(p);
    if (inflight.size >= CONC) await Promise.race(inflight);
  }
  const rl = readline.createInterface({ input: fs.createReadStream(file).pipe(zlib.createGunzip()), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    let o; try { o = JSON.parse(line); } catch (e) { continue; }
    if (!o.id) continue;
    batch.push(co.recordToDoc(o)); total++;
    if (batch.length >= CHUNK) {
      await flush(batch); batch = [];
      if (total % (CHUNK * 50) === 0) { const s = (Date.now() - t0) / 1000; console.error(`  ${total.toLocaleString()} docs | ${Math.round(total / s)}/s | ${errs} err`); }
    }
  }
  if (batch.length) await flush(batch);
  await Promise.all(inflight);
  console.error(`DONE: ${total.toLocaleString()} companies -> ${co.INDEX}, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('load error:', e.message); process.exit(1); });
