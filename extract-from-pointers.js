/**
 * extract-from-pointers.js — Hop 2 of the universe-refresh routine: turn discovered BIO URLs (people/team
 * pages) into PERSON contacts in the OpenSearch `contacts` index (the Contact Crawler / Master DB).
 *
 *   OPENSEARCH_ENDPOINT=… node extract-from-pointers.js --ptr bio-ptr.jsonl [--live bio-miss-urls.txt] [--tag slice]
 *
 * --ptr   : WARC pointers for bio pages resolved in Common Crawl (from cc-athena-miner --resolve-urls);
 *           fetched S3-direct, no live traffic. Source = "Common Crawl".
 * --live  : plain URL list (one per line) of bio pages NOT found in CC — fetched live through the NetNut
 *           proxy chain (cc-engine.liveFetchPage). Source = "Live Crawl". Optional.
 *
 * Mirrors the proven lambda-extract → load-extracted recipe: extractRecord → modelMissingEmails →
 * analyzePhones → opensearch.recordToDoc → opensearch.bulkUpsert (score-gated, _id = email).
 */
const fs = require('fs');
const readline = require('readline');
const path = require('path');
const extractor = require('./extractor');
const os = require('./opensearch');
const { makeCcS3 } = require('./cc-s3');
const { modelMissingEmails } = require('./email-model');
let ccEngine = null; try { ccEngine = require('./cc-engine'); } catch (e) { /* live fallback optional */ }

function argOf(flag) { const i = process.argv.indexOf(flag); return i > -1 ? process.argv[i + 1] : ''; }

(async () => {
  const ptrFile = argOf('--ptr');
  const liveFile = argOf('--live');
  const tag = argOf('--tag') || 'bio';
  if (!ptrFile && !liveFile) { console.error('need --ptr <ptr.jsonl> and/or --live <urls.txt>'); process.exit(1); }

  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const genderMap = extractor.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const fetchWarc = makeCcS3();
  const now = new Date().toISOString();
  const CONC = Number(process.env.CONC) || 12;
  const LIVE_CONC = Number(process.env.LIVE_CONC) || 4;
  const CHUNK = Number(process.env.CHUNK) || 5000;
  const tsOf = (t) => String(t || '').slice(0, 8).replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');

  const TRANSIENT = /429|rejected_execution|too_many_requests|timeout|ETIMEDOUT|ECONN|socket|Connection|hang up|EAI_AGAIN|502|503|throttl/i;
  async function withRetry(fn, tries = 5) {
    let last; for (let a = 0; a < tries; a++) { try { return await fn(); } catch (e) { last = e; if (!TRANSIENT.test(String((e && e.message) || e))) throw e; await new Promise((r) => setTimeout(r, 250 * Math.pow(2, a))); } }
    throw last;
  }

  // ---- accumulate extracted records; flush in chunks (model emails + analyze phones + upsert) ----
  let pending = [];
  let submitted = 0, withEmail = 0, upErrs = 0;
  async function flush() {
    if (!pending.length) return;
    const recs = pending; pending = [];
    try { await modelMissingEmails(recs, { dbQuery: async () => [] }); } catch (e) { /* best-effort */ }
    let out = recs; try { out = extractor.analyzePhones(recs) || recs; } catch (e) { /* best-effort */ }
    const docs = out.map((r) => os.recordToDoc(r, now)).filter((d) => d && d.email);
    withEmail += docs.length;
    for (let i = 0; i < docs.length; i += 2000) {
      const batch = docs.slice(i, i + 2000);
      try { const res = await withRetry(() => os.bulkUpsert(client, batch)); const b = res && (res.body || res); if (b && b.items) for (const it of b.items) if (it.update && it.update.error) upErrs++; submitted += batch.length; }
      catch (e) { upErrs += batch.length; }
    }
  }

  // ---- generic worker pool over a job list; each job returns {html, url, source} ----
  async function pump(jobs, conc, run, label) {
    let i = 0, fetched = 0, extracted = 0, ferr = 0; const t0 = Date.now();
    async function worker() {
      for (;;) {
        const k = i++; if (k >= jobs.length) return;
        try {
          const { html, url, source } = await run(jobs[k]);
          if (!html) continue; fetched++;
          const rec = extractor.extractRecord(html, url, { genderMap, directoryRules: {}, source, timestamp: jobs[k].ts || '', allowNoEmail: true });
          if (rec) { extracted++; pending.push(rec); if (pending.length >= CHUNK) await flush(); }
        } catch (e) { ferr++; }
        if ((k + 1) % 500 === 0) console.error(`  [${label}] ${k + 1}/${jobs.length} | fetched ${fetched} | extracted ${extracted} | ${ferr} err | ${Math.round((k + 1) / ((Date.now() - t0) / 1000))}/s`);
      }
    }
    await Promise.all(Array.from({ length: conc }, worker));
    console.error(`  [${label}] done: ${fetched} fetched, ${extracted} extracted, ${ferr} fetch-err`);
    return { fetched, extracted, ferr };
  }

  const readLines = async (f, map) => { const out = []; const rl = readline.createInterface({ input: fs.createReadStream(f), crlfDelay: Infinity }); for await (const l of rl) { if (!l.trim()) continue; const v = map(l.trim()); if (v) out.push(v); } return out; };

  const t0 = Date.now();
  // CC pointers
  if (ptrFile) {
    const ptrs = await readLines(ptrFile, (l) => { let o; try { o = JSON.parse(l); } catch { return null; } return (o && o.url && o.filename) ? { ...o, ts: tsOf(o.timestamp) } : null; });
    console.error(`CC bio pointers: ${ptrs.length.toLocaleString()}`);
    await pump(ptrs, CONC, async (p) => ({ html: await withRetry(() => fetchWarc({ url: p.url, filename: p.filename, offset: p.offset, length: p.length })), url: p.url, source: 'Common Crawl' }), 'cc');
  }
  // Live fallback
  if (liveFile) {
    if (!ccEngine || typeof ccEngine.liveFetchPage !== 'function') {
      console.error('live fallback requested but cc-engine.liveFetchPage unavailable — skipping live');
    } else {
      const urls = await readLines(liveFile, (l) => (/^https?:\/\//i.test(l) ? { url: l } : { url: 'https://' + l }));
      console.error(`live bio URLs: ${urls.length.toLocaleString()}`);
      await pump(urls, LIVE_CONC, async (u) => ({ html: await ccEngine.liveFetchPage(u.url), url: u.url, source: 'Live Crawl' }), 'live');
    }
  }
  await flush();
  console.error(`DONE: ${withEmail.toLocaleString()} email-bearing contacts, ${submitted.toLocaleString()} upserted to contacts index, ${upErrs} upsert-err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
