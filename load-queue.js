/**
 * load-queue.js — stream a WARC-pointer JSONL file into the Postgres crawl_queue.
 * ------------------------------------------------------------------------------
 * Input is the `--warc-out` JSONL from cc-athena-miner / cc-domain-miner: one
 * {url, filename, offset, length, timestamp} per line. The file is large (the full
 * columnar harvest is ~1 GB / 3.73M lines), so it's STREAMED line-by-line and
 * enqueued in chunks — never read whole into memory. Idempotent: re-running skips
 * URLs already queued (ON CONFLICT DO NOTHING).
 *
 *   node load-queue.js cc-warc.jsonl                 # load pointers into the queue
 *   node load-queue.js --selftest                    # offline test
 *
 * Env: DATABASE_URL, PGSSL=1 for TLS, LOAD_CHUNK (default 5000).
 */
const fs = require('fs');
const readline = require('readline');

// Stream a JSONL pointer file through `queue.enqueueMany` in chunks. Testable: pass a fake queue.
// Returns { lines, parsed, inserted, bad }.
async function loadFile(filePath, { queue, chunkSize = 5000, onProgress = () => {} } = {}) {
  if (!queue) throw new Error('loadFile: queue required');
  const rl = readline.createInterface({ input: fs.createReadStream(filePath), crlfDelay: Infinity });
  let lines = 0, parsed = 0, inserted = 0, bad = 0;
  let chunk = [];
  const flush = async () => {
    if (!chunk.length) return;
    const r = await queue.enqueueMany(chunk);
    inserted += r.inserted;
    chunk = [];
    onProgress({ lines, parsed, inserted });
  };
  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    lines++;
    let o; try { o = JSON.parse(t); } catch (e) { bad++; continue; }
    if (!o || !o.url) { bad++; continue; }
    parsed++;
    chunk.push({ url: o.url, filename: o.filename, offset: o.offset, length: o.length, timestamp: o.timestamp });
    if (chunk.length >= chunkSize) await flush();
  }
  await flush();
  return { lines, parsed, inserted, bad };
}

module.exports = { loadFile };

// ---------------------------------------------------------------------------------------------
if (require.main === module) {
  if (process.argv.includes('--selftest')) { runSelftest(); }
  else { runMain().catch((e) => { console.error('load-queue fatal:', e); process.exit(1); }); }
}

async function runMain() {
  const file = process.argv.find((a) => !a.startsWith('-') && /\.jsonl?$/i.test(a)) || process.argv[2];
  if (!file || !fs.existsSync(file)) { console.error('usage: node load-queue.js <pointers.jsonl>'); process.exit(2); }
  const { makeDb } = require('./db-pg');
  const { makeQueue } = require('./crawl-queue');
  const dbpg = await makeDb({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL });
  const queue = await makeQueue(dbpg._pool);
  const chunkSize = Number(process.env.LOAD_CHUNK) || 5000;
  console.log(`Loading ${file} into crawl_queue (chunk ${chunkSize})…`);
  const t0 = Date.now();
  const res = await loadFile(file, {
    queue, chunkSize,
    onProgress: ({ lines, inserted }) => { if (lines % 50000 === 0) console.log(`  …${lines.toLocaleString()} lines, ${inserted.toLocaleString()} new in queue`); },
  });
  const secs = Math.round((Date.now() - t0) / 1000);
  console.log(`Done: ${res.parsed.toLocaleString()} pointers parsed, ${res.inserted.toLocaleString()} newly queued, ${res.bad} bad line(s), ${secs}s.`);
  console.log('Queue stats:', JSON.stringify(await queue.stats()));
  await dbpg.close();
}

async function runSelftest() {
  const os = require('os'); const path = require('path');
  let pass = 0, fail = 0;
  const ok = (n, c) => { if (c) { pass++; console.log('  ✓', n); } else { fail++; console.log('  ✗', n); } };

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lq-'));
  const f = path.join(dir, 'p.jsonl');
  fs.writeFileSync(f, [
    JSON.stringify({ url: 'https://a.com/1', filename: 'w1', offset: '10', length: '20', timestamp: '20260601' }),
    '',                                                   // blank line -> skipped
    '{not json',                                          // bad line -> counted bad
    JSON.stringify({ filename: 'w2' }),                   // no url -> bad
    JSON.stringify({ url: 'https://a.com/2', filename: 'w3', offset: '30', length: '40' }),
    JSON.stringify({ url: 'https://a.com/3' }),
  ].join('\n') + '\n');

  const seen = [];
  const queue = { async enqueueMany(ptrs) { seen.push(...ptrs); return { inserted: ptrs.length }; } };
  const res = await loadFile(f, { queue, chunkSize: 2 });

  ok('parses the valid pointers, skips blank/bad/url-less lines', res.parsed === 3 && res.bad === 2);
  ok('inserts all parsed pointers', res.inserted === 3 && seen.length === 3);
  ok('preserves the WARC pointer fields', seen[0].url === 'https://a.com/1' && seen[0].filename === 'w1' && seen[0].offset === '10' && seen[0].length === '20');
  ok('chunking does not drop rows across chunk boundary', seen.map((s) => s.url).join(',') === 'https://a.com/1,https://a.com/2,https://a.com/3');

  try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) { /* temp */ }
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
}
