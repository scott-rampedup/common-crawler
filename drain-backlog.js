/**
 * drain-backlog.js — export the crawl_queue's PENDING WARC pointers to a JSONL for the Lambda pipeline.
 * Lets us Lambda-extract the abandoned queue backlog (fast, S3-direct) instead of the throttled fleet.
 * Keyset-paginated by url so it streams millions without a cursor.
 *
 *   DATABASE_URL=… node drain-backlog.js /tmp/backlog.jsonl
 */
const { Pool } = require('pg');
const fs = require('fs');

(async () => {
  const out = process.argv[2] || '/tmp/backlog.jsonl';
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2, ssl: !!process.env.PGSSL });
  const ws = fs.createWriteStream(out);
  let last = '', n = 0;
  for (;;) {
    const r = await pool.query(
      "SELECT url, warc_filename, warc_offset, warc_length, warc_timestamp FROM crawl_queue " +
      "WHERE status = 'pending' AND warc_filename IS NOT NULL AND url > $1 ORDER BY url LIMIT 50000", [last]);
    if (!r.rows.length) break;
    for (const row of r.rows) {
      if (!ws.write(JSON.stringify({ url: row.url, filename: row.warc_filename, offset: row.warc_offset, length: row.warc_length, timestamp: row.warc_timestamp }) + '\n')) {
        await new Promise((res) => ws.once('drain', res));
      }
      n++;
    }
    last = r.rows[r.rows.length - 1].url;
    if (n % 500000 === 0) console.error(`  exported ${n.toLocaleString()}…`);
  }
  await new Promise((res) => ws.end(res));
  await pool.end();
  console.log(`EXPORTED ${n.toLocaleString()} pointers -> ${out}`);
})().catch((e) => { console.error('export error:', e.message); process.exit(1); });
