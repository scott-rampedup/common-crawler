#!/usr/bin/env node
//
// sitemap-library-replace.js — replace the Sitemap Library with a curated people-only export, keeping
// the operational history of every sitemap that survives.
//
// WHY THIS IS NOT A DELETE-THEN-LOAD. The Library is not just a list of URLs; each row accumulates state
// that only exists because a monitor pass put it there -- last_checked, total_new, zero_passes (which is
// what retires a dead sitemap after N empty passes), and the monitored opt-out. Measured 2026-11-14:
// 428,967 of 433,656 rows carried some of it. Dropping the index and reloading would hand every surviving
// sitemap back to the monitor as never-checked, so the known-dead ones get re-crawled from scratch.
//
// So: UPSERT every row in the file (which refreshes the metadata and leaves the monitor fields alone),
// then PRUNE whatever the file did not mention. Survivors keep their history; the rest are removed.
//
// The prune keys on last_seen rather than a list of ids. Every upsert in phase 2 stamps last_seen=runStart,
// so "not in the file" is exactly "last_seen is older than this run" -- no 200k-element id list, and no
// second pass over the CSV. A row another process touches mid-run keeps its newer last_seen and is spared,
// which is the safe direction to be wrong in.
//
// ABOUT THE FILE (Sitemap Library Only.csv, 197,235 rows, 2026-11-13):
//   Kind  is blank on every row. These are people directories by construction, so kind is forced to
//         'People' -- recorded here because the file itself does not say so.
//   Type  is blank on 75% and arrives as 'child'/'Child'/'Directory'; anything not already one of the
//         three allowed values is derived from the URL by sitemaps.deriveType, same as everywhere else.
//   Count is missing on 14,545 rows, and Industry/Sector/Size on ~19%. Writing those through as 0 or ''
//         would erase counts the Library learned by actually crawling, so EMPTY CELLS ARE NOT WRITTEN.
//         That is the whole reason this does its own bulk call instead of sitemaps.bulkUpsert, which
//         writes `d.url_count || 0` unconditionally.
//   Col 10 is labelled 'Sitemap URL' a second time but holds the domain. Ignored; column 2 has it.
//
//   node sitemap-library-replace.js --file "../Sitemap Library Only.csv"            # dry run
//   node sitemap-library-replace.js --s3 s3://bucket/key.csv --write
//   node sitemap-library-replace.js --s3 … --write --no-prune                       # load only, keep extras
const fs = require('fs');
const zlib = require('zlib');
const sitemaps = require('./sitemaps');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const argv = process.argv.slice(2);
const arg = (n, d) => { const i = argv.indexOf('--' + n); return i > -1 && argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : d; };
const has = (f) => argv.includes('--' + f);

const FILE = arg('file', '');
const S3URL = arg('s3', '');
const WRITE = has('write');
const PRUNE = !has('no-prune');
const BACKUP = !has('no-backup');
const LIMIT = Number(arg('limit', 0)) || 0;
const REGION = process.env.AWS_REGION || 'us-east-1';
const BUCKET = process.env.OUT_BUCKET || `aws-athena-query-results-475987770186-${REGION}`;
const BATCH = 1000;

const s3 = new S3Client({ region: REGION });
const f = (n) => Number(n || 0).toLocaleString();

// Quote-aware CSV parse (quotes, embedded commas, embedded newlines) — same shape as sitemap-csv-load.
function* parseCsv(text) {
  let field = '', row = [], q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
    else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); yield row; row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); yield row; }
}

const readBody = async (b) => { const c = []; for await (const x of b) c.push(x); return Buffer.concat(c); };
const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./i, '').toLowerCase(); } catch (e) { return ''; } };
const normDomain = (d) => String(d || '').toLowerCase().trim()
  .replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];

const TYPES = ['Parent', 'Child', 'Sub-Domain'];
const canonType = (raw, url, domain) => {
  const t = String(raw || '').trim().toLowerCase();
  const hit = TYPES.find((x) => x.toLowerCase() === t);
  return hit || sitemaps.deriveType(url, domain);     // blank, 'Directory', anything else -> derive it
};

// Stream the whole Library to a gzipped NDJSON in S3 before anything is changed. 433k docs is ~25MB
// gzipped; small enough for one PutObject and the only way back if the prune turns out to be wrong.
async function backup(client, stamp) {
  const key = `sitemap-library-backups/sitemaps-${stamp}.ndjson.gz`;
  const gz = zlib.createGzip();
  const chunks = [];
  gz.on('data', (d) => chunks.push(d));
  const done = new Promise((res, rej) => { gz.on('end', res); gz.on('error', rej); });
  let n = 0, after = null;
  for (;;) {
    const body = { size: 2000, query: { match_all: {} }, sort: [{ sitemap_url: 'asc' }] };
    if (after) body.search_after = after;
    const r = await client.search({ index: sitemaps.INDEX, body });
    const hits = ((r.body || r).hits.hits) || [];
    if (!hits.length) break;
    for (const h of hits) { gz.write(JSON.stringify(h._source) + '\n'); n++; }
    after = hits[hits.length - 1].sort;
    if (n % 50000 === 0) console.log(`    backed up ${f(n)}…`);
  }
  gz.end();
  await done;
  const buf = Buffer.concat(chunks);
  await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: buf, ContentType: 'application/gzip' }));
  console.log(`  backup: ${f(n)} doc(s), ${(buf.length / 1048576).toFixed(1)}MB -> s3://${BUCKET}/${key}`);
  return { n, key };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!FILE && !S3URL) { console.error('need --file or --s3'); process.exit(1); }

  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(client);                 // adds sector + company_employee_size_range in place

  const before = ((await client.count({ index: sitemaps.INDEX })).body || {}).count || 0;
  console.log(`${WRITE ? 'WRITE' : 'DRY RUN'} — Library holds ${f(before)} doc(s) before this run\n`);

  let text;
  if (S3URL) {
    const m = /^s3:\/\/([^/]+)\/(.+)$/.exec(S3URL);
    if (!m) { console.error('bad --s3 url'); process.exit(1); }
    console.log(`reading ${S3URL}`);
    text = (await readBody((await s3.send(new GetObjectCommand({ Bucket: m[1], Key: m[2] }))).Body)).toString('utf8');
  } else {
    console.log(`reading ${FILE}`);
    text = fs.readFileSync(FILE, 'utf8');
  }

  const stamp = new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);
  const runStart = new Date().toISOString();

  if (BACKUP && WRITE) { console.log('\nphase 1 — backup'); await backup(client, stamp); }
  else console.log(BACKUP ? '\nphase 1 — backup SKIPPED (dry run)' : '\nphase 1 — backup SKIPPED (--no-backup)');

  // ---- phase 2: upsert every row -------------------------------------------------------------
  console.log('\nphase 2 — upsert');
  const stat = { rows: 0, bad: 0, dup: 0, upserted: 0, inserted: 0, errors: 0,
    derivedType: 0, noCount: 0, noIndustry: 0, noSector: 0, noSize: 0 };
  const seen = new Set();
  let header = true, batch = [];

  const flush = async () => {
    if (!batch.length) return;
    if (!WRITE) { stat.upserted += batch.length; batch = []; return; }
    const body = [];
    for (const d of batch) {
      const partial = { ...d.partial, last_seen: runStart };
      const full = {
        sitemap_url: d.id, parent_url: '', keyword: '', url_count: 0, item_count: 0, ratio: 0,
        by_name: false, industry: '', company_id: '', lastmod: '',
        source: 'sitemap-library-only', discovered_at: runStart, monitored: true, ...partial,
      };
      body.push({ update: { _index: sitemaps.INDEX, _id: d.id } });
      body.push({ doc: partial, upsert: full });
    }
    const r = await client.bulk({ body, refresh: false });
    for (const it of (((r.body || r).items) || [])) {
      const u = it.update || {};
      if (u.error) { stat.errors++; continue; }
      stat.upserted++;
      if (u.result === 'created') stat.inserted++;       // needed to reconcile the prune against the counts
    }
    batch = [];
  };

  for (const cols of parseCsv(text)) {
    if (header) { header = false; continue; }
    if (cols.length === 1 && !String(cols[0]).trim()) continue;
    if (LIMIT && stat.rows >= LIMIT) break;
    stat.rows++;

    const url = String(cols[0] || '').trim();
    if (!/^https?:\/\/\S+/i.test(url) || Buffer.byteLength(url) > 512) { stat.bad++; continue; }
    if (seen.has(url)) { stat.dup++; continue; }
    seen.add(url);

    const domain = normDomain(cols[1]) || hostOf(url);
    if (!domain) { stat.bad++; continue; }
    const rawType = String(cols[3] || '').trim();
    const type = canonType(rawType, url, domain);
    if (!TYPES.some((x) => x.toLowerCase() === rawType.toLowerCase())) stat.derivedType++;

    // Only fields the row actually carries. An empty cell means "the file does not know", NOT "set to
    // blank" — the Library may already hold a better value learned by crawling.
    const partial = { domain, kind: 'People', type, status: 'active' };
    const keyword = String(cols[4] || '').trim().toLowerCase();
    if (keyword) partial.keyword = keyword;
    const count = Number(String(cols[5] || '').replace(/[^0-9]/g, ''));
    if (count > 0) { partial.url_count = count; partial.item_count = count; } else stat.noCount++;
    const size = String(cols[6] || '').trim();
    if (size) partial.company_employee_size_range = size; else stat.noSize++;
    const industry = String(cols[7] || '').trim().toLowerCase();   // lower-case: matches the 300k+ already stored
    if (industry) partial.industry = industry; else stat.noIndustry++;
    const sector = String(cols[8] || '').trim();
    if (sector) partial.sector = sector; else stat.noSector++;

    batch.push({ id: url, partial });
    if (batch.length >= BATCH) await flush();
  }
  await flush();
  if (WRITE) { try { await client.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* best-effort */ } }

  console.log(`  rows read            ${f(stat.rows)}`);
  console.log(`  unusable / duplicate ${f(stat.bad)} / ${f(stat.dup)}`);
  console.log(`  ${WRITE ? 'upserted' : 'would upsert'}       ${f(stat.upserted)}`
    + `${WRITE ? `   (${f(stat.inserted)} new, ${f(stat.upserted - stat.inserted)} updated in place)` : ''}`
    + `${stat.errors ? `   errors ${f(stat.errors)}` : ''}`);
  console.log(`  Type derived from URL ${f(stat.derivedType)}`);
  console.log(`  cells left untouched: Count ${f(stat.noCount)} · Industry ${f(stat.noIndustry)} · Sector ${f(stat.noSector)} · Size ${f(stat.noSize)}`);

  // ---- phase 3: prune anything the file did not mention ---------------------------------------
  if (!PRUNE) { console.log('\nphase 3 — prune SKIPPED (--no-prune)'); }
  else {
    console.log('\nphase 3 — prune');
    const stale = { bool: { should: [
      { range: { last_seen: { lt: runStart } } },
      { bool: { must_not: { exists: { field: 'last_seen' } } } },
    ], minimum_should_match: 1 } };
    if (!WRITE) {
      // A dry run upserts nothing, so NOTHING has a refreshed last_seen and the stale query matches the
      // whole index. Reporting that as "would delete 433,656" is both wrong and the kind of cry-wolf that
      // gets a dry run ignored, so count the real thing: how many ids in the file already exist.
      const ids = [...seen];
      let present = 0;
      for (let i = 0; i < ids.length; i += 1000) {
        const r = await client.mget({ index: sitemaps.INDEX, body: { ids: ids.slice(i, i + 1000) },
          _source: false });
        for (const d of ((r.body || r).docs || [])) if (d.found) present++;
      }
      console.log(`  file ids already in the Library  ${f(present)}  (${((present / ids.length) * 100).toFixed(1)}%)`);
      console.log(`  file ids that are new            ${f(ids.length - present)}`);
      console.log(`  WOULD DELETE                     ${f(before - present)}  (held now, not in the file)`);
      console.log(`  Library would end at             ${f(ids.length)}`);
      console.log('  (dry run — nothing deleted)');
    } else {
    const n = ((await client.count({ index: sitemaps.INDEX, body: { query: stale } })).body || {}).count || 0;
    console.log(`  ${f(n)} doc(s) not present in the file`);
    if (n) {
      const r = await client.deleteByQuery({ index: sitemaps.INDEX, body: { query: stale },
        refresh: true, conflicts: 'proceed', wait_for_completion: true });
      const b = r.body || r;
      // b.deleted has been seen to under-report badly (51,205 against 252,913 actually removed on the
      // 2026-11-14 run) while the end state was correct, so it is printed as the API's claim and the
      // real figure is taken from the counts either side. Never report a number you cannot stand behind.
      console.log(`  deleteByQuery reported ${f(b.deleted)} deleted, ${f(b.total)} matched`
        + `${b.failures && b.failures.length ? `, failures ${b.failures.length}` : ''}`);
    }
    }
  }

  if (WRITE) { try { await client.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* best-effort */ } }
  const after = ((await client.count({ index: sitemaps.INDEX })).body || {}).count || 0;
  console.log(`\nLibrary: ${f(before)} -> ${f(after)}`);
  if (WRITE) console.log(`  net ${after >= before ? '+' : ''}${f(after - before)}  (removed ${f(before + stat.inserted - after)} by the counts)`);
  if (!WRITE) console.log('(dry run — pass --write to apply)');
})().catch((e) => { console.error('FAILED:', e.message, e.stack); process.exit(1); });
