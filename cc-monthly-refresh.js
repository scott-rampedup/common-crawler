/**
 * cc-monthly-refresh.js — the monthly Common Crawl pass over the Company Crawler, which also feeds the
 * Contact Crawler bio URLs on the same cadence.
 * ---------------------------------------------------------------------------------------------
 * Why this exists: cc_crawl is set on 14 of 30,733,034 company records. The home-page enrichment has
 * never run at scale, which is why logo, map and description sit near 0% while being 71% present on the
 * handful of records that WERE crawled. enrich-companies-cc.js is the local trickle version — it resolves
 * one URL at a time through index.commoncrawl.org, which refuses the Fly IP and caps out around 200
 * companies a run. This is the bulk version, and it runs where the data is.
 *
 * Everything here is orchestration of parts that already work:
 *   cc-athena-miner  normalizeKey / buildKeysTableSql / buildResolveSql   exact-URL -> WARC pointer
 *                    buildDomainDiscoverySql                              bio pages on our domains
 *   cc-engine        fetchWarc / warcToHtml                               pointer -> HTML
 *   cc-home-enrich   enrichFromHtml                                       HTML -> company fields
 *
 * Two halves, run in order:
 *   A. REFRESH   due companies -> home-page (or location-page) pointers -> enrich -> write fields
 *   B. DISCOVER  the same domains -> bio-page pointers -> monitor-queue/pending/
 *      The pump drains that prefix continuously, so bio URLs arrive on the refresh cadence with no
 *      second schedule to keep in step and nothing to switch on.
 *
 * The LOCATION rule is honoured here: a record flagged Location contributes its OWN page_url and
 * nothing else, so a location is never enriched from the shared company home page. A Location with no
 * page_url is skipped rather than fall back to the home page — Bing locations have no own page at all.
 *
 *   node cc-monthly-refresh.js --dry                 # select + report, no AWS spend, no writes
 *   node cc-monthly-refresh.js --max 50000
 *   node cc-monthly-refresh.js --refresh-only | --discover-only
 *
 * Cost: a filtered single-crawl Athena query scans ~14GB (~$0.07). Both halves are one query each.
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const readline = require('readline');
const co = require('./companies');
const cc = require('./cc-engine');
const che = require('./cc-home-enrich');
const ex = require('./extractor');
const miner = require('./cc-athena-miner');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const argOf = (f, d) => { const i = args.indexOf('--' + f); return i > -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : d; };
const DRY = has('--dry');
const MAX = Number(argOf('max', '')) || Number(process.env.CC_REFRESH_MAX) || 100000;
const REFRESH_DAYS = Number(argOf('days', '')) || Number(process.env.CC_REFRESH_DAYS) || 30;
const CONC = Number(argOf('conc', '')) || Number(process.env.CC_REFRESH_CONC) || 8;
const DO_REFRESH = !has('--discover-only');
const DO_DISCOVER = !has('--refresh-only');
const BUCKET = process.env.CC_ATHENA_BUCKET || process.env.ATHENA_BUCKET || '';
const QUEUE_BUCKET = process.env.MONITOR_QUEUE_BUCKET || process.env.S3_BUCKET || '';
const QUEUE_PREFIX = process.env.MONITOR_QUEUE_PREFIX || 'monitor-queue/pending/';
const N = (x) => Number(x || 0).toLocaleString();
const stamp = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14);

// ---- selection ---------------------------------------------------------------------------------
// Due = never refreshed, or refreshed longer ago than REFRESH_DAYS. Ordered by contact_count desc so a
// capped run spends its budget on the companies that carry the most contacts, not on an arbitrary slice.
function dueQuery(cutoffIso) {
  return {
    bool: {
      must: [{ exists: { field: 'domain' } }],
      must_not: [{ term: { domain: '' } }],
      should: [
        { bool: { must_not: [{ exists: { field: 'cc_refreshed_at' } }] } },
        { range: { cc_refreshed_at: { lt: cutoffIso } } },
      ],
      minimum_should_match: 1,
    },
  };
}

async function selectDue(client) {
  const cutoff = new Date(Date.now() - REFRESH_DAYS * 86400000).toISOString();
  const query = dueQuery(cutoff);
  const out = [];
  let after = null;
  // search_after on (contact_count desc, id asc): a stable total order, and no 10k window limit.
  while (out.length < MAX) {
    const body = {
      size: Math.min(2000, MAX - out.length),
      query,
      _source: ['domain', 'website', 'page_url', 'company_type', 'full_address', 'phone', 'postal_code',
                'locality', 'region', 'country', 'contact_count'],
      sort: [{ contact_count: { order: 'desc', missing: '_last' } }, { id: 'asc' }],
    };
    if (after) body.search_after = after;
    const r = await client.search({ index: co.INDEX, body }, { requestTimeout: 300000 }).then((x) => (x.body || x));
    const hits = r.hits.hits || [];
    if (!hits.length) break;
    for (const h of hits) out.push({ id: h._id, ...h._source });
    after = hits[hits.length - 1].sort;
  }
  return { targets: out, cutoff };
}

// The URL this record should be enriched FROM. A Location is its own single page, never the home page.
function targetUrl(t) {
  const isLoc = String(t.company_type || '') === 'Location';
  if (isLoc) return String(t.page_url || '').trim() || '';
  const dom = String(t.domain || '').trim();
  if (!dom) return '';
  const w = String(t.website || '').trim();
  if (/^https?:\/\//i.test(w)) return w;
  return 'https://' + dom + '/';
}

// ---- half A: refresh ---------------------------------------------------------------------------
async function refresh(client, targets, A, output, crawls, genderMap, altList) {
  // key -> the records that asked for it. One URL can serve several records (a domain with several
  // rows), so the pointer result fans back out rather than matching one-to-one.
  const byKey = new Map();
  let noUrl = 0, badKey = 0;
  for (const t of targets) {
    const url = targetUrl(t);
    if (!url) { noUrl++; continue; }
    const k = miner.normalizeKey(url);
    if (!k) { badKey++; continue; }
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(t);
  }
  console.error(`  keys: ${N(byKey.size)} distinct (${N(noUrl)} record(s) with no usable URL, ${N(badKey)} unparseable)`);
  if (!byKey.size) return { found: 0, updated: 0 };

  const keysFile = path.join(os.tmpdir(), `cc-refresh-keys-${stamp}.tsv`);
  fs.writeFileSync(keysFile, [...byKey.keys()].join('\n') + '\n');
  const prefix = `cc-refresh-keys/${stamp}/`;
  if (DRY) { console.error(`  [dry] would upload ${N(byKey.size)} key(s) to s3://${BUCKET}/${prefix}`); return { found: 0, updated: 0 }; }

  await A.s3.send(new A.PutObjectCommand({ Bucket: BUCKET, Key: prefix + 'keys.tsv', Body: fs.readFileSync(keysFile) }));
  const keysTable = `refresh_keys_${stamp}`;
  await miner.runAthena(A, miner.buildKeysTableSql({ keysTable, bucket: BUCKET, prefix }), output, 'create keys table');
  const res = await miner.runAthena(A, miner.buildResolveSql({ crawls, keysTable }), output, 'resolve home pages');

  // Stream the pointer rows and enrich as they arrive, bounded concurrency.
  const rows = [];
  await miner.s3StreamRows(A, res.output, (cols) => { rows.push(cols); });
  const header = rows.shift();                      // url,filename,offset,length,timestamp
  console.error(`  pointers: ${N(rows.length)} (header: ${header && header.join(',')})`);

  let i = 0, found = 0, updated = 0, errs = 0, withLogo = 0, withPostal = 0, bioUrls = 0;
  const t0 = Date.now();
  const worker = async () => {
    for (;;) {
      const k = i++;
      if (k >= rows.length) return;
      const [url, filename, offset, length] = rows[k];
      const key = miner.normalizeKey(url);
      const recs = (key && byKey.get(key)) || [];
      if (!recs.length) continue;
      try {
        const warc = await cc.fetchWarc({ filename, offset: Number(offset), length: Number(length) });
        const html = cc.warcToHtml(warc);
        if (!html) continue;
        found++;
        for (const rec of recs) {
          const r = che.enrichFromHtml(rec, html, { genderMap, altList, now: new Date().toISOString(), crawl: crawls[0] });
          if (r.updates.logo) withLogo++;
          if (r.updates.postal_code) withPostal++;
          if (r.updates.bio_url) bioUrls += r.updates.bio_url.split(';').filter(Boolean).length;
          await co.update(client, rec.id, r.updates);
          updated++;
        }
      } catch (e) { errs++; }
      if ((k + 1) % 500 === 0) {
        const el = (Date.now() - t0) / 1000;
        console.error(`    ${N(k + 1)}/${N(rows.length)} | found ${N(found)} | updated ${N(updated)} | ${errs} err | ${((k + 1) / el).toFixed(1)}/s`);
      }
    }
  };
  await Promise.all(Array.from({ length: CONC }, worker));
  console.error(`  refresh done: ${N(found)} page(s) read, ${N(updated)} record(s) updated, ${N(withLogo)} logo, ${N(withPostal)} postal code, ${N(bioUrls)} bio URL(s), ${errs} error(s)`);
  return { found, updated, withLogo, withPostal, bioUrls };
}

// ---- half B: bio-page discovery on the same domains --------------------------------------------
async function discover(targets, A, output, crawls) {
  const domains = [...new Set(targets.map((t) => String(t.domain || '').trim().toLowerCase()).filter(Boolean))];
  console.error(`  domains for bio discovery: ${N(domains.length)}`);
  if (!domains.length) return { pointers: 0 };
  if (DRY) { console.error(`  [dry] would discover bio pages across ${N(domains.length)} domain(s)`); return { pointers: 0 }; }

  const prefix = `cc-refresh-domains/${stamp}/`;
  await A.s3.send(new A.PutObjectCommand({ Bucket: BUCKET, Key: prefix + 'domains.tsv', Body: Buffer.from(domains.join('\n') + '\n') }));
  const domainsTable = `refresh_domains_${stamp}`;
  await miner.runAthena(A, miner.buildKeysTableSql({ keysTable: domainsTable, bucket: BUCKET, prefix }), output, 'create domains table');
  const res = await miner.runAthena(A, miner.buildDomainDiscoverySql({ crawls, domainsTable, tlds: miner.DEFAULT_TLDS }), output, 'discover bio pages');

  // Straight into the queue prefix the pump already drains. No job record, no promotion, no gate —
  // the queue is the only thing that needs to know, which is how the rest of the bio path now works.
  let n = 0, batch = [], files = 0;
  const flush = async () => {
    if (!batch.length) return;
    const key = `${QUEUE_PREFIX}cc-refresh-${stamp}-${String(++files).padStart(5, '0')}.jsonl`;
    await A.s3.send(new A.PutObjectCommand({ Bucket: QUEUE_BUCKET, Key: key, Body: Buffer.from(batch.join('\n') + '\n') }));
    batch = [];
  };
  let first = true;
  await miner.s3StreamRows(A, res.output, async (cols) => {
    if (first) { first = false; return; }                 // CSV header
    const [url, filename, offset, length, timestamp] = cols;
    if (!url || !filename) return;
    batch.push(JSON.stringify({ url, filename, offset: Number(offset), length: Number(length), timestamp }));
    n++;
    if (batch.length >= 20000) await flush();
  });
  await flush();
  console.error(`  discovery done: ${N(n)} bio pointer(s) in ${files} file(s) -> s3://${QUEUE_BUCKET}/${QUEUE_PREFIX}`);
  return { pointers: n, files };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const t0 = Date.now();

  const { targets, cutoff } = await selectDue(client);
  const byType = targets.reduce((m, t) => { const k = t.company_type || '(none)'; m[k] = (m[k] || 0) + 1; return m; }, {});
  console.error(`due since ${cutoff}: selected ${N(targets.length)} (cap ${N(MAX)})  [${Object.entries(byType).map(([k, v]) => k + ' ' + N(v)).join(', ')}]`);
  const locNoPage = targets.filter((t) => t.company_type === 'Location' && !String(t.page_url || '').trim()).length;
  if (locNoPage) console.error(`  ${N(locNoPage)} Location(s) have no page_url and are skipped (a Location is never enriched from the shared home page)`);
  if (!targets.length) { console.error('nothing due'); process.exit(0); }

  if (DRY) {
    console.error('\nsample targets:');
    for (const t of targets.slice(0, 8)) console.error('    ' + String(t.company_type || '').padEnd(9) + String(t.contact_count || 0).padStart(6) + '  ' + targetUrl(t));
    console.error('\nDry run — no AWS calls, no writes.');
    process.exit(0);
  }

  if (!BUCKET) { console.error('CC_ATHENA_BUCKET required (Athena results + staging)'); process.exit(1); }
  if (DO_DISCOVER && !QUEUE_BUCKET) { console.error('MONITOR_QUEUE_BUCKET required for bio discovery'); process.exit(1); }

  const A = miner.aws();
  const output = `s3://${BUCKET}/athena-results/`;
  await miner.ensureBucket(A, BUCKET);
  const crawls = await miner.latestCrawls(1);
  const crawl = Array.isArray(crawls) ? crawls[0] : crawls;
  console.error(`crawl: ${crawl}`);
  await miner.ensureTable(A, crawl, output);

  const genderMap = (() => { try { return ex.loadGenderMap('names-genders.csv'); } catch { return {}; } })();
  const altList = await co.getAltWebsites(client);

  let a = null, b = null;
  if (DO_REFRESH) { console.error('\n== A. refresh company records =='); a = await refresh(client, targets, A, output, [crawl], genderMap, altList); }
  if (DO_DISCOVER) { console.error('\n== B. discover bio pages =='); b = await discover(targets, A, output, [crawl]); }

  console.error(`\nMONTHLY REFRESH DONE in ${Math.round((Date.now() - t0) / 1000)}s`);
  if (a) console.error(`  companies updated : ${N(a.updated)} (${N(a.found)} pages read)`);
  if (b) console.error(`  bio pointers queued: ${N(b.pointers)} -> the pump drains these without being told`);
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
