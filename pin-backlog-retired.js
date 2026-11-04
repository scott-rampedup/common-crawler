// Permanently retire the 2026-08 sitemap-sweep backlog in the queue_consumed ledger.
//
// WHY ONLY AUGUST: the sweep ran 2026-08-13 .. 2026-08-26 and nothing since, so every object in that
// window is sweep output -- and two consecutive passes overlap 94.2%, making the 52.2M URLs there roughly
// 2.5M distinct URLs written about 22 times. The 363 objects dated Sept/Oct/Nov came from OTHER producers
// (Common Crawl discovery, the monthly refresh, the Google Maps imports). A sitemap sweep will never
// regenerate those, so retiring them would destroy 2.77M URLs of real unprocessed work.
//
// Nothing is deleted. The IAM user has no s3:DeleteObject; this makes the drain skip them and, unlike an
// ordinary retirement, never lapse. The URLs are not lost -- they carry no crawl-ledger entry, so the next
// sweep rediscovers the ones that are still real.
const fs = require('fs');
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');
const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');

const APPLY = process.argv.includes('--apply');
const OUT = process.argv.includes('--out') ? process.argv[process.argv.indexOf('--out') + 1] : null;
const BUCKET = 'aws-athena-query-results-475987770186-us-east-1';
const PREFIX = 'monitor-queue/pending/';
const BEFORE = Date.parse('2026-09-01T00:00:00Z');
const REASON = 'sitemap-sweep 2026-08 backlog: ~22x duplicate re-queues, written off 2026-11-03';

const s3 = new S3Client({ region: 'us-east-1' });
const os = new Client({
  ...AwsSigv4Signer({ region: 'us-east-1', service: 'es', getCredentials: () => defaultProvider()() }),
  node: 'https://search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com',
});

(async () => {
  let tok, keys = [], skipped = 0, urls = 0;
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX, ContinuationToken: tok }));
    for (const o of r.Contents || []) {
      if (o.LastModified.getTime() >= BEFORE) { skipped++; continue; }
      keys.push(o.Key);
      urls += Number((o.Key.match(/-(\d+)\.txt$/) || [])[1] || 0);
    }
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);

  if (OUT) fs.writeFileSync(OUT, keys.join('\n') + '\n');
  console.log(keys.length.toLocaleString() + ' object(s) to pin, ' + urls.toLocaleString() + ' URL(s)');
  console.log(skipped.toLocaleString() + ' object(s) left alone (dated on/after 2026-09-01)');
  if (OUT) console.log('key list: ' + OUT);
  if (!APPLY) { console.log('DRY RUN — nothing written'); return; }

  const at = new Date().toISOString();
  let ok = 0, errors = 0;
  for (let i = 0; i < keys.length; i += 2000) {
    const body = [];
    for (const k of keys.slice(i, i + 2000)) {
      body.push({ update: { _index: 'queue_consumed', _id: k } });
      body.push({ doc: { key: k, consumed_at: at, run: 'pin-backlog-2026-11-03', permanent: true, retired_reason: REASON },
                  doc_as_upsert: true });
    }
    const r = await os.bulk({ body, refresh: false });
    for (const it of ((r.body || r).items || [])) { if (it.update && it.update.error) errors++; else ok++; }
  }
  try { await os.indices.refresh({ index: 'queue_consumed' }); } catch (e) { /* best-effort */ }
  console.log('APPLIED: ' + ok.toLocaleString() + ' pinned, ' + errors.toLocaleString() + ' error(s)');
})();
