/**
 * load-enriched.js — apply Lambda-enriched company JSONL from S3 to the `companies` OpenSearch index,
 * and emit the discovered bio URLs for Hop 2. Sibling of load-extracted.js (which loads person contacts).
 *
 *   OPENSEARCH_ENDPOINT=… OUT_BUCKET=… BIO_OUT=bio-urls.txt node load-enriched.js cc-enriched/<run>/
 *
 * Each JSONL line: { id, domain, updates, contacts:[…], bio:[…] } (from lambda-enrich). Applies `updates`
 * to the company via a bulk partial-update, score-gated-upserts the email-bearing grouped contacts into
 * the contacts index, and writes the union of bio URLs to BIO_OUT. Idempotent — safe to re-run.
 */
const fs = require('fs');
const https = require('https');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const co = require('./companies');
const os = require('./opensearch');

const CC_FIELDS = new Set(co.CC_FIELDS.concat(['website', 'phone', 'cc_refreshed_at', 'cc_crawl']));

(async () => {
  const prefix = process.argv[2] || 'cc-enriched/';
  const bucket = process.env.OUT_BUCKET || `rampedup-cc-extracted-${process.env.AWS_ACCOUNT || ''}`;
  const region = process.env.AWS_REGION || 'us-east-1';
  const bioOut = process.env.BIO_OUT || '';
  const s3 = new S3Client({ region, requestHandler: new NodeHttpHandler({ httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 64 }) }) });
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const CONC = Number(process.env.LOAD_CONC) || 8;
  const now = new Date().toISOString(), today = now.slice(0, 10);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  console.error(`loading s3://${bucket}/${prefix} -> companies index (conc ${CONC})`);

  async function withRetry(fn) { for (let a = 0; ; a++) { try { return await fn(); } catch (e) { if (a >= 6) throw e; await sleep(Math.min(16000, 500 * 2 ** a)); } } }

  // Bulk partial-update companies by id (only whitelisted CC fields; website edits keep domain in sync).
  async function bulkUpdateCompanies(items) {
    if (!items.length) return 0;
    const body = [];
    for (const it of items) {
      if (!it.id) continue;
      const doc = {};
      for (const k in (it.updates || {})) { if (CC_FIELDS.has(k)) doc[k] = it.updates[k]; }
      if ('website' in doc) doc.domain = co.normDomain(doc.website);
      if (!Object.keys(doc).length) continue;
      body.push({ update: { _index: co.INDEX, _id: it.id } }, { doc });
    }
    if (!body.length) return 0;
    const res = await withRetry(() => client.bulk({ body, refresh: false }));
    const b = res.body || res; let errs = 0;
    if (b.errors) for (const x of b.items) if (x.update && x.update.error) errs++;
    return errs;
  }

  const bioSet = bioOut ? new Set() : null;
  const contactBuf = [];
  async function flushContacts() { if (!contactBuf.length) return; const batch = contactBuf.splice(0); try { await withRetry(() => os.bulkUpsert(client, batch)); } catch (e) { /* skip */ } }

  const keys = [];
  let token;
  do {
    const list = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token }));
    for (const obj of (list.Contents || [])) if (obj.Key.endsWith('.jsonl')) keys.push(obj.Key);
    token = list.IsTruncated ? list.NextContinuationToken : null;
  } while (token);
  console.error(`${keys.length.toLocaleString()} file(s) to load`);

  let idx = 0, files = 0, updated = 0, upErrs = 0, contacts = 0;
  const t0 = Date.now();
  async function worker() {
    for (;;) {
      const k = idx++; if (k >= keys.length) return;
      try {
        const r = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: keys[k] }));
        const lines = (await r.Body.transformToString()).trim().split('\n').filter(Boolean);
        const items = [];
        for (const l of lines) {
          let o; try { o = JSON.parse(l); } catch { continue; }
          if (o.id) items.push(o);
          if (bioSet) for (const u of (o.bio || [])) if (u) bioSet.add(u);
          for (const c of (o.contacts || [])) {
            if (!c.email) continue;
            const doc = os.recordToDoc({ 'Time Stamp': today, 'Source': 'CC Home', 'Web Source URL': c.bio || ('https://' + (o.domain || '') + '/'), 'Domain': o.domain || '', 'First': c.first, 'Last': c.last, 'Gender': c.gender, 'Email Address': c.email, 'LinkedIn URL': c.linkedin }, now);
            if (doc.email) { contactBuf.push(doc); contacts++; }
          }
        }
        for (let i = 0; i < items.length; i += 1000) upErrs += await bulkUpdateCompanies(items.slice(i, i + 1000));
        updated += items.length;
        if (contactBuf.length >= 2000) await flushContacts();
      } catch (e) { upErrs++; }
      if (++files % 500 === 0) console.error(`  ${files.toLocaleString()}/${keys.length.toLocaleString()} files | ${updated.toLocaleString()} companies | ${contacts.toLocaleString()} contacts`);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(CONC, keys.length)) }, worker));
  await flushContacts();
  if (bioSet) { fs.writeFileSync(bioOut, [...bioSet].join('\n') + '\n'); console.error(`bio URLs written: ${bioSet.size.toLocaleString()} -> ${bioOut}`); }
  console.error(`DONE: ${updated.toLocaleString()} companies enriched, ${contacts.toLocaleString()} contacts, ${upErrs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('load error:', e.message); process.exit(1); });
