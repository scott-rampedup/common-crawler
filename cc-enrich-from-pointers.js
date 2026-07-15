/**
 * cc-enrich-from-pointers.js — fetch Athena-resolved home-page WARC pointers from s3://commoncrawl,
 * parse each, and write the CC enrichment fields back to the matching company.
 *   OPENSEARCH_ENDPOINT=… node cc-enrich-from-pointers.js ptr.jsonl targets.ndjson
 * (ptr.jsonl from `cc-athena-miner --resolve-urls home-urls.txt`; targets.ndjson from dump-company-urls.js)
 */
const co = require('./companies');
const che = require('./cc-home-enrich');
const ex = require('./extractor');
const { makeCcS3 } = require('./cc-s3');
const fs = require('fs');
const readline = require('readline');

(async () => {
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const gm = ex.loadGenderMap('./names-genders.csv');
  const fetchWarc = makeCcS3();                         // S3-direct (needs AWS creds; fast, un-throttled)
  const norm = (u) => { try { return new URL(u).hostname.toLowerCase().replace(/^www\./, ''); } catch { return ''; } };

  const targets = new Map();
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[3]), crlfDelay: Infinity });
    for await (const l of rl) { if (!l) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.domain) targets.set(o.domain, o); } }
  console.error('targets: ' + targets.size.toLocaleString());

  const ptrs = [];
  { const rl = readline.createInterface({ input: fs.createReadStream(process.argv[2]), crlfDelay: Infinity });
    for await (const l of rl) { if (!l) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.url && o.filename) ptrs.push(o); } }
  console.error('pointers: ' + ptrs.length.toLocaleString());

  const CONC = Number(process.env.CONC) || 16;
  let i = 0, updated = 0, contacts = 0, errs = 0;
  const t0 = Date.now();
  async function worker() {
    for (;;) {
      const k = i++; if (k >= ptrs.length) return;
      const p = ptrs[k];
      const company = targets.get(norm(p.url)); if (!company) continue;
      try {
        const html = await fetchWarc({ url: p.url, filename: p.filename, offset: p.offset, length: p.length });
        if (!html) continue;
        const r = che.enrichFromHtml(company, html, { genderMap: gm });
        await co.update(client, company.id, r.updates); updated++; contacts += (r.updates.contacts_count || 0);
      } catch (e) { errs++; }
      if ((k + 1) % 1000 === 0) { const el = (Date.now() - t0) / 1000; console.error(`  ${k + 1}/${ptrs.length} | updated ${updated} | contacts ${contacts} | ${errs} err | ${Math.round((k + 1) / el)}/s`); }
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  console.error(`DONE: updated ${updated.toLocaleString()}, ${contacts.toLocaleString()} contacts, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
