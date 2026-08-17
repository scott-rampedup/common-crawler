/**
 * probe-schema-person.js — measure schema.org/Person presence + contact yield on a sample of
 * person/bio-page WARC pointers from a recent Common Crawl (S3-direct, no live traffic).
 *
 *   node probe-schema-person.js probe-person-2026-30.jsonl [--conc 20] [--max 10000]
 */
const fs = require('fs');
const readline = require('readline');
const { makeCcS3 } = require('./cc-s3');
const extractor = require('./extractor');
const wbc = require('./wireless-block-classifier');

const file = process.argv[2];
const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 ? Number(process.argv[i + 1]) : d; };
const CONC = arg('--conc', 20);
const MAX = arg('--max', 0);
if (!file) { console.error('usage: node probe-schema-person.js <pointers.jsonl>'); process.exit(1); }

const fetchWarc = makeCcS3();
const genderMap = extractor.loadGenderMap(require('path').join(__dirname, 'names-genders.csv'));
let wireless = null; try { wireless = wbc.loadWirelessBlocks(wbc.PHONE_BLOCKS_CSV); } catch (e) { /* phones blank */ }

// schema.org Person-ish detection: JSON-LD first (most common), then Microdata itemtype.
const P = 'Person|RealEstateAgent|Attorney|Physician|Dentist|LocalBusiness';
function schemaPerson(html) {
  const scripts = String(html).match(/<script[^>]+application\/ld\+json[^>]*>([\s\S]*?)<\/script>/gi) || [];
  for (const s of scripts) if (new RegExp('"@type"\\s*:\\s*(\\[[^\\]]*)?["\'\\s]*(' + P + ')\\b', 'i').test(s)) return 'jsonld';
  if (new RegExp('itemtype\\s*=\\s*["\']https?://schema\\.org/(' + P + ')\\b', 'i').test(String(html))) return 'microdata';
  return '';
}
const isGeneric = (e) => /^(info|contact|admin|hello|office|sales|support|team|careers?|jobs?|hr|help|service|no-?reply)/i.test(String(e || '').split('@')[0]);

(async () => {
  const ptrs = [];
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  for await (const l of rl) { const t = l.trim(); if (!t) continue; try { const o = JSON.parse(t); if (o && o.url && o.filename) ptrs.push(o); } catch (e) { /* skip */ } if (MAX && ptrs.length >= MAX) break; }
  console.error(`probing ${ptrs.length.toLocaleString()} pointers (conc ${CONC})`);

  const t = { fetched: 0, empty: 0, err: 0, jsonld: 0, microdata: 0, sp: 0, spName: 0, spEmail: 0, spPhone: 0, anyName: 0, anyEmail: 0, anyPhone: 0 };
  let i = 0;
  async function worker() {
    for (;;) {
      const k = i++; if (k >= ptrs.length) return;
      const p = ptrs[k];
      let html = '';
      try { html = await fetchWarc({ url: p.url, filename: p.filename, offset: p.offset, length: p.length }); }
      catch (e) { t.err++; continue; }
      if (!html) { t.empty++; continue; }
      t.fetched++;
      const sp = schemaPerson(html);
      if (sp === 'jsonld') t.jsonld++; if (sp === 'microdata') t.microdata++;
      let rec = null; try { rec = extractor.extractRecord(html, p.url, { wireless, genderMap, source: 'Common Crawl', allowNoEmail: true }); } catch (e) { /* */ }
      const name = rec && rec['First'] && rec['Last'];
      const email = rec && rec['Email Address'] && !isGeneric(rec['Email Address']) && rec['Email Type'] !== 'Modelled';
      const phone = rec && String(rec['Phone'] || '').startsWith('+');
      if (name) t.anyName++; if (email) t.anyEmail++; if (phone) t.anyPhone++;
      if (sp) { t.sp++; if (name) t.spName++; if (email) t.spEmail++; if (phone) t.spPhone++; }
      if ((k + 1) % 1000 === 0) console.error(`  ${k + 1}/${ptrs.length} | schema:Person ${t.sp} | email ${t.anyEmail} | phone ${t.anyPhone}`);
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  const pct = (n, d) => d ? (100 * n / d).toFixed(1) + '%' : '0%';
  console.log('\n===== CC-MAIN person-page probe =====');
  console.log(`pointers ${ptrs.length.toLocaleString()} | fetched ${t.fetched.toLocaleString()} (empty ${t.empty}, err ${t.err})`);
  console.log(`schema.org Person markup: ${t.sp.toLocaleString()} (${pct(t.sp, t.fetched)})  [jsonld ${t.jsonld}, microdata ${t.microdata}]`);
  console.log(`  of those -> name ${pct(t.spName, t.sp)} | real-email ${pct(t.spEmail, t.sp)} | phone ${pct(t.spPhone, t.sp)}`);
  console.log(`ALL fetched pages -> name ${pct(t.anyName, t.fetched)} | real-email ${pct(t.anyEmail, t.fetched)} | phone ${pct(t.anyPhone, t.fetched)}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
