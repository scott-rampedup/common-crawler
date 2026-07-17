/**
 * gm-build.js — Phases 2/3/5 of the Google-Maps -> Company Crawler ETL. Reads the Phase-1
 * gm-locations.ndjson, groups Locations by root domain, resolves the HQ (existing company or a
 * synthesized one), merges same-City/Region/Country Locations into the HQ, rolls up bio/LinkedIn/email
 * to the HQ (stripped from the children), counts Locations, and builds contacts. Emits:
 *   gm-hq.ndjson       {id, isNew, doc}        -> HQ upserts (update existing / create synthesized)
 *   gm-loc.ndjson      {id, doc}               -> child Location upserts
 *   gm-contacts.ndjson <extractor display rec> -> person contacts for the Master DB
 *
 *   OPENSEARCH_ENDPOINT=… node gm-build.js --in <dir>/gm-locations.ndjson --out <dir> [--limit-domains N]
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const co = require('./companies');
const ex = require('./extractor');
const che = require('./cc-home-enrich');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('--in', ''); const OUT = arg('--out', path.dirname(IN || '.'));
const LIMIT_DOMAINS = Number(arg('--limit-domains', '0')) || 0;
const CONC = Number(process.env.CONC) || 12;
if (!IN || !process.env.OPENSEARCH_ENDPOINT) { console.error('need --in <gm-locations.ndjson> + OPENSEARCH_ENDPOINT'); process.exit(1); }

const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));

// ---- address -> {city,region,country} (US-biased heuristic) ----
const COUNTRIES = new Set(['united states', 'usa', 'us', 'united kingdom', 'uk', 'canada', 'australia', 'ireland', 'new zealand', 'india', 'germany']);
function parseAddr(addr) {
  const parts = String(addr || '').split(',').map((s) => s.trim()).filter(Boolean);
  let country = '', region = '', city = '';
  if (parts.length && COUNTRIES.has(parts[parts.length - 1].toLowerCase())) country = parts.pop();
  const stzip = parts[parts.length - 1] || '';
  const m = stzip.match(/^([A-Za-z]{2})\s+\w/); if (m) { region = m[1]; if (!country) country = 'United States'; }
  city = parts[parts.length - 2] || '';
  return { city: city.toLowerCase(), region: region.toLowerCase(), country: (country || '').toLowerCase() };
}
const splitL = (s) => String(s || '').split(/[\s;]+/).map((x) => x.trim()).filter(Boolean);
const score = (r) => (r.email ? 2 : 0) + (r.website ? 1 : 0) + (r.phone ? 1 : 0) + (r.bio_url ? 2 : 0) + (r.name ? 1 : 0) + (r.full_address ? 1 : 0);

async function lookupHQ(domain) {
  try {
    const r = await client.search({ index: co.INDEX, body: { size: 1, query: { term: { domain } },
      _source: ['id', 'name', 'website', 'locality', 'region', 'country', 'phone', 'full_address', 'linkedin_url', 'email', 'facebook', 'instagram', 'bio_url', 'linkedin_contact'] } });
    const h = (r.body || r).hits.hits[0];
    return h ? { ...h._source, _id: h._id } : null;
  } catch (e) { return null; }
}

async function processGroup(domain, locs) {
  const hq0 = await lookupHQ(domain);
  const rep = locs.reduce((a, b) => (score(b) > score(a) ? b : a), locs[0]);   // most complete Location
  const hqCRC = hq0 && (hq0.locality || hq0.region)
    ? { city: (hq0.locality || '').toLowerCase(), region: (hq0.region || '').toLowerCase(), country: (hq0.country || '').toLowerCase() }
    : parseAddr(rep.full_address);

  // rollup sets (from HQ + every Location)
  const bio = new Set(), li = new Set(), emails = new Set();
  const collect = (r) => { splitL(r.bio_url).forEach((u) => bio.add(u)); splitL(r.linkedin_contact).forEach((u) => li.add(u)); if (r.email) emails.add(r.email); };
  if (hq0) { collect(hq0); if (hq0.linkedin_url) li.add(hq0.linkedin_url); }
  locs.forEach(collect);

  // merge (same City+Region as HQ) vs keep as child
  const children = [];
  for (const loc of locs) {
    if (hq0 == null && loc === rep) continue;                 // rep becomes the synthesized HQ, not a child
    const crc = parseAddr(loc.full_address);
    const same = crc.city && crc.city === hqCRC.city && crc.region === hqCRC.region;
    if (!same) children.push(loc);
  }

  // HQ doc — update existing, or synthesize from the rep Location
  const hqDoc = hq0
    ? { company_type: 'HQ' }                                   // enrich the existing company
    : { company_type: 'HQ', name: rep.name, website: rep.website, domain, category: rep.category, full_address: rep.full_address, phone: rep.phone, phone_type: rep.phone_type,
        website_type: rep.website_type, facebook: rep.facebook, instagram: rep.instagram, whatsapp: rep.whatsapp, cid: rep.cid, time_stamp: rep.time_stamp,
        locality: hqCRC.city, region: hqCRC.region, country: hqCRC.country };
  // rolled-up contact fields onto the HQ (dedup already via sets)
  hqDoc.bio_url = [...bio].join('; ');
  hqDoc.linkedin_contact = [...li].join('; ');
  if (emails.size) { hqDoc.email = [...emails][0]; hqDoc.email_type = ex.classifyEmail([...emails][0]); }
  hqDoc.location_count = locs.length;

  // strip the rolled-up fields off the child Location records
  const childDocs = children.map((c) => ({ ...c, bio_url: '', linkedin_contact: '', email: '', email_type: '' }));

  // Phase 5 — contacts from the HQ's rolled-up signals
  let contacts = [];
  try { const b = che.buildContacts({ linkedin: [...li], emails: [...emails], bio: [...bio] }, { genderMap, address: hqDoc.full_address || (hq0 && hq0.full_address) || '' }); contacts = b.structured || []; }
  catch (e) { /* best-effort */ }

  return { hqId: hq0 ? hq0._id : 'gm:' + domain, isNew: !hq0, hqDoc, childDocs, contacts, domain };
}

(async () => {
  // ---- external sort by root_domain: write "domain\tline" -> sort -> stream grouped ----
  const tmp = path.join(OUT, '_gm-keyed.tsv'), sorted = path.join(OUT, '_gm-sorted.tsv');
  console.error('keying by root_domain…');
  { const w = fs.createWriteStream(tmp); const rl = readline.createInterface({ input: fs.createReadStream(IN), crlfDelay: Infinity });
    for await (const l of rl) { if (!l.trim()) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.root_domain) w.write(o.root_domain + '\t' + l + '\n'); }
    await new Promise((r) => w.end(r)); }
  console.error('sorting…');
  const s = spawnSync('sort', ['-t', '\t', '-k1,1', '-T', OUT, tmp, '-o', sorted], { stdio: 'inherit' });
  if (s.status !== 0) { console.error('sort failed'); process.exit(1); }

  const hqW = fs.createWriteStream(path.join(OUT, 'gm-hq.ndjson'));
  const locW = fs.createWriteStream(path.join(OUT, 'gm-loc.ndjson'));
  const conW = fs.createWriteStream(path.join(OUT, 'gm-contacts.ndjson'));
  let domains = 0, hqNew = 0, hqUpd = 0, childN = 0, conN = 0; const t0 = Date.now();

  // stream sorted, dispatch each completed domain group to a bounded pool
  const rl = readline.createInterface({ input: fs.createReadStream(sorted), crlfDelay: Infinity });
  let curDom = null, group = []; const inflight = new Set(); let stop = false;
  async function dispatch(domain, locs) {
    const p = (async () => {
      const r = await processGroup(domain, locs);
      hqW.write(JSON.stringify({ id: r.hqId, isNew: r.isNew, doc: r.hqDoc }) + '\n'); r.isNew ? hqNew++ : hqUpd++;
      for (const c of r.childDocs) { locW.write(JSON.stringify({ id: c.cid || ('gm:' + r.domain + ':' + Math.random().toString(36).slice(2)), doc: c }) + '\n'); childN++; }
      for (const ct of r.contacts) { conW.write(JSON.stringify(ct) + '\n'); conN++; }
    })().catch(() => {}).finally(() => inflight.delete(p));
    inflight.add(p);
    if (inflight.size >= CONC) await Promise.race(inflight);
  }
  for await (const line of rl) {
    if (stop) break;
    const tab = line.indexOf('\t'); if (tab < 0) continue;
    const dom = line.slice(0, tab); const json = line.slice(tab + 1);
    let o; try { o = JSON.parse(json); } catch { continue; }
    if (dom !== curDom) {
      if (curDom && group.length) { await dispatch(curDom, group); domains++; if (domains % 20000 === 0) console.error(`  ${domains.toLocaleString()} domains | ${hqUpd} upd + ${hqNew} new HQ | ${childN} loc | ${conN} contacts | ${Math.round(domains / ((Date.now() - t0) / 1000))}/s`); if (LIMIT_DOMAINS && domains >= LIMIT_DOMAINS) { stop = true; break; } }
      curDom = dom; group = [];
    }
    group.push(o);
  }
  if (!stop && curDom && group.length) { await dispatch(curDom, group); domains++; }
  await Promise.all(inflight);
  for (const w of [hqW, locW, conW]) await new Promise((r) => w.end(r));
  try { fs.unlinkSync(tmp); if (!LIMIT_DOMAINS) fs.unlinkSync(sorted); } catch (e) { /* keep for debug */ }
  console.error(`DONE: ${domains.toLocaleString()} domains -> ${hqUpd.toLocaleString()} HQ updates + ${hqNew.toLocaleString()} synthesized HQ | ${childN.toLocaleString()} child Locations | ${conN.toLocaleString()} contacts | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
