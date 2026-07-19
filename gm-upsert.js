/**
 * gm-upsert.js — Phase 4: apply the gm-build output to OpenSearch.
 *   - gm-hq.ndjson       -> update the matched HQ company (partial) OR create a synthesized HQ
 *   - gm-loc.ndjson      -> index child Location company records (deduped by cid)
 *   - gm-contacts.ndjson -> score-gated upsert into the contacts index (Master DB)
 * Plus --backfill-hq: tag every EXISTING company company_type=HQ (server-side update_by_query).
 *
 *   OPENSEARCH_ENDPOINT=… node gm-upsert.js --in <dir> [--backfill-hq]
 * Idempotent (HQ update = partial, Location = index by cid, contacts = score-gated).
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const co = require('./companies');
const os = require('./opensearch');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('--in', ''); const now = new Date().toISOString();
const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function withRetry(fn) { for (let a = 0; ; a++) { try { return await fn(); } catch (e) { if (a >= 6) throw e; await sleep(Math.min(16000, 500 * 2 ** a)); } } }

const CC = (g) => ({ name: g.name || '', website: g.website || '', category: g.category || '', full_address: g.full_address || '',
  phone: g.phone || '', phone_type: g.phone_type || '', website_type: g.website_type || '', facebook: g.facebook || '', instagram: g.instagram || '',
  whatsapp: g.whatsapp || '', cid: g.cid || '', team_page: g.team_page || '', bio_url: g.bio_url || '', linkedin_contact: g.linkedin_contact || '',
  email: g.email || '', email_type: g.email_type || '',
  // componentized geo (Bing UK carries these) + provenance; Google leaves them blank and keeps full_address
  ...(g.locality ? { locality: g.locality } : {}), ...(g.region ? { region: g.region } : {}), ...(g.country ? { country: g.country } : {}),
  ...(g.time_stamp ? { time_stamp: g.time_stamp } : {}), ...(g.source_map ? { source_map: g.source_map } : {}) });
const locToDoc = (g) => ({ id: g.cid || ('gm:' + g.root_domain), domain: g.root_domain, company_type: 'Location', cc_refreshed_at: now, ...CC(g) });
const hqNewToDoc = (id, d) => ({ id, domain: d.domain, company_type: 'HQ', locality: d.locality || '', region: d.region || '', country: d.country || '',
  location_count: d.location_count || 0, cc_refreshed_at: now, ...CC(d) });

async function bulkFlush(body, tag) {
  if (!body.length) return 0;
  const res = await withRetry(() => client.bulk({ body, refresh: false }));
  const b = res.body || res; let errs = 0;
  if (b.errors) for (const it of b.items) { const r = it.update || it.index; if (r && r.error) errs++; }
  return errs;
}

async function loadFile(file, toActions, label) {
  if (!fs.existsSync(file)) { console.error(`  (no ${label})`); return; }
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  let body = [], n = 0, errs = 0;
  for await (const l of rl) {
    if (!l.trim()) continue; let o; try { o = JSON.parse(l); } catch { continue; }
    const acts = toActions(o); if (!acts) continue;
    body.push(...acts); n++;
    if (body.length >= 2000) { errs += await bulkFlush(body); body = []; if (n % 100000 === 0) console.error(`    ${label}: ${n.toLocaleString()} (${errs} err)`); }
  }
  errs += await bulkFlush(body);
  console.error(`  ${label}: ${n.toLocaleString()} applied, ${errs} err`);
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const t0 = Date.now();
  // HQ: existing -> partial update (company_type + rollups + location_count); synthesized -> index new
  await loadFile(path.join(IN, 'gm-hq.ndjson'), (o) => o.isNew
    ? [{ index: { _index: co.INDEX, _id: o.id } }, hqNewToDoc(o.id, o.doc)]
    : [{ update: { _index: co.INDEX, _id: o.id } }, { doc: { company_type: 'HQ', location_count: o.doc.location_count || 0, bio_url: o.doc.bio_url || '', linkedin_contact: o.doc.linkedin_contact || '', ...(o.doc.email ? { email: o.doc.email, email_type: o.doc.email_type || '' } : {}) } }], 'HQ');
  // child Locations -> index by cid (dedupes the same business across scrape dates)
  await loadFile(path.join(IN, 'gm-loc.ndjson'), (o) => { const d = locToDoc(o.doc); return [{ index: { _index: co.INDEX, _id: d.id } }, d]; }, 'Locations');
  // contacts -> score-gated upsert into the Master DB (email-keyed)
  { const file = path.join(IN, 'gm-contacts.ndjson');
    if (fs.existsSync(file)) { const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
      let buf = [], n = 0; const flush = async () => { if (!buf.length) return; const b = buf.splice(0); try { await withRetry(() => os.bulkUpsert(client, b)); } catch (e) {} };
      for await (const l of rl) { if (!l.trim()) continue; let c; try { c = JSON.parse(l); } catch { continue; }
        if (!c.email) continue;
        const doc = os.recordToDoc({ 'Time Stamp': now.slice(0, 10), 'Source': 'Google Maps', 'Web Source URL': c.bio || '', 'Domain': (c.email || '').split('@')[1] || '', 'First': c.first, 'Last': c.last, 'Gender': c.gender, 'Email Address': c.email, 'LinkedIn URL': c.linkedin }, now);
        if (doc.email) { buf.push(doc); n++; } if (buf.length >= 2000) await flush(); }
      await flush(); console.error(`  contacts: ${n.toLocaleString()} upserted to Master DB`); }
    else console.error('  (no gm-contacts.ndjson)'); }

  // backfill LAST (retried) so a connection blip on the big 35.6M update_by_query can't abort the GM writes
  if (process.argv.includes('--backfill-hq')) {
    console.error('backfilling company_type=HQ on all existing companies (update_by_query, async)…');
    try {
      const r = await withRetry(() => client.updateByQuery({ index: co.INDEX, conflicts: 'proceed', wait_for_completion: false, requestTimeout: 120000,
        body: { query: { bool: { must_not: [{ exists: { field: 'company_type' } }] } }, script: { lang: 'painless', source: "ctx._source.company_type='HQ'" } } }));
      console.error('  submitted as task:', (r.body || r).task, '(runs server-side; track: GET _tasks/<task>)');
    } catch (e) { console.error('  backfill submit failed:', e.message, '(re-run: node gm-upsert.js --backfill-hq)'); }
  }

  console.error(`DONE in ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
