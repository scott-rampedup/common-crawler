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
const { emailHash } = require('./email-hash');
const os = require('./opensearch');
const naics = require('./naics');
const { classifyEmail } = require('./extractor');   // Employee contacts need an Email Type like any other

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('--in', ''); const now = new Date().toISOString();
const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function withRetry(fn) { for (let a = 0; ; a++) { try { return await fn(); } catch (e) { if (a >= 6) throw e; await sleep(Math.min(16000, 500 * 2 ** a)); } } }

// "has a value", for the STRING-or-ARRAY fields below. These guards used a bare truthiness test,
// which was right
// while the fields were strings and became wrong the moment they turned into arrays: [] is TRUTHY in
// JavaScript, so an empty array would have been spread into the update and blanked the field -- the
// exact bio_url blanking this branch was fixed to prevent, walking back in through the array change.
const hasVal = (v) => (Array.isArray(v) ? v.length > 0 : !!String(v == null ? '' : v).trim());

const CC = (g) => ({ name: g.name || '', website: g.website || '', category: g.category || '', full_address: g.full_address || '',
  // The Location's own page, carried through from gm-load so a per-location crawl has something to
  // fetch other than the shared homepage.
  ...(g.page_url ? { page_url: g.page_url } : {}),
  phone: g.phone || '', phone_type: g.phone_type || '', website_type: g.website_type || '', facebook: g.facebook || '', instagram: g.instagram || '',
  whatsapp: g.whatsapp || '', cid: g.cid || '', team_page: g.team_page || '',
  // multi-value fields as ARRAYS, the same shape the refresh writes. gm-upsert builds its bulk bodies
  // directly rather than through co.update(), so it does not get coerceField() for free -- left as
  // '; ' strings here the index would have carried two shapes of the same field at once.
  bio_url: co.asList(g.bio_url),
  linkedin_contact: co.asList(g.linkedin_contact), linkedin_url: co.asList(g.linkedin_url),   // /in vs /company kept distinct
  email: g.email || '', email_type: g.email_type || '',
  // hashed here as well as in the refresh: populated on only one of the two write paths the field
  // would be useless for a join, which is the single thing it exists to do.
  ...(emailHash(g.email || '') ? { email_hash: emailHash(g.email || '') } : {}),
  // per-LOCATION closure status from the Maps export (Open / Temporarily closed / blank). Only set on
  // Location records -- an HQ is an aggregate over a domain, where a closure would be meaningless.
  company_status: g.company_status || '',
  // componentized geo (Bing UK carries these) + provenance; Google leaves them blank and keeps full_address
  // lower-cased on the way in: the country/industry filters lower-case their input, so a Location
  // stored as "United States" was unreachable through the very facet it counted into.
  ...(g.locality ? { locality: co.canonPlace(g.locality) } : {}), ...(g.region ? { region: co.canonPlace(g.region) } : {}),
  ...(g.country ? { country: co.canonPlace(g.country) } : {}),
  ...(g.time_stamp ? { time_stamp: g.time_stamp } : {}), ...(g.source_map ? { source_map: g.source_map } : {}) });
const locToDoc = (g) => naics.assignNaics({ id: g.cid || ('gm:' + g.root_domain), domain: g.root_domain, company_type: 'Location', cc_refreshed_at: now,
  ...(g.company_name ? { company_name: g.company_name } : {}), ...(g.root_domain ? { root_domain: co.registrableDomain(g.root_domain) } : {}), ...CC(g) });
const hqNewToDoc = (id, d) => naics.assignNaics({ id, domain: d.domain, company_type: 'HQ', locality: co.canonPlace(d.locality), region: co.canonPlace(d.region), country: co.canonPlace(d.country),
  location_count: d.location_count || 0, cc_refreshed_at: now, ...CC(d) });

// WHY THE REASONS ARE KEPT AND NOT JUST COUNTED. On 2026-11-21 the Overture and Google Maps runs
// reported "1,140,906 applied, 1140906 err" and exited 0 while OpenSearch refused every single write
// behind a flood-stage read-only block; 6.6M Location writes and 744k HQ updates were lost over about
// six hours and the logs looked normal throughout. A count with no reason beside it is not a report,
// and an exit code that ignores it is a lie, so keep the distinct reasons and fail on them below.
const errReasons = new Map();
async function bulkFlush(body, tag) {
  if (!body.length) return 0;
  const res = await withRetry(() => client.bulk({ body, refresh: false }));
  const b = res.body || res; let errs = 0;
  if (b.errors) for (const it of b.items) {
    const r = it.update || it.index;
    if (r && r.error) {
      errs++;
      const k = r.error.type + (r.error.reason ? ': ' + String(r.error.reason).slice(0, 160) : '');
      errReasons.set(k, (errReasons.get(k) || 0) + 1);
    }
  }
  return errs;
}

// Refuse to start when the index will not take writes. OpenSearch applies
// index.blocks.read_only_allow_delete at the flood-stage disk watermark and does NOT lift it when disk
// frees up again, so a green cluster says nothing about whether a write will land. Checking once here
// costs one request; not checking cost the run described above.
async function assertWritable() {
  const s = await client.transport.request({ method: 'GET', path: `/${co.INDEX}/_settings?filter_path=**.blocks` });
  const blocks = ((((((s.body || s) || {})[co.INDEX] || {}).settings || {}).index || {}).blocks) || {};
  const on = Object.keys(blocks).filter((k) => String(blocks[k]) === 'true');
  if (on.length) throw new Error(`${co.INDEX} will not accept writes: index.blocks.${on.join('=true, index.blocks.')}=true.\n`
    + '  The flood-stage disk watermark sets read_only_allow_delete and does not clear it on its own.\n'
    + '  Free or add disk, then clear it:\n'
    + `  PUT /_all/_settings {"index.blocks.read_only_allow_delete":null,"index.blocks.write":null}`);
}

async function loadFile(file, toActions, label) {
  if (!fs.existsSync(file)) { console.error(`  (no ${label})`); return 0; }
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  let body = [], n = 0, errs = 0;
  for await (const l of rl) {
    if (!l.trim()) continue; let o; try { o = JSON.parse(l); } catch { continue; }
    const acts = toActions(o); if (!acts) continue;
    body.push(...acts); n++;
    if (body.length >= 2000) { errs += await bulkFlush(body); body = []; if (n % 100000 === 0) console.error(`    ${label}: ${(n - errs).toLocaleString()} applied / ${errs.toLocaleString()} rejected`); }
  }
  errs += await bulkFlush(body);
  // "applied" means APPLIED. It used to print the attempt counter under that word, which is how a run
  // that landed nothing still read as a success.
  console.error(`  ${label}: ${(n - errs).toLocaleString()} applied, ${errs.toLocaleString()} rejected, ${n.toLocaleString()} attempted`);
  return errs;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const t0 = Date.now();
  await assertWritable();
  let rejected = 0;
  // HQ: existing -> partial update (company_type + rollups + location_count); synthesized -> index new
  rejected += await loadFile(path.join(IN, 'gm-hq.ndjson'), (o) => o.isNew
    ? [{ index: { _index: co.INDEX, _id: o.id } }, hqNewToDoc(o.id, o.doc)]
    // bio_url and linkedin_contact are written ONLY when the incoming record actually has one, the same
    // way linkedin_url / email / email_type beside them already behave. They used to be written
    // unconditionally with a `|| ''` fallback, which turns an absent value into a destructive blank on an
    // EXISTING company. That was harmless for Google Maps, whose team-profile columns usually carry a
    // value, and is not harmless for Overture, which has a bio URL on 0.5% of records: measured against
    // the real gm-build output for one shard, 347,131 existing HQs would be updated, 1.8% of them hold a
    // bio_url today, so ~6,300 companies per shard (~100,900 across the release) would lose the field
    // that feeds the Contact Crawler, in exchange for nothing.
    : [{ update: { _index: co.INDEX, _id: o.id } }, { doc: { company_type: 'HQ', location_count: o.doc.location_count || 0, ...(hasVal(o.doc.bio_url) ? { bio_url: o.doc.bio_url } : {}), ...(hasVal(o.doc.linkedin_contact) ? { linkedin_contact: o.doc.linkedin_contact } : {}), ...(hasVal(o.doc.linkedin_url) ? { linkedin_url: o.doc.linkedin_url } : {}), ...(hasVal(o.doc.email) ? { email: o.doc.email, email_type: o.doc.email_type || '' } : {}), ...(hasVal(o.doc.company_name) ? { company_name: o.doc.company_name } : {}), ...(hasVal(o.doc.root_domain) ? { root_domain: o.doc.root_domain } : {}) } }], 'HQ');
  // child Locations -> index by cid (dedupes the same business across scrape dates)
  rejected += await loadFile(path.join(IN, 'gm-loc.ndjson'), (o) => { const d = locToDoc(o.doc); return [{ index: { _index: co.INDEX, _id: d.id } }, d]; }, 'Locations');
  // contacts -> score-gated upsert into the Master DB (email-keyed)
  { const file = path.join(IN, 'gm-contacts.ndjson');
    if (fs.existsSync(file)) { const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
      // This catch used to be empty, so a contacts batch could fail wholesale and still be counted as
      // upserted. Count what was actually lost and keep the reason.
      let buf = [], n = 0, cerr = 0, cwhy = '';
      const flush = async () => { if (!buf.length) return; const b = buf.splice(0);
        try { await withRetry(() => os.bulkUpsert(client, b)); }
        catch (e) { cerr += b.length; if (!cwhy) cwhy = (e && e.message) || String(e); } };
      for await (const l of rl) { if (!l.trim()) continue; let c; try { c = JSON.parse(l); } catch { continue; }
        if (!c.email) continue;
        const doc = os.recordToDoc({ 'Time Stamp': now.slice(0, 10), 'Source': 'Employee', 'Web Source URL': c.bio || c.src || '', 'Domain': (c.email || '').split('@')[1] || '', 'First': c.first, 'Last': c.last, 'Gender': c.gender, 'Email Address': c.email, 'Email Type': classifyEmail(c.email), 'LinkedIn URL': c.linkedin, 'Phone': c.phone || '', 'Phone Type': c.phone_type || '' }, now);
        if (doc.email) { buf.push(doc); n++; } if (buf.length >= 2000) await flush(); }
      await flush();
      console.error(`  employees: ${(n - cerr).toLocaleString()} upserted to Master DB (Source="Employee")`
        + (cerr ? `, ${cerr.toLocaleString()} LOST — ${cwhy}` : ''));
      rejected += cerr; }
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

  if (rejected) {
    console.error(`\nREJECTED ${rejected.toLocaleString()} writes. Reasons:`);
    for (const [k, v] of [...errReasons.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8)) {
      console.error(`  ${v.toLocaleString()} x ${k}`);
    }
    console.error(`FAILED after ${Math.round((Date.now() - t0) / 1000)}s — the input is unchanged, re-run once the cause is fixed.`);
    process.exit(2);          // non-zero, so a chain script stops instead of marching on
  }
  console.error(`DONE in ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
