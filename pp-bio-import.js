#!/usr/bin/env node
//
// Import observed, domain-aligned bio-page addresses from Public Prospects into the Contact Crawler.
//
// WHAT THIS IS FOR. PP holds 185.7M addresses seen on public pages with evidence of where and when.
// 16.5M of them are flagged is_bio_url by Scott's own Path IDs sheet (whole-segment matching, so
// /team/ matches `team` and teamsters.com does not). Of those, ~31.5% have their bio URL on the
// address's OWN domain, and of those ~85% survive our email classifier. Measured 2026-11-12 on a
// 7,000-row random sample: ~35.5% of the survivors are addresses the Contact Crawler does not hold.
//
//   16,513,416  is_bio_url
//    4,883,636  + bio URL on the address's own domain (and not a /bioXXX word-form)
//    4,169,230  + Professional by our classifier, not PP's mailbox_type
//    1,735,086  + email net-new to the Contact Crawler      <- what this imports
//    1,667,413  + URL net-new                                <- what is worth crawling
//
// WHY NOT JUST CRAWL THEM. Because the addresses are the asset. They are OBSERVED -- published on
// the company's own site, at the company's own domain -- so they never go near the email modeller.
// The pages are worth crawling too, but that is a separate, slower job, and it should not gate
// getting 1.7M real addresses into the database.
//
// TWO TRAPS THIS AVOIDS, both found the hard way:
//
//   1. email_rollup.domain_aligned_ever is bool_or() across EVERY sighting of an address, while
//      path_flags stores ONE page_url chosen separately. Combining them reports "aligned" for
//      0303842@spar.dk whose stored bio URL is a facebook.com page. Alignment is therefore computed
//      HERE, from the same row, by comparing the stored page_url's host to the address's domain.
//
//   2. PP's mailbox_type='personal' misses role addresses in other languages and WordPress /author/
//      pages that carry the site's generic mailbox -- info@hsh-haus.de on /author/gino2024heinen/.
//      Our own classifyEmail catches 14.6% more of those, so it is the gate, not mailbox_type.
//
// Names are taken from the URL only when the EMAIL CORROBORATES them (see nameFromUrl). Gender is
// left blank on purpose: normalizeContact and ensureNameGender both fill it from the lexicon, but
// ONLY when it is blank, so writing a guess here would freeze it permanently.
//
// Usage:
//   node pp-bio-import.js --limit 5000                 # dry run, prints what it would do
//   node pp-bio-import.js --limit 5000 --write         # actually import
//   node pp-bio-import.js --limit 0 --write            # the whole qualifying set
//   node pp-bio-import.js --limit 20000 --urls out.txt # also write the crawlable URL list
//
// LIFTING THE GATES. The two quality gates above can each be switched off:
//   --allow-unaligned   keep addresses whose bio URL is NOT on the address's own domain
//   --allow-role        keep role and freemail addresses, not just Professional ones
//   --survey            measure what each combination would yield, import nothing
//
// Run --survey first. The gates drop 16.5M candidates to 1.7M, but the rows they drop are also
// the rows least likely to carry a NAME, and an address with no name can never carry a gender --
// so lifting a gate can add volume while adding nothing countable. --survey prints both numbers
// per combination so the trade is visible before several million rows are written.
//
// Resumable: --after <email> continues from where a previous run stopped (keyset on email_norm).
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { makeDb, FIELDS } = require('./db-pg');
const os = require('./opensearch');
const { classifyEmail, cleanEmail } = require('./extractor');
const { templateFor } = require('./email-pattern');
// Replays what ensureNameGender will do at index time, so the survey predicts the countable
// outcome rather than one of its two inputs.
const { nameFromEmail } = require('./cc-home-enrich');
const { loadGenderMap } = require('./extractor');
let _survGmap = null;
const survGmap = () => (_survGmap || (_survGmap = loadGenderMap(path.join(__dirname, 'names-genders.csv'))));
// A row is countable only if it ends up with a gender: that is the metric the database is judged
// on, and an address with no resolvable first name can never reach it.
function wouldGender(url, email) {
  const fromUrl = nameFromUrl(url, email);
  const first = (fromUrl && fromUrl.first) || (nameFromEmail(email) || {}).first || '';
  return !!(first && survGmap()[String(first).toLowerCase()]);
}

const argv = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = argv.indexOf('--' + name);
  return i >= 0 ? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true) : dflt;
};
const LIMIT = Number(flag('limit', 5000)) || 0;          // 0 = no limit
const WRITE = !!flag('write', false);
const URLS_OUT = flag('urls', '');
// Re-emit rows for addresses we already hold, instead of skipping them. For correcting earlier
// output after a logic fix: upsertMany is score-gated, so a record of EQUAL score whose fields
// differ still updates, while one that lost a field is refused -- which is the right asymmetry.
const REFRESH = !!flag('refresh', false);
const BATCH = Math.max(100, Number(flag('batch', 2000)));
// Gate lifts. Each drops a filter that exists for a stated reason (see the header traps), so both
// default to OFF and the survey below is the way to decide whether the extra rows are worth it.
const ALLOW_UNALIGNED = !!flag('allow-unaligned', false);
const ALLOW_ROLE = !!flag('allow-role', false);
const SURVEY = !!flag('survey', false);
let after = String(flag('after', '') || '');

const SOURCE_TAG = 'Public Prospects';

// The four gate configurations --survey measures. `cands` counts rows that pass the gates,
// `fresh` those not already in the Contact Crawler, and `named` the subset carrying a name the
// email corroborates -- the only ones that can ever gain a gender.
const COMBOS = [
  { label: 'current (aligned + professional)', pass: (a, p) => a && p, cands: 0, fresh: 0, named: 0, gendered: 0 },
  { label: '--allow-unaligned',                pass: (a, p) => p,      cands: 0, fresh: 0, named: 0, gendered: 0 },
  { label: '--allow-role',                     pass: (a) => a,         cands: 0, fresh: 0, named: 0, gendered: 0 },
  { label: 'both (all gates lifted)',          pass: () => true,       cands: 0, fresh: 0, named: 0, gendered: 0 },
];

// Host of a URL, www-stripped, for the alignment test.
function hostOf(u) {
  const m = String(u || '').toLowerCase().match(/^[a-z]+:\/\/(?:www\.)?([^/:?#]+)/);
  return m ? m[1] : '';
}
function alignedToEmail(url, email) {
  const h = hostOf(url);
  const d = String(email || '').split('@')[1] || '';
  if (!h || !d) return false;
  return h === d || h.endsWith('.' + d) || d.endsWith('.' + h);
}
function lastPath(u) {
  try {
    const segs = new URL(u).pathname.split('/').filter(Boolean);
    return segs.length ? segs[segs.length - 1] : '';
  } catch (e) { return ''; }
}

// A name from the URL, accepted ONLY if the email's local part fits it.
//
// /team/knoebl-eugen/ with eugen.knoebl@ingenos.at is two independent sources agreeing, which is
// evidence. The same slug with info@ingenos.at is one source and a generic mailbox, which is not --
// and templateFor returning null is exactly that distinction, already written and already tested.
const STOP = new Set(['index', 'default', 'home', 'page', 'profile', 'profiles', 'bio', 'bios',
  'staff', 'team', 'about', 'people', 'person', 'detail', 'details', 'view', 'en', 'de', 'fr', 'es']);
function nameFromUrl(url, email) {
  const seg = lastPath(url).replace(/\.(html?|php|aspx?|jsp)$/i, '');
  if (!seg || STOP.has(seg.toLowerCase())) return null;
  const parts = seg.split(/[-_.]+/).filter((p) => p && !/^\d+$/.test(p) && !STOP.has(p.toLowerCase()));
  if (parts.length < 2) return null;
  const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
  const local = String(email).split('@')[0];

  // Try the slug both ways round -- /knoebl-eugen/ is surname-first, /robin-karlsson/ is not.
  //
  // A match alone is NOT enough to fix the order, because TEMPLATES contains {last}.{first}: the
  // slug knoebl-eugen read as first=Knoebl,last=Eugen matches it perfectly against the address
  // eugen.knoebl@, and taking that at face value writes the surname into the First field. The
  // TEMPLATE that matched says which way round the pair actually is, so read the answer off that
  // rather than off the order we happened to try first.
  const out = [];
  for (const cand of [{ first: cap(parts[0]), last: cap(parts[parts.length - 1]) },
                      { first: cap(parts[parts.length - 1]), last: cap(parts[0]) }]) {
    if (/[^A-Za-z'-]/.test(cand.first + cand.last)) continue;
    if (cand.first.length < 2 || cand.last.length < 2) continue;
    const tpl = templateFor(local, cand.first, cand.last);
    if (!tpl) continue;
    const both = /\{(first|f)\}/.test(tpl) && /\{(last|l)\}/.test(tpl);
    // Only a TWO-PART template can tell us the order. {last}.{first} matching eugen.knoebl proves
    // the slug knoebl-eugen is surname-first, so the pair is swapped. A one-part template cannot:
    // {last} matching altenpohl against the slug martina-altenpohl only says which token is the
    // surname -- it says nothing about order, and swapping on it turned Martina Altenpohl into
    // "Altenpohl Martina". For those, keep the slug order, which is first-last by convention.
    const fixed = (both && /^\{(last|l)\}/.test(tpl)) ? { first: cand.last, last: cand.first } : cand;
    out.push({ ...fixed, strong: both });
  }
  if (!out.length) return null;
  const best = out.find((o) => o.strong) || out[0];
  return { first: best.first, last: best.last };
}

function emptyRecord() {
  const r = {};
  for (const f of FIELDS) r[f] = '';
  return r;
}

(async () => {
  if (!process.env.PP_DATABASE_URL) { console.error('PP_DATABASE_URL is not set.'); process.exit(1); }
  if (!process.env.DATABASE_URL) { console.error('DATABASE_URL is not set.'); process.exit(1); }

  const pp = new Pool({ connectionString: process.env.PP_DATABASE_URL, max: 2,
    statement_timeout: 600000, query_timeout: 600000 });
  const db = await makeDb({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL });
  const client = os.makeClient();
  const urlStream = URLS_OUT ? fs.createWriteStream(path.resolve(String(URLS_OUT)), { flags: 'a' }) : null;

  const stat = { scanned: 0, malformed: 0, notAligned: 0, roleOrPersonal: 0, alreadyHeld: 0, named: 0,
    imported: 0, changed: 0, addedRows: 0, urlsNew: 0 };

  async function heldEmails(emails) {
    const have = new Set();
    for (let i = 0; i < emails.length; i += 1000) {
      const batch = emails.slice(i, i + 1000);
      const r = await client.search({ index: os.INDEX, body: {
        size: 0, query: { bool: { filter: [{ terms: { email: batch } }] } },
        aggs: { v: { terms: { field: 'email', size: batch.length } } } } });
      for (const b of (((r.body || r).aggregations || {}).v || {}).buckets || []) have.add(b.key);
    }
    return have;
  }
  async function heldUrls(urls) {
    const have = new Set();
    for (let i = 0; i < urls.length; i += 1000) {
      const batch = urls.slice(i, i + 1000);
      const r = await client.search({ index: os.INDEX, body: {
        size: 0, query: { bool: { filter: [{ terms: { web_source_url: batch } }] } },
        aggs: { v: { terms: { field: 'web_source_url', size: batch.length } } } } });
      for (const b of (((r.body || r).aggregations || {}).v || {}).buckets || []) have.add(b.key);
    }
    return have;
  }

  console.log(`[pp-import] ${WRITE ? 'WRITE' : 'DRY RUN'}${REFRESH ? ' REFRESH' : ''}  limit=${LIMIT || 'none'}  batch=${BATCH}`);

  for (;;) {
    if (LIMIT && stat.scanned >= LIMIT) break;
    const take = LIMIT ? Math.min(BATCH, LIMIT - stat.scanned) : BATCH;
    // Keyset on the primary key: stable, index-ordered, and resumable after a kill.
    const q = await pp.query(
      `SELECT email_norm, page_url, COALESCE(path_id,'') AS path_id
         FROM path_flags
        WHERE is_bio_url AND email_norm > $1
        ORDER BY email_norm
        LIMIT $2`, [after, take]);
    if (!q.rows.length) break;
    after = q.rows[q.rows.length - 1].email_norm;
    stat.scanned += q.rows.length;

    // Alignment and our own classifier, both before touching the Contact Crawler.
    const cands = [];
    const surveyRows = [];
    for (const row of q.rows) {
      const email = cleanEmail(row.email_norm).toLowerCase();
      // Counted, not silently skipped: PP carries addresses with ORCIDs and UUIDs welded to the
      // local part, and a funnel whose numbers do not add up hides how many.
      if (!email || !row.page_url) { stat.malformed++; continue; }
      // A /bioXXX word-form is not a bio path; it is counted with the alignment failures because
      // it is the same kind of evidence failure.
      const aligned = alignedToEmail(row.page_url, email)
        && !/\/(bio|profil)[a-z0-9]/i.test(row.page_url);
      const professional = classifyEmail(email) === 'Professional';
      if (SURVEY) { surveyRows.push({ email, url: row.page_url, aligned, professional }); continue; }
      if (!aligned && !ALLOW_UNALIGNED) { stat.notAligned++; continue; }
      if (!professional && !ALLOW_ROLE) { stat.roleOrPersonal++; continue; }
      cands.push({ email, url: row.page_url, pid: row.path_id, professional });
    }

    // Survey: tally every gate combination over the same rows, then move on without importing.
    if (SURVEY) {
      if (surveyRows.length) {
        const held = await heldEmails(surveyRows.map((r) => r.email));
        for (const r of surveyRows) {
          const fresh = !held.has(r.email);
          // The name is what decides whether a row can ever carry a gender, so it is measured
          // per row rather than assumed.
          const named = !!nameFromUrl(r.url, r.email);
          const gendered = wouldGender(r.url, r.email);
          for (const combo of COMBOS) {
            if (!combo.pass(r.aligned, r.professional)) continue;
            combo.cands++;
            if (fresh) { combo.fresh++; if (named) combo.named++; if (gendered) combo.gendered++; }
          }
        }
      }
      continue;
    }
    if (!cands.length) continue;

    const held = REFRESH ? new Set() : await heldEmails(cands.map((c) => c.email));
    const fresh = cands.filter((c) => !held.has(c.email));
    stat.alreadyHeld += cands.length - fresh.length;
    if (!fresh.length) continue;

    if (urlStream) {
      const heldU = await heldUrls(fresh.map((c) => c.url));
      for (const c of fresh) if (!heldU.has(c.url)) { urlStream.write(c.url + '\n'); stat.urlsNew++; }
    }

    const recs = [];
    const stamp = new Date().toISOString();
    for (const c of fresh) {
      const rec = emptyRecord();
      rec['Time Stamp'] = stamp;
      rec['Source'] = SOURCE_TAG;
      rec['Web Source URL'] = c.url;
      rec['Directory'] = c.pid || '';
      rec['Last Path'] = lastPath(c.url);
      rec['Email Address'] = c.email;
      // OBSERVED on the company's own site -- never modelled. With --allow-role the set is no
      // longer all Professional, and hardcoding it would mislabel every role address it let in.
      rec['Email Type'] = c.professional ? 'Professional' : classifyEmail(c.email);
      const nm = nameFromUrl(c.url, c.email);
      if (nm) { rec['First'] = nm.first; rec['Last'] = nm.last; stat.named++; }
      // Gender deliberately left blank: the lexicon fills it downstream, and only when blank.
      recs.push(rec);
    }

    if (WRITE) {
      const res = await db.upsertMany(recs);
      stat.imported += res.processed || 0;
      stat.changed += res.changed || 0;
      stat.addedRows += res.added || 0;
    } else {
      stat.imported += recs.length;
    }

    if (stat.scanned % (BATCH * 5) === 0 || !LIMIT) {
      console.log(`[pp-import] scanned ${stat.scanned}  new ${stat.imported}  named ${stat.named}  (at ${after})`);
    }
  }

  if (urlStream) await new Promise((r) => urlStream.end(r));
  console.log('');
  console.log('---------------------------------------------------------------');
  if (SURVEY) {
    console.log(`SURVEY over ${stat.scanned.toLocaleString()} is_bio_url row(s) -- nothing imported\n`);
    console.log('gate configuration                     passes     net-new   url-named    GENDERED  gender%');
    const base = COMBOS[0];
    for (const c of COMBOS) {
      const pctG = c.fresh ? (c.gendered / c.fresh * 100).toFixed(1) + '%' : '-';
      console.log('  ' + c.label.padEnd(36)
        + String(c.cands).padStart(8) + String(c.fresh).padStart(12)
        + String(c.named).padStart(12) + String(c.gendered).padStart(12) + pctG.padStart(9));
    }
    console.log('');
    console.log('  what each lift ADDS over the current configuration:');
    for (const c of COMBOS.slice(1)) {
      const addFresh = c.fresh - base.fresh, addG = c.gendered - base.gendered;
      console.log(`  ${c.label.padEnd(26)} +${String(addFresh).padStart(8)} row(s), `
        + `+${String(addG).padStart(6)} with a gender`
        + ` (${addFresh ? (addG / addFresh * 100).toFixed(1) + '%' : '-'} of what it adds)`);
    }
    console.log(`\nresume with --after ${after}`);
    await pp.end();
    if (db.close) await db.close();
    return;
  }
  console.log(`scanned is_bio_url rows      ${stat.scanned}`);
  console.log(`  dropped: malformed address  ${stat.malformed}`);
  console.log(`  dropped: not domain-aligned ${stat.notAligned}`);
  console.log(`  dropped: role / freemail    ${stat.roleOrPersonal}`);
  console.log(`  dropped: already held       ${stat.alreadyHeld}`);
  console.log(`  ${WRITE ? 'IMPORTED' : 'would import'}            ${stat.imported}`);
  console.log(`     of those, named from URL ${stat.named}`);
  if (WRITE) console.log(`     rows written / new       ${stat.changed} / ${stat.addedRows}`);
  if (urlStream) console.log(`  crawlable URLs written     ${stat.urlsNew} -> ${URLS_OUT}`);
  console.log(`resume with --after ${after}`);
  await pp.end();
  if (db.close) await db.close();
})().catch((e) => { console.error('FAILED:', e.message, e.stack); process.exit(1); });
