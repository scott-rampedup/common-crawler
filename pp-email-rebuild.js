#!/usr/bin/env node
//
// Rebuild Modelled addresses whose LOCAL PART was guessed wrong, and confirm the result against
// Public Prospects.
//
// SAME DOMAIN ONLY, deliberately. The blind {first}.{last} default produced two distinct errors and
// only one of them is safely fixable in bulk:
//
//   LOCAL  the right company, the wrong format. uwhealth.org is {f}{last}, so 2,591 rows there are
//          aanandan@-shaped people written as apoorva.anandan@. Rebuilding is low risk: the worst
//          case is swapping one wrong local part for another at the same employer.
//   CROSS  a different mail domain entirely. Sometimes real (pennmedicine.org -> upenn.edu at 100%
//          of 230 samples), but on a DIRECTORY it is catastrophic: bestlawyers.com resolves to
//          vonbriesen.com on 3% agreement, which would re-address 65,957 lawyers to one firm that
//          employs almost none of them. This tool refuses every cross-domain change, full stop.
//
// WHY DELETE-AND-INDEX RATHER THAN UPDATE. The store is email-keyed: the address IS the _id, so
// changing it is a new document, not an edited one. The naive version of that loses everything the
// record had accumulated, which is why this carries the whole _source across and only swaps the
// address fields. It also writes through bulkUpsert rather than indexDocs, to keep the three things
// that path provides: opt-out suppression, the score gate (so a re-addressed row cannot clobber a
// better record that already sits at the new address), and the firmographic carry-over.
//
// ORDER IS INDEX-THEN-DELETE. If the delete ran first and the index then failed, the person would be
// gone. This way the worst case is a duplicate, which is recoverable.
//
//   node pp-email-rebuild.js --domain uwhealth.org            # dry run, one domain
//   node pp-email-rebuild.js --domain uwhealth.org --write
//   node pp-email-rebuild.js --domains-top 200 --write        # every qualifying domain
const os = require('./opensearch');
const pp = require('./pp-source');
const emailModel = require('./email-model');
const { inferEmailPattern, render } = require('./email-pattern');
const { Client } = require('pg');

const argv = process.argv.slice(2);
const flag = (n, d) => { const i = argv.indexOf('--' + n); return i >= 0 ? (argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true) : d; };
const ONE_DOMAIN = String(flag('domain', '') || '');
const TOP = Number(flag('domains-top', 200)) || 200;
const WRITE = !!flag('write', false);
const LIMIT = Number(flag('limit', 0)) || 0;
const PAGE = 1000;

(async () => {
  const client = os.makeClient();
  const pg = new Client({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL ? { rejectUnauthorized: false } : false });
  await pg.connect();
  await pg.query("SET statement_timeout='240s'");

  // ---- which domains, and what pattern ------------------------------------
  async function samplesFor(domain) {
    const out = []; const seen = new Set();
    const take = (rows) => { for (const r of rows || []) {
      const e = String(r['Email Address'] || '').trim().toLowerCase();
      if (!e || seen.has(e)) continue;
      if (String(r['Email Type'] || '').toLowerCase() === 'modelled') continue;   // never learn from a guess
      seen.add(e); out.push(r); } };
    try { take((await pg.query(
      'SELECT first AS "First", last AS "Last", email_address AS "Email Address", email_type AS "Email Type"'
      + " FROM contacts WHERE domain=$1 AND lower(email_type)='professional' LIMIT 400", [domain])).rows); } catch (e) { /* */ }
    try { take((await os.search(client, { domain, emailType: 'Professional', pageSize: 400 })).rows); } catch (e) { /* */ }
    if (pp.enabled()) { try { take(await pp.patternSamples([domain, emailModel.registrableDomain(domain)])); } catch (e) { /* */ } }
    return out;
  }

  async function resolveDomain(domain) {
    const s = (await samplesFor(domain)).filter((r) => r.First && r.Last)
      .map((r) => ({ first: r.First, last: r.Last, email: String(r['Email Address']).toLowerCase() }));
    if (s.length < 2) return { skip: 'no evidence' };
    const p = inferEmailPattern(s);
    if (!p || !p.pattern) return { skip: 'no pattern' };
    const own = emailModel.registrableDomain(domain) || domain;
    if (p.domain && p.domain !== domain && p.domain !== own) return { skip: 'cross-domain (refused)' };
    if (p.pattern === '{first}.{last}') return { skip: 'already {first}.{last}' };
    return { pattern: p.pattern, samples: s.length };
  }

  let domains = [];
  if (ONE_DOMAIN) domains = [ONE_DOMAIN];
  else {
    const agg = await client.search({ index: os.INDEX, body: {
      size: 0, query: { term: { email_type: 'Modelled' } },
      aggs: { d: { terms: { field: 'domain', size: TOP } } } } });
    domains = ((((agg.body || agg).aggregations || {}).d || {}).buckets || []).map((b) => b.key);
  }

  const plan = [];
  for (const d of domains) {
    const r = await resolveDomain(d);
    if (r.skip) { if (ONE_DOMAIN) console.log(`${d}: SKIPPED -- ${r.skip}`); continue; }
    plan.push({ domain: d, ...r });
  }
  console.log(`${WRITE ? 'WRITE' : 'DRY RUN'}  ${plan.length} domain(s) qualify (same-domain local-part change only)\n`);
  if (!plan.length) { await pg.end(); await pp.close(); return; }

  // ---- rebuild ------------------------------------------------------------
  const tot = { seen: 0, noName: 0, unchanged: 0, rebuilt: 0, collided: 0, confirmed: 0, indexed: 0, deleted: 0, errors: 0 };

  for (const { domain, pattern, samples } of plan) {
    let after = null, dSeen = 0, dRebuilt = 0, dConfirmed = 0;
    for (;;) {
      if (LIMIT && tot.seen >= LIMIT) break;
      const body = {
        size: PAGE, sort: [{ email: 'asc' }],
        query: { bool: { filter: [{ term: { domain } }, { term: { email_type: 'Modelled' } }] } },
      };
      if (after) body.search_after = after;
      const r = await client.search({ index: os.INDEX, body });
      const hits = ((r.body || r).hits.hits) || [];
      if (!hits.length) break;
      after = hits[hits.length - 1].sort;

      const pairs = [];
      for (const h of hits) {
        const src = h._source || {};
        tot.seen++; dSeen++;
        const oldEmail = String(src.email || '').trim().toLowerCase();
        const at = oldEmail.lastIndexOf('@');
        if (at < 1) continue;
        const mailDomain = oldEmail.slice(at + 1);
        if (!src.first || !src.last) { tot.noName++; continue; }
        const local = render(pattern, src.first, src.last);
        if (!local) { tot.noName++; continue; }
        const newEmail = `${local}@${mailDomain}`;
        if (newEmail === oldEmail) { tot.unchanged++; continue; }
        pairs.push({ oldEmail, newEmail, src });
      }
      if (!pairs.length) continue;

      // Public Prospects as the validator: an address it has SEEN is not a guess any more. This is
      // also the only deliverability signal available -- the Exhaust API answers status:"error" for
      // every address, including real ones (see fly.toml).
      let observed = new Map();
      if (pp.enabled()) {
        try { observed = await pp.observed(pairs.map((p) => p.newEmail)); } catch (e) { /* non-fatal */ }
      }

      // Does the corrected address ALREADY exist as its own record?
      //
      // If it does, the modelled row is a duplicate of a person we already hold properly -- often the
      // real crawled record, Professional, with a bio page behind it. Upserting over it would be
      // decided purely by the score gate, and a Modelled row with more filled columns can outscore a
      // verified one. Provenance is not a column you can win on points, so the modelled row is simply
      // removed instead and the existing record is left untouched.
      const existing = new Set();
      try {
        const chk = await client.search({ index: os.INDEX, body: { size: 0,
          query: { bool: { filter: [{ terms: { email: pairs.map((p) => p.newEmail) } }] } },
          aggs: { e: { terms: { field: 'email', size: pairs.length } } } } });
        for (const b of ((((chk.body || chk).aggregations || {}).e || {}).buckets || [])) existing.add(b.key);
      } catch (e) { /* on failure, fall through and let the score gate decide */ }

      const docs = [];
      const dropOnly = [];
      for (const p of pairs) {
        if (existing.has(p.newEmail)) { tot.collided++; dropOnly.push(p.oldEmail); continue; }
        const ev = observed.get(pp.normEmail(p.newEmail));
        const doc = { ...p.src, email: p.newEmail, email_address: p.newEmail };
        Object.defineProperty(doc, '_oldEmail', { value: p.oldEmail, enumerable: false });
        if (ev) { doc.email_type = 'Professional'; tot.confirmed++; dConfirmed++; }
        docs.push(doc);
      }

      if (WRITE) {
        // The old modelled address goes in BOTH cases: when it has been re-indexed under the
        // corrected address, and when the corrected address already existed and the row was a
        // duplicate. The only difference is whether a replacement was written first.
        let removable = [...dropOnly];
        if (docs.length) {
          const up = await os.bulkUpsert(client, docs);
          const errs = (up && up.errors) || 0;
          tot.indexed += docs.length - errs;
          tot.errors += errs;
          // Index-then-delete, and only when the index reported no failures. A hole is unrecoverable;
          // a duplicate is not, so on any error the old rows are left where they are.
          if (!errs) removable = removable.concat(docs.map((d) => d._oldEmail));
        }
        if (removable.length) {
          const del = await os.bulkDelete(client, removable);
          tot.deleted += (del && del.deleted) || 0;
        }
      }
      tot.rebuilt += docs.length; dRebuilt += docs.length;
    }
    console.log(`  ${domain.padEnd(32)} ${pattern.padEnd(16)} samples=${String(samples).padStart(4)}`
      + `  scanned=${String(dSeen).padStart(6)} rebuilt=${String(dRebuilt).padStart(6)} confirmedByPP=${dConfirmed}`);
  }

  console.log('\n---------------------------------------------------------------');
  console.log(`scanned Modelled docs        ${tot.seen}`);
  console.log(`  no usable name             ${tot.noName}`);
  console.log(`  already correct            ${tot.unchanged}`);
  console.log(`  corrected address already held -> modelled row dropped  ${tot.collided}`);
  console.log(`  ${WRITE ? 'REBUILT' : 'would rebuild'}              ${tot.rebuilt}`);
  console.log(`     confirmed by PP -> Professional  ${tot.confirmed}`);
  if (WRITE) console.log(`  indexed ${tot.indexed} / deleted ${tot.deleted} / errors ${tot.errors}`);
  if (!WRITE) console.log('\n(dry run -- pass --write to apply)');
  await pg.end(); await pp.close();
})().catch((e) => { console.error('FAILED:', e.message, e.stack); process.exit(1); });
