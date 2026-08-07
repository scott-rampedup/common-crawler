/**
 * model-and-ingest-archives.js — recover email-less contacts from the archived job files.
 *
 * Bio crawls (sitemap/webpage) often extract a person (name + phone + LinkedIn) but NO email; the
 * contacts store is keyed by email, so those records were silently dropped and never reached the Contact
 * Crawler. This driver scans every job file, collects the email-less-but-named records, MODELS an email
 * for each, then ingests them through the normal upsert (dedupe by email). Company-level data is added
 * afterward by the existing firmographic sweep (or --enrich to trigger it here).
 *
 * Email modelling, in priority order (all tagged Email Type = "Modelled"):
 *   1. the company's STORED email model (Bulk Edit) — companies.getEmailModel(domain)
 *   2. the domain's REAL pattern learned from existing Professional emails in the DB (dbQuery)
 *   3. a default {first}.{last}@<root-domain> guess (configurable via --pattern)
 *
 *   OPENSEARCH_ENDPOINT=… DATABASE_URL=… node model-and-ingest-archives.js [--dry] [--enrich]
 *     [--pattern={first}.{last}] [--domain=<substr>] [--limit=N] [--jobs=/data/jobs]
 *
 * Idempotent: upsert is by email (score-gated), so re-runs and modelled↔real collisions dedupe safely.
 */
const fs = require('fs');
const path = require('path');
const { makeDb } = require('./db-pg');
const emailModel = require('./email-model');
const { render } = require('./email-pattern');
const { cleanEmail, analyzePhones } = require('./extractor');
const companies = require('./companies');
const firmoEnrich = require('./enrich-firmographics');
const openSearch = require('./opensearch');
const emailVerify = require('./email-verify');

const arg = (n, d) => { const a = process.argv.find((x) => x.startsWith('--' + n + '=')); return a ? a.split('=').slice(1).join('=') : d; };
const has = (n) => process.argv.includes('--' + n);

// registrable-ish domain (strip subdomains); keeps two-part TLDs like co.uk together
function rootDomain(d) {
  d = String(d || '').toLowerCase().replace(/^https?:\/\//, '').split('/')[0].replace(/^www\./, '').trim();
  if (!d) return '';
  const parts = d.split('.');
  if (parts.length <= 2) return d;
  const last2 = parts.slice(-2).join('.');
  return /^(co|com|org|net|gov|edu|ac)\.[a-z]{2}$/.test(last2) ? parts.slice(-3).join('.') : last2;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT || !process.env.DATABASE_URL) { console.error('need OPENSEARCH_ENDPOINT + DATABASE_URL'); process.exit(1); }
  const dry = has('dry');
  const DEFAULT_TPL = arg('pattern', '{first}.{last}');
  const domainFilter = arg('domain', '').toLowerCase();
  const limit = Number(arg('limit', 0)) || 0;
  const JOBS_DIR = arg('jobs', process.env.JOBS_DIR || '/data/jobs');
  // Correction mode: force ONE pattern + email domain for the matched records (skips learn/default),
  // optionally delete the previously-modelled records for that email domain first, and store the model.
  const forcePattern = arg('force-pattern', '');
  const forceEmailDomain = arg('force-email-domain', '').toLowerCase();
  const deleteModelled = has('delete-modelled');
  const storeModel = has('store-model');
  const verify = has('verify');                       // validate candidates against the deliverability API
  const requireGood = !has('no-require-good');        // with --verify, drop records that never validate GOOD

  const db = await makeDb({ connectionString: process.env.DATABASE_URL, ssl: !!process.env.PGSSL });
  const coClient = companies.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const osClient = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const OSINDEX = process.env.OPENSEARCH_INDEX || 'contacts';

  // 1) Collect email-less-but-named records across all job files (dedupe by source URL).
  const files = fs.readdirSync(JOBS_DIR).filter((f) => f.endsWith('.json') && f.startsWith('j_'));
  const seen = new Set();
  const emailless = [];
  let scannedRecs = 0;
  for (const f of files) {
    let j; try { j = JSON.parse(fs.readFileSync(path.join(JOBS_DIR, f), 'utf8')); } catch (e) { continue; }
    for (const r of (j.records || [])) {
      scannedRecs++;
      if (cleanEmail(r['Email Address'])) continue;                 // already has an email → already loadable
      if (!(r['First'] && r['Last'])) continue;                     // need a name to model
      const dom = String(r['Domain'] || '').toLowerCase() || emailModel.rootDomain(r['Web Source URL']);
      if (domainFilter && !dom.includes(domainFilter)) continue;
      const key = String(r['Web Source URL'] || '') || (dom + '|' + r['First'] + '|' + r['Last']);
      if (seen.has(key)) continue; seen.add(key);
      emailless.push(r);
      if (limit && emailless.length >= limit) break;
    }
    if (limit && emailless.length >= limit) break;
  }
  console.error(`Scanned ${scannedRecs.toLocaleString()} archived record(s) across ${files.length} job file(s).`);
  console.error(`Email-less, named, unique: ${emailless.length.toLocaleString()}`);
  if (!emailless.length) { console.error('Nothing to recover.'); process.exit(0); }

  let learned = 0, defaulted = 0, forced = 0;
  if (forcePattern && forceEmailDomain) {
    // Correction mode: model EVERY matched record with the one explicit pattern + email domain.
    for (const r of emailless) {
      const local = render(forcePattern, r['First'], r['Last']);
      if (!local) continue;
      r['Email Address'] = `${local}@${forceEmailDomain}`;
      r['Email Type'] = 'Modelled';
      forced++;
    }
    console.error(`Forced pattern "${forcePattern}" @${forceEmailDomain}: ${forced.toLocaleString()} modelled`);
  } else if (verify) {
    // Validated mode: stored/learned/default candidates tried against the deliverability API until GOOD.
    learned = await emailModel.modelMissingEmails(emailless, {
      dbQuery: (domain) => db.query({ domain, emailType: 'Professional', pageSize: 500 }).then((r) => r.rows || []),
      patternQuery: async (domain) => (await companies.getEmailModel(coClient, domain)) || (await companies.getEmailModel(coClient, emailModel.registrableDomain(domain))),
      defaultPattern: DEFAULT_TPL, verify: emailVerify.verifyEmail, requireGood,
    });
  } else {
    // 2) Learn where possible (stored model, then real emails for the domain).
    learned = await emailModel.modelMissingEmails(emailless, {
      dbQuery: (domain) => db.query({ domain, emailType: 'Professional', pageSize: 500 }).then((r) => r.rows || []),
      patternQuery: (domain) => companies.getEmailModel(coClient, domain),
    });
    // 3) Default-pattern fallback for whatever's still email-less.
    for (const r of emailless) {
      if (cleanEmail(r['Email Address'])) continue;
      const dom = rootDomain(r['Domain'] || emailModel.rootDomain(r['Web Source URL']));
      if (!dom) continue;
      const local = render(DEFAULT_TPL, r['First'], r['Last']);
      if (!local) continue;
      r['Email Address'] = `${local}@${dom}`;
      r['Email Type'] = 'Modelled';
      defaulted++;
    }
  }

  const modelled = emailless.filter((r) => cleanEmail(r['Email Address']));
  console.error(`Modelled: ${modelled.length.toLocaleString()} (learned ${learned.toLocaleString()}, default-pattern ${defaulted.toLocaleString()}, forced ${forced.toLocaleString()})`);

  // by-domain breakdown (top 20)
  const byDom = new Map();
  for (const r of modelled) { const d = rootDomain(r['Domain'] || emailModel.rootDomain(r['Web Source URL'])); byDom.set(d, (byDom.get(d) || 0) + 1); }
  const top = [...byDom.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);
  console.error('Top domains:', top.map(([d, n]) => `${d}:${n}`).join('  '));

  if (dry) { console.error('\n--dry: no writes. Sample modelled emails:'); for (const r of modelled.slice(0, 8)) console.error(`  ${r['First']} ${r['Last']} <${r['Email Address']}> [${r['Email Type']}] ${r['Domain']}`); process.exit(0); }

  // 3b) Correction: remove the previously-modelled records for this email domain from BOTH stores first,
  // so the wrong-pattern versions don't linger alongside the corrected ones (osSync doesn't mirror deletes).
  if (deleteModelled && forceEmailDomain) {
    const q = { bool: { must: [{ term: { email_type: 'Modelled' } }, { wildcard: { email: `*@${forceEmailDomain}` } }] } };
    const emails = [];
    let after = null;
    for (;;) {
      const body = { size: 5000, _source: false, query: q, sort: [{ email: 'asc' }] };
      if (after) body.search_after = after;
      const hits = (await osClient.search({ index: OSINDEX, body })).body.hits.hits;
      if (!hits.length) break;
      for (const h of hits) emails.push(h._id);
      after = hits[hits.length - 1].sort;
      if (hits.length < 5000) break;
    }
    console.error(`Deleting ${emails.length.toLocaleString()} previously-modelled *@${forceEmailDomain} record(s) from both stores…`);
    for (const e of emails) { try { await db.deleteByEmail(e); } catch (err) { /* */ } }
    try { await openSearch.bulkDelete(osClient, emails); } catch (err) { console.error('OS bulkDelete:', err.message); }
  }

  // 4) Ingest via the normal upsert (dedupe by email, score-gated).
  const analyzed = analyzePhones(modelled);
  let processed = 0, added = 0;
  for (let i = 0; i < analyzed.length; i += 2000) {
    const r = await db.upsertMany(analyzed.slice(i, i + 2000));
    processed += r.processed || 0; added += r.added || 0;
    if ((i / 2000) % 10 === 0) console.error(`  ingested ${Math.min(i + 2000, analyzed.length).toLocaleString()}/${analyzed.length.toLocaleString()} (added ${added.toLocaleString()})`);
  }
  console.error(`DONE ingest: processed ${processed.toLocaleString()}, added ${added.toLocaleString()} new. (Postgres → OpenSearch via delta sync.)`);

  // Store the email model on the company so FUTURE crawls/modelling of this domain use it automatically.
  if (storeModel && forcePattern && forceEmailDomain) {
    try {
      const r = await companies.setEmailModelByDomain(coClient, forceEmailDomain, { pattern: forcePattern, email_domain: forceEmailDomain });
      console.error(`Stored email model on company ${forceEmailDomain}: pattern "${forcePattern}" (updated ${((r && r.updated) || 0)} company doc(s)).`);
    } catch (e) { console.error('store-model failed:', e.message); }
  }

  // 5) Optional: trigger the firmographic (company-level) enrichment now, newest-first.
  if (has('enrich')) {
    console.error('Waiting 60s for the OpenSearch delta sync to catch up, then enriching…');
    await new Promise((r) => setTimeout(r, 60000));
    const client = openSearch.makeClient(process.env.OPENSEARCH_ENDPOINT);
    const fe = await firmoEnrich.enrichMissing({ client, coClient, endpoint: process.env.OPENSEARCH_ENDPOINT, limit: Math.max(50000, added * 2), newestFirst: true, log: (m) => console.error('[firmo]', m) });
    console.error(`firmo enriched ${((fe && fe.updated) || 0).toLocaleString()} contact(s).`);
  } else {
    console.error('Company-level data will be filled by the periodic firmographic sweep (newest-first). Use --enrich to force it now.');
  }
  process.exit(0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
