/**
 * enrich-companies-cc.js — LOCAL batch: enrich Company-Crawler companies from their CC home page.
 * Runs locally because index.commoncrawl.org (the resolver) refuses the Fly IP; from a normal IP it works.
 *   OPENSEARCH_ENDPOINT=… node enrich-companies-cc.js '{"industry":"real estate","contactMin":"10"}' [CAP]
 * Reads companies matching the filter, resolves + parses each home page, writes the CC fields back.
 */
const co = require('./companies');
const cc = require('./cc-engine');
const che = require('./cc-home-enrich');
const ex = require('./extractor');

(async () => {
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const gm = ex.loadGenderMap('./names-genders.csv');
  const crawl = await cc.resolveLatestCrawl();
  const crawls = [crawl];
  const filter = process.argv[2] ? JSON.parse(process.argv[2]) : { contactMin: '1' };
  const CAP = Number(process.argv[3]) || Number(process.env.CAP) || 200;
  const CONC = Number(process.env.CONC) || 4;

  const targets = [];
  let from = 0;
  for (;;) {
    const r = await co.search(client, filter, { from, size: Math.min(200, CAP - targets.length), sort: 'contact_count', dir: 'desc' });
    if (!r.rows.length) break;
    targets.push(...r.rows); from += r.rows.length;
    if (targets.length >= CAP || from >= r.total) break;
  }
  // The admin-editable list, NOT the built-in seed. Omitting it here silently fell back to ALT_DEFAULT's
  // 12 patterns while the saved list held 21, so everything the admin added (calendly.com, github.com,
  // meetup.com, lnk.bio, patreon.com, tinyurl.com, angel.co, godaddy.com) was never matched.
  const altList = await co.getAltWebsites(client);
  console.error(`enriching ${targets.length} companies from ${crawl} (conc ${CONC}), ${altList.length} alternate-website pattern(s)`);

  let i = 0, found = 0, updated = 0, contacts = 0, errs = 0, reclassed = 0;
  const t0 = Date.now();
  async function worker() {
    for (;;) {
      const k = i++; if (k >= targets.length) return;
      const s = targets[k];
      try {
        const r = await che.enrichCompany(s, { genderMap: gm, crawls, fetchWarc: cc.fetchWarc, altList });
        // A company whose Website is an alternate host usually has no home page in CC, so "not found"
        // is the NORMAL outcome for exactly the records this rule exists to fix. Apply the move anyway.
        if (!r.found) {
          if (r.reclassified) { await co.update(client, s.id, r.updates); reclassed++; updated++; }
          continue;
        }
        found++; await co.update(client, s.id, r.updates); updated++; contacts += (r.updates.contacts_count || 0);
      } catch (e) { errs++; }
      if ((k + 1) % 25 === 0) { const el = (Date.now() - t0) / 1000; console.error(`  ${k + 1}/${targets.length} | found ${found} | updated ${updated} | contacts ${contacts} | ${errs} err | ${((k + 1) / el).toFixed(1)}/s`); }
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  console.error(`DONE: ${found} found, ${updated} updated, ${reclassed} website(s) reclassified, ${contacts} contacts, ${errs} err, ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
