/**
 * clean-contact-linkedin.js — clear malformed/company linkedin_url on existing CONTACTS so a real /in URL
 * can populate the field (mirrors opensearch.cleanContactLinkedin). Scopes to wildcard candidates; the
 * painless script clears only the truly-bad values (company pages + concatenated junk), noops person slugs
 * that merely contain "company" (e.g. /in/the-ev-charging-company-ltd).
 *   OPENSEARCH_ENDPOINT=… node clean-contact-linkedin.js [--dry]
 */
const os = require('./opensearch');
const SCRIPT = `
  String u = ctx._source.linkedin_url;
  if (u == null || u.isEmpty()) { ctx.op = 'noop'; return; }
  String low = u.toLowerCase();
  boolean bad = false;
  int i1 = low.indexOf('linkedin.com');
  if (i1 >= 0 && low.indexOf('linkedin.com', i1 + 1) >= 0) bad = true;   // two linkedin.com -> concatenated
  int s1 = low.indexOf('://');
  if (s1 >= 0 && low.indexOf('://', s1 + 1) >= 0) bad = true;            // two schemes -> concatenated
  if (low.contains('linkedin.com/company')) bad = true;                  // company page
  if (low.contains('linkedin.com/in/company/')) bad = true;             // /in/company/… mis-path
  if (low.endsWith('linkedin.com/in/company')) bad = true;
  if (bad) { ctx._source.linkedin_url = ''; } else { ctx.op = 'noop'; }
`;
const QUERY = { bool: { should: [
  { wildcard: { linkedin_url: '*company*' } },
  { wildcard: { linkedin_url: '*linkedin.com*linkedin.com*' } },
  { wildcard: { linkedin_url: '*/in/http*' } },
], minimum_should_match: 1 } };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const cand = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.log('candidate contacts (contain company/concatenated):', cand.toLocaleString());
  if (process.argv.includes('--dry')) return;
  const r = await client.updateByQuery({ index: os.INDEX, conflicts: 'proceed', refresh: true, wait_for_completion: true,
    body: { query: QUERY, script: { lang: 'painless', source: SCRIPT } } }, { requestTimeout: 300000 });
  const b = r.body || r;
  console.log(`cleared ${b.updated.toLocaleString()} bad linkedin_url / ${b.total.toLocaleString()} scanned | ${b.version_conflicts} conflicts`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 400) : e.message); process.exit(1); });
