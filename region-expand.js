/**
 * region-expand.js — expand two-letter region codes to the spelled-out name the rest of the index uses.
 * ---------------------------------------------------------------------------------------------
 * `region` carries both forms: PDL writes "california" (18,959,057 records on spelled-out values),
 * the Google/Bing location loaders write "ca" (3,632,314 records on 207 two-letter values). Both are
 * lower-case after canon-place-repair, so the split is no longer about casing — it is two vocabularies.
 * A filter on one never sees the other.
 *
 * The mapping is keyed on (country, code), never on the code alone, because the codes collide:
 *   wa  = washington (us) BUT western australia (australia)
 *   sa  = south australia (australia) BUT shaanxi (china)
 *   nt  = northwest territories (canada) BUT northern territory (australia)
 *   mi  = michigan (us) BUT milano (italy)
 * A code-only map would have rewritten Australian and Italian rows with US state names.
 *
 * Scope: the US (4,177,995 records over 56 codes) and Canada (5,113 over 13) — 99.9% of the problem.
 * Italy's 102 province codes (1,673 records), China's 30 (818), Switzerland's 27 (777) are left alone:
 * they need their own province tables and together they are under 3,300 records.
 * Codes that are not real subdivisions are left alone too — "le" and "us" under united states, and
 * "ny" under canada, which is mislabelled data rather than a code to expand.
 *
 *   node region-expand.js             # dry run: what would change, per code
 *   node region-expand.js --apply
 */
const co = require('./companies');

const APPLY = process.argv.includes('--apply');
const N = (x) => Number(x || 0).toLocaleString();

const US = {
  al: 'alabama', ak: 'alaska', az: 'arizona', ar: 'arkansas', ca: 'california', co: 'colorado',
  ct: 'connecticut', de: 'delaware', dc: 'district of columbia', fl: 'florida', ga: 'georgia',
  hi: 'hawaii', id: 'idaho', il: 'illinois', in: 'indiana', ia: 'iowa', ks: 'kansas', ky: 'kentucky',
  la: 'louisiana', me: 'maine', md: 'maryland', ma: 'massachusetts', mi: 'michigan', mn: 'minnesota',
  ms: 'mississippi', mo: 'missouri', mt: 'montana', ne: 'nebraska', nv: 'nevada', nh: 'new hampshire',
  nj: 'new jersey', nm: 'new mexico', ny: 'new york', nc: 'north carolina', nd: 'north dakota',
  oh: 'ohio', ok: 'oklahoma', or: 'oregon', pa: 'pennsylvania', ri: 'rhode island',
  sc: 'south carolina', sd: 'south dakota', tn: 'tennessee', tx: 'texas', ut: 'utah', vt: 'vermont',
  va: 'virginia', wa: 'washington', wv: 'west virginia', wi: 'wisconsin', wy: 'wyoming',
  pr: 'puerto rico', vi: 'virgin islands', gu: 'guam', as: 'american samoa', mp: 'northern mariana islands',
};
const CA = {
  ab: 'alberta', bc: 'british columbia', mb: 'manitoba', nb: 'new brunswick',
  nl: 'newfoundland and labrador', ns: 'nova scotia', nt: 'northwest territories', nu: 'nunavut',
  on: 'ontario', pe: 'prince edward island', qc: 'quebec', sk: 'saskatchewan', yt: 'yukon',
};
// "usa" is a second spelling of the same country, 55 records of it — see the note at the bottom.
const PLAN = [
  { countries: ['united states', 'usa'], map: US },
  { countries: ['canada'], map: CA },
];

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const count = (q) => c.count({ index: co.INDEX, body: { query: q } }, { requestTimeout: 600000 }).then((r) => Number((r.body || r).count));

  let grand = 0;
  for (const { countries, map } of PLAN) {
    console.log('\n' + countries.join(' / '));
    for (const [code, name] of Object.entries(map)) {
      const q = { bool: { filter: [{ terms: { country: countries } }, { term: { region: code } }] } };
      const n = await count(q);
      if (!n) continue;
      grand += n;
      const already = await count({ bool: { filter: [{ terms: { country: countries } }, { term: { region: name } }] } });
      process.stdout.write('  ' + code + ' -> ' + name.padEnd(26) + N(n).padStart(9) + '  (joins ' + N(already) + ' already spelled out)');
      if (!APPLY) { console.log(''); continue; }

      // One short call per code, deliberately: a single long update_by_query over millions of rows
      // collides with concurrent writes for its whole life and then reports a tally that is wrong.
      const res = await c.updateByQuery({
        index: co.INDEX, conflicts: 'proceed', slices: 'auto', wait_for_completion: true,
        body: { query: q, script: { lang: 'painless', params: { name },
                source: "if (ctx._source.region != params.name) { ctx._source.region = params.name; } else { ctx.op = 'noop'; }" } },
      }, { requestTimeout: 3600000 }).then((x) => (x.body || x));
      const left = await count(q);
      console.log('  -> updated ' + N(res.updated) + (res.version_conflicts ? ', conflicts ' + N(res.version_conflicts) : '') + (left ? ', ' + N(left) + ' LEFT' : ''));
    }
  }
  console.log('\ntotal records in scope: ' + N(grand));

  if (APPLY) {
    await c.indices.refresh({ index: co.INDEX });
    // The country column has the same two-vocabulary problem in miniature: "usa" alongside
    // "united states". Fixed here because it is the same 55 records this pass just touched.
    const usa = await count({ term: { country: 'usa' } });
    if (usa) {
      await c.updateByQuery({
        index: co.INDEX, conflicts: 'proceed', slices: 'auto', wait_for_completion: true, refresh: true,
        body: { query: { term: { country: 'usa' } },
                script: { lang: 'painless', source: "ctx._source.country = 'united states';" } },
      }, { requestTimeout: 1800000 });
      console.log('country "usa" -> "united states": ' + N(usa) + ' record(s), ' + N(await count({ term: { country: 'usa' } })) + ' left');
    }
  } else {
    console.log('\nDry run — nothing written. Add --apply.');
  }
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
