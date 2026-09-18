/**
 * team-page-load.js — attach a supplied list of team-page URLs to Company Crawler records.
 *
 *   OPENSEARCH_ENDPOINT=… node team-page-load.js --in list.txt [--dry] [--overwrite]
 *
 * The list is one URL per line (a header line like "Team Pages" is ignored). A scheme is optional --
 * "acme.com/our-team" is accepted and normalised to https. Supplied by spreadsheet, so expect both.
 *
 * Records are matched on HOST: every company on that domain whose team_page is blank gets the URL, so
 * the "Must have Team Page" filter in Company Crawler starts returning them. Existing values are left
 * alone unless --overwrite, because a team page already on the record came from the Maps crawl and has
 * at least been seen live, whereas a list entry has not.
 *
 * EVERY URL IS PUT THROUGH isTeamPage FIRST, the same test team-page-clean.js uses. A supplied list is
 * not self-certifying: of 79,016 URLs in the first sheet, 52% were /about-us, /contact, /index or a
 * bare host. Loading those would refill the field with exactly what the cleanup just removed, and the
 * search filter would go back to returning contact pages. What fails the test is reported, not written,
 * so it can be reviewed rather than silently dropped.
 */
const fs = require('fs');
const co = require('./companies');
const { isTeamPage } = require('./team-page-clean');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('--in', '');
const DRY = process.argv.includes('--dry');
const OVERWRITE = process.argv.includes('--overwrite');
const PAGE = 500;

const withScheme = (u) => (/^https?:\/\//i.test(u) ? u : 'https://' + u.replace(/^\/+/, ''));
const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return ''; } };

// Prefer the more specific team page when a host appears several times: a shorter path is usually the
// canonical roster ("/team") over a deep variant ("/about/leadership/bios/archive").
function better(a, b) {
  if (!a) return b;
  try { return new URL(b).pathname.length < new URL(a).pathname.length ? b : a; } catch (e) { return a; }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!IN || !fs.existsSync(IN)) { console.error('need --in <file>'); process.exit(1); }
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const t0 = Date.now();

  const raw = fs.readFileSync(IN, 'utf8').split('\n').map((s) => s.trim()).filter(Boolean);
  const byHost = new Map();
  let rejected = 0, noHost = 0;
  const rejects = [];
  for (const line of raw) {
    const u = withScheme(line.split(',')[0].trim());
    const h = hostOf(u);
    if (!h || !h.includes('.')) { noHost++; continue; }          // header rows land here
    if (!isTeamPage(u)) { rejected++; if (rejects.length < 12) rejects.push(u); continue; }
    byHost.set(h, better(byHost.get(h), u));
  }
  console.error(`lines read            : ${raw.length.toLocaleString()}`);
  console.error(`  not a URL           : ${noHost.toLocaleString()}`);
  console.error(`  not a team page     : ${rejected.toLocaleString()}  (reported below, not written)`);
  console.error(`  usable hosts        : ${byHost.size.toLocaleString()}`);

  const hosts = [...byHost.keys()];
  let known = 0, targeted = 0, skippedHasOne = 0, updated = 0, errors = 0;
  for (let i = 0; i < hosts.length; i += PAGE) {
    const chunk = hosts.slice(i, i + PAGE);
    // every company record on these domains
    let after = null;
    for (;;) {
      const body = {
        size: 2000, query: { terms: { domain: chunk } },
        _source: ['domain', 'team_page'], sort: [{ _doc: 'asc' }],
      };
      if (after) body.search_after = after;
      const hits = ((await client.search({ index: co.INDEX, body })).body || {}).hits.hits;
      if (!hits || !hits.length) break;
      const actions = [];
      for (const h of hits) {
        known++;
        const dom = String(h._source.domain || '').toLowerCase();
        const url = byHost.get(dom);
        if (!url) continue;
        const cur = String(h._source.team_page || '').trim();
        if (cur && !OVERWRITE) { skippedHasOne++; continue; }
        if (cur === url) continue;
        targeted++;
        if (!DRY) actions.push({ update: { _index: co.INDEX, _id: h._id } }, { doc: { team_page: url } });
      }
      if (actions.length) {
        try { await client.bulk({ body: actions, refresh: false }); updated += actions.length / 2; }
        catch (e) { errors += actions.length / 2; console.error('  bulk failed:', e.message); }
      }
      after = hits[hits.length - 1].sort;
    }
    if (i && i % 5000 < PAGE) console.error(`  ${i.toLocaleString()}/${hosts.length.toLocaleString()} hosts | ${targeted.toLocaleString()} record(s) to fill`);
  }
  if (!DRY) { try { await client.indices.refresh({ index: co.INDEX }); } catch (e) { /* */ } }

  console.error(`\n====== ${DRY ? 'DRY RUN' : 'DONE'} · ${Math.round((Date.now() - t0) / 1000)}s ======`);
  console.error(`  company records on those domains : ${known.toLocaleString()}`);
  console.error(`  already had a team page          : ${skippedHasOne.toLocaleString()} (left alone${OVERWRITE ? ' — but --overwrite set' : ''})`);
  console.error(`  ${DRY ? 'would fill' : 'FILLED'}                       : ${(DRY ? targeted : updated).toLocaleString()}${errors ? `  (${errors} error(s))` : ''}`);
  console.error('\n  rejected examples (not team pages):');
  for (const r of rejects) console.error('    ' + r);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
