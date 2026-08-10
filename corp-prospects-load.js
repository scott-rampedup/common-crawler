/**
 * corp-prospects-load.js — load an exported child-sitemap catalog ("Corp Prospects Child Sitemaps") into
 * the Sitemap Library. The export already carries its own People/Location profiling, so this is a pure
 * catalog load: no crawling, no classification guesswork.
 *
 *   OPENSEARCH_ENDPOINT=… node corp-prospects-load.js ["Corp Prospects Child Sitemaps.csv"] [--dry] [--limit N]
 *
 * Header-driven (columns are looked up by NAME, not position, so a re-export with extra/reordered columns
 * still loads):
 *   sitemap_type      people | location   -> kind People/Location, the field the monitor batches on
 *   domain            company domain
 *   sitemap_url       the child sitemap   -> the Library _id (dedup across every source)
 *   bio_count         pages seen on it    -> url_count/item_count (the "Pages" side of Have vs Pages)
 *   bio_page_keyword  e.g. /team/         -> keyword, which the monitor's second pass tokenizes
 *   site path         the filename        (informational; deriveType reads structure from the URL)
 *
 * Every row lands with source='corp-prospects' and monitored ON (sitemaps.bulkUpsert defaults it true on
 * first insert), so the nightly Library monitor picks them up on its own rotation. Idempotent: upsert by
 * sitemap_url, and a re-sight refreshes the mutable fields without resetting discovered_at or an opt-out.
 */
const fs = require('fs');
const path = require('path');
const sitemaps = require('./sitemaps');
const co = require('./companies');

const args = process.argv.slice(2);
const arg = (f, d) => { const i = args.indexOf(f); return i > -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : d; };
const DRY = args.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
// First true positional = the CSV path. A bare word that FOLLOWS a --flag is that flag's value, not a path.
const positional = args.filter((a, i) => !a.startsWith('--') && !(i > 0 && args[i - 1].startsWith('--')));
const CSV = positional[0] || path.join(__dirname, 'Corp Prospects Child Sitemaps.csv');
const SOURCE = arg('--source', 'corp-prospects');

// Streaming quote-aware CSV parser -> field arrays (same one sitemap-csv-load uses).
function* parseCsv(text) {
  let field = '', row = [], q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
    else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); yield row; row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); yield row; }
}

const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); } catch (e) { return ''; } };
const norm = (s) => String(s || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_');
// The export says people/location; anything else is left unclassified rather than guessed, so an admin
// can triage it in the Library editor instead of it silently joining the People monitor rotation.
const KIND = { people: 'People', person: 'People', bio: 'People', location: 'Location', locations: 'Location', office: 'Location' };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!fs.existsSync(CSV)) { console.error('CSV not found:', CSV); process.exit(1); }
  const client = sitemaps.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await sitemaps.ensureIndex(client);

  const text = fs.readFileSync(CSV, 'utf8');
  const now = new Date().toISOString();
  const rows = parseCsv(text);
  const head = rows.next().value || [];
  const H = {}; head.forEach((h, i) => { H[norm(h)] = i; });
  for (const need of ['sitemap_type', 'domain', 'sitemap_url']) {
    if (H[need] === undefined) { console.error(`CSV is missing the "${need}" column; got: ${head.join(', ')}`); process.exit(1); }
  }
  console.error(`${path.basename(CSV)} — columns: ${head.join(', ')}${DRY ? '  [DRY RUN — no writes]' : ''}`);

  const t0 = Date.now();
  const tally = { seen: 0, kept: 0, skipped: 0, people: 0, location: 0, unknown: 0, dupInFile: 0, alreadyInLibrary: 0, upserted: 0, errors: 0 };
  const seenUrl = new Set();
  let batch = [];

  // Which of this batch's sitemap_urls the Library already has (the _id is the URL) — reported so a
  // re-export tells you how much of it is genuinely new rather than a no-op refresh.
  async function countExisting(ids) {
    for (let i = 0; i < ids.length; i += 1000) {
      const chunk = ids.slice(i, i + 1000);
      try {
        const r = await client.mget({ index: sitemaps.INDEX, body: { ids: chunk }, _source: false });
        for (const d of ((r.body || r).docs || [])) if (d && d.found) tally.alreadyInLibrary++;
      } catch (e) { /* best-effort: the count is reporting only */ }
    }
  }

  const flush = async () => {
    if (!batch.length) return;
    const b = batch; batch = [];
    await countExisting(b.map((d) => d.sitemap_url));
    if (DRY) return;
    const r = await sitemaps.bulkUpsert(client, b, now);
    tally.upserted += r.upserted; tally.errors += r.errors;
  };

  for (const cols of rows) {
    if (!cols.length || (cols.length === 1 && !cols[0].trim())) continue;
    tally.seen++;
    const smUrl = String(cols[H.sitemap_url] || '').trim();
    if (!/^https?:\/\/\S+/i.test(smUrl)) { tally.skipped++; continue; }
    if (seenUrl.has(smUrl)) { tally.dupInFile++; continue; }
    seenUrl.add(smUrl);
    const domain = co.normDomain(String(cols[H.domain] || '').trim()) || hostOf(smUrl);
    if (!domain) { tally.skipped++; continue; }

    const kind = KIND[String(cols[H.sitemap_type] || '').trim().toLowerCase()] || '';
    if (kind === 'People') tally.people++; else if (kind === 'Location') tally.location++; else tally.unknown++;
    const count = Number(String(cols[H.bio_count] !== undefined ? cols[H.bio_count] : '').replace(/[^0-9]/g, '')) || 0;

    batch.push({
      sitemap_url: smUrl,
      domain,
      parent_url: '',
      kind,
      keyword: String(cols[H.bio_page_keyword] !== undefined ? cols[H.bio_page_keyword] : '').trim().toLowerCase(),
      url_count: count,
      item_count: count,
      ratio: 0,
      by_name: false,
      industry: '',
      company_id: '',
      lastmod: '',
      source: SOURCE,
      status: 'active',
      // monitored is intentionally NOT set: bulkUpsert defaults it true on first insert and leaves an
      // existing opt-out alone.
    });
    tally.kept++;
    if (batch.length >= 1000) {
      await flush();
      if (tally.kept % 20000 === 0) console.error(`  kept ${tally.kept.toLocaleString()} | upserted ${tally.upserted.toLocaleString()} | already in Library ${tally.alreadyInLibrary.toLocaleString()}`);
    }
    if (LIMIT && tally.kept >= LIMIT) break;
  }
  await flush();

  const isNew = tally.kept - tally.alreadyInLibrary;
  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: seen ${tally.seen.toLocaleString()} | kept ${tally.kept.toLocaleString()} `
    + `(People ${tally.people.toLocaleString()} / Location ${tally.location.toLocaleString()}${tally.unknown ? ` / unclassified ${tally.unknown.toLocaleString()}` : ''})`
    + ` | NEW to Library ${isNew.toLocaleString()}, already present ${tally.alreadyInLibrary.toLocaleString()}`
    + ` | skipped ${tally.skipped.toLocaleString()}${tally.dupInFile ? ` + ${tally.dupInFile.toLocaleString()} dup-in-file` : ''}`
    + `${DRY ? '' : ` | upserted ${tally.upserted.toLocaleString()}, errors ${tally.errors}`} | ${Math.round((Date.now() - t0) / 1000)}s`);
  try { console.error('Library now:', JSON.stringify(await sitemaps.stats(client))); } catch (e) { /* */ }
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
