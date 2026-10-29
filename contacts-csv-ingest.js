/**
 * contacts-csv-ingest.js — ingest a LOCAL csv of contacts into the Master DB, merging by email.
 * ---------------------------------------------------------------------------------------------
 * The sibling loaders each refuse this job for a good reason:
 *   - sheet-import.js does the mapping we want but only reads a Google Sheet by id.
 *   - contacts-csv-correct.js reads a local file but REPORTS rows whose email is not already
 *     stored instead of inserting them, because it exists to correct, not to add.
 * This one adds and overwrites: new emails are inserted, known emails take the sheet's values.
 *
 *   node contacts-csv-ingest.js "ALTA Office .csv"              # dry run (default): map + report
 *   node contacts-csv-ingest.js "ALTA Office .csv" --apply
 *   node contacts-csv-ingest.js file.csv --source "ALTA Office" # what lands in the Source column
 *
 * Header matching is EXPLICIT, not loose. Real exports arrive with "Web source" (not "Web Source
 * URL"), "First Name" (not "First") and, in this one, "Gedner" for Gender — and a loose matcher
 * answers "" for all three without complaining, so the rows import stripped of their names and
 * bio URL. Anything in the file that maps to nothing is listed before a single row is written.
 */
const fs = require('fs');
const path = require('path');
const ex = require('./extractor');
const { parseCsv } = require('./sheet-import');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const argOf = (f, d) => { const i = args.indexOf(f); return i > -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : d; };
const APPLY = has('--apply');
const LIMIT = Number(argOf('--limit', '0')) || 0;
const CSV = args.find((a, i) => !a.startsWith('--') && !(i > 0 && args[i - 1].startsWith('--') && args[i - 1] !== '--apply')) || '';
const SOURCE = argOf('--source', '') || (CSV ? path.basename(CSV).replace(/\.csv$/i, '').trim() : 'CSV Import');

// normalised header -> canonical field. Every spelling seen in a real export is listed; an
// unmapped column is reported rather than ignored.
const ALIAS = {
  web_source: 'url', web_source_url: 'url', url: 'url', bio_url: 'url', source_url: 'url', page: 'url',
  first: 'first', first_name: 'first', firstname: 'first', given_name: 'first',
  last: 'last', last_name: 'last', lastname: 'last', surname: 'last', family_name: 'last',
  gender: 'gender', gedner: 'gender', sex: 'gender',            // "Gedner" is a typo in the exports
  email: 'email', email_address: 'email', e_mail: 'email',
  email_type: 'email_type',
  position: 'position', title: 'title', job_title: 'title', role: 'position',
  phone: 'phone', telephone: 'phone', phone_number: 'phone', tel: 'phone',
  phone_2: 'phone_2', phone2: 'phone_2', mobile: 'phone_2',
  linkedin: 'linkedin_url', linkedin_url: 'linkedin_url',
  description: 'description', image_url: 'image_url', image: 'image_url',
  employer: 'employer', company: 'employer', company_name: 'employer',
  location: 'work_location', work_location: 'work_location',
  domain: 'domain', root_domain: 'domain',
  time_stamp: 'time_stamp', scan_timestamp: 'time_stamp', date: 'time_stamp',
  // A job export already carries where each row came from; overwriting that with the filename would
  // erase the provenance of a rescued job.
  source: 'source', facebook: 'facebook',
  directory: 'directory', path_id: 'path_id', last_path: 'last_path', bio_check: 'bio_check',
  google_maps: 'google_maps', twitter: 'twitter', whatsapp: 'whatsapp', vcard: 'vcard',
  phone_type: 'phone_type', phone_location: 'phone_location', phone_2_type: 'phone_2_type',
};
const norm = (s) => String(s || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

function buildRecords(text, genderMap) {
  const rows = parseCsv(text);
  if (!rows.length) return { records: [], stats: { read: 0, blank: 0, noEmail: 0, noUrl: 0 }, unmapped: [], mapped: {} };

  const header = rows[0].map(norm);
  const colOf = {}, unmapped = [], mapped = {};
  header.forEach((h, i) => {
    if (!h) return;
    const f = ALIAS[h];
    if (!f) { unmapped.push(rows[0][i]); return; }
    if (colOf[f] == null) { colOf[f] = i; mapped[rows[0][i]] = f; }
  });

  const today = new Date().toISOString().slice(0, 10);
  const get = (row, f) => (colOf[f] == null ? '' : String(row[colOf[f]] == null ? '' : row[colOf[f]]).trim());
  const byEmail = new Map();
  const records = [], noEmailRows = [];
  let read = 0, blank = 0, noUrl = 0, dupes = 0;

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (!row || !row.length || row.every((c) => !String(c || '').trim())) { blank++; continue; }
    read++;
    const url = get(row, 'url').split('#')[0].split('?')[0];
    const email = ex.cleanEmail(get(row, 'email'));
    const domain = ex.getBaseDomain(url) || get(row, 'domain').toLowerCase();
    const cc = ex.countryCodeFromDomain(domain);
    const pathHit = url ? ex.pathIdFromUrl(url) : null;
    const position = get(row, 'position') || get(row, 'title');
    const rec = {
      'Time Stamp': /^\d{4}-\d{2}-\d{2}$/.test(get(row, 'time_stamp')) ? get(row, 'time_stamp') : today,
      'Source': get(row, 'source') || SOURCE,
      'Web Source URL': url,
      'Directory': get(row, 'directory') || (pathHit ? 'Team' : (url ? ex.classifyDirectory(url, '', {}, genderMap) : '')),
      'Path ID': get(row, 'path_id') || (pathHit ? pathHit.id : ''),
      'Domain': domain,
      'Last Path': get(row, 'last_path'),
      'Bio Check': get(row, 'bio_check'),
      'First': get(row, 'first'),
      'Last': get(row, 'last'),
      'Gender': /^m/i.test(get(row, 'gender')) ? 'M' : /^f/i.test(get(row, 'gender')) ? 'F' : '',
      // Position and Title mirror each other here, as the backfills in db-pg do: the exports carry
      // one job-title column and which name it uses varies by source.
      'Title': position,
      'Position': position,
      'Description': get(row, 'description').slice(0, 600),
      'Image URL': get(row, 'image_url'),
      'Email Address': email,
      // The sheet's own Email Type is kept when it says something; otherwise classify. These files
      // tend to carry one value for every row, which is a label, not a measurement.
      'Email Type': get(row, 'email_type') || (email ? ex.classifyEmail(email) : ''),
      'LinkedIn URL': get(row, 'linkedin_url'),
      'Facebook': get(row, 'facebook'),
      'Google Maps': get(row, 'google_maps'),
      'Twitter': get(row, 'twitter'),
      'WhatsApp': get(row, 'whatsapp'),
      'vCard': get(row, 'vcard'),
      // Extensions are dropped, not mangled: no phone in the 15M-record store carries one, so there
      // is nowhere to put "ext 109" and the main number is what every consumer expects.
      'Phone': ex.toE164(get(row, 'phone'), cc),
      'Phone Type': '', 'Phone Location': '',
      'Phone 2': ex.toE164(get(row, 'phone_2'), cc),
      'Phone 2 Type': '', 'Phone 2 Location': '',
      'Employer': get(row, 'employer'),
      'Work Location': get(row, 'work_location'),
    };
    if (!url) noUrl++;
    if (!email) { noEmailRows.push(rec); continue; }   // the store is email-keyed: cannot be written
    const key = email.toLowerCase();
    if (byEmail.has(key)) dupes++;
    byEmail.set(key, rec);                              // last row for an address wins
  }
  for (const v of byEmail.values()) records.push(v);
  return { records, noEmailRows, stats: { read, blank, noUrl, dupes, noEmail: noEmailRows.length }, unmapped, mapped };
}

(async () => {
  if (!CSV) { console.error('usage: node contacts-csv-ingest.js <file.csv> [--apply] [--source NAME]'); process.exit(1); }
  const file = path.isAbsolute(CSV) ? CSV : path.join(process.cwd(), CSV);
  if (!fs.existsSync(file)) { console.error('no such file: ' + file); process.exit(1); }
  let genderMap = {};
  try { genderMap = ex.loadGenderMap('names-genders.csv'); } catch { /* optional */ }

  const { records, noEmailRows, stats, unmapped, mapped } = buildRecords(fs.readFileSync(file, 'utf8'), genderMap);
  const N = (x) => Number(x || 0).toLocaleString();

  console.log('file   : ' + file);
  console.log('source : ' + JSON.stringify(SOURCE));
  console.log('columns mapped:');
  for (const [h, f] of Object.entries(mapped)) console.log('    ' + String(h).padEnd(16) + '-> ' + f);
  if (unmapped.length) console.log('  UNMAPPED (ignored): ' + unmapped.map((u) => JSON.stringify(u)).join(', '));
  console.log('rows   : ' + N(stats.read) + ' data, ' + N(stats.blank) + ' blank, ' + N(stats.dupes) + ' duplicate email(s) collapsed');
  console.log('         ' + N(records.length) + ' writable, ' + N(stats.noEmail) + ' without an email (cannot be stored: email is the key)');
  if (stats.noUrl) console.log('         ' + N(stats.noUrl) + ' row(s) without a bio URL');
  for (const r of noEmailRows) console.log('    no email: ' + (r.First + ' ' + r.Last).trim() + '  ' + (r.Position || ''));

  console.log('\nsample:');
  for (const r of records.slice(0, 3)) {
    console.log('    ' + (r.First + ' ' + r.Last).trim().padEnd(20) + r['Email Address'].padEnd(36) +
                r.Phone.padEnd(15) + r['Email Type'].padEnd(14) + r.Position);
  }

  // Where to write. The UI serves the Master DB from OpenSearch (SEARCH_BACKEND=opensearch), so OS is
  // what makes a contact visible in Contact Crawler; Postgres is the crawl-side store that os-sync
  // pushes from. --pg needs DATABASE_URL, which only the Fly machines hold.
  const TO_PG = has('--pg');

  if (!APPLY) {
    console.log('\nwould write to: ' + (TO_PG ? 'Postgres (DATABASE_URL)' : 'OpenSearch index "contacts"'));
    console.log('Dry run — nothing written. Re-run with --apply.');
    process.exit(0);
  }

  if (TO_PG) {
    const { makeDb } = require('./db-pg');
    const db = await makeDb();
    const res = await db.upsertMany(records);
    console.log('\napplied to Postgres: processed ' + N(res.processed) + ', added ' + N(res.added) +
                ', changed ' + N(res.changed) + ', db total ' + N(res.total));
    process.exit(0);
  }

  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const os = require('./opensearch');
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const now = new Date().toISOString();

  // Read first: an address already in the store keeps every field this sheet does not carry. A blind
  // index would blank firmographics, image, description and the rest, because a correction sheet is a
  // subset of the schema — the same reason contacts-csv-correct overlays onto the stored _source.
  const ids = records.map((r) => r['Email Address'].toLowerCase());
  const existing = new Map();
  for (let i = 0; i < ids.length; i += 500) {
    const res = await client.mget({ index: os.INDEX, body: { ids: ids.slice(i, i + 500) } }, { requestTimeout: 120000 });
    for (const d of ((res.body || res).docs || [])) if (d.found) existing.set(d._id, d._source);
  }

  const docs = [];
  for (const r of records) {
    const id = r['Email Address'].toLowerCase();
    const fresh = os.recordToDoc(r, now);
    const prior = existing.get(id);
    if (!prior) { docs.push(fresh); continue; }
    // overlay: only the fields this CSV actually provided replace what is stored
    const merged = { ...prior };
    for (const k in fresh) {
      const v = fresh[k];
      if (v === '' || v === null || v === undefined) continue;     // absent column: keep the stored value
      merged[k] = v;
    }
    merged.updated_at = now;
    docs.push(merged);
  }

  const res = await os.indexDocs(client, docs);
  await client.indices.refresh({ index: os.INDEX });
  console.log('\napplied to OpenSearch: ' + N(res.indexed) + ' doc(s), ' + N(res.errors) + ' error(s)');
  console.log('  new addresses    : ' + N(records.length - existing.size));
  console.log('  already present  : ' + N(existing.size) + ' (merged onto the stored record)');
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
