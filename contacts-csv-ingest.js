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
// classifyDirectory reads the page type out of the path, and answers "Company" when the path has no
// directory term in it -- lennarmortgage.com/AbrahamContreras is a person page that looks like none.
// data/bio-domain-allotments.txt fixes that per DOMAIN, but it applies to every page on the domain
// forever, which is wrong for a company site that merely happens to host bios. For a curated file,
// say what the rows are and skip the guess. A per-row Directory column still wins -- EXCEPT when it
// says "Company", which is classifyDirectory's fall-through for "no directory term in the path",
// not an assertion that the page is a company page. An export carries that default back in as if
// it were a finding: New American Funding's file says "Company" for 289 rows that are all
// /mortgage-loans/<person> bio pages. A stated --directory outranks the guess; "Team" still wins.
const DIRECTORY = argOf('--directory', '');
// A Gender column can be unreliable without being uniform, so the "one value for every row" test
// below cannot catch it. New American Funding's file disagrees with the name map on 58 of the 216
// rows the map can judge, in BOTH directions -- 54 women marked M, and Alvaro/Charlie/Xavier/
// Anthony marked F -- and the map is right every time it was spot-checked. Opt in per file rather
// than widening the automatic rule, which would override columns that were genuinely filled in by
// hand. Where the map has never seen the name there is nothing better, so the file's value stands.
const GENDER_FROM_NAMES = has('--gender-from-names');
// A column called "Source" means PROVENANCE in a job export and the PAGE URL in a scraped team sheet,
// and nothing in the file tells them apart -- so ALIAS cannot decide it and guessing from the values
// would silently re-point a rescued job's provenance. Name the column instead. Without this, a sheet
// whose only URL lives under "Source" imports every row with no Web Source URL, no Domain and no
// Directory, and reports them as "no url" rather than failing.
const URL_COLUMN = argOf('--url-column', '');
// Domain normally follows the page, which is right when the page belongs to the employer. It is wrong
// for a holding company's team page: groupe-adit.fr lists 16 executives and not one of them has an
// email there -- they sit at adit.fr, nsi-adit.com, stratinfo.fr, madame-bovary.fr, groupegeos.com and
// salveo.international. Storing the group's domain would point every firmographic lookup at the parent.
// Web Source URL still records the page they were found on; this only changes whose company they are.
const DOMAIN_FROM_EMAIL = has('--domain-from-email');
// A DIRECTORY is the case neither of the above covers. agentreview.net lists 37,908 insurance agents
// and employs none of them, so the page domain is wrong for every row; but --domain-from-email is
// wrong too, because about half of them are reachable only at gmail.com, and storing gmail.com as an
// employer is worse than storing nothing. The caller has to decide per row -- the agent's own site
// where the page states one, the email domain where it is a real company address, the directory
// itself otherwise -- and that waterfall cannot be expressed by a flag. So let the file carry the
// answer: with this set, a non-empty Domain column wins outright. Opt-in, because for an ordinary
// export the page domain is right and a stray Domain column should not silently override it.
const DOMAIN_COLUMN = has('--domain-column');
// Accept the date spellings exports actually use. An unparsed Time Stamp silently became today,
// which overwrites when a person was last seen with when they were re-imported.
const isoDate = (v) => {
  const s = String(v || '').trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);                       // M/D/YYYY (US)
  if (!m) return '';
  const [, mo, d, y] = m;
  if (+mo < 1 || +mo > 12 || +d < 1 || +d > 31) return '';
  return y + '-' + String(mo).padStart(2, '0') + '-' + String(d).padStart(2, '0');
};

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

  // --url-column overrides ALIAS for one named header. A typo throws rather than falling back, because
  // the fallback is the exact silent failure the flag exists to prevent.
  if (URL_COLUMN) {
    const want = norm(URL_COLUMN);
    const i = header.indexOf(want);
    if (i < 0) throw new Error(`--url-column "${URL_COLUMN}" is not in the header: ${rows[0].join(', ')}`);
    for (const f of Object.keys(colOf)) if (colOf[f] === i) { delete colOf[f]; delete mapped[rows[0][i]]; }
    colOf.url = i;
    mapped[rows[0][i]] = 'url';
  }

  const today = new Date().toISOString().slice(0, 10);
  const get = (row, f) => (colOf[f] == null ? '' : String(row[colOf[f]] == null ? '' : row[colOf[f]]).trim());

  // A Gender column holding ONE value for every row is a label, not a measurement -- the same
  // reading Email Type gets below. Lennar Mortgage's export says "M" for all 458 rows, 191 of whom
  // the name map knows as women. So: uniform column -> ignore it and classify from the first name,
  // leaving blank where the map has never seen the name. A column that actually varies was filled
  // in per person and still wins, because a curated value beats a statistical one.
  const letter = (g) => (/^m/i.test(g) ? 'M' : /^f/i.test(g) ? 'F' : '');
  const seen = new Set();
  for (let r = 1; r < rows.length; r++) { const v = letter(get(rows[r] || [], 'gender')); if (v) seen.add(v); }
  const genderIsLabel = colOf.gender != null && seen.size === 1 && rows.length > 20;
  const useMap = genderIsLabel || GENDER_FROM_NAMES;
  let genderFixed = 0, genderBlank = 0;
  const genderOf = (fileVal, first) => {
    if (!useMap) return letter(fileVal);
    // genderFromMap, not a bare index: letter() turns an inherited Object.prototype function into a
    // confident 'F' (String(fn) starts "function"), so a first name of "Constructor"/"Tostring" would
    // be ingested as female rather than as obvious garbage. See extractor.loadGenderMap.
    const g = letter(ex.genderFromMap(genderMap, String(first || '').toLowerCase()));
    // A uniform column carries no information, so an unknown name is left blank rather than given
    // the repeated value back. A varying column does carry some, so it stands where the map is silent.
    if (!g) { genderBlank++; return genderIsLabel ? '' : letter(fileVal); }
    if (g !== letter(fileVal)) genderFixed++;
    return g;
  };
  const byEmail = new Map();
  const records = [], noEmailRows = [];
  let read = 0, blank = 0, noUrl = 0, dupes = 0;

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (!row || !row.length || row.every((c) => !String(c || '').trim())) { blank++; continue; }
    read++;
    // Exports disagree about whether the URL carries a scheme -- "lennarmortgage.com/JaneDoe" is a
    // real value from a real file. Bare, classifyDirectory throws on new URL() and getBaseDomain
    // answers "" without complaining, so every row would import domain-less. Add the scheme.
    const rawUrl = get(row, 'url').split('#')[0].split('?')[0];
    const url = rawUrl && !/^[a-z][a-z0-9+.-]*:\/\//i.test(rawUrl) && /^[^/\s]+\.[^/\s]/.test(rawUrl)
      ? 'https://' + rawUrl
      : rawUrl;
    const email = ex.cleanEmail(get(row, 'email'));
    // Run the email's host through getBaseDomain too, so it is normalised the same way a URL's is
    // (sub-domains stripped, multi-part TLDs respected) rather than taken raw off the address.
    const emailHost = email.includes('@') ? email.split('@').pop().toLowerCase() : '';
    const emailDomain = emailHost ? (ex.getBaseDomain('https://' + emailHost) || emailHost) : '';
    const domain = (DOMAIN_COLUMN ? get(row, 'domain').toLowerCase() : '')
      || (DOMAIN_FROM_EMAIL ? emailDomain : '')
      || ex.getBaseDomain(url) || get(row, 'domain').toLowerCase();
    const cc = ex.countryCodeFromDomain(domain);
    const pathHit = url ? ex.pathIdFromUrl(url) : null;
    const position = get(row, 'position') || get(row, 'title');
    const rec = {
      'Time Stamp': isoDate(get(row, 'time_stamp')) || today,
      'Source': get(row, 'source') || SOURCE,
      'Web Source URL': url,
      'Directory': (DIRECTORY && /^company$/i.test(get(row, 'directory')) ? DIRECTORY : get(row, 'directory'))
        || DIRECTORY || (pathHit ? 'Team' : (url ? ex.classifyDirectory(url, '', {}, genderMap) : '')),
      'Path ID': get(row, 'path_id') || (pathHit ? pathHit.id : ''),
      'Domain': domain,
      'Last Path': get(row, 'last_path'),
      'Bio Check': get(row, 'bio_check'),
      'First': get(row, 'first'),
      'Last': get(row, 'last'),
      'Gender': genderOf(get(row, 'gender'), get(row, 'first')),
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
      // Honour the mapped columns instead of discarding them. ALIAS has listed phone_type and
      // phone_location for as long as it has listed the rest, and the dry run prints
      // "Phone type -> phone_type" among the columns it mapped -- but these four were pinned to ''
      // regardless, so a file that carried Mobile/Office was read, reported as mapped, and written
      // blank. A loader that says it took a column has to take it; teamtarter's 21 rows lost their
      // phone type that way. Still EMPTY when the file has no such column: Type and Location are
      // derived from the number elsewhere in the pipeline, and inventing them here would freeze a
      // guess over a measurement.
      'Phone Type': get(row, 'phone_type'), 'Phone Location': get(row, 'phone_location'),
      'Phone 2': ex.toE164(get(row, 'phone_2'), cc),
      'Phone 2 Type': get(row, 'phone_2_type'), 'Phone 2 Location': '',
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
  return { records, noEmailRows, unmapped, mapped,
    stats: { read, blank, noUrl, dupes, noEmail: noEmailRows.length,
      genderIsLabel, genderFromNames: GENDER_FROM_NAMES, genderFixed, genderBlank } };
}

(async () => {
  if (!CSV) { console.error('usage: node contacts-csv-ingest.js <file.csv> [--apply] [--source NAME]'); process.exit(1); }
  const file = path.isAbsolute(CSV) ? CSV : path.join(process.cwd(), CSV);
  if (!fs.existsSync(file)) { console.error('no such file: ' + file); process.exit(1); }
  // __dirname, not a bare relative name: resolved against the CWD this silently missed the file
  // whenever the script was run from anywhere but the repo root, and fell through to the empty map --
  // which is how the plain-{} path above was reachable at all rather than merely theoretical.
  let genderMap = Object.create(null);
  try { genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv')); } catch { /* optional */ }

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
  if (stats.genderIsLabel || stats.genderFromNames) {
    console.log('gender : ' + (stats.genderIsLabel
      ? 'column holds ONE value for every row — treated as a label, classified from the name map'
      : '--gender-from-names: the name map outranks the column where it knows the first name'));
    console.log('         ' + N(stats.genderFixed) + ' corrected, ' + N(stats.genderBlank) +
      (stats.genderIsLabel ? ' left blank' : ' kept from the file') + ' (name unknown to the map)');
  }
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
