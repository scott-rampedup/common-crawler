/**
 * contacts-csv-correct.js — apply a hand-corrected CSV of contacts over the Master DB, authoritatively.
 *
 *   OPENSEARCH_ENDPOINT=… node contacts-csv-correct.js "CMG Home Loans.csv" [--dry] [--limit N]
 *
 * This is the analyst-correction path, so it deliberately differs from every crawl loader:
 *
 *   - AUTHORITATIVE. Writes via opensearch.indexDocs, bypassing the score gate. A correction must land
 *     even when the stored record scores higher, which bulkUpsert would (correctly, for a crawl) refuse.
 *   - MERGE, not replace. indexDocs does a full `index` op, so any field absent from the doc is GONE.
 *     A correction sheet is a subset of the schema — the CMG sheet carries no Image URL column, and
 *     firmographics (industry/company_size/company_name/company_linkedin) aren't in the display record at
 *     all — so we start from the STORED _source and overlay only the columns the CSV actually provides.
 *     Everything else survives untouched.
 *   - NO INFERENCE. recordToDoc runs ensureNameGender, which would let the LinkedIn-slug rule second-guess
 *     a human's name/gender. A corrected row is ground truth; we build the doc directly instead.
 *   - Score is RECOMPUTED from the merged record, so a correction that fills First/Last/Gender raises the
 *     record's score and future crawls can't quietly overwrite it.
 *
 * Rows whose email isn't already in the index are REPORTED, not inserted: the email is the _id, so an
 * unmatched row usually means the address itself was edited, and inserting would leave the old record
 * behind as a duplicate. Re-key those deliberately rather than as a side effect of a bulk correction.
 */
const fs = require('fs');
const path = require('path');
const os = require('./opensearch');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const argOf = (f, d) => { const i = args.indexOf(f); return i > -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : d; };
const DRY = has('--dry');
const LIMIT = Number(argOf('--limit', '0')) || 0;
const positional = args.filter((a, i) => !a.startsWith('--') && !(i > 0 && args[i - 1].startsWith('--')));
const CSV = positional[0] || '';

// CSV header -> OpenSearch doc field. Header names are matched loosely (case/punctuation-insensitive) and
// both "First"/"First Name" spellings are accepted, since exports vary.
const COLS = {
  time_stamp: 'time_stamp', source: 'source', web_source_url: 'web_source_url', directory: 'directory',
  path_id: 'path_id', domain: 'domain', last_path: 'last_path', gender: 'gender',
  first: 'first', first_name: 'first', last: 'last', last_name: 'last',
  title: 'title', position: 'position', employer: 'employer', location: 'work_location',
  description: 'description', image_url: 'image_url',
  email_address: 'email', email: 'email', email_type: 'email_type',
  linkedin_url: 'linkedin_url', facebook: 'facebook', twitter: 'twitter', whatsapp: 'whatsapp',
  google_maps: 'google_maps', vcard: 'vcard',
  phone: 'phone', phone_type: 'phone_type', phone_location: 'phone_location',
  phone_2: 'phone_2', phone_2_type: 'phone_2_type', phone_2_location: 'phone_2_location',
  type: 'type', bio_check: 'bio_check',
};
const norm = (s) => String(s || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

// Same quote-aware parser the other CSV loaders use (commas + newlines inside quoted fields).
function parseCsv(text) {
  const rows = []; let row = [], field = '', q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
    else if (c === '"') q = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows;
}

// Spreadsheets hand back dates in the locale format (6/10/2026) while the index stores ISO (2026-06-10) —
// the crawl path normalizes to ISO in extract-from-pointers' tsOf(). Writing the sheet's string verbatim
// would leave these records formatted differently from every other row in the index, so re-normalize.
// Ambiguous only for day<=12, and the export is US M/D/YYYY, so month-first is the right read.
function normalizeDate(v) {
  const s = String(v || '').trim();
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s);
  if (!m) return s;                                        // already ISO, or something we shouldn't touch
  const mm = String(m[1]).padStart(2, '0'), dd = String(m[2]).padStart(2, '0');
  if (Number(m[1]) < 1 || Number(m[1]) > 12 || Number(m[2]) < 1 || Number(m[2]) > 31) return s;
  return `${m[3]}-${mm}-${dd}`;
}

// Score mirrors opensearch.recordScore: one point per populated high-value field.
const SCORE_FIELDS = ['first', 'last', 'title', 'position', 'phone', 'phone_2', 'linkedin_url', 'gender',
  'phone_location', 'image_url', 'description', 'google_maps'];
const scoreOf = (doc) => SCORE_FIELDS.reduce((n, f) => n + (String(doc[f] || '').trim() ? 1 : 0), 0);

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!CSV || !fs.existsSync(CSV)) { console.error('CSV not found:', CSV || '(none given)'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);

  const rows = parseCsv(fs.readFileSync(CSV, 'utf8'));
  const head = (rows.shift() || []).map(norm);
  const mapped = head.map((h) => COLS[h] || null);
  const emailIdx = head.findIndex((h) => h === 'email_address' || h === 'email');
  if (emailIdx < 0) { console.error('CSV has no Email Address column; got:', head.join(', ')); process.exit(1); }
  const carried = [...new Set(mapped.filter(Boolean))];
  const ignored = head.filter((h, i) => h && !mapped[i]);
  console.error(`${path.basename(CSV)}: ${rows.length.toLocaleString()} row(s)${DRY ? '  [DRY RUN — no writes]' : ''}`);
  console.error(`  columns applied : ${carried.join(', ')}`);
  console.error(`  columns ignored : ${ignored.length ? ignored.join(', ') : '(none)'}`);
  console.error(`  NOT in the sheet -> preserved from the stored record (never blanked).`);

  const t0 = Date.now();
  const tally = { rows: 0, dupEmail: 0, noEmail: 0, notFound: 0, unchanged: 0, updated: 0, errors: 0 };
  const fieldHits = {};                  // field -> how many records it actually changes
  const notFound = [];
  const samples = [];
  const seen = new Set();
  let batch = [];

  const flush = async () => {
    if (!batch.length) return;
    const b = batch; batch = [];
    // mget the stored docs, overlay, index. Order is preserved by mget, so index-align the two arrays.
    const ids = b.map((x) => x.email);
    let docs = [];
    try { docs = ((await client.mget({ index: os.INDEX, body: { ids } })).body || {}).docs || []; }
    catch (e) { tally.errors += b.length; console.error('  mget failed:', e.message); return; }

    const out = [];
    for (let i = 0; i < b.length; i++) {
      const { email, overlay } = b[i];
      const d = docs[i];
      if (!d || !d.found) { tally.notFound++; if (notFound.length < 10) notFound.push(email); continue; }
      const stored = d._source;
      const next = { ...stored };
      const changed = [];
      for (const [f, v] of Object.entries(overlay)) {
        const before = String(stored[f] == null ? '' : stored[f]);
        if (before === v) continue;
        next[f] = v;
        changed.push(f);
        fieldHits[f] = (fieldHits[f] || 0) + 1;
      }
      if (!changed.length) { tally.unchanged++; continue; }
      next.name = `${String(next.first || '').trim()} ${String(next.last || '').trim()}`.trim();
      if (next.name !== stored.name) { if (!changed.includes('name')) { changed.push('name'); fieldHits.name = (fieldHits.name || 0) + 1; } }
      next.score = scoreOf(next);
      if (next.score !== stored.score) fieldHits._score = (fieldHits._score || 0) + 1;
      next.updated_at = new Date().toISOString();
      if (samples.length < 8) samples.push({ email, changed: changed.join(','), score: `${stored.score}->${next.score}` });
      tally.updated++;
      out.push(next);
    }
    if (!DRY && out.length) {
      try { const r = await os.indexDocs(client, out); tally.errors += r.errors || 0; }
      catch (e) { tally.errors += out.length; console.error('  index failed:', e.message); }
    }
  };

  for (const cols of rows) {
    if (!cols.length || (cols.length === 1 && !String(cols[0]).trim())) continue;
    tally.rows++;
    const email = String(cols[emailIdx] || '').trim().toLowerCase();
    if (!email) { tally.noEmail++; continue; }
    if (seen.has(email)) { tally.dupEmail++; continue; }
    seen.add(email);

    const overlay = {};
    for (let i = 0; i < cols.length; i++) {
      const f = mapped[i];
      if (!f || f === 'email') continue;                    // email is the _id, never overlaid
      let v = String(cols[i] == null ? '' : cols[i]).trim();
      if (f === 'linkedin_url') v = os.cleanContactLinkedin(v);
      if (f === 'time_stamp') v = normalizeDate(v);
      overlay[f] = v;
    }
    batch.push({ email, overlay });
    if (batch.length >= 500) await flush();
    if (LIMIT && tally.rows >= LIMIT) break;
  }
  await flush();

  console.error(`\nfield-level changes (records affected):`);
  for (const [f, n] of Object.entries(fieldHits).sort((a, b) => b[1] - a[1])) console.error(`  ${f.padEnd(18)}${n.toLocaleString()}`);
  if (samples.length) {
    console.error(`\nsamples:`);
    for (const s of samples) console.error(`  ${s.email.padEnd(38)} score ${s.score.padEnd(8)} ${s.changed}`);
  }
  if (notFound.length) console.error(`\nnot in the index (skipped, NOT inserted): ${notFound.join(', ')}${tally.notFound > notFound.length ? ` … +${tally.notFound - notFound.length} more` : ''}`);
  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: ${tally.rows.toLocaleString()} row(s) | ${tally.updated.toLocaleString()} updated`
    + ` | ${tally.unchanged.toLocaleString()} already matched | ${tally.notFound.toLocaleString()} not found`
    + `${tally.dupEmail ? ` | ${tally.dupEmail} dup email(s) in file` : ''}${tally.noEmail ? ` | ${tally.noEmail} without an email` : ''}`
    + ` | ${tally.errors} error(s) | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
