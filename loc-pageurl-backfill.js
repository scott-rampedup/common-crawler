/**
 * loc-pageurl-backfill.js — give the existing Location records their own page back.
 * ---------------------------------------------------------------------------------
 * gm-load used to read `website_location` (the specific page the Maps export gave for THIS place),
 * derive the domain from it, and then throw the URL away -- storing `https://<domain>/` instead. The
 * result: all 4,510,566 Location records point at a bare root, and three different NHS blood donor
 * centres all claim to live at blood.co.uk. A per-location crawl against that fetches the same
 * homepage once per branch, which is worse than not running it.
 *
 * gm-load now keeps the page. This recovers it for everything already indexed, by re-reading the
 * source exports still on disk and updating by cid.
 *
 *   node loc-pageurl-backfill.js --dir "/path/to/POI Places Data" [--dry] [--limit-files N]
 *
 * Two details that matter:
 *   - the cid COLUMN is Excel-mangled ("3.58E+16"), so the real cid comes out of company_url's
 *     ?cid= parameter, exactly as gm-load does it. Using the column would match nothing.
 *   - gm-upsert keys a Location doc on _id = cid, so this is a direct update by id: no search, no
 *     join, and re-running it is idempotent.
 *
 * Bing UK exports are deliberately NOT read here: their PageUrl is a bing.com/maps/overlay link, not
 * the business's own page, so there is nothing per-location to recover. Those ~1.3M records keep the
 * root and should be excluded from per-location crawling.
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const co = require('./companies');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const DIR = arg('dir', '');
const LIMIT_FILES = Number(arg('limit-files', '')) || 0;
const CHUNK = Number(process.env.BACKFILL_CHUNK) || 2000;

if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
if (!DIR) { console.error('need --dir "<POI Places Data>"'); process.exit(1); }

const clean = (v) => String(v == null ? '' : v).trim();
// Google appends redirect tracking to the website URL in the Maps export:
//   https://www.midflorida.com/locations/kissimmee-branch&opi=79508299&sa=U&ved=0ahUKEwj...
// Note the bare "&" with no "?" -- it is not even a valid query string. Left in place these URLs
// resolve against nothing in Common Crawl, which would quietly defeat the per-location crawl: the
// field looks populated and every lookup misses. Strip from the first tracking key.
const GOOG_TRACK = /[?&](?:opi|sa|ved|usg|sca_esv|source|cd|cad|rct|uact)=/;
const cleanUrl = (u) => { const s = String(u || '').trim(); if (!s) return ''; return s.split(GOOG_TRACK)[0].replace(/[?&]+$/, ''); };

const abs = (u) => { u = clean(u); return /^https?:/i.test(u) ? u : (u ? 'https://' + u : ''); };

// Streaming CSV reader: yields each record as an array. Handles quoted fields and embedded newlines.
async function eachRecord(file, onRec, onHeader) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file, { encoding: 'utf8' });
    let row = [], field = '', q = false, header = false, stopped = false;
    const endField = () => { row.push(field); field = ''; };
    const endRow = () => {
      endField();
      if (!header) { header = true; if (onHeader) onHeader(row); }
      else if (!stopped) { if (onRec(row) === false) { stopped = true; stream.destroy(); } }
      row = [];
    };
    stream.on('data', (chunk) => {
      for (let i = 0; i < chunk.length; i++) {
        const c = chunk[i];
        if (q) { if (c === '"') { if (chunk[i + 1] === '"') { field += '"'; i++; } else q = false; } else field += c; }
        else if (c === '"') q = true;
        else if (c === ',') endField();
        else if (c === '\n') endRow();
        else if (c !== '\r') field += c;
      }
    });
    stream.on('close', () => { if (field || row.length) endRow(); resolve(); });
    stream.on('error', reject);
  });
}

(async () => {
  const client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  let files = fs.readdirSync(DIR)
    .filter((f) => /^Google_Maps_Contact_Info_.*\.csv$/i.test(f))
    .sort();
  if (LIMIT_FILES) files = files.slice(-LIMIT_FILES);           // newest exports first-ish
  console.error(`${files.length} Google Maps export(s) to read from ${DIR}`);

  let seen = 0, pairs = 0, updated = 0, missing = 0, errors = 0, badCid = 0;
  let batch = [];

  async function flush() {
    if (!batch.length) return;
    if (DRY) { batch = []; return; }
    const body = [];
    for (const [cid, url] of batch) {
      body.push({ update: { _index: co.INDEX, _id: cid } });
      // doc only (no upsert): this restores a field on a record that must already exist. If the cid is
      // not in the index the update simply does nothing, rather than inventing a Location from a CSV row.
      body.push({ doc: { page_url: url, website: url } });
    }
    let r;
    try { r = await client.bulk({ body, refresh: false }, { requestTimeout: 300000 }); }
    catch (e) { errors += batch.length; console.error('  batch FAILED (' + e.message.slice(0, 90) + ') — continuing'); batch = []; return; }
    for (const it of (((r.body || r).items) || [])) {
      if (it.update && it.update.error) {
        if (it.update.status === 404) missing++; else { errors++; if (errors < 3) console.error('  ERR ' + JSON.stringify(it.update.error).slice(0, 140)); }
      } else updated++;
    }
    batch = [];
  }

  for (const f of files) {
    const full = path.join(DIR, f);
    let iCompanyUrl = -1, iCid = -1, iWebLoc = -1;
    let fileRows = 0, filePairs = 0;
    await eachRecord(full,
      (r) => {
        fileRows++; seen++;
        // the real cid lives in company_url (?cid=…); the cid column is Excel scientific notation
        const cid = (String(r[iCompanyUrl] || '').match(/[?&]cid=(-?\d+)/) || [])[1] || clean(r[iCid]);
        const url = cleanUrl(abs(r[iWebLoc]));
        if (!cid || !url) return;
        // A cid is a Google numeric id or a Bing YN… id. NEVER trust the column blindly: in some older
        // exports the positional read lands on xb_phones, and a 8,764-byte phone list went out as a
        // document _id ("is too long, must be no longer than 512 bytes"), aborting the whole run. Shape-
        // check it here so a misaligned file is skipped loudly instead of poisoning the batch.
        if (!/^(YN[0-9A-Fa-f]+|-?\d{5,25})$/.test(cid)) { badCid++; return; }
        // only a page is worth storing — a bare root is what we are trying to get away from
        if (!/^https?:\/\/[^/]+\/.+/i.test(url)) return;
        pairs++; filePairs++;
        batch.push([cid, url]);
      },
      (hdr) => {
        const H = hdr.map((h) => String(h || '').replace(/^﻿/, '').trim().toLowerCase());
        iCompanyUrl = H.indexOf('company_url');
        iCid = H.indexOf('cid');
        iWebLoc = H.indexOf('website_location');
      });
    // The CSV callback is synchronous, so nothing can be awaited inside it. Collect the file's pairs,
    // then send them in chunks here. One export is ~500k rows; the pairs it yields fit comfortably.
    const all = batch; batch = [];
    for (let i = 0; i < all.length; i += CHUNK) { batch = all.slice(i, i + CHUNK); await flush(); }
    console.error(`  ${f}  rows ${fileRows.toLocaleString()}  page URLs ${filePairs.toLocaleString()}  (updated ${updated.toLocaleString()}${badCid ? ', bad cid ' + badCid.toLocaleString() : ''})`);
  }
  await flush();

  console.error('\n--- backfill ' + (DRY ? '(DRY RUN)' : 'complete') + ' ---');
  console.error('  rows read        : ' + seen.toLocaleString());
  console.error('  page URLs found  : ' + pairs.toLocaleString());
  console.error('  rejected bad cid : ' + badCid.toLocaleString() + '   (misaligned columns in older exports)');
  if (!DRY) {
    console.error('  records updated  : ' + updated.toLocaleString());
    console.error('  cid not indexed  : ' + missing.toLocaleString());
    console.error('  errors           : ' + errors.toLocaleString());
  }
  process.exit(0);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
