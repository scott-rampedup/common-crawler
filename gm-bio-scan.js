/**
 * gm-bio-scan.js — one BIO URL list per Google Maps export, pushed to the queue under that export's
 * name, with the pages labelled BIO URL in the Company Crawler.
 * ---------------------------------------------------------------------------------------------
 * The standard procedure for a Maps file, old or new:
 *
 *   1. SCAN     every row of the export for bio URLs — the team-profile column, plus the row's OWN
 *               page when that page is itself a person page.
 *   2. FILE     write one list per export: "<export>.bio-urls.txt", so a list can always be traced
 *               back to the file it came from and a single export can be reprocessed on its own.
 *   3. QUEUE    push each list under the export's name. It goes straight into monitor-queue/pending/
 *               and the pump drains it continuously — the name is a RECEIPT, not a checkpoint.
 *   4. LABEL    where a record's own page is a bio URL, make sure the Company Crawler says so.
 *
 * On step 4: BIO URL is a FIELD on the company record (bio_url), holding the person pages found for
 * that business as a semicolon-separated list. Labelling means making sure each page this scan found
 * is in that field on its own record. Existing entries are kept and the new ones merged in — a record
 * that already lists a page is left untouched, and a page already there is never duplicated.
 *
 *   node gm-bio-scan.js                                  # dry run over every CSV in POI Places
 *   node gm-bio-scan.js --out C:/Users/scott/gm-bio      # write the per-file lists
 *   node gm-bio-scan.js --out … --push --label           # queue them and label the pages
 *   node gm-bio-scan.js --file "<one export>.csv" --out … --push --label
 *
 * Reads the CSVs through gm-load's own parser, so the column mapping cannot drift from the ingest.
 */
const fs = require('fs');
const path = require('path');
const gm = require('./gm-load');
const ex = require('./extractor');
const co = require('./companies');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const argOf = (f, d) => { const i = args.indexOf('--' + f); return i > -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : d; };
const SRC = argOf('src', 'C:/Users/scott/OneDrive/Desktop/POI Places Data');
const OUT = argOf('out', '');
const ONE = argOf('file', '');
const PUSH = has('--push');
const LABEL = has('--label');
const LIMIT = Number(argOf('limit', '')) || 0;
const N = (x) => Number(x || 0).toLocaleString();

const isHttp = (u) => /^https?:\/\/[^\s/]+\.[^\s]/i.test(u);

// A website_location is a BIO URL when its path contains one of the Path ID keywords — the directory
// segments in extractor's BIO_DIR_RAW / KEYWORD_TITLES (/agents/, /staff/, /our-team/, /dt_team/ …).
// pathIdFromUrl is the same function that fills the Path ID column, so "BIO URL" means here exactly
// what it means everywhere else in the system.
function hasBioPathId(u) {
  try { const p = ex.pathIdFromUrl(u); return !!(p && p.id); } catch { return false; }
}

// Merge the pages this export found into each record's BIO URL field.
//
// bio_url holds a '; '-separated list, so this is a UNION, never a replacement: whatever the record
// already lists is kept, and only genuinely new pages are added. A record that already has all of
// them is not written at all, which keeps a re-run of an old export cheap and leaves updated_at alone.
async function labelBioUrls(client, byCid, fileLabel) {
  const ids = [...byCid.keys()];
  if (!ids.length) return { already: 0, added: 0, records: 0, missing: 0, errors: 0 };
  let already = 0, addedUrls = 0, records = 0, missing = 0, errors = 0;
  for (let i = 0; i < ids.length; i += 500) {
    const chunk = ids.slice(i, i + 500);
    // _source is a request PARAMETER on mget; inside the body next to `ids` the API rejects it with
    // "unknown key [_source] for a START_ARRAY".
    const res = await client.mget({ index: co.INDEX, _source: ['bio_url'], body: { ids: chunk } }, { requestTimeout: 180000 }).then((x) => (x.body || x));
    const body = [];
    for (const d of (res.docs || [])) {
      if (!d.found) { missing++; continue; }
      const cur = String((d._source && d._source.bio_url) || '').split(';').map((s) => s.trim()).filter(Boolean);
      const have = new Set(cur);
      const want = byCid.get(d._id) || new Set();
      const add = [...want].filter((u) => !have.has(u));
      if (!add.length) { already++; continue; }
      records++; addedUrls += add.length;
      if (LABEL) body.push({ update: { _index: co.INDEX, _id: d._id } }, { doc: { bio_url: cur.concat(add).join('; ') } });
    }
    if (LABEL && body.length) {
      const r = await client.bulk({ body, refresh: false }, { requestTimeout: 300000 }).then((x) => (x.body || x));
      if (r.errors) for (const it of (r.items || [])) if (it.update && it.update.error) errors++;
    }
  }
  console.error(`    BIO URL field: ${N(records)} record(s) ${LABEL ? 'updated' : 'would gain'} ${N(addedUrls)} page(s)` +
                `; ${N(already)} already complete; ${N(missing)} cid(s) not in the index${errors ? `; ${N(errors)} errors` : ''}`);
  return { already, added: addedUrls, records, missing, errors };
}

// Push one list through the app so the Data Importer records it under the export's name. Falls back
// to nothing (and says so) rather than writing to S3 behind the app's back — for a deliberate, named
// procedure the receipt is the point.
async function pushList(urls, listName) {
  const base = String(process.env.APP_URL || process.env.COMMON_CRAWLER_URL || '').replace(/\/+$/, '');
  const token = process.env.LOADER_TOKEN || '';
  if (!base || !token) { console.error('    (no APP_URL / LOADER_TOKEN — not pushed)'); return 0; }
  // Chunk by BYTES, not by URL count. readJsonBody caps a request body at 2,000,000 bytes and drops
  // the connection when it is exceeded, which arrives here as an unexplained 502. A fixed 20,000-URL
  // chunk is ~1.1MB for a typical export but 3.65MB for one whose URLs run long (17-07 Part 2 averages
  // 180 characters, with the longest at 751), so three exports failed every attempt while 23 sailed
  // through. Counting URLs was never the right measure of a request's size.
  const MAX_BODY = 1500000;                 // headroom under the server's body cap
  // The endpoint also caps a single request at BIO_URL_INGEST_MAX (20,000) URLs and SILENTLY DROPS the
  // rest — it reports `capped` and returns 200. Byte-sized chunks ran to ~23,800 URLs, so every chunk
  // lost its tail: 100,810 of 863,965 vanished while the push reported success. Respect both limits.
  const MAX_URLS = 20000;                 // headroom under the server's 2MB cap
  const chunks = [];
  {
    let cur = [], curBytes = 0;
    for (const u of urls) {
      const cost = u.length + 3;            // the quotes and comma JSON will add
      if (cur.length && (curBytes + cost > MAX_BODY || cur.length >= MAX_URLS)) { chunks.push(cur); cur = []; curBytes = 0; }
      cur.push(u); curBytes += cost;
    }
    if (cur.length) chunks.push(cur);
  }
  let sent = 0, accepted = 0;
  let listId = '';            // the receipt the first chunk created; the rest append to it
  for (let ci = 0; ci < chunks.length; ci++) {
    const chunk = chunks[ci];
    // Retry: the app serves the UI as well, and a 20k-URL POST landing while someone is running a
    // query gets a 502 from the proxy. Three exports (94,696 URLs) were dropped that way on the first
    // full run — a transient refusal should cost a pause, not a list.
    let ok = false;
    for (let a = 0; a < 5 && !ok; a++) {
      if (a) await new Promise((s) => setTimeout(s, Math.min(30000, 3000 * 2 ** (a - 1))));
      const r = await fetch(base + '/api/bio-urls', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-loader-token': token },
        body: JSON.stringify({ urls: chunk, sourceUrl: listName, source: 'Google Maps', label: 'Google Maps: ' + listName, listId }),
      }).catch((e) => ({ ok: false, status: 0, _err: e.message }));
      if (r && r.ok) {
        ok = true;
        // Count what the SERVER says it queued, never what we sent. Those differ whenever it caps or
        // drops a request, and trusting the local number is how 100,810 URLs went missing silently.
        try {
          const j = await r.json();
          if (!listId) listId = j.listId || '';
          accepted += Number(j.queued || 0);
          if (j.capped) console.error(`    WARNING: server capped this request at ${j.capped} URL(s)`);
        } catch { accepted += chunk.length; }
        break;
      }
      console.error(`    push attempt ${a + 1}/5 failed (${(r && (r.status || r._err)) || '?'})`);
    }
    if (!ok) { console.error(`    push FAILED after 5 attempts — list written to disk, not queued`); return accepted; }
    sent += chunk.length;
  }
  if (sent !== accepted) console.error(`    NOTE: sent ${N(sent)} but the server accepted ${N(accepted)}`);
  return accepted;
}

(async () => {
  const files = ONE ? [path.isAbsolute(ONE) ? ONE : path.join(SRC, ONE)]
    : fs.readdirSync(SRC).filter((f) => /\.csv$/i.test(f)).sort().map((f) => path.join(SRC, f));
  if (OUT) fs.mkdirSync(OUT, { recursive: true });
  console.error(`${files.length} export(s) in ${SRC}`);
  console.error(OUT ? `lists -> ${OUT}` : 'DRY RUN — no lists written (pass --out to write them)');

  // A client is needed to REPORT what labelling would do, not only to do it, so it is built
  // whenever the endpoint is set; --label decides whether anything is written.
  const client = process.env.OPENSEARCH_ENDPOINT ? co.makeClient(process.env.OPENSEARCH_ENDPOINT) : null;
  let grandUrls = 0, grandRows = 0, grandOwn = 0, grandPushed = 0, grandTeam = 0;
  const lab = { records: 0, added: 0, already: 0, missing: 0, errors: 0 };

  for (const file of files) {
    const base = path.basename(file);
    const bio = new Set();          // person pages -> the bio_url field AND the queue
    const team = new Set();         // listing pages -> the queue only (crawled to find people)
    const byCid = new Map();          // cid -> Set(bio URLs found for THAT record)
    const noteCid = (cid, u) => {
      if (!cid) return;
      const k = String(cid);
      if (!byCid.has(k)) byCid.set(k, new Set());
      byCid.get(k).add(u);
    };
    let rows = 0, ownBio = 0, siHint = -1, skip = false;
    await gm.eachRecord(file, (r) => {
      if (skip) return;
      const f = gm.fields(r, siHint);
      if (!f) return;
      rows++;
      // 1. xb_team_profile_urls — already known to be person pages, several per cell, '||' separated.
      //    Split on '||' ONLY: splitMulti also breaks on ',' and ';', which are legal inside a URL
      //    (10 of 14,987 profile URLs in the 17-07 export contain a comma) and would be cut in half.
      for (const raw of String(f.xb_team_profile_urls || '').split(/\s*\|\|\s*/)) {
        const u = gm.cleanUrl(gm.abs(raw.trim()));
        if (isHttp(u)) { bio.add(u); noteCid(f.cid, u); }
      }
      // 2. website_location — ONE url, and a bio only when its path carries a Path ID keyword
      //    (pathIdFromUrl matches the 410 BIO_DIR_RAW directory segments: /agents/, /staff/, /dt_team/…).
      //
      //    NOT isBioOrContactUrl, which this used to use: that is bio OR CONTACT, so it accepted
      //    /contact/ and /contact-us/ as bios while rejecting /agents/meredith-martin/ and
      //    /staff/bio.php?doc_id=59241. On the 17-07 export it found 1,936 pages that are not bios and
      //    missed 3,767 that are.
      const own = gm.cleanUrl(gm.abs(f.website_location || f.website_url));
      if (own && isHttp(own) && hasBioPathId(own)) {
        bio.add(own);
        ownBio++;
        noteCid(f.cid, own);
      }
      // TEAM / CONTACT / ABOUT pages are LISTINGS, not bios: one page naming many people. They do not
      // go in bio_url — that field means "a page about this person" — but they are the richest thing
      // in these exports to hand the Contact Crawler, which exists to turn one such page into many
      // contacts. Collected separately and queued alongside.
      //
      // This is where the missing volume was. xb_team_profile_urls is sparse (1.6% of rows at best,
      // 0.15% in the July exports), while xb_team_page is populated on 44-92% of rows — and it was
      // being written to the record and never queued. An export can therefore hold almost no per-person
      // URLs and still be full of people, reachable only by crawling its team pages.
      for (const col of [f.xb_team_page, f.xb_contact_page, f.xb_about_page]) {
        for (const raw of String(col || '').split(/\s*\|\|\s*/)) {
          const u = gm.cleanUrl(gm.abs(raw.trim()));
          if (isHttp(u)) team.add(u);
        }
      }
      if (LIMIT && rows >= LIMIT) return;
    }, (headerRow) => {
      siHint = gm.setHeaderIndex(headerRow);
      // The POI folder holds more than Maps exports — an 8-column "All the places…" sheet, a
      // 16-column Facebook batch. Read with the Maps column mapping they yield nonsense, and a junk
      // list would then be queued under that file's name. A real export declares xb_ columns, or at
      // minimum carries the ~37-column layout.
      const cells = (headerRow || []).map((h) => String(h || '').trim().toLowerCase());
      const hasXb = cells.some((h) => h.startsWith('xb_'));
      if (!hasXb && cells.length < 30) {
        skip = true;
        console.error(`  ${base.padEnd(52)} SKIPPED — ${cells.length} columns, no xb_ fields: not a Maps export`);
      }
    });

    if (skip) continue;
    // Kept APART on purpose. A person page is crawled for that one person and goes to the queue now.
    // A team page names several people and needs a different treatment — a pass that pulls multiple
    // contacts off a single page — which does not exist yet. Mixing them would put 3.6M listing pages
    // through a per-person extractor built for the other shape, so they are written to their own file
    // per export and left for that process.
    for (const u of bio) team.delete(u);
    const list = [...bio];
    grandRows += rows; grandUrls += bio.size; grandTeam += team.size; grandOwn += ownBio;
    console.error(`  ${base.padEnd(52)} ${N(rows).padStart(9)} rows -> ${N(list.length).padStart(8)} URL(s)` +
                  `  [${N(bio.size)} bio${ownBio ? ` incl. ${N(ownBio)} own-page` : ''} + ${N(team.size)} team/contact]`);

    if (OUT && list.length) {
      const outFile = path.join(OUT, base.replace(/\.csv$/i, '') + '.bio-urls.txt');
      fs.writeFileSync(outFile, list.join('\n') + '\n');
      fs.writeFileSync(outFile.replace(/\.txt$/, '') + '.meta.json', JSON.stringify({
        sourceFile: base, count: list.length, bioUrls: bio.size,
        teamPages: team.size, teamPagesQueued: false,
        rows, ownPageBioUrls: ownBio, at: new Date().toISOString(),
      }, null, 1));
      if (PUSH) {
        const sent = await pushList(list, base);
        grandPushed += sent;
        if (sent) console.error(`    queued ${N(sent)} under "${base}"`);
      }
    }

    // Written on its OWN condition, not nested under the bio list: an export can hold thousands of
    // team pages and no person pages at all (09-07 Part 1 has 24,333 against 487), and nesting this
    // under `list.length` would throw those away precisely where they are the only thing there.
    if (OUT && team.size) {
      const teamFile = path.join(OUT, base.replace(/\.csv$/i, '') + '.team-pages.txt');
      fs.writeFileSync(teamFile, [...team].join('\n') + '\n');
      fs.writeFileSync(teamFile.replace(/\.txt$/, '') + '.meta.json', JSON.stringify({
        sourceFile: base, count: team.size, kind: 'team/contact listing pages',
        note: 'NOT queued: awaiting the process that pulls multiple contacts off one page.',
        at: new Date().toISOString(),
      }, null, 1));
    }

    // Label per file rather than accumulating every cid across 25 exports: the map stays bounded by
    // one file's rows, and a run that stops half way has still labelled the files it finished.
    if (client && byCid.size) {
      const t = await labelBioUrls(client, byCid, base);
      lab.records += t.records; lab.added += t.added; lab.already += t.already; lab.missing += t.missing; lab.errors += t.errors;
    }
  }

  console.error(`\nscanned ${N(grandRows)} row(s) across ${files.length} export(s)`);
  console.error(`  BIO URLs (person pages)          : ${N(grandUrls)}`);
  console.error(`  team / contact pages (listings)  : ${N(grandTeam)}   written to their own files, NOT queued`);
  console.error(`  rows whose OWN page is a bio URL : ${N(grandOwn)}`);
  if (PUSH) console.error(`  queued                           : ${N(grandPushed)}`);

  if (client) {
    console.error(`
BIO URL field (${LABEL ? 'written' : 'dry'})`);
    console.error(`  records ${LABEL ? 'updated' : 'that would gain pages'} : ${N(lab.records)}`);
    console.error(`  pages added                       : ${N(lab.added)}`);
    console.error(`  records already complete          : ${N(lab.already)}`);
    console.error(`  cids not in the index             : ${N(lab.missing)}`);
    if (lab.errors) console.error(`  errors                            : ${N(lab.errors)}`);
    if (!LABEL) console.error('  (add --label to write these)');
  } else {
    console.error('');
    console.error('(no OPENSEARCH_ENDPOINT — the BIO URL field was not inspected)');
  }
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
