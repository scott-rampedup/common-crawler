/**
 * li-name-backfill.js — apply the LinkedIn-slug name/gender rule (li-name.resolve) to contacts ALREADY in
 * the Master DB. The going-forward half of the same rule lives in opensearch.ensureNameGender, so a contact
 * crawled tonight and one crawled last year end up identical.
 *
 * Scans: contacts with a linkedin.com/in URL that have NO name, or a name the 131k names map can't gender.
 *   - no name          -> take the LinkedIn name (gendered or not)
 *   - name, no gender  -> take the LinkedIn name ONLY when it resolves to a gender (may replace the name)
 *
 * When a name is REPLACED and that contact's email was 'Modelled' (synthesized from the old name), the
 * address is re-modelled from the new name. The index is keyed by email, so a changed address means
 * index-new + delete-old rather than a partial update — done last, per doc, and reported separately.
 *
 *   OPENSEARCH_ENDPOINT=… node li-name-backfill.js --dry [--limit N] [--samples 20]   (report yield)
 *   OPENSEARCH_ENDPOINT=… node li-name-backfill.js [--limit N] [--fill-only]          (apply)
 *
 *   --fill-only   only fill contacts with NO name; never replace an existing name (cautious first pass)
 *   --no-remodel  update names/genders but leave modelled emails (and their _id) alone
 *
 * Idempotent: a fixed contact stops matching the scan query, so re-running converges (and a re-run is the
 * intended way to mop up anything the live scan drifted past while writes were landing).
 */
const os = require('./opensearch');
const liName = require('./li-name');
const { modelMissingEmails } = require('./email-model');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const FILL_ONLY = process.argv.includes('--fill-only');
const NO_REMODEL = process.argv.includes('--no-remodel');
const LIMIT = Number(arg('--limit', '0')) || 0;
const SAMPLES = Number(arg('--samples', '12')) || 12;
const PAGE = Number(process.env.PAGE || 5000);

// contacts with a /in/ URL AND (no first name OR no gender) — including legacy docs missing the field.
const blankOrMissing = (field, kw) => [{ term: { [kw || field]: '' } }, { bool: { must_not: [{ exists: { field } }] } }];
const QUERY = {
  bool: {
    must: [{ exists: { field: 'linkedin_url' } }],
    must_not: [{ term: { linkedin_url: '' } }],
    should: FILL_ONLY ? blankOrMissing('first', 'first.kw') : [...blankOrMissing('first', 'first.kw'), ...blankOrMissing('gender')],
    minimum_should_match: 1,
  },
};

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  let client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);

  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.error(`candidates (linkedin + no-name${FILL_ONLY ? '' : ' or no-gender'}): ${total.toLocaleString()}${DRY ? '  [DRY RUN — no writes]' : ''}`);

  const t0 = Date.now();
  const tally = { scanned: 0, resolved: 0, genderOnly: 0, nameChanged: 0, gained: 0, updated: 0, remodelled: 0, rekeyed: 0, remodelFail: 0 };
  const samples = [];
  const rekey = [];          // name changed + email was 'Modelled' -> needs a new address (and a new _id)
  let buf = [], after = null;

  const flush = async () => {
    if (!buf.length) return;
    const body = buf; buf = [];
    for (let a = 0; a < 5; a++) {
      try { await client.bulk({ body, refresh: false }); tally.updated += body.length / 2; return; }
      catch (e) { if (a === 2) client = os.makeClient(process.env.OPENSEARCH_ENDPOINT); await new Promise((r) => setTimeout(r, 400 * 2 ** a)); }
    }
    console.error('  bulk failed after retries — continuing');
  };

  for (;;) {
    // Sort by email (the _id), NOT _doc: this scan mutates the very docs it is paging over, and a fixed
    // contact drops out of the result set. Under _doc ordering that shifts every later doc up a slot and
    // search_after silently skips them (the first production run scanned 230,000 of 237,362 and needed two
    // mop-up passes). Sorting on a stable keyword makes the cursor immune to the writes behind it.
    const body = { size: PAGE, query: QUERY, _source: ['first', 'last', 'gender', 'linkedin_url', 'email', 'email_type', 'domain'], sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const hits = (await client.search({ index: os.INDEX, body })).body.hits.hits;
    if (!hits.length) break;

    for (const h of hits) {
      tally.scanned++;
      const s = h._source;
      const r = liName.resolve({ first: s.first, last: s.last, gender: s.gender, linkedinUrl: s.linkedin_url });
      if (r) {
        // --fill-only: never replace a name that is already there
        if (FILL_ONLY && String(s.first || '').trim() && String(s.last || '').trim()) { /* skip */ }
        else {
          tally.resolved++;
          if (r.nameChanged) tally.nameChanged++; else tally.genderOnly++;
          if (r.gender && !String(s.gender || '').trim()) tally.gained++;
          if (samples.length < SAMPLES) samples.push({ email: s.email, was: `${s.first || ''} ${s.last || ''}`.trim() || '(blank)', now: r.name, gender: r.gender || '(none)', li: s.linkedin_url, type: s.email_type || '' });

          const needsRemodel = r.nameChanged && String(s.email_type || '') === 'Modelled' && !NO_REMODEL;
          if (needsRemodel) rekey.push({ id: h._id, src: s, next: r });
          else if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: h._id } }, { doc: { first: r.first, last: r.last, name: r.name, gender: r.gender } }); if (buf.length >= 4000) await flush(); }
        }
      }
      if (LIMIT && tally.scanned >= LIMIT) break;
    }
    after = hits[hits.length - 1].sort;
    if (tally.scanned % 50000 < PAGE) console.error(`  scanned ${tally.scanned.toLocaleString()}/${total.toLocaleString()} | resolved ${tally.resolved.toLocaleString()} (${tally.nameChanged.toLocaleString()} name changes, ${tally.gained.toLocaleString()} genders gained)`);
    if (LIMIT && tally.scanned >= LIMIT) break;
  }
  if (!DRY) await flush();

  // ---- re-model the addresses that were synthesized from a name we just replaced ----
  // The index is keyed by email, so a new address = new doc + delete the old one. Rare by construction
  // (modelling requires First+Last+Gender, so a modelled contact rarely lacks a gender) — handled per doc.
  if (rekey.length) {
    console.error(`\nre-modelling ${rekey.length.toLocaleString()} address(es) whose name changed…`);
    const patternCache = new Map();
    const dbQuery = async (domain) => {
      if (patternCache.has(domain)) return patternCache.get(domain);
      let rows = [];
      try { rows = (await os.search(client, { domain, emailType: 'Professional', pageSize: 200 })).rows || []; } catch (e) { /* best-effort */ }
      patternCache.set(domain, rows);
      return rows;
    };
    for (const item of rekey) {
      const { id, src, next } = item;
      // Rebuild the display record from the stored doc, clear the stale modelled address, re-model.
      let full = null;
      try { full = (await client.get({ index: os.INDEX, id })).body._source; } catch (e) { /* deleted mid-run */ }
      if (!full) continue;
      const rec = os.docToRecord(full);
      rec.Domain = full.domain || '';
      rec['First'] = next.first; rec['Last'] = next.last; rec['Gender'] = next.gender;
      rec['Email Address'] = ''; rec['Email Type'] = '';
      // Modelling only mutates `rec` in memory (dbQuery is a read), so run it under --dry too — that is
      // what makes the dry report say how many addresses would actually be replaced.
      try { await modelMissingEmails([rec], { dbQuery }); } catch (e) { /* best-effort */ }
      const newEmail = String(rec['Email Address'] || '').trim().toLowerCase();
      if (!newEmail) {
        tally.remodelFail++;
        // Couldn't synthesize a replacement — keep the old address rather than leaving the person email-less,
        // but still land the corrected name/gender.
        if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: id } }, { doc: { first: next.first, last: next.last, name: next.name, gender: next.gender } }); }
        continue;
      }
      tally.remodelled++;
      if (newEmail === String(src.email || '').toLowerCase()) {   // same address after all -> plain update
        if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: id } }, { doc: { first: next.first, last: next.last, name: next.name, gender: next.gender } }); }
        continue;
      }
      // Never overwrite a DIFFERENT person who already owns the new address — keep the old one instead.
      let taken = false;
      try { taken = (await client.exists({ index: os.INDEX, id: newEmail })).body === true; } catch (e) { /* treat as free */ }
      if (taken) {
        tally.remodelled--; tally.remodelFail++;
        if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: id } }, { doc: { first: next.first, last: next.last, name: next.name, gender: next.gender } }); }
        continue;
      }
      tally.rekeyed++;
      if (!DRY) {
        // Start from the STORED source so read-only enrichment (firmographics, employer, work_location —
        // fields recordToDoc deliberately does not write back) survives the re-key, then overlay the
        // recomputed fields. A partial update can't be used here: the address is the _id.
        const doc = { ...full, ...os.recordToDoc(rec, full.updated_at || null) };
        try { await os.indexDocs(client, [doc]); await os.bulkDelete(client, [src.email]); }
        catch (e) { console.error('  rekey failed for', src.email, '-', e.message); }
      }
      if (buf.length >= 4000) await flush();
    }
    if (!DRY) await flush();
  }

  if (samples.length) {
    console.error('\nsamples:');
    for (const s of samples) console.error(`  ${s.was.padEnd(24)} -> ${s.now.padEnd(24)} ${(s.gender).padEnd(7)} ${s.type.padEnd(12)} ${s.li}`);
  }
  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: scanned ${tally.scanned.toLocaleString()} | resolved ${tally.resolved.toLocaleString()} `
    + `(${tally.genderOnly.toLocaleString()} gender-only, ${tally.nameChanged.toLocaleString()} name changed) | ${tally.gained.toLocaleString()} genders gained`
    + `${DRY ? '' : ` | ${tally.updated.toLocaleString()} updated`} | re-modelled ${tally.remodelled.toLocaleString()} (${tally.rekeyed.toLocaleString()} re-keyed, ${tally.remodelFail.toLocaleString()} kept old address)`
    + ` | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.stack || e.message || e)); process.exit(1); });
