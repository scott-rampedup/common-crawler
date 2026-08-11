/**
 * backfill-gender.js — assign Gender to contacts that never got one, using the two signals bio-signals
 * added: the nickname root, and the pronouns in the contact's own description.
 *
 *   OPENSEARCH_ENDPOINT=… node backfill-gender.js --dry [--limit N]
 *   OPENSEARCH_ENDPOINT=… node backfill-gender.js
 *
 * WHY THIS EXISTS: bio-signals only affects contacts extracted from now on. 45.6% of the database has no
 * gender and ~100% of it has a description, so the signal to fix that is already stored — it has simply
 * never been read. Gender also gates email modelling, so every gender recovered here makes a contact
 * eligible for an address it could not previously be given.
 *
 * ORDER MATTERS: run this BEFORE harvest-name-gender. That pass learns name->gender facts by aggregating
 * the genders on contacts, so while every gender in the index came from the names map it can only
 * re-derive the map (measured: 38,948 of 40,000 names already known, 1 new). Once pronoun-derived genders
 * are in the data, the same aggregation starts finding names the map never had — which is the whole point.
 */
const path = require('path');
const os = require('./opensearch');
const ex = require('./extractor');
const bs = require('./bio-signals');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const PAGE = Number(process.env.PAGE || 5000);

// Contacts with no gender. A description is not required up front — the nickname root works without one.
const QUERY = {
  bool: {
    should: [{ term: { gender: '' } }, { bool: { must_not: [{ exists: { field: 'gender' } }] } }],
    minimum_should_match: 1,
    must_not: [{ term: { 'first.kw': '' } }],
  },
};

// Does this description actually talk about THIS person? Whole-word first-name match (so "Ann" doesn't
// match "Announcing"), or the last name when the first is very short.
function descMentions(description, first, last) {
  const d = String(description || '').toLowerCase();
  const f = String(first || '').trim().toLowerCase();
  const l = String(last || '').trim().toLowerCase();
  const word = (t) => t.length >= 3 && new RegExp('\\b' + t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b').test(d);
  return Boolean((f && word(f)) || (l && word(l)));
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  let client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.error(`contacts with a name and no gender: ${total.toLocaleString()}${DRY ? '  [DRY RUN — no writes]' : ''}`);
  console.error(`  names map ${Object.keys(genderMap).length.toLocaleString()} · nicknames ${Object.keys(bs.nicknameMap()).length.toLocaleString()}\n`);
  if (!total) { console.error('nothing to do.'); process.exit(0); }

  const t0 = Date.now();
  const tally = { scanned: 0, byNickname: 0, byPronoun: 0, updated: 0, noSignal: 0 };
  const samples = [];
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
    const body = { size: PAGE, query: QUERY, _source: ['email', 'first', 'last', 'description'], sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const hits = (await client.search({ index: os.INDEX, body })).body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      tally.scanned++;
      const s = h._source;
      // 1) the nickname root — cheapest and most certain (Bob -> Robert -> M)
      let g = bs.genderOfName(s.first, genderMap), how = 'nickname';
      // 2) the person's own bio prose — but ONLY when the prose is demonstrably ABOUT this contact.
      //    Many contacts are extracted from a shared roster page and inherit ONE description between them,
      //    so its pronouns describe whichever person the blurb is about, not all of them. A dry run made
      //    this obvious: "Satyajit Phadke" appeared twice carrying unrelated emails (hussain.babu@,
      //    john.veidt@) off the same page and the same text. Requiring the description to name the contact
      //    makes the signal self-referential and kills that whole failure mode.
      if (!g && s.description && s.first && descMentions(s.description, s.first, s.last)) {
        g = bs.genderFromDescription(s.description); how = 'pronoun';
      }
      if (!g) { tally.noSignal++; continue; }
      if (how === 'nickname') tally.byNickname++; else tally.byPronoun++;
      if (samples.length < 12) samples.push({ email: s.email, name: `${s.first} ${s.last || ''}`.trim(), g, how });
      if (!DRY) { buf.push({ update: { _index: os.INDEX, _id: h._id } }, { doc: { gender: g } }); if (buf.length >= 4000) await flush(); }
      if (LIMIT && tally.scanned >= LIMIT) break;
    }
    after = hits[hits.length - 1].sort;
    if (tally.scanned % 200000 < PAGE) {
      const got = tally.byNickname + tally.byPronoun;
      console.error(`  scanned ${tally.scanned.toLocaleString()} | gendered ${got.toLocaleString()} (${((got / tally.scanned) * 100).toFixed(1)}%) — ${tally.byNickname.toLocaleString()} nickname, ${tally.byPronoun.toLocaleString()} pronoun`);
    }
    if (LIMIT && tally.scanned >= LIMIT) break;
  }
  if (!DRY) await flush();

  if (samples.length) {
    console.error('\nsamples:');
    for (const s of samples) console.error(`  ${s.g}  ${String(s.name).padEnd(28)} via ${s.how.padEnd(9)} ${s.email}`);
  }
  const got = tally.byNickname + tally.byPronoun;
  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: scanned ${tally.scanned.toLocaleString()} | gender recovered ${got.toLocaleString()} (${((got / Math.max(1, tally.scanned)) * 100).toFixed(1)}%)`
    + ` — ${tally.byNickname.toLocaleString()} via nickname, ${tally.byPronoun.toLocaleString()} via pronouns | ${tally.noSignal.toLocaleString()} had no signal`
    + `${DRY ? '' : ` | ${tally.updated.toLocaleString()} updated`} | ${Math.round((Date.now() - t0) / 1000)}s`);
  if (got) console.error('Next: re-run harvest-name-gender — these pronoun-derived genders are what make it find names the map never had.');
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.stack || e.message || e)); process.exit(1); });
