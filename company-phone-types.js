/**
 * company-phone-types.js — put phone_type on the companies index into the agreed vocabulary.
 * ---------------------------------------------------------------------------------------------
 * The spec is Mobile / Office / Toll Free. The index holds something else:
 *
 *   Direct      4,936,320     <- the contacts vocabulary, never translated for companies
 *   (empty)     2,261,405     <- of which 1,571,151 HAVE a phone and could be typed
 *   Toll Free     292,942
 *   Mobile        192,503
 *
 * Two jobs, independently runnable:
 *   --rename    Direct -> Office. A phone on a company record is an office line; "Direct" is the
 *               contacts-side word for a desk line and means nothing on a company.
 *   --classify  type the 1,571,151 records that have a phone and no type. 75% are UK numbers the
 *               Bing loader left blank ("NANP line-typing N/A"), and they are perfectly typeable.
 *
 *   node company-phone-types.js                      # dry run, both jobs
 *   node company-phone-types.js --rename --apply
 *   node company-phone-types.js --classify --apply
 *
 * Classification follows the split the extractor already made deliberately: libphonenumber outside
 * the NANP, the wireless-block table inside it. Number portability makes libphonenumber's mobile /
 * fixed answer unreliable for +1, which is why that 41MB table exists; using it for the 254,253
 * NANP rows and libphonenumber for the rest keeps one rule per place rather than one rule everywhere.
 * A number that resolves to nothing is LEFT BLANK — a wrong type is worse than no type.
 */
const co = require('./companies');
const ex = require('./extractor');
const { classifyLineType, loadWirelessBlocks, nanpDigits, PHONE_BLOCKS_CSV } = require('./wireless-block-classifier');

const args = process.argv.slice(2);
const APPLY = args.includes('--apply');
const DO_RENAME = args.includes('--rename') || (!args.includes('--classify'));
const DO_CLASSIFY = args.includes('--classify') || (!args.includes('--rename'));
const PAGE = 2000;
const N = (x) => Number(x || 0).toLocaleString();

// NANP toll-free area codes. Checked before the block table: a toll-free number is not a line type
// the wireless table has an opinion about.
const TOLL_FREE_NPA = new Set(['800', '833', '844', '855', '866', '877', '888', '822', '880', '881', '882', '889']);

let wireless = null;
function typeOf(raw) {
  let s = String(raw || '').trim();
  if (!s) return '';
  // Some rows store the number without its "+" ("18608800417"); intlLineType requires explicit
  // international form, and would return "" for every one of them.
  if (!s.startsWith('+')) {
    const d = s.replace(/\D/g, '');
    if (!d) return '';
    s = '+' + (d.length === 10 ? '1' + d : d);
  }
  const d = nanpDigits(s);                            // 10 NANP digits, or '' when not +1
  if (d) {
    if (TOLL_FREE_NPA.has(d.slice(0, 3))) return 'Toll Free';
    const t = (classifyLineType(d, wireless) || {}).type || '';
    if (t === 'Mobile') return 'Mobile';
    if (t && t !== 'Unknown') return 'Office';        // the table's "Direct"/landline answer
    return 'Office';                                  // a company's published +1 number: office line
  }
  const t = ex.intlLineType(s);
  if (t === 'Mobile' || t === 'Toll Free') return t;
  if (t === 'Direct') return 'Office';
  return '';                                          // unparseable / invalid: leave it alone
}

const hasPhone = { bool: { must: [{ exists: { field: 'phone.keyword' } }], must_not: [{ term: { 'phone.keyword': '' } }] } };
const hasType = { bool: { must: [{ exists: { field: 'phone_type.keyword' } }], must_not: [{ term: { 'phone_type.keyword': '' } }] } };
const UNTYPED = { bool: { must: [hasPhone], must_not: [hasType] } };

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('OPENSEARCH_ENDPOINT required'); process.exit(1); }
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const count = (q) => c.count({ index: co.INDEX, body: { query: q } }, { requestTimeout: 600000 }).then((r) => Number((r.body || r).count));

  if (DO_RENAME) {
    const before = await count({ term: { 'phone_type.keyword': 'Direct' } });
    console.log('--rename  Direct -> Office : ' + N(before) + ' record(s)');
    if (APPLY && before) {
      const t0 = Date.now();
      const res = await c.updateByQuery({
        index: co.INDEX, conflicts: 'proceed', slices: 'auto', wait_for_completion: true, refresh: true,
        body: { query: { term: { 'phone_type.keyword': 'Direct' } },
                script: { lang: 'painless', source: "if (ctx._source.phone_type == 'Direct') { ctx._source.phone_type = 'Office'; } else { ctx.op = 'noop'; }" } },
      }, { requestTimeout: 7200000 }).then((x) => (x.body || x));
      // The task's own tally is not trusted here: a long update_by_query under concurrent writes
      // reports a fraction of what it actually wrote (3.2M documents once came back as 167,995).
      // A fresh count is the only honest answer.
      const after = await count({ term: { 'phone_type.keyword': 'Direct' } });
      console.log('    reported updated ' + N(res.updated) + ', conflicts ' + N(res.version_conflicts) +
                  ', took ' + Math.round((Date.now() - t0) / 1000) + 's');
      console.log('    Direct remaining : ' + N(after) + (after ? '  (re-run to finish)' : '  <- done'));
      console.log('    Office now       : ' + N(await count({ term: { 'phone_type.keyword': 'Office' } })));
    }
  }

  if (DO_CLASSIFY) {
    const total = await count(UNTYPED);
    console.log('\n--classify  phone but no type: ' + N(total) + ' record(s)');
    if (!wireless) { const t0 = Date.now(); wireless = loadWirelessBlocks(PHONE_BLOCKS_CSV); console.log('    wireless block table loaded in ' + Math.round((Date.now() - t0) / 1000) + 's'); }

    let scanned = 0, typed = 0, blank = 0, written = 0, errors = 0;
    const tally = new Map();
    let after = null, buf = [];
    const flush = async () => {
      if (!buf.length) return;
      const body = [];
      for (const b of buf) body.push({ update: { _index: co.INDEX, _id: b.id } }, { doc: { phone_type: b.type } });
      buf = [];
      if (!APPLY) return;
      for (let a = 0; ; a++) {
        try {
          const r = await c.bulk({ body, refresh: false }, { requestTimeout: 300000 }).then((x) => (x.body || x));
          if (r.errors) for (const it of (r.items || [])) if (it.update && it.update.error) errors++;
          written += body.length / 2;
          return;
        } catch (e) {
          if (a >= 5) throw e;
          await new Promise((s) => setTimeout(s, 2000 * 2 ** a));
        }
      }
    };

    const t0 = Date.now();
    for (;;) {
      const body = { size: PAGE, query: UNTYPED, _source: ['phone'], sort: [{ _doc: 'asc' }] };
      if (after) body.search_after = after;
      const r = await c.search({ index: co.INDEX, body }, { requestTimeout: 300000 }).then((x) => (x.body || x));
      const hits = r.hits.hits || [];
      if (!hits.length) break;
      for (const h of hits) {
        scanned++;
        const t = typeOf(h._source.phone);
        if (!t) { blank++; continue; }
        typed++;
        tally.set(t, (tally.get(t) || 0) + 1);
        buf.push({ id: h._id, type: t });
      }
      if (buf.length >= 2000) await flush();
      after = hits[hits.length - 1].sort;
      if (scanned % 200000 < PAGE) {
        console.log('    scanned ' + N(scanned) + ' / ' + N(total) + '   typed ' + N(typed) +
                    '   (' + Math.round((Date.now() - t0) / 1000) + 's)');
      }
      // Without --apply the query keeps matching the same documents, so one pass is the sample.
      if (!APPLY && scanned >= 20000) { console.log('    (dry run: stopping after ' + N(scanned) + ' sampled)'); break; }
    }
    await flush();
    console.log('    scanned ' + N(scanned) + ', typeable ' + N(typed) + ', unresolved ' + N(blank) + ', written ' + N(written) + ', errors ' + N(errors));
    for (const [k, v] of [...tally.entries()].sort((a, b) => b[1] - a[1])) console.log('      ' + k.padEnd(12) + N(v));
    if (APPLY) {
      await c.indices.refresh({ index: co.INDEX });
      console.log('    still untyped after: ' + N(await count(UNTYPED)));
    }
  }

  if (!APPLY) console.log('\nDry run — nothing written. Add --apply.');
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
