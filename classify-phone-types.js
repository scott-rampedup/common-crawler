/**
 * classify-phone-types.js — backfill Phone Type / Phone 2 Type on records that have a phone NUMBER
 * but a blank type (e.g. the Lambda-backfilled phones, captured without the block table).
 *
 * Scans OpenSearch for docs with a phone (starts "+") and no phone_type, classifies the stored E.164
 * against phone-blocks.csv (NANP line-type) + intl-mobile (non-NANP mobile), and PARTIAL-updates only
 * the *_type fields (never touches anything else). Idempotent; safe to re-run.
 *
 *   OPENSEARCH_ENDPOINT=… node classify-phone-types.js [--apply]   (default = dry run)
 */
const os = require('./opensearch');
const { classifyLineType, loadWirelessBlocks, nanpDigits, PHONE_BLOCKS_CSV } = require('./wireless-block-classifier');
const { intlMobileType } = require('./intl-mobile');

const APPLY = process.argv.includes('--apply');
const wireless = loadWirelessBlocks(PHONE_BLOCKS_CSV);

function typeOf(phone) {
  if (!phone || !String(phone).startsWith('+')) return '';
  const d = nanpDigits(phone);                 // 10 NANP digits, or '' if not +1
  let t = d ? (classifyLineType(d, wireless).type || '') : '';
  if ((!t || t === 'Unknown') && intlMobileType(phone)) t = 'Mobile';
  return t === 'Unknown' ? '' : t;
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const INDEX = 'contacts';
  // real phone present, but phone_type blank or missing
  const query = { bool: {
    must: [{ prefix: { phone: '+' } }],
    should: [{ term: { phone_type: '' } }, { bool: { must_not: { exists: { field: 'phone_type' } } } }],
    minimum_should_match: 1,
  } };

  let scanned = 0, changed = 0, batch = [];
  const flush = async () => {
    if (!batch.length) return;
    if (APPLY) { const r = await client.bulk({ body: batch, refresh: false }); if ((r.body || r).errors) { /* count below */ } }
    batch = [];
  };

  let resp = await client.search({ index: INDEX, scroll: '5m', size: 1000,
    _source: ['phone', 'phone_2', 'phone_type', 'phone_2_type', 'email'], body: { query } });
  let sid = (resp.body || resp)._scroll_id;
  for (;;) {
    const hits = (resp.body || resp).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const s = h._source; const id = h._id;
      const pt = typeOf(s.phone);
      const p2t = s.phone_2 ? typeOf(s.phone_2) : (s.phone_2_type || '');
      const doc = {};
      if (pt && pt !== (s.phone_type || '')) doc.phone_type = pt;
      if (s.phone_2 && p2t && p2t !== (s.phone_2_type || '')) doc.phone_2_type = p2t;
      if (Object.keys(doc).length) { changed++; batch.push({ update: { _index: INDEX, _id: id } }, { doc }); }
    }
    if (batch.length >= 2000) await flush();
    if (scanned % 50000 === 0) console.error(`  scanned ${scanned.toLocaleString()} | to-update ${changed.toLocaleString()}`);
    resp = await client.scroll({ scroll_id: sid, scroll: '5m' });
    sid = (resp.body || resp)._scroll_id;
  }
  await flush();
  try { await client.clearScroll({ body: { scroll_id: [sid] } }); } catch (e) { /* ignore */ }
  console.error(`DONE: scanned ${scanned.toLocaleString()} | ${APPLY ? 'updated' : 'would update'} ${changed.toLocaleString()} phone-type(s)${APPLY ? '' : '  (dry run — add --apply)'}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
