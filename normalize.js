/**
 * normalize.js — contact-normalization rules applied on EVERY central-DB write (db.upsertMany).
 *
 * Some sources reintroduce known-bad field values on every sync (e.g. the Google Sheet stores
 * Bankers Life agents' office hours in Position/Title, so a one-off DB fix gets reverted on the
 * next import). Encoding the rule here makes it durable: it runs at the single write chokepoint,
 * so re-imports, crawls, and Site Search all produce the corrected value. Add rules to RULES.
 */
const RULES = [
  {
    // Bankers Life agent bios put office hours in Position/Title — force the role. Matches by
    // email domain (reliable) or the source/Domain being bankerslife.com (any subdomain).
    name: 'bankerslife-agent',
    match: (r) =>
      /[@.]bankerslife\.com$/i.test(String(r['Email Address'] || '').trim()) ||
      /(?:^|\.)bankerslife\.com$/i.test(String(r['Domain'] || '').trim().toLowerCase()),
    apply: (r) => { r['Position'] = 'Bankers Life Agent'; r['Title'] = 'Bankers Life Agent'; },
  },
];

// Apply all matching rules to a record (mutates + returns it).
function normalizeContact(rec) {
  if (!rec || typeof rec !== 'object') return rec;
  for (const rule of RULES) { try { if (rule.match(rec)) rule.apply(rec); } catch { /* skip bad rule */ } }
  return rec;
}

module.exports = { normalizeContact, RULES };
