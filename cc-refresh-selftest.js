/**
 * cc-refresh-selftest.js — the Common Crawl monthly-refresh field policy, as executable checks.
 *
 *   node cc-refresh-selftest.js
 *
 * Covers the decisions recorded in the field-policy doc:
 *   1  blank-write guard   — an empty page must never write '' over a stored value
 *   2  due-selector        — gate on cc_crawl, not cc_refreshed_at (checked in cc-monthly-refresh)
 *   5  arrays              — multi-value fields are arrays end to end, including the write path
 *   7  four new fields     — linkedin_url, team_page, whatsapp, founded
 *
 * No network and no credentials: every case runs against literal HTML.
 */
const che = require('./cc-home-enrich');
const co = require('./companies');
const { asList, mergeList, enrichFromHtml } = che;
let pass = 0, fail = 0;
const T = (n, f) => { try { f(); pass++; } catch (e) { fail++; console.log('FAIL  ' + n + '\n      ' + e.message); } };
const eq = (a, b, m) => { const A = JSON.stringify(a), B = JSON.stringify(b); if (A !== B) throw new Error(m + ': got ' + A + ' want ' + B); };
const page = (body, head = '') => `<html><head>${head}</head><body>${body}</body></html>`;
const LD = (o) => `<script type="application/ld+json">${JSON.stringify(o)}</script>`;
const EMPTY = page('');

// ---- asList: must read both shapes ----
T('asList array',            ()=>eq(asList(['a','b']),['a','b'],'x'));
T('asList joined string',    ()=>eq(asList('a; b'),['a','b'],'x'));
T('asList legacy whitespace',()=>eq(asList('a b'),['a','b'],'x'));
T('asList empty string',     ()=>eq(asList(''),[],'x'));
T('asList null/undefined',   ()=>{eq(asList(null),[],'n');eq(asList(undefined),[],'u');});
T('asList trims + drops gaps',()=>eq(asList(' a ;; b '),['a','b'],'x'));
T('asList array w/ blanks',  ()=>eq(asList(['a','','  ','b']),['a','b'],'x'));

// ---- mergeList: union, order kept, dedup ----
T('merge string + array',    ()=>eq(mergeList('a; b',['c']),['a','b','c'],'x'));
T('merge dedupes',           ()=>eq(mergeList('a; b',['b','c']),['a','b','c'],'x'));
T('merge keeps first-seen order',()=>eq(mergeList(['z','a'],['a','m']),['z','a','m'],'x'));
T('merge of two empties',    ()=>eq(mergeList('',[]),[],'x'));

// ---- the blank-write guard: an empty page must not blank anything ----
T('empty page writes no blanking keys', ()=>{
  const r = enrichFromHtml({ domain:'a.com', website:'https://a.com/' }, EMPTY, { now:'2026-11-20T00:00:00Z', crawl:'CC-MAIN-2026-47' });
  for (const k of ['description','facebook','instagram','map','linkedin_contact','bio_url','alternate_websites','contacts','contacts_count']) {
    if (k in r.updates) throw new Error('wrote '+k+'='+JSON.stringify(r.updates[k])+' from an empty page');
  }
});
T('empty page still stamps the crawl', ()=>{
  const r = enrichFromHtml({ domain:'a.com' }, EMPTY, { now:'2026-11-20T00:00:00Z', crawl:'CC-MAIN-2026-47' });
  eq(r.updates.cc_crawl,'CC-MAIN-2026-47','cc_crawl'); eq(r.updates.cc_refreshed_at,'2026-11-20T00:00:00Z','cc_refreshed_at');
});
T('existing bio_url survives an empty page, as an array', ()=>{
  const r = enrichFromHtml({ domain:'a.com', bio_url:'https://a.com/t/x; https://a.com/t/y' }, EMPTY, {});
  eq(r.updates.bio_url,['https://a.com/t/x','https://a.com/t/y'],'bio_url');
});
T('existing linkedin_contact survives, as an array', ()=>{
  const r = enrichFromHtml({ domain:'a.com', linkedin_contact:'https://www.linkedin.com/in/aa' }, EMPTY, {});
  eq(r.updates.linkedin_contact,['https://www.linkedin.com/in/aa'],'linkedin_contact');
});

// ---- merge, not replace ----
const PAGE = '<html><body>'
  + '<a href="https://www.linkedin.com/in/newperson">New</a>'
  + '<a href="/team/zoe">Zoe</a>'
  + '</body></html>';
T('page bios merge with existing, no loss', ()=>{
  const r = enrichFromHtml({ domain:'a.com', bio_url:'https://a.com/team/old' }, PAGE, {});
  const got = r.updates.bio_url || [];
  if (!Array.isArray(got)) throw new Error('bio_url is not an array: '+JSON.stringify(got));
  if (!got.includes('https://a.com/team/old')) throw new Error('LOST the existing bio url: '+JSON.stringify(got));
});
T('page linkedin merges with existing, no loss', ()=>{
  const r = enrichFromHtml({ domain:'a.com', linkedin_contact:'https://www.linkedin.com/in/olduser' }, PAGE, {});
  const got = r.updates.linkedin_contact || [];
  if (!got.includes('https://www.linkedin.com/in/olduser')) throw new Error('LOST existing linkedin: '+JSON.stringify(got));
  if (!got.includes('https://www.linkedin.com/in/newperson')) throw new Error('did not add the new one: '+JSON.stringify(got));
});
T('no duplicate when the page repeats what is stored', ()=>{
  const r = enrichFromHtml({ domain:'a.com', linkedin_contact:'https://www.linkedin.com/in/newperson' }, PAGE, {});
  const got = r.updates.linkedin_contact || [];
  eq(got.filter(x=>x==='https://www.linkedin.com/in/newperson').length,1,'duplicated');
});

// the bug this prevents: String(['a','b']) === 'a,b'
T('array survives the write path as an array', ()=>eq(co.coerceField('bio_url',['a','b']),['a','b'],'bio_url'));
T('legacy joined string converts on write',    ()=>eq(co.coerceField('bio_url','a; b'),['a','b'],'bio_url'));
T('multi-value dedupes',                       ()=>eq(co.coerceField('linkedin_contact',['a','a','b']),['a','b'],'li'));
T('explicit clear stays a clear',              ()=>eq(co.coerceField('bio_url',''),[],'clear'));
T('single-value field still a string',         ()=>eq(co.coerceField('description','hi'),'hi','desc'));
T('single-value null becomes empty string',    ()=>eq(co.coerceField('description',null),'','desc'));
T('founded still numeric',                     ()=>eq(co.coerceField('founded','1998'),1998,'founded'));
T('founded blank becomes null',                ()=>eq(co.coerceField('founded',''),null,'founded'));
T('a non-multi field is NOT turned into a list',()=>eq(co.coerceField('name','Smith & Sons'),'Smith & Sons','name'));

// CSV must keep the '; ' contract, not emit commas inside a comma-separated file
T('csv renders an array with "; "', ()=>{
  const line = co.rowToCsvLine({ name:'X', domain:'x.com', bio_url:['https://x.com/a','https://x.com/b'] });
  if (!line.includes('"https://x.com/a; https://x.com/b"')) throw new Error('csv cell wrong: '+line.slice(0,300));
  if (line.includes('https://x.com/a,https://x.com/b')) throw new Error('array leaked commas into the CSV');
});
T('csv unchanged for a legacy string', ()=>{
  const line = co.rowToCsvLine({ name:'X', domain:'x.com', bio_url:'https://x.com/a; https://x.com/b' });
  if (!line.includes('"https://x.com/a; https://x.com/b"')) throw new Error('csv cell wrong: '+line.slice(0,300));
});
T('csv still escapes quotes', ()=>{
  const line = co.rowToCsvLine({ name:'He said "hi"', domain:'x.com' });
  if (!line.includes(`"He Said ""hi"""`)) throw new Error('quote escaping broke: '+line.slice(0,80));
});


T('company linkedin page -> linkedin_url (array)', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('<a href="https://www.linkedin.com/company/acme">us</a>'), {});
  eq(r.updates.linkedin_url, ['https://www.linkedin.com/company/acme'], 'linkedin_url');
});
T('personal and company linkedin do not mix', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page(
    '<a href="https://www.linkedin.com/company/acme">co</a><a href="https://www.linkedin.com/in/jane">jane</a>'), {});
  eq(r.updates.linkedin_url, ['https://www.linkedin.com/company/acme'], 'linkedin_url');
  eq(r.updates.linkedin_contact, ['https://www.linkedin.com/in/jane'], 'linkedin_contact');
});
T('whatsapp url captured, number still reaches phones', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('<a href="https://wa.me/14125551234">chat</a>'), {});
  eq(r.updates.whatsapp,'https://wa.me/14125551234','whatsapp');
  eq(r.updates.phone,'14125551234','phone');       // unchanged behaviour
});
T('whatsapp does not overwrite an existing one', ()=>{
  const r = che.enrichFromHtml({domain:'a.com', whatsapp:'https://wa.me/19999999999'}, page('<a href="https://wa.me/14125551234">x</a>'), {});
  if ('whatsapp' in r.updates) throw new Error('overwrote a verified whatsapp');
});
T('team listing page -> team_page, bio membership unchanged', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('<a href="https://a.com/our-team">team</a>'), {});
  eq(r.updates.team_page,'https://a.com/our-team','team_page');
  // NOT in bio_url: isBioOrContactUrl rejects a listing page, and this change does not alter that
  if ('bio_url' in r.updates) throw new Error('bio membership changed: '+JSON.stringify(r.updates.bio_url));
});
T("a person's bio page is not a team_page", ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('<a href="https://a.com/team/john-smith">John</a>'), {});
  if ('team_page' in r.updates) throw new Error('called a person page a team page');
  if (!(r.updates.bio_url||[]).includes('https://a.com/team/john-smith')) throw new Error('lost the bio url');
});
T('founded from schema.org foundingDate', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('', LD({'@type':'Organization',name:'A',foundingDate:'1998-05-01'})), {});
  eq(r.updates.founded,1998,'founded');
});
T('founded from a bare year', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('', LD({'@type':'Organization',name:'A',foundingDate:'1887'})), {});
  eq(r.updates.founded,1887,'founded');
});
T('implausible founding year ignored', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('', LD({'@type':'Organization',name:'A',foundingDate:'0001-01-01'})), {});
  if ('founded' in r.updates) throw new Error('accepted year 1');
});
T('no schema.org -> founded not written', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page('<p>hi</p>'), {});
  if ('founded' in r.updates) throw new Error('wrote founded with no source');
});
T('new fields are absent on an empty page', ()=>{
  const r = che.enrichFromHtml({domain:'a.com'}, page(''), {});
  for (const k of ['linkedin_url','team_page','whatsapp','founded']) if (k in r.updates) throw new Error('wrote '+k);
});

// ---- decision 8: sitemap_url from robots.txt ----
const cc = require('./cc-engine');
const ROBOTS = ['User-agent: *', 'Disallow: /tmp/', 'Sitemap: https://x.com/sitemap-news.xml', 'Sitemap: https://x.com/sitemap-team.xml'].join(String.fromCharCode(10));
const ROBOTS_NONE = ['User-agent: *', 'Disallow: /tmp/'].join(String.fromCharCode(10));
T('picks the sitemap carrying a People path ID', () => eq(che.pickSitemap(cc.parseRobots(ROBOTS).sitemaps), 'https://x.com/sitemap-team.xml', 'pick'));
T('falls back to a sitemap index', () => eq(che.pickSitemap(['https://x.com/news.xml','https://x.com/sitemap_index.xml']), 'https://x.com/sitemap_index.xml', 'pick'));
T('falls back to the first declared', () => eq(che.pickSitemap(['https://x.com/a.xml','https://x.com/b.xml']), 'https://x.com/a.xml', 'pick'));
T('nothing declared -> empty, so no key is written', () => eq(che.pickSitemap([]), '', 'pick'));
T('robots.txt with no Sitemap line', () => eq(che.pickSitemap(cc.parseRobots(ROBOTS_NONE).sitemaps), '', 'pick'));

// ---- decision 9: email_hash ----
const { emailHash } = require('./email-hash');
T('enrichFromHtml writes email_hash beside email', () => {
  const r = enrichFromHtml({ domain: 'a.com' }, page('<a href="mailto:Jane.Doe@a.com">j</a>'), {});
  eq(r.updates.email, 'jane.doe@a.com', 'email');
  eq(r.updates.email_hash, emailHash('jane.doe@a.com'), 'email_hash');
});
T('no email -> no email_hash key', () => {
  const r = enrichFromHtml({ domain: 'a.com' }, EMPTY, {});
  if ('email_hash' in r.updates) throw new Error('wrote a hash with no address');
});
T('hash is of the PP canonical form, not the raw address', () => eq(emailHash('Jane.Doe+x@googlemail.com'), emailHash('janedoe@gmail.com'), 'canonical'));

// ---- the write-block guard (gm-upsert assertWritable) ----
// The payload below is the REAL body the cluster returned on 2026-11-21 while the flood-stage block
// was on, copied verbatim, so these check the parser against the shape it actually meets rather than
// against one invented to suit it.
const BLOCKED_REAL = { companies: { settings: { index: { blocks: { read_only_allow_delete: 'true', write: 'true' } } } } };
T('blocked: the real flood-stage payload is detected',
  () => eq(co.blockedIndexWrites(BLOCKED_REAL, 'companies'), ['read_only_allow_delete', 'write'], 'blocks'));
T('blocked: an unblocked index is absent from the body, so nothing is reported',
  () => eq(co.blockedIndexWrites({}, 'companies'), [], 'empty body'));
T('blocked: filter_path can return the index with no blocks key',
  () => eq(co.blockedIndexWrites({ companies: { settings: { index: {} } } }, 'companies'), [], 'no blocks'));
T('blocked: "false" is a STRING and must not count as a block',
  () => eq(co.blockedIndexWrites({ companies: { settings: { index: { blocks: { read_only_allow_delete: 'false' } } } } }, 'companies'), [], 'string false'));
T('blocked: a block on a DIFFERENT index is not ours',
  () => eq(co.blockedIndexWrites(BLOCKED_REAL, 'contacts'), [], 'other index'));
T('blocked: read_only alone still blocks writes',
  () => eq(co.blockedIndexWrites({ companies: { settings: { index: { blocks: { read_only: 'true' } } } } }, 'companies'), ['read_only'], 'read_only'));
T('blocked: null/undefined body does not throw',
  () => { eq(co.blockedIndexWrites(null, 'companies'), [], 'null'); eq(co.blockedIndexWrites(undefined, 'companies'), [], 'undef'); });

// ---- leastFreeBytes: the scheduler write gate reads _cat/allocation ----
// The rows below are the real shapes _cat/allocation emits. A header row or an UNASSIGNED row parsed
// as 0 would read as "no disk left" and stop every scheduled job, which is a worse outage than the one
// the gate exists to prevent.
T('leastFree: picks the smallest of several nodes',
  () => eq(co.leastFreeBytes('n1 116000000000\nn2 110000000000'), 110000000000, 'min'));
T('leastFree: skips the header row that ?v adds',
  () => eq(co.leastFreeBytes('node disk.avail\nn1 116000000000'), 116000000000, 'header'));
T('leastFree: skips the UNASSIGNED row, which has no numbers',
  () => eq(co.leastFreeBytes('n1 116000000000\nUNASSIGNED'), 116000000000, 'unassigned'));
T('leastFree: empty or null input is null, NOT zero',
  () => { eq(co.leastFreeBytes(''), null, 'empty'); eq(co.leastFreeBytes(null), null, 'null'); });
T('leastFree: a genuinely low value is reported, not swallowed',
  () => eq(co.leastFreeBytes('n1 5000000000\nn2 110000000000'), 5000000000, 'low'));

// ---- chunkByBytes: both caps must hold ----
// The 20,000-URL cap is the endpoint's own, and exceeding it is SILENT: it reports `capped` and still
// returns 200. Chunking by bytes alone once ran to ~23,800 URLs per chunk and lost 100,810 of 863,965
// URLs while every push reported success. A test that only checked bytes would not have caught that.
const { chunkByBytes } = require('./queue-bio-urls');
T('chunk: respects the 20,000-URL cap even when the bytes would allow more', () => {
  const lines = Array.from({ length: 50000 }, (_, i) => 'https://x.com/' + i);
  const cs = chunkByBytes(lines, 1e9, 20000);
  if (cs.some((c) => c.length > 20000)) throw new Error('a chunk exceeded the URL cap');
  eq(cs.reduce((a, c) => a + c.length, 0), 50000, 'no URL dropped');
});
T('chunk: respects the byte cap even when the count would allow more', () => {
  const lines = Array.from({ length: 200 }, () => 'https://x.com/' + 'a'.repeat(500));
  const cs = chunkByBytes(lines, 10000, 20000);
  for (const c of cs) { const b = c.reduce((a, u) => a + u.length + 3, 0); if (b > 10000 && c.length > 1) throw new Error('chunk over the byte cap: ' + b); }
  eq(cs.reduce((a, c) => a + c.length, 0), 200, 'no URL dropped');
});
T('chunk: every URL survives chunking', () => {
  const lines = Array.from({ length: 4321 }, (_, i) => 'https://x.com/' + i);
  eq(chunkByBytes(lines, 5000, 100).flat(), lines, 'round trip');
});
T('chunk: empty input yields no chunks', () => eq(chunkByBytes([]), [], 'empty'));
T('chunk: one oversized URL still gets its own chunk rather than being dropped', () => {
  const big = 'https://x.com/' + 'a'.repeat(9999);
  eq(chunkByBytes([big], 100, 20000), [[big]], 'oversized');
});

// ---- genderFromMap: a plain {} must not answer with its prototype ----
// cc-engine.js defaulted `genderMap = {}` in six signatures. A plain object answers "constructor",
// "tostring" and "valueof" with inherited FUNCTIONS, which are truthy, and 'constructor' passes the
// /^[a-z]+$/ first-name test -- so a page could mint a person whose gender stringified to
// "function Object() { [native code] }". One such record reached the contacts index.
const { genderFromMap } = require('./extractor');
T('genderFromMap: plain {} does not leak constructor', () => eq(genderFromMap({}, 'constructor'), '', 'ctor'));
T('genderFromMap: plain {} does not leak tostring/valueof',
  () => { eq(genderFromMap({}, 'tostring'), '', 'toString'); eq(genderFromMap({}, 'valueof'), '', 'valueOf'); });
T('genderFromMap: a real entry still answers', () => { const m = Object.create(null); m.jane = 'F'; eq(genderFromMap(m, 'jane'), 'F', 'jane'); });
T('genderFromMap: an unknown name is empty', () => { const m = Object.create(null); m.jane = 'F'; eq(genderFromMap(m, 'zzz'), '', 'zzz'); });
T('genderFromMap: a non-string value is not a gender',
  () => { const m = Object.create(null); m.jane = 1; eq(genderFromMap(m, 'jane'), '', 'numeric'); });
T('genderFromMap: null map / empty name are safe',
  () => { eq(genderFromMap(null, 'x'), '', 'null'); eq(genderFromMap({}, ''), '', 'empty'); });

// The dangerous shape is not the visible function -- it is a coercion that makes one look plausible.
// contacts-csv-ingest letter()s the map's answer, and String(Object) starts "function", so /^f/i
// matched and a first name of "Constructor" was ingested as a confident 'F'. Garbage announces
// itself; a wrong gender on a real-looking contact does not. Guard the pair, not just the map.
const letter = (g) => (/^m/i.test(g) ? 'M' : /^f/i.test(g) ? 'F' : '');
T('letter(): raw prototype leak would read as F -- the bug this guards',
  () => eq(letter({}['constructor']), 'F', 'unguarded coercion still does this'));
T('letter(genderFromMap()): a prototype name yields no gender',
  () => { eq(letter(genderFromMap({}, 'constructor')), '', 'ctor'); eq(letter(genderFromMap({}, 'tostring')), '', 'toString'); });
T('letter(genderFromMap()): a real entry survives the pair',
  () => { const m = Object.create(null); m.jane = 'F'; m.bob = 'M';
          eq(letter(genderFromMap(m, 'jane')), 'F', 'jane'); eq(letter(genderFromMap(m, 'bob')), 'M', 'bob'); });

console.log(pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
