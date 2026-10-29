/**
 * psl-suffix.js — registrable ("root") domain via the Public Suffix List.
 * ---------------------------------------------------------------------------------------------
 * Why not "TLD Look UP.csv": that table is a list of TLDs, and a root domain is not a TLD question.
 * Folding 30.7M company records by its answer produced three distinct wrong results —
 *   - sh.cn, net.cn, k12.ga.us are public suffixes, so foo.sh.cn folded INTO sh.cn and unrelated
 *     companies merged (the dry run found sh.cn holding 3 "HQs" of its own);
 *   - .games, .capital, .email, .ventures are real gTLDs the CSV never had, so 6,198 records per
 *     200k domains got flagged as junk that were perfectly valid;
 *   - foo.myshopify.com and foo.netlify.app are separate businesses, which only the PSL's private
 *     section knows.
 * The PSL answers all three. Every rule the CSV lacked is in it.
 *
 * Data file: public_suffix_list.dat beside this module (refresh with --update; it changes slowly,
 * a few rules a month, and a stale copy degrades gracefully to the old answer for new TLDs only).
 *
 *   const psl = require('./psl-suffix');
 *   psl.registrable('www.locations.kw.com')   -> 'kw.com'
 *   psl.registrable('shop.foo.myshopify.com') -> 'foo.myshopify.com'   (private rule: own business)
 *   psl.registrable('sh.cn')                  -> ''   (a bare public suffix is not a company)
 *   psl.knownSuffix('164.53')                 -> false (not a domain at all)
 *
 *   node psl-suffix.js --test     # self-check against the cases that broke the CSV
 *   node psl-suffix.js --update   # re-download the list
 */
const fs = require('fs');
const path = require('path');

const DAT = path.join(__dirname, 'public_suffix_list.dat');
const URL = 'https://publicsuffix.org/list/public_suffix_list.dat';

// rule -> 1 (ICANN section) | 2 (PRIVATE section). Wildcards keep their '*' label; exceptions are
// held separately because they win over every normal rule however long it is.
const RULES = new Map();
const EXCEPT = new Map();

// Enough of the list to keep the common cases right if the data file is ever missing. This module is
// now on the hot path of enrich-firmographics, email-model and serp-lookup, so a missing data file has
// to degrade to slightly worse answers, not take those three down with an exception.
const FALLBACK = ['com', 'org', 'net', 'edu', 'gov', 'mil', 'int', 'io', 'ai', 'co', 'us', 'uk', 'ca',
  'au', 'de', 'fr', 'nl', 'es', 'it', 'se', 'no', 'dk', 'fi', 'ie', 'nz', 'za', 'br', 'mx', 'in', 'jp',
  'cn', 'kr', 'sg', 'hk', 'ch', 'at', 'be', 'pl', 'pt', 'gr', 'tr', 'ru', 'ua', 'il', 'ae',
  'co.uk', 'org.uk', 'ac.uk', 'gov.uk', 'co.jp', 'co.nz', 'co.za', 'co.in', 'co.il', 'co.kr',
  'com.au', 'net.au', 'org.au', 'com.br', 'com.mx', 'com.cn', 'com.sg', 'com.hk', 'com.tr'];
let warned = false;

function load() {
  if (RULES.size) return;
  if (!fs.existsSync(DAT)) {
    if (!warned) { warned = true; console.error('[psl-suffix] ' + DAT + ' missing — using the built-in fallback suffix list. Refresh with: node psl-suffix.js --update'); }
    for (const r of FALLBACK) RULES.set(r, 1);
    return;
  }
  let section = 1;
  for (const raw of fs.readFileSync(DAT, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith('//')) {
      if (line.includes('BEGIN PRIVATE DOMAINS')) section = 2;
      else if (line.includes('BEGIN ICANN DOMAINS')) section = 1;
      continue;
    }
    const rule = line.split(/\s/)[0].toLowerCase();
    if (!rule) continue;
    if (rule.startsWith('!')) EXCEPT.set(rule.slice(1), section);
    else RULES.set(rule, section);
  }
}

// host -> bare lower-case hostname (no scheme, port, path, trailing dot, leading www.)
function normHost(h) {
  let s = String(h == null ? '' : h).trim().toLowerCase();
  s = s.replace(/^[a-z][a-z0-9+.-]*:\/\//, '').split('/')[0].split('?')[0].split('#')[0];
  s = s.split('@').pop().split(':')[0].replace(/\.+$/, '');
  return s.startsWith('www.') ? s.slice(4) : s;
}

/**
 * The PSL algorithm. Returns the matched suffix, which section it came from, and whether any rule
 * matched at all (an unmatched host means the TLD is not real — "164.53", "0.5").
 */
function suffixOf(host) {
  load();
  const labels = host.split('.').filter(Boolean);
  if (!labels.length) return { suffix: '', section: 0, known: false };

  // an exception rule wins outright, and its own leftmost label is removed
  for (let i = 0; i < labels.length; i++) {
    const cand = labels.slice(i).join('.');
    const sec = EXCEPT.get(cand);
    if (sec) return { suffix: labels.slice(i + 1).join('.'), section: sec, known: true };
  }

  let best = '', bestSec = 0;
  for (let i = labels.length - 1; i >= 0; i--) {
    const tail = labels.slice(i);
    const exact = tail.join('.');
    const sec = RULES.get(exact);
    if (sec && tail.length > best.split('.').filter(Boolean).length) { best = exact; bestSec = sec; }
    // a wildcard rule matches any single label in that position: *.ck matches foo.ck
    if (tail.length > 1) {
      const wild = ['*'].concat(tail.slice(1)).join('.');
      const wsec = RULES.get(wild);
      if (wsec && tail.length > best.split('.').filter(Boolean).length) { best = exact; bestSec = wsec; }
    }
  }
  if (best) return { suffix: best, section: bestSec, known: true };
  // No rule at all. Per the PSL spec the implicit rule is "*", i.e. the last label is the suffix —
  // but for our data an unmatched TLD means the value is not a domain, so say so.
  return { suffix: labels[labels.length - 1], section: 0, known: false };
}

/** The registrable domain: the public suffix plus one more label. '' when the host IS a bare suffix. */
function registrable(hostOrUrl) {
  const host = normHost(hostOrUrl);
  if (!host) return '';
  const { suffix } = suffixOf(host);
  if (!suffix) return host;
  if (host === suffix) return '';                 // "sh.cn", "co.uk" — a suffix, not a company
  const extra = host.slice(0, host.length - suffix.length - 1).split('.');
  return extra[extra.length - 1] + '.' + suffix;
}

/**
 * registrable(), but never empty: a bare public suffix ("sh.cn") and an unparseable value come back
 * as the host itself. Callers that use the result as a LOOKUP KEY want this — an empty key silently
 * matches nothing, which is worse than a slightly odd key that matches what is actually stored.
 */
function registrableOrHost(hostOrUrl) {
  const host = normHost(hostOrUrl);
  if (!host) return '';
  return registrable(host) || host;
}

/** Does the host end in a suffix the PSL actually knows? False for "164.53", "0.5", "localhost". */
function knownSuffix(hostOrUrl) { return suffixOf(normHost(hostOrUrl)).known; }

/** True when the registrable domain came from the PSL's PRIVATE section (foo.myshopify.com). */
function isPrivateSuffix(hostOrUrl) { return suffixOf(normHost(hostOrUrl)).section === 2; }

function publicSuffix(hostOrUrl) { return suffixOf(normHost(hostOrUrl)).suffix; }

async function update() {
  const https = require('https');
  await new Promise((res, rej) => {
    https.get(URL, (r) => {
      if (r.statusCode !== 200) return rej(new Error('HTTP ' + r.statusCode));
      const c = [];
      r.on('data', (d) => c.push(d));
      r.on('end', () => { fs.writeFileSync(DAT, Buffer.concat(c)); res(); });
    }).on('error', rej);
  });
  RULES.clear(); EXCEPT.clear(); load();
  console.log('updated ' + DAT + ': ' + RULES.size + ' rules, ' + EXCEPT.size + ' exceptions');
}

// --- self-test: every case below is one the TLD-CSV version got wrong -----------------------------
function test() {
  const cases = [
    ['kw.com', 'kw.com'], ['locations.kw.com', 'kw.com'], ['www.kw.com', 'kw.com'],
    ['https://www.locations.kw.com/office/1?x=2', 'kw.com'],
    ['bbc.co.uk', 'bbc.co.uk'], ['news.bbc.co.uk', 'bbc.co.uk'], ['a.b.c.bbc.co.uk', 'bbc.co.uk'],
    ['foo.sh.cn', 'foo.sh.cn'], ['sh.cn', ''], ['net.cn', ''], ['foo.net.cn', 'foo.net.cn'],
    ['clayton.k12.ga.us', 'clayton.k12.ga.us'], ['k12.ga.us', ''],
    ['shop.foo.myshopify.com', 'foo.myshopify.com'], ['foo.netlify.app', 'foo.netlify.app'],
    ['0-100.games', '0-100.games'], ['0-8.capital', '0-8.capital'], ['0.email', '0.email'],
    ['0.ventures', '0.ventures'], ['example.com', 'example.com'], ['com', ''],
  ];
  let pass = 0;
  for (const [input, want] of cases) {
    const got = registrable(input);
    const ok = got === want;
    if (ok) pass++;
    console.log('  ' + (ok ? 'PASS' : 'FAIL') + '  ' + input.padEnd(40) + JSON.stringify(got) + (ok ? '' : '   want ' + JSON.stringify(want)));
  }
  // values that are not domains at all must be reported as such
  for (const [input, want] of [['164.53', false], ['0.5', false], ['151.119', false], ['example.com', true], ['foo.sh.cn', true]]) {
    const got = knownSuffix(input);
    const ok = got === want;
    if (ok) pass++;
    console.log('  ' + (ok ? 'PASS' : 'FAIL') + '  knownSuffix(' + input + ') = ' + got + (ok ? '' : '   want ' + want));
  }
  console.log('  ' + pass + '/' + (cases.length + 5));
  if (pass !== cases.length + 5) process.exit(1);
}

module.exports = { registrable, registrableOrHost, knownSuffix, isPrivateSuffix, publicSuffix, normHost, update };

if (require.main === module) {
  (async () => {
    if (process.argv.includes('--update')) await update();
    if (process.argv.includes('--test')) test();
    if (!process.argv.includes('--update') && !process.argv.includes('--test')) {
      const h = process.argv[2];
      if (!h) { console.log('usage: node psl-suffix.js <host> | --test | --update'); process.exit(0); }
      console.log(registrable(h) + '   (suffix=' + publicSuffix(h) + ', known=' + knownSuffix(h) + ', private=' + isPrivateSuffix(h) + ')');
    }
  })().catch((e) => { console.error('ERR', e.message); process.exit(1); });
}
