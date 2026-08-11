/**
 * bio-signals.js — the derivations that squeeze more Gender, Name and Position out of a bio page than the
 * names map alone can. All pure and offline-testable; every function returns "" / null rather than a guess.
 *
 *   genderFromDescription(desc)   pronouns in the person's own bio prose  -> M | F | ''
 *   nicknameRoot(first)           Bob -> robert, so the names map can answer
 *   genderOfName(first, map)      map lookup, then the nickname root
 *   nameFromDescription(desc)     "Jane Smith is an experienced..." -> {first,last}
 *
 * WHY THESE: Description is populated on ~100% of contacts while Gender sits at 54% and Position at 40%.
 * The prose is the richest unmined field in the record — it names the person, states their role, and is
 * saturated with pronouns. Gender in particular gates email modelling, so recovering it directly converts
 * into modelled addresses that would otherwise never be attempted.
 */
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------- nicknames
let _nick = null;
function nicknameMap() {
  if (_nick) return _nick;
  _nick = Object.create(null);                     // never a plain {} — see loadGenderMap's prototype bug
  try {
    const txt = fs.readFileSync(path.join(__dirname, 'nicknames.csv'), 'utf8');
    for (const line of txt.split(/\r?\n/)) {
      const [n, f] = line.split(',');
      const nick = String(n || '').trim().toLowerCase(), formal = String(f || '').trim().toLowerCase();
      if (!nick || !formal || nick === 'nickname') continue;
      _nick[nick] = formal;
    }
  } catch (e) { /* file absent -> nicknames simply never resolve */ }
  return _nick;
}
// "Bob" -> "robert". Returns '' when the name isn't a known diminutive.
function nicknameRoot(first) {
  const f = String(first || '').trim().toLowerCase();
  if (!f) return '';
  return nicknameMap()[f] || '';
}
// Gender for a first name: the map, then the name's formal root. Deliberately NOT fuzzy beyond that.
function genderOfName(first, genderMap) {
  const f = String(first || '').trim().toLowerCase();
  if (!f || !genderMap) return '';
  const direct = genderMap[f];
  if (typeof direct === 'string' && direct) return direct;
  const root = nicknameRoot(f);
  if (root) { const g = genderMap[root]; if (typeof g === 'string' && g) return g; }
  return '';
}

// ---------------------------------------------------------------- gender from prose
// Standalone pronoun/honorific tokens. Word-boundary matched so "she" never matches inside "Shelby",
// and possessives are counted because bios lean on them ("...his clients", "...her practice").
const F_TOKENS = /\b(she|her|hers|herself|ms|mrs|miss)\b/gi;
const M_TOKENS = /\b(he|him|his|himself|mr)\b/gi;
const countMatches = (re, s) => { const m = String(s || '').match(re); return m ? m.length : 0; };

/**
 * Gender from the person's own bio prose. A bio is written about ONE person, so a decisive pronoun
 * majority is strong evidence — and it works exactly where the names map fails (non-Western and rare
 * names). Returns '' unless the margin is decisive, because team-page blurbs sometimes mention a
 * colleague, a spouse or a client of the other gender.
 */
function genderFromDescription(description, opts = {}) {
  const minLead = Math.max(1, Number(opts.minLead) || 2);      // winner must exceed loser by this many
  const text = String(description || '');
  if (text.length < 12) return '';
  const f = countMatches(F_TOKENS, text), m = countMatches(M_TOKENS, text);
  if (f === 0 && m === 0) return '';
  // UNCONTESTED evidence is decisive on its own: a bio saying "She specializes in…" once, with no male
  // pronoun anywhere, is not ambiguous. Most real bios carry exactly one pronoun, so demanding a margin
  // here would throw away the majority of the signal. A margin is only required when BOTH appear —
  // that's the case that means a colleague, spouse or client got mentioned.
  if (m === 0) return 'F';
  if (f === 0) return 'M';
  if (f >= m + minLead) return 'F';
  if (m >= f + minLead) return 'M';
  return '';                                                   // contested -> say nothing
}

// ---------------------------------------------------------------- name from prose
// Bio prose overwhelmingly opens "<Full Name> is/has/was/joined/serves/brings/specializes...". Taking the
// capitalized run BEFORE that verb is the last resort in the name waterfall — it only runs when the page,
// the vCard, the URL slug, the email and the LinkedIn slug have all produced nothing.
const LEAD_VERB = /\b(is|was|has|have|had|joined|joins|serves|serve|brings|brought|specializes|specialises|works|worked|began|started|leads|leverages|holds|earned|received|graduated|founded|co-founded|owns|represents|helps|provides|offers|focuses|enjoys|lives|grew|comes|returns|became|currently|proudly|first)\b/i;
const NAME_STOP = new Set(['the', 'a', 'an', 'our', 'we', 'you', 'your', 'this', 'that', 'as', 'at', 'in', 'on', 'of', 'for', 'with',
  'welcome', 'meet', 'about', 'contact', 'team', 'staff', 'office', 'company', 'agency', 'group', 'llc', 'inc',
  'mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'rev', 'hon', 'sir', 'attorney', 'agent', 'realtor', 'broker']);
const NAME_TOKEN = /^[A-Z][a-zà-ÿ'’-]{1,23}$/;                 // one capitalized word, sane length
const properCase = (s) => String(s || '').replace(/\S+/g, (w) => w[0].toUpperCase() + w.slice(1).toLowerCase());

/**
 * "Zhanjing Lin is an experienced mortgage lender located in Bayside, NY..." -> {first:'Zhanjing', last:'Lin'}
 * Returns null unless the opening reads unambiguously as a person's name followed by a biographical verb.
 */
function nameFromDescription(description) {
  const text = String(description || '').trim();
  if (!text) return null;
  // Only the opening clause is trustworthy; a name deeper in the prose is usually someone else.
  // Drop the period from honorifics and suffixes FIRST — otherwise "Dr. Jane Smith is…" splits after
  // "Dr" and the clause we examine is just the honorific.
  const deAbbrev = text.replace(/\b(Dr|Mr|Mrs|Ms|Prof|Rev|Hon|Sr|Jr|St|Fr|Lt|Col|Gen|Capt|Sgt)\./gi, '$1');
  const head = deAbbrev.split(/[.!?;:(]/)[0].trim();
  if (!head) return null;
  const words = head.split(/\s+/);
  const lead = [];
  let i = 0;                                                    // words CONSUMED, which is not lead.length:
  for (; i < words.length; i++) {                               // a skipped honorific advances i but not lead
    const bare = words[i].replace(/^[^A-Za-zà-ÿ]+|[^A-Za-zà-ÿ'’-]+$/g, '');
    if (!bare) break;
    if (LEAD_VERB.test(bare)) break;                            // the verb ends the name run
    if (!NAME_TOKEN.test(bare)) break;                          // lowercase or punctuation -> not a name
    if (NAME_STOP.has(bare.toLowerCase())) { if (lead.length) break; else continue; }  // skip a leading honorific
    lead.push(bare);
    if (lead.length >= 4) { i++; break; }
  }
  if (lead.length < 2) return null;
  // Require the run to actually be FOLLOWED by a biographical verb — otherwise this is a headline, not a bio.
  const after = words.slice(i).find((w) => /[A-Za-z]/.test(w)) || '';
  if (!LEAD_VERB.test(after.replace(/[^A-Za-z-]/g, ''))) return null;
  return { first: properCase(lead[0]), last: properCase(lead[lead.length - 1]) };
}

module.exports = { genderFromDescription, nicknameRoot, genderOfName, nameFromDescription, nicknameMap };

// ---------------------------------------------------------------------------------------------
if (require.main === module && process.argv.includes('--selftest')) {
  let p = 0, f = 0;
  const ok = (n, c, d = '') => { if (c) { p++; console.log('  PASS ', n, d ? '— ' + d : ''); } else { f++; console.log('  FAIL ', n, d ? '— ' + d : ''); } };
  const ex = require('./extractor');
  const gm = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));

  console.log('nicknames loaded:', Object.keys(nicknameMap()).length, '\n');

  ok('nickname resolves to formal root', nicknameRoot('Bob') === 'robert', nicknameRoot('Bob'));
  ok('unknown name has no root', nicknameRoot('Zephyrine') === '');
  ok('gender via nickname root (Bob -> Robert -> M)', genderOfName('Bob', gm) === 'M');
  ok('gender via nickname root (Liz -> Elizabeth -> F)', genderOfName('Liz', gm) === 'F');
  ok('direct map hit still works', genderOfName('Michael', gm) === 'M');
  ok('unknown stays blank', genderOfName('Zephyrine', gm) === '');
  ok('prototype keys cannot leak a function', genderOfName('constructor', gm) === '');

  // --- gender from prose ---
  ok('female bio', genderFromDescription('Jane is an experienced advisor. She helps her clients plan for retirement.') === 'F');
  ok('male bio', genderFromDescription('Ahmed joined the firm in 2010. He advises his clients on tax matters.') === 'M');
  ok('real CMG-style bio', genderFromDescription('Zhanjing Lin is an experienced mortgage lender located in Bayside, NY. She specializes in new purchase and refinance needs.') === 'F');
  ok('no pronouns -> blank', genderFromDescription('Experienced mortgage lender serving the Bayside area.') === '');
  ok('contested -> blank', genderFromDescription('She works with him and his team on her accounts.') === '');
  ok('does not match inside a word (Shelby/Hesper)', genderFromDescription('Shelby Hesper founded the practice in Thermopolis.') === '');
  ok('honorific counts', genderFromDescription('Mr. Smith brings 20 years of experience to the team.') === 'M');

  // --- name from prose ---
  const n1 = nameFromDescription('Zhanjing Lin is an experienced mortgage lender located in Bayside, NY that specializes in refinance.');
  ok('name from a real bio opening', n1 && n1.first === 'Zhanjing' && n1.last === 'Lin', JSON.stringify(n1));
  const n2 = nameFromDescription('Dr. Jane Marie Smith has served the community for 20 years.');
  ok('honorific skipped, first+last taken', n2 && n2.first === 'Jane' && n2.last === 'Smith', JSON.stringify(n2));
  ok('no biographical verb -> null', nameFromDescription('Premier Mortgage Lending Solutions For You') === null);
  ok('single leading word -> null', nameFromDescription('Welcome is our motto') === null);
  ok('lowercase opening -> null', nameFromDescription('our team is here to help you today') === null);
  ok('empty -> null', nameFromDescription('') === null);
  const n3 = nameFromDescription('Maria Homs joined the firm in 2019 and leads the tax practice.');
  ok('joined-style opening', n3 && n3.first === 'Maria' && n3.last === 'Homs', JSON.stringify(n3));

  console.log(`\nbio-signals self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
}
