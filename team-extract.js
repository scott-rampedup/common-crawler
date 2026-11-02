/**
 * team-extract.js — turn ONE team/contact listing page into many person records.
 * ---------------------------------------------------------------------------------------------
 * A BIO URL is one page, one person, so everything on it can be attributed to that person. A team
 * page breaks that: the page is no longer the right scope. The fix is not to attribute less, it is
 * to attribute inside a SMALLER scope — find the per-person region first, then apply the ordinary
 * one-page-one-person logic within it.
 *
 * Measured on 600 pages sampled from the 02-11 export (325 reachable, 290 with >=2 names):
 *   - 64.5% of rosters segment into repeating cards; median 10 cards, median coverage 0.55
 *   - only 16.4% of one-name cards link out to a per-person bio page, so in-card extraction is the
 *     main event and the bio link is a by-product, not the strategy
 *   - 73.4% of rosters carry NO on-domain email at all; names:titles:phones:emails = 6018:4582:1200:592.
 *     Team pages are a NAMES-AND-TITLES source. A record with no email is the normal outcome here,
 *     not a failure, so this never invents one — modelling happens downstream, with the domain's
 *     pattern taken from the contacts index rather than from the page (only 3.8% of pages carry
 *     enough name+email pairs to infer a convention locally).
 *   - 3.1% carry >=2 schema.org Person objects, so structured data is a freebie, not a tier.
 *
 * Binding uses two INDEPENDENT witnesses so a wrong attribution can be detected rather than shipped:
 *   proximity  — the attribute sits inside this person's card
 *   signature  — the email local-part / LinkedIn slug spells this person's name
 * Agreement is high confidence; disagreement DROPS the identifier rather than guessing. The signature
 * witness was validated against a null model: 64.4% of on-domain addresses bind to a name on their
 * own page versus 0.2% against a foreign page's names, with 1.8% ambiguous within a page.
 *
 * No DOM parser — the rest of this codebase works on markup directly and adding one would be a new
 * dependency for every consumer. elementIndex() below is a ~100-line stand-in that gives us the one
 * thing a real parser is needed for: which elements are SIBLINGS.
 */
const ex = require('./extractor');

// Elements that never have a closing tag, so they must not push onto the open-element stack.
const VOID = new Set(['area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta',
  'param', 'source', 'track', 'wbr']);
// Elements whose CONTENT can never hold a person, so the scan jumps over them wholesale.
// Keep this list tiny. Skipping to the next matching close tag is only safe when the tag reliably
// has one: <header> is routinely used INSIDE a card, <form> often wraps the whole page body, and
// inline <svg> is everywhere and frequently malformed. Listing those cost a 1.99MB page all but 608
// of its elements -- one unmatched closer swallowed the entire roster -- and segmentation collapsed
// from 64% of rosters to 33%.
const SKIP_BODY = new Set(['script', 'style', 'noscript', 'svg']);
const MAX_SKIP = 100000;   // never jump further than this on one tag, however far the closer is
// Indexed normally (so their children are still seen) but never eligible to BE a person card.
const NON_CARD = new Set(['nav', 'header', 'footer', 'form', 'select', 'option', 'aside', 'table',
  'script', 'style', 'noscript', 'svg', 'html', 'body']);

const TITLE_SRC = '\\b(ceo|cfo|coo|cto|cmo|president|vice president|vp|svp|evp|director|managing director|' +
  'manager|partner|principal|owner|founder|co-founder|attorney|lawyer|counsel|agent|broker|realtor|' +
  'loan officer|mortgage|advisor|adviser|consultant|specialist|coordinator|supervisor|administrator|' +
  'analyst|engineer|architect|nurse|physician|doctor|dentist|therapist|counselor|counsellor|instructor|' +
  'teacher|professor|associate|assistant|officer|chief|head of|superintendent|pastor|paralegal|' +
  'technician|stylist|trainer|representative|executive|secretary|treasurer|chair|chairman|chairwoman|' +
  // the Maps corpus skews mortgage, insurance and real estate, so these carry real weight here
  'underwriter|processor|originator|appraiser|adjuster|surveyor|inspector|estimator|' +
  'hygienist|practitioner|psychologist|psychiatrist|surgeon|veterinarian|optometrist|pharmacist|' +
  'paramedic|technologist|librarian|custodian|dispatcher|foreman|apprentice|intern|volunteer|' +
  'board member|trustee|clerk|planner|bookkeeper|accountant|controller|recruiter|designer|developer)\\b';
const TITLE_RE = new RegExp(TITLE_SRC, 'i');

const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z]/g, '');

function stripTags(h) {
  return String(h || '')
    .replace(/<(script|style|noscript)[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&#0?39;|&rsquo;|&#8217;/g, "'")
    .replace(/&quot;|&#34;/g, '"').replace(/&[a-z]+;/gi, ' ')
    .replace(/\s+/g, ' ').trim();
}

/**
 * Walk the markup once and return a flat element list with parent/sibling structure.
 * Each entry: {tag, cls, start, end, depth, parent}. `end` is the index just past the close tag,
 * so html.slice(start, end) is the element's full outer markup.
 *
 * Real pages have unclosed <p>/<li> and stray closers. Rather than attempt recovery, a close tag
 * that does not match the top of the stack pops at most a few frames looking for its partner and
 * is otherwise ignored — wrong nesting costs us one bad group, which scoring then rejects.
 */
function elementIndex(html, limit = 60000) {
  const els = [];
  const stack = [];
  const re = /<(\/?)([a-zA-Z][\w-]*)((?:"[^"]*"|'[^']*'|[^>"'])*)(\/?)>/g;
  let m;
  while ((m = re.exec(html)) && els.length < limit) {
    const closing = m[1] === '/';
    const tag = m[2].toLowerCase();
    const attrs = m[3] || '';
    const selfClose = m[4] === '/';
    if (!closing) {
      if (VOID.has(tag) || selfClose) continue;
      const clsM = /class\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))/i.exec(attrs);
      const cls = clsM ? (clsM[2] || clsM[3] || clsM[4] || '') : '';
      const el = { tag, cls: cls.trim().replace(/\s+/g, ' '), start: m.index, end: -1,
        depth: stack.length, parent: stack.length ? stack[stack.length - 1] : -1 };
      els.push(el);
      stack.push(els.length - 1);
      // Skip the body of elements that can never hold a person -- but only over a bounded span, so a
      // missing closer costs us one element rather than the rest of the document.
      if (SKIP_BODY.has(tag)) {
        const close = new RegExp('</' + tag + '\\s*>', 'gi');
        close.lastIndex = re.lastIndex;
        const c = close.exec(html);
        if (c && c.index - m.index <= MAX_SKIP) {
          el.end = c.index + c[0].length; stack.pop(); re.lastIndex = close.lastIndex;
        }
      }
      continue;
    }
    // Look back a few frames for this tag's partner — unclosed <p> and <li> are everywhere. Frames
    // skipped over are closed implicitly at this point. If the partner isn't there the tag is stray,
    // so ignore it rather than unwinding the whole document on one bad closer.
    let found = -1;
    for (let k = stack.length - 1; k >= 0 && k > stack.length - 7; k--) {
      if (els[stack[k]].tag === tag) { found = k; break; }
    }
    if (found < 0) continue;
    for (let k = stack.length - 1; k > found; k--) if (els[stack[k]].end < 0) els[stack[k]].end = m.index;
    els[stack[found]].end = m.index + m[0].length;
    stack.length = found;
  }
  // Anything still open ran to the end of the document.
  for (const i of stack) if (els[i].end < 0) els[i].end = html.length;
  for (const e of els) if (e.end < 0) e.end = html.length;
  return els;
}

// Ranks and honorifics. "Maj Terrance Schmitt" must yield Terrance Schmitt, not "Maj Terrance" --
// the regex would otherwise consume the rank as the given name and the given name as the surname.
const HONORIFIC = new Set(['mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'professor', 'rev', 'fr', 'sr',
  'maj', 'major', 'capt', 'cpt', 'lt', 'sgt', 'col', 'gen', 'cmdr', 'chief', 'officer', 'coach',
  'pastor', 'rabbi', 'imam', 'sister', 'brother', 'judge', 'hon', 'sen', 'rep', 'gov', 'atty',
  'deacon', 'elder', 'master', 'madam', 'sir', 'lord', 'lady', 'saint', 'st']);

// A surname is never a role, a street type or page furniture. The first-name lexicon holds 131,153
// entries -- Gift, Web, Apple, Santa and Harris are all in it -- so without this the regex happily
// reports "Gift Cards", "Web Store" and "Apple Park" as people.
const NOT_SURNAME = new Set(['ave', 'avenue', 'st', 'street', 'rd', 'road', 'blvd', 'boulevard',
  'lane', 'ln', 'drive', 'dr', 'court', 'ct', 'place', 'pl', 'way', 'park', 'plaza', 'square',
  'suite', 'floor', 'building', 'center', 'centre', 'city', 'county', 'state', 'county',
  'store', 'shop', 'cards', 'card', 'wrapping', 'gardener', 'training', 'officer', 'office',
  'services', 'service', 'school', 'college', 'university', 'hospital', 'clinic', 'church',
  'team', 'staff', 'board', 'members', 'member', 'directors', 'director', 'manager', 'management',
  'news', 'events', 'event', 'hours', 'policy', 'privacy', 'terms', 'contact', 'about', 'home',
  'login', 'search', 'menu', 'more', 'info', 'faq', 'blog', 'press', 'media', 'careers', 'jobs',
  'reviews', 'review', 'gallery', 'photos', 'video', 'videos', 'resources', 'products', 'product']);

/** Every person-looking name in a block of plain text, deduped, in document order. */
function namesIn(txt, firsts) {
  const out = new Map();
  const re = /\b([A-Z][a-z]{1,14})\.?\s+(?:([A-Z])\.?\s+)?([A-Z][a-zA-Z'’-]{1,20})\b/g;
  let m;
  while ((m = re.exec(txt))) {
    const first = m[1], mid = m[2] || '', last = m[3];
    // A rank in the first slot: rewind to just past it so the real name is read on the next pass.
    if (HONORIFIC.has(first.toLowerCase())) { re.lastIndex = m.index + m[1].length; continue; }
    if (!firsts.has(first.toLowerCase())) continue;
    const f = norm(first), l = norm(last);
    if (!f || !l || l.length < 2) continue;
    if (NOT_SURNAME.has(l) || HONORIFIC.has(l)) continue;
    const key = f + '|' + l;
    if (!out.has(key)) out.set(key, { first, last, middle: mid, at: m.index });
  }
  return [...out.values()];
}

// Candidate local-parts/slugs for a name, in a fixed order so the index names the convention.
function localCandidates(f, l) {
  f = norm(f); l = norm(l);
  if (!f || !l) return [];
  const fi = f[0], li = l[0];
  return [`${f}.${l}`, `${f}_${l}`, `${f}-${l}`, `${f}${l}`, `${fi}${l}`, `${fi}.${l}`, `${fi}-${l}`,
    `${f}${li}`, `${f}.${li}`, `${l}${fi}`, `${l}.${f}`, `${l}${f}`, `${l}-${f}`, f, l];
}
const PATTERNS = ['first.last', 'first_last', 'first-last', 'firstlast', 'flast', 'f.last', 'f-last',
  'firstl', 'first.l', 'lastf', 'last.first', 'lastfirst', 'last-first', 'first', 'last'];

/** Which people on this page could own this local-part, and under which convention. */
function signatureOwners(local, people) {
  const clean = String(local || '').toLowerCase().split('+')[0].replace(/[^a-z0-9._-]/g, '');
  const hits = [];
  for (const p of people) {
    const i = localCandidates(p.first, p.last).indexOf(clean);
    if (i > -1) hits.push({ person: p, pattern: PATTERNS[i] });
  }
  return hits;
}

function linkedinOwners(slug, people) {
  const s = String(slug || '').toLowerCase().replace(/-?[0-9a-f]{6,}$/, '').replace(/-\d+$/, '');
  const hits = [];
  for (const p of people) {
    const f = norm(p.first), l = norm(p.last);
    if (!f || !l) continue;
    if (s === `${f}-${l}` || s === `${f}${l}` || s.startsWith(`${f}-${l}-`)) hits.push({ person: p });
  }
  return hits;
}

/** Person objects the page declares outright. Rare (3.1%) but free and exact. */
function jsonLdPeople(html) {
  const out = [];
  const re = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html))) {
    let j; try { j = JSON.parse(m[1].trim()); } catch (e) { continue; }
    const seen = new Set();
    const walk = (o) => {
      if (!o || typeof o !== 'object' || seen.has(o)) return;
      seen.add(o);
      if (Array.isArray(o)) { o.forEach(walk); return; }
      const t = Array.isArray(o['@type']) ? o['@type'].join(',') : String(o['@type'] || '');
      if (/\b(Person|RealEstateAgent|Attorney|Physician)\b/i.test(t) && (o.name || o.givenName)) {
        const name = String(o.name || `${o.givenName || ''} ${o.familyName || ''}`).trim();
        const parts = name.split(/\s+/).filter(Boolean);
        if (parts.length >= 2) {
          out.push({
            first: o.givenName || parts[0],
            last: o.familyName || parts[parts.length - 1],
            title: String(o.jobTitle || '').trim(),
            email: String(o.email || '').replace(/^mailto:/i, '').trim().toLowerCase(),
            phone: String(o.telephone || '').trim(),
            linkedin: [].concat(o.sameAs || []).find((u) => /linkedin\.com\/in\//i.test(u)) || '',
            bio_url: String(o.url || '').trim(),
          });
        }
      }
      for (const k of ['@graph', 'employee', 'member', 'founder', 'employees', 'itemListElement', 'item'])
        if (o[k]) walk(o[k]);
    };
    walk(j);
  }
  return out;
}

function hrefsIn(frag) {
  return [...String(frag).matchAll(/<a[^>]+href\s*=\s*["']([^"']+)["']/gi)].map((x) => x[1]);
}

/**
 * Does this link's LAST path segment read as a person's name? pathIdFromUrl alone is far too loose
 * on a listing page — it matches /team/ and /about/, i.e. every nav link. Measured: swapping to this
 * took a representative page from 69 "bio links" to 3.
 */
function bioLinkFrom(href, pageUrl, host, firsts) {
  if (/^(mailto:|tel:|#|javascript:)/i.test(href)) return '';
  if (/linkedin|facebook|twitter|instagram|youtube|\.(jpe?g|png|gif|webp|pdf|svg)(\?|$)/i.test(href)) return '';
  let abs; try { abs = new URL(href, pageUrl).toString().split('#')[0]; } catch (e) { return ''; }
  let u; try { u = new URL(abs); } catch (e) { return ''; }
  if (u.hostname.replace(/^www\./, '') !== host) return '';
  if (abs.replace(/\/$/, '') === String(pageUrl).split('#')[0].replace(/\/$/, '')) return '';
  const segs = u.pathname.split('/').filter(Boolean);
  if (!segs.length) return '';
  const leaf = segs[segs.length - 1].replace(/\.(html?|php|aspx?)$/i, '');
  const toks = leaf.split(/[-_.]+/).filter((t) => /^[A-Za-z]{2,}$/.test(t));
  if (toks.length < 2 || toks.length > 4) return '';
  return (firsts.has(toks[0].toLowerCase()) || firsts.has(toks[1].toLowerCase())) ? abs : '';
}

function emailsIn(frag) {
  const out = new Set();
  for (const m of String(frag).matchAll(/mailto:([^"'?>\s]+)/gi)) {
    try { out.add(decodeURIComponent(m[1]).toLowerCase()); } catch (e) { out.add(m[1].toLowerCase()); }
  }
  for (const m of stripTags(frag).matchAll(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g)) {
    out.add(m[0].toLowerCase());
  }
  return [...out];
}

function phonesIn(frag) {
  const out = new Set();
  for (const m of String(frag).matchAll(/tel:([^"'?>\s]+)/gi)) out.add(decodeURIComponent(m[1]).trim());
  const txt = stripTags(frag);
  for (const m of txt.matchAll(/(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{2,4}\)|\d{2,4})[\s.-]?\d{3,4}[\s.-]?\d{3,4}/g)) {
    const d = m[0].replace(/\D/g, '');
    if (d.length >= 9 && d.length <= 15) out.add(m[0].trim());
  }
  return [...out];
}

/**
 * Like stripTags but keeps a boundary where the markup had one. Plain stripping welds a card's
 * fields together -- "<p>Managing Partner</p><a>email</a>" becomes "Managing Partner email" -- and
 * the title then swallows its neighbours. Only used for TITLE text: name detection still runs on the
 * welded text, because <span>Jane</span> <span>Smith</span> is a real pattern and a boundary there
 * would hide the name.
 */
function blockText(h) {
  return stripTags(String(h || '').replace(
    /<\/?(p|div|li|ul|ol|h[1-6]|br|tr|td|th|section|article|span|a|strong|em|b|i|small|label|address)\b[^>]*>/gi,
    ' | '));
}

function titleIn(txt, name) {
  if (!txt) return '';
  // Prefer a title that FOLLOWS the name — "Jane Smith, Managing Partner" — then anything on the card.
  const after = name ? txt.slice(txt.indexOf(name) + name.length) : txt;
  for (const chunk of [after, txt]) {
    for (const piece of String(chunk).split(/[|•\n\r]|(?:\s[-–—]\s)|,|\.\s/)) {
      const s = piece.trim();
      if (s.length < 2 || s.length > 80) continue;
      if (TITLE_RE.test(s)) return s.replace(/\s+/g, ' ');
    }
  }
  return '';
}

/**
 * Candidate card groups: >=3 SIBLING elements sharing tag + class, each holding about one person.
 * Scored on people covered rather than on ratio — picking by ratio alone prefers a tidy 3-card match
 * over a 40-card one that actually segments the roster.
 */
function findCardGroup(html, els, people, firsts) {
  if (people.length < 2) return null;
  const groups = new Map();
  for (let i = 0; i < els.length; i++) {
    const e = els[i];
    if (NON_CARD.has(e.tag) || e.end - e.start < 40) continue;
    // Two keys per element. Cards are often siblings with DIFFERING class attributes -- a trailing
    // "first"/"last"/"col-3" varies by position -- so an exact-class key alone misses them, while a
    // tag-only key alone is too loose. Offer both and let the scoring gate choose.
    for (const key of [e.parent + '\u0000' + e.tag + '\u0000' + e.cls, e.parent + '\u0000' + e.tag]) {
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(i);
    }
  }
  let best = null;
  for (const [, idxs] of groups) {
    if (idxs.length < 3 || idxs.length > 400) continue;
    let one = 0, withName = 0;
    const members = [];
    for (const i of idxs) {
      const frag = html.slice(els[i].start, els[i].end);
      if (frag.length > 60000) { members.push(null); continue; }
      const ns = namesIn(stripTags(frag), firsts);
      if (ns.length >= 1) withName++;
      if (ns.length === 1) one++;
      members.push({ idx: i, frag, names: ns });
    }
    if (withName < 3) continue;
    const score = one / idxs.length;
    if (score < 0.4) continue;
    if (!best || one > best.oneName) {
      best = { members: members.filter(Boolean), cards: idxs.length, oneName: one,
        score: Number(score.toFixed(3)), coverage: Number((one / people.length).toFixed(3)) };
    }
  }
  return best;
}

/**
 * Fallback for flat markup — a table or a run of <p> with no repeating structure. Cut the plain text
 * at name boundaries: everything between name[i] and name[i+1] belongs to name[i], and anything
 * before the first name belongs to nobody. Weaker than cards (no markup to bound a person), so the
 * records it makes are capped at 'low' confidence.
 */
function windowCards(txt, people) {
  const out = [];
  for (let i = 0; i < people.length; i++) {
    const start = people[i].at;
    const end = i + 1 < people.length ? people[i + 1].at : Math.min(txt.length, start + 400);
    out.push({ frag: txt.slice(start, end), names: [people[i]] });
  }
  return out;
}

/**
 * Identifiers that belong to the OFFICE, not a person. A templated roster repeats the same switchboard
 * number and info@ in every card; binding those gives every person the same confident-looking wrong
 * contact detail. Anything appearing in more than one card, or classified Role-Based, is org-level.
 */
function sharedValues(cards, pick) {
  const count = new Map();
  for (const c of cards) for (const v of new Set(pick(c))) count.set(v, (count.get(v) || 0) + 1);
  return new Set([...count].filter(([, n]) => n > 1).map(([v]) => v));
}

/**
 * extractPeople(html, pageUrl, {firsts}) -> {people, meta}
 * Never invents an identifier: an address is only ever SELECTED from those present on the page.
 */
function extractPeople(html, pageUrl, opts = {}) {
  const firsts = opts.firsts || new Set();
  const requireCorroboration = opts.requireCorroboration !== false;
  html = String(html || '');
  const host = (() => { try { return new URL(pageUrl).hostname.replace(/^www\./, ''); } catch (e) { return ''; } })();
  const bare = host.split('.')[0] || '';
  const txt = stripTags(html);
  const people = namesIn(txt, firsts);
  const meta = { url: pageUrl, host, names: people.length, method: 'none', cards: 0,
    coverage: 0, segScore: 0, orgEmails: [], orgPhones: [], conflicts: 0, uncorroborated: 0 };
  if (!people.length) return { people: [], meta };

  // Rung 1: the page states its people outright. Rare, exact, no inference.
  const ld = jsonLdPeople(html);
  if (ld.length >= 2) {
    meta.method = 'jsonld';
    // One person routinely appears in several graph nodes -- as @graph member, as author, as founder --
    // and each node is a separate object. Keep the richest instance per name, not one per node.
    const byName = new Map();
    for (const p of ld) {
      const k = norm(p.first) + '|' + norm(p.last);
      if (!k.replace('|', '')) continue;
      const prev = byName.get(k);
      const score = (q) => (q.title ? 2 : 0) + (q.email ? 2 : 0) + (q.phone ? 1 : 0) + (q.linkedin ? 1 : 0) + (q.bio_url ? 1 : 0);
      if (!prev || score(p) > score(prev)) byName.set(k, p);
    }
    const uniq = [...byName.values()];
    meta.cards = uniq.length;
    meta.coverage = 1;
    return {
      people: uniq.map((p) => ({
        first: p.first, last: p.last, title: p.title, email: p.email, phone: p.phone,
        linkedin_url: p.linkedin, bio_url: p.bio_url, domain: host, source_url: pageUrl,
        binding: 'jsonld', confidence: 'high', witnesses: ['declared'], email_status: p.email ? 'observed' : '',
      })),
      meta,
    };
  }

  // Rung 2: repeating sibling cards. Rung 3: name-anchored text windows.
  const els = elementIndex(html);
  const group = findCardGroup(html, els, people, firsts);
  let cards, method;
  if (group) {
    cards = group.members.filter((c) => c.names.length === 1);
    method = 'card';
    meta.cards = group.cards; meta.coverage = group.coverage; meta.segScore = group.score;
  } else {
    cards = windowCards(txt, people);
    method = 'window';
    meta.cards = cards.length; meta.coverage = 1;
  }
  meta.method = method;

  const sharedEmail = sharedValues(cards, (c) => emailsIn(c.frag));
  const sharedPhone = sharedValues(cards, (c) => phonesIn(c.frag));
  // An address on several cards is usually the office's -- but if it spells exactly one person on the
  // page it is theirs, and the repeat is a "contact Jane" link or a mis-cut card. Signature outranks
  // the shared test in that one case; everything else stays org-level.
  const rescued = new Set([...sharedEmail].filter((e) => signatureOwners(e.split('@')[0], people).length === 1));
  meta.orgEmails = [...sharedEmail].filter((e) => !rescued.has(e));
  meta.orgPhones = [...sharedPhone];

  const out = [];
  for (const c of cards) {
    const person = c.names[0];
    if (!person) continue;
    const frag = c.frag;
    const ctext = method === 'card' ? blockText(frag) : frag;
    const full = `${person.first} ${person.last}`;

    // --- email: proximity (in this card) x signature (spells this person) ---
    const cardEmails = emailsIn(frag);
    const onDomain = cardEmails.filter((e) => (e.split('@')[1] || '').includes(bare));
    const usable = (onDomain.length ? onDomain : cardEmails)
      .filter((e) => ex.classifyEmail(e) !== 'Role-Based');
    let email = '', emailWitness = [], conflict = false, pattern = '';
    for (const e of usable) {
      const owners = signatureOwners(e.split('@')[0], people);
      const mine = owners.some((o) => norm(o.person.first) === norm(person.first) && norm(o.person.last) === norm(person.last));
      if (mine && owners.length === 1) { email = e; emailWitness = ['proximity', 'signature']; pattern = owners[0].pattern; break; }
      if (sharedEmail.has(e)) continue;        // on other cards too and it does not spell this person
      if (!owners.length) { if (!email) { email = e; emailWitness = ['proximity']; } continue; }
      // The address is on this person's card but spells somebody ELSE on the page. Two witnesses
      // disagreeing is exactly the case we built them for: drop it rather than pick a side.
      conflict = true;
    }
    if (conflict && emailWitness.length < 2) { email = ''; emailWitness = []; meta.conflicts++; }

    // --- linkedin: same two witnesses ---
    let linkedin = '';
    for (const m of frag.matchAll(/linkedin\.com\/in\/([A-Za-z0-9\-_%]+)/gi)) {
      const owners = linkedinOwners(m[1], people);
      const mine = owners.some((o) => norm(o.person.first) === norm(person.first) && norm(o.person.last) === norm(person.last));
      if (mine || !owners.length) { linkedin = 'https://www.linkedin.com/in/' + m[1]; if (mine) break; }
    }

    const phone = phonesIn(frag).filter((p) => !sharedPhone.has(p))[0] || '';
    const bio = hrefsIn(frag).map((h) => bioLinkFrom(h, pageUrl, host, firsts)).find(Boolean) || '';
    const title = titleIn(ctext, full);

    const witnesses = emailWitness.slice();
    if (bio) witnesses.push('bio-link');
    const confidence = method === 'window' ? 'low'
      : (emailWitness.length === 2 ? 'high' : (title || bio ? 'medium' : 'low'));

    // A bare name with nothing attached cannot be told apart from page furniture that happens to
    // read as two capitalised words. Require ONE corroborating signal -- a title, an identifier or
    // a bio link -- before emitting. This is the line between a roster entry and "Gift Cards".
    if (requireCorroboration && !(title || email || phone || linkedin || bio)) { meta.uncorroborated++; continue; }
    out.push({
      first: person.first, last: person.last, title,
      email, phone, linkedin_url: linkedin, bio_url: bio,
      domain: host, source_url: pageUrl,
      binding: method, confidence, witnesses,
      email_status: email ? 'observed' : '', email_pattern: pattern,
    });
  }
  return { people: out, meta };
}

module.exports = { extractPeople, elementIndex, namesIn, signatureOwners, linkedinOwners,
  bioLinkFrom, stripTags, jsonLdPeople, PATTERNS };
