/**
 * cc-news-spike.js — measure the daily new-hire signal in Common Crawl's NEWS dataset (CC-NEWS).
 *
 * CC-NEWS is crawled DAILY (~12-30 WARCs/day, ~1 GiB each) but — unlike the CC-MAIN crawls — it has NO
 * URL index: not in the columnar table (cc-index/table/cc-main), not in the CDX server. So there is no
 * Athena "find URLs matching X" step; you stream the WARCs and scan. This spike does exactly that for one
 * day, runs hire/appointment extraction over the article text, and reports how many CLEAN
 * (person, title, company) triples a day of news actually yields — the number that decides whether a real
 * pipeline is worth building.
 *
 *   node cc-news-spike.js [--date 20260809] [--files N] [--conc 4] [--range-mb N] [--out hits.jsonl]
 *                         [--resolve] [--verbose] [--dump-sentences N]
 *
 *   --date            day to scan (YYYYMMDD, default: yesterday UTC)
 *   --files           cap WARCs scanned (default: all for that day) — use 1 for a smoke test
 *   --conc            WARCs streamed in parallel (default 4)
 *   --range-mb        read only the first N MB of each WARC (cheap partial sample; results are a floor)
 *   --resolve         look each company up in the companies index -> domain (needs OPENSEARCH_ENDPOINT)
 *   --verbose         print every validated hit as it is found
 *   --dump-sentences  print N hire-verb sentences verbatim (tuning aid, no extraction)
 *
 * Reads over HTTPS from data.commoncrawl.org (free, no AWS creds). Inside us-east-1 you'd swap in
 * s3://commoncrawl for speed — same bytes.
 *
 * NOTE ON DATES: CC-NEWS crawls news SITES daily, but a crawled page can be an old archive page (we see
 * 2012 press releases in today's WARCs). So the spike reads each article's published date and reports
 * fresh hits separately — that, not the raw hit count, is the new-hire feed rate.
 */
const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');
const { loadGenderMap } = require('./extractor');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);
const HOST = 'data.commoncrawl.org';

// ---------------------------------------------------------------- fetch helpers
function httpsGet(urlStr, { rangeBytes = 0, stream = false } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const headers = rangeBytes ? { Range: `bytes=0-${rangeBytes - 1}` } : {};
    const req = https.get({ hostname: u.hostname, path: u.pathname + u.search, headers, timeout: 120000 }, (res) => {
      if (res.statusCode !== 200 && res.statusCode !== 206) { res.resume(); return reject(new Error(`${urlStr} -> ${res.statusCode}`)); }
      if (stream) return resolve(res);
      const chunks = [];
      res.on('data', (d) => chunks.push(d));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(new Error('timeout')); });
  });
}
async function withRetry(fn, tries = 3, label = '') {
  let last;
  for (let i = 0; i < tries; i++) {
    try { return await fn(); } catch (e) { last = e; if (i < tries - 1) await new Promise((r) => setTimeout(r, 2000 * (i + 1))); }
  }
  throw new Error(`${label}: ${last && last.message}`);
}

// ---------------------------------------------------------------- WARC record streaming
// A .warc.gz is a concatenation of per-record gzip members; Node's streaming gunzip walks all of them.
// We parse records incrementally and NEVER buffer a body we don't want (a news WARC is ~4 GB decompressed).
function streamWarc(res, { onRecord, ranged }) {
  return new Promise((resolve, reject) => {
    const gunzip = zlib.createGunzip();
    let head = Buffer.alloc(0);      // header-scan buffer (small)
    let state = 'headers';
    let need = 0, got = 0, keep = false, hdrs = null, parts = [];

    const finishBody = () => {
      if (keep) { try { onRecord(hdrs, Buffer.concat(parts)); } catch (e) { /* per-record */ } }
      parts = []; keep = false; hdrs = null; state = 'headers'; got = 0; need = 0;
    };
    const parseHeaders = (txt) => {
      const out = {};
      for (const line of txt.split('\r\n')) {
        const i = line.indexOf(':');
        if (i > 0) out[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim();
      }
      return out;
    };

    const pump = (chunk) => {
      let buf = chunk;
      for (;;) {
        if (state === 'headers') {
          head = head.length ? Buffer.concat([head, buf]) : buf;
          buf = Buffer.alloc(0);
          const i = head.indexOf('\r\n\r\n');
          if (i < 0) { if (head.length > 1 << 20) head = head.slice(-4); return; }   // guard runaway
          hdrs = parseHeaders(head.slice(0, i).toString('latin1'));
          need = Number(hdrs['content-length'] || 0);
          keep = (hdrs['warc-type'] === 'response');    // only response records carry a page
          got = 0;
          buf = head.slice(i + 4);
          head = Buffer.alloc(0);
          state = 'body';
          if (!need) { finishBody(); if (!buf.length) return; continue; }
        }
        const room = need - got;                        // state === 'body'
        if (buf.length < room) { if (keep) parts.push(buf); got += buf.length; return; }
        if (keep) parts.push(buf.slice(0, room));
        const rest = buf.slice(room);
        finishBody();
        if (!rest.length) return;
        buf = rest;
      }
    };

    gunzip.on('data', pump);
    gunzip.on('end', () => resolve());
    gunzip.on('error', (e) => {
      if (ranged && /unexpected end|incorrect header|invalid/i.test(e.message)) return resolve();  // --range-mb truncation
      reject(e);
    });
    res.on('error', reject);
    res.pipe(gunzip);
  });
}

// ---------------------------------------------------------------- HTML -> text
const ENT = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ', rsquo: '’', lsquo: '‘', ldquo: '“', rdquo: '”', mdash: '—', ndash: '–', hellip: '…' };
function decodeEntities(s) {
  return s.replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z]+);/g, (m, g) => {
    if (g[0] === '#') { const n = g[1] === 'x' || g[1] === 'X' ? parseInt(g.slice(2), 16) : parseInt(g.slice(1), 10); return Number.isFinite(n) ? String.fromCodePoint(n) : m; }
    return ENT[g.toLowerCase()] !== undefined ? ENT[g.toLowerCase()] : m;
  });
}
function htmlToText(html) {
  return decodeEntities(String(html)
    .replace(/<(script|style|noscript|svg)\b[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<\/(p|div|li|h[1-6]|section|article)>/gi, '. ')
    .replace(/<br\s*\/?>/gi, '. ')
    .replace(/<[^>]*>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim();
}
function httpBody(block) {
  const i = block.indexOf('\r\n\r\n');
  if (i < 0) return { headers: '', body: '' };
  const headers = block.slice(0, i).toString('latin1');
  let body = block.slice(i + 4);
  if (body.length > 2 && body[0] === 0x1f && body[1] === 0x8b) { try { body = zlib.gunzipSync(body); } catch (e) { /* leave as-is */ } }
  return { headers, body: body.toString('utf8') };
}
// Published date from meta tags / JSON-LD — the crawl date is NOT the article date.
function articleDate(html) {
  const head = html.slice(0, 40000);
  const m = head.match(/<meta[^>]+(?:property|name)\s*=\s*["'](?:article:published_time|datePublished|publish[-_]?date|pubdate|date)["'][^>]+content\s*=\s*["']([^"']{4,40})["']/i)
    || head.match(/<meta[^>]+content\s*=\s*["']([^"']{4,40})["'][^>]+(?:property|name)\s*=\s*["'](?:article:published_time|datePublished)["']/i)
    || head.match(/"datePublished"\s*:\s*"([^"]{4,40})"/i)
    || head.match(/<time[^>]+datetime\s*=\s*["'](\d{4}-\d{2}-\d{2}[^"']*)["']/i);
  if (!m) return '';
  const d = String(m[1]).match(/\d{4}-\d{2}-\d{2}/);
  return d ? d[0] : '';
}

// ---------------------------------------------------------------- hire-announcement extraction
// Sentence-level, not one monolithic regex: real announcements vary far too much in shape
// ("X has joined Y as Z", "Y announced today that X has been appointed Z", "Y welcomes X as Z").
// Per candidate sentence we independently find a PERSON (validated against the 131k first-name lexicon),
// a TITLE (position dictionary, longest n-gram first, with a head-word fallback) and a COMPANY.
const HIRE_VERB = /\b(join(?:s|ed|ing)?|appoint(?:ed|s|ment)?|named?|names|hire[ds]?|welcom(?:es|ed)|promot(?:ed|ion)|elevated|succeeds|takes over as|steps into)\b/i;
const NAME_RE = /\b([A-Z][a-z'’\-]{1,20}(?:\s+(?:[A-Z]\.|[A-Z][a-z'’\-]{1,20})){1,3})\b/g;
const ORG_RE = /\b([A-Z][\w&'’.\-]*(?:\s+(?:of|and|the|for|&|[A-Z][\w&'’.\-]*)){0,5})\b/g;
const CORP = /\b(Inc|LLC|Ltd|LLP|PLC|Corp|Corporation|Company|Co|Group|Partners|Holdings|Capital|Bank|Associates|Realty|Insurance|Health|Hospital|Law|Advisors|Advisers|Solutions|Systems|Technologies|Ventures|Trust|Foundation|University|Institute|Agency|Media|Financial|Properties|Consulting|Enterprises|Industries|Services)\b/;
const TITLE_HEAD = /\b((?:Senior|Executive|Deputy|Assistant|Associate|Global|Regional|Interim|Acting|Chief|Managing|Vice|Group|National|General)\s+)*(President|Director|Officer|Manager|Partner|Counsel|Head|Chairman|Chairwoman|Chair|CEO|CFO|COO|CTO|CMO|CIO|CHRO|Principal|Analyst|Engineer|Advisor|Adviser|Editor|Superintendent|Dean|Attorney|Broker|Agent|Controller|Treasurer|Administrator|Supervisor|Coordinator|Economist|Strategist|Scientist|Architect|Consultant)\b/;
const NOT_A_NAME = new Set(['the', 'new', 'this', 'that', 'chief', 'vice', 'senior', 'junior', 'president', 'board', 'company', 'group', 'their', 'his', 'her', 'our', 'we', 'it', 'today', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'north', 'south', 'east', 'west', 'first', 'second', 'third', 'former', 'current', 'global', 'national', 'international', 'american', 'british', 'united', 'read', 'more', 'also', 'about', 'photo', 'image', 'credit', 'source', 'share', 'follow']);
const NOT_A_COMPANY = new Set(['the company', 'the firm', 'the group', 'the board', 'the team', 'the university', 'the bank', 'the organization', 'the organisation', 'the business', 'the role', 'the position', 'the new', 'his', 'her', 'their', 'this', 'that', 'it']);
// Capitalised words that pass the org regex but are never an employer.
const FILLER_ORG = new Set(['recently', 'previously', 'today', 'yesterday', 'tomorrow', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'q&a', 'read more', 'the', 'he', 'she', 'they', 'prior', 'before', 'after', 'following', 'meanwhile', 'however', 'additionally']);
// Earnings-call / speaker-roster pages: "Joining me on the call today is …" is not a hire, and these
// transcripts pair a name with a title over and over — the single biggest false-positive source.
const TRANSCRIPT = /\b(conference call|earnings call|prepared remarks|call transcript|Q[1-4]\s+20\d\d\s+earnings|operator\s*[:—-]|analyst\s*[:—-]|thanks?\s+for\s+joining\s+(?:us|the call))\b/i;
// A "company" that is really a person (transcript rosters again): both ends look like given names.
const GEO_TAIL = /\b(County|City|Township|Borough|Parish|State|Province|District|Beach|Valley|Park|Heights|Springs|Falls)$/i;
const TITLE_STOP = new Set(['of', 'for', 'and', 'the', 'at', 'in', 'to', 'a', 'an', 'its', 'their', 'new', 'with']);
const STRONG = /\b(chief|ceo|cfo|coo|cto|cmo|cio|president|partner|principal|director|head|chair|vp|vice\s+president|managing|general\s+counsel|executive|founder|owner)\b/i;

function loadTitlePhrases() {
  try {
    const list = JSON.parse(fs.readFileSync(path.join(__dirname, 'data', 'position-titles.json'), 'utf8'));
    return new Set(list.map((t) => String(t).toLowerCase().replace(/[^a-z0-9& ]/g, ' ').replace(/\s+/g, ' ').trim()).filter((t) => t.length >= 3));
  } catch (e) { return new Set(); }
}
const normName = (s) => String(s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/[^a-z]/g, '');
const cleanEdge = (s) => String(s || '').replace(/\s+/g, ' ').replace(/^[\s,.;:'"‘’“”-]+|[\s,.;:'"‘’“”-]+$/g, '').trim();

// First capitalised token-run that looks like a real person (first name in the lexicon).
function findPerson(sentence, genderMap) {
  NAME_RE.lastIndex = 0;
  let m;
  while ((m = NAME_RE.exec(sentence)) !== null) {
    const cand = cleanEdge(m[1]);
    const parts = cand.split(' ');
    if (parts.length < 2) continue;
    const first = normName(parts[0]);
    if (!first || NOT_A_NAME.has(first) || !genderMap[first]) continue;
    const last = normName(parts[parts.length - 1]);
    if (!last || last.length < 2 || NOT_A_NAME.has(last)) continue;
    if (GEO_TAIL.test(cand)) continue;                    // "Montgomery County" is a place, not a hire
    return { text: cand, start: m.index, end: m.index + m[1].length };
  }
  return null;
}
// Longest dictionary title present, else a head-word title ("Managing Director", "Chief Revenue Officer").
function findTitle(sentence, titlePhrases) {
  const words = sentence.split(/\s+/);
  for (let n = Math.min(7, words.length); n >= 2; n--) {
    for (let i = 0; i + n <= words.length; i++) {
      const raw = words.slice(i, i + n).join(' ');
      const norm = raw.toLowerCase().replace(/[^a-z0-9& ]/g, ' ').replace(/\s+/g, ' ').trim();
      if (norm.length < 4 || !titlePhrases.has(norm)) continue;
      const start = sentence.indexOf(raw);
      return { text: cleanEdge(raw), start, end: start + raw.length };
    }
  }
  const m = sentence.match(TITLE_HEAD);
  if (m) return { text: cleanEdge(m[0]), start: m.index, end: m.index + m[0].length };
  return null;
}
// The employer: prefer an org right after at/of/for/with/join, then one right before an announce verb,
// then any capitalised span carrying a corporate suffix.
function findCompany(sentence, person, title, titlePhrases) {
  const overlaps = (a, b) => !(a.end <= b.start || a.start >= b.end);
  const cands = [];
  const isTitleLike = (t) => TITLE_HEAD.test(t) || (titlePhrases && titlePhrases.has(t.toLowerCase().replace(/[^a-z0-9& ]/g, ' ').replace(/\s+/g, ' ').trim()));
  ORG_RE.lastIndex = 0;
  let m;
  while ((m = ORG_RE.exec(sentence)) !== null) {
    // A possessive ("Team17’s newly appointed …") gets absorbed by the word class — split it back off,
    // because it is one of the strongest employer signals there is.
    const poss = /['’]s$/.test(m[1]);
    const raw = poss ? m[1].replace(/['’]s$/, '') : m[1];
    const span = { text: cleanEdge(raw), start: m.index, end: m.index + raw.length };
    if (span.text.length < 3) continue;
    if (person && overlaps(span, person)) continue;
    if (title && overlaps(span, title)) continue;
    const low = span.text.toLowerCase();
    if (NOT_A_COMPANY.has(low)) continue;
    if (NOT_A_NAME.has(normName(span.text.split(' ')[0])) && span.text.split(' ').length < 3) continue;
    if (person && person.text.includes(span.text)) continue;
    // An employer is never a job title, and never (here) a bare month/filler word.
    if (isTitleLike(span.text)) continue;
    if (FILLER_ORG.has(low)) continue;
    const before = sentence.slice(Math.max(0, span.start - 24), span.start);
    const after = sentence.slice(span.end, span.end + 26);
    let score = 0, strongLink = false;
    if (/\b(?:at|of|for|with|joins|joined|join|joining|to)\s+$/i.test(before)) { score += 3; strongLink = true; }
    if (/^\s*(?:announced|announces|appointed|named|welcomes|welcomed|has\s+(?:appointed|named|hired|promoted)|is\s+pleased)/i.test(after)) { score += 3; strongLink = true; }
    if (poss) { score += 3; strongLink = true; }           // "Team17's newly appointed finance director …"
    if (CORP.test(span.text)) score += 2;
    if (span.text.split(' ').length >= 2) score += 1;
    // Require a real syntactic link to the hire, not just "a capitalised phrase nearby" — that weak
    // fallback is what turned transcript speaker rosters into fake employers.
    if (!strongLink) continue;
    if (span.text.split(' ').length < 2 && !CORP.test(span.text) && !poss) continue;
    cands.push({ ...span, score });
  }
  if (!cands.length) return null;
  cands.sort((a, b) => b.score - a.score || b.text.length - a.text.length);
  return cands[0];
}

function extractFromSentence(sentence, ctx) {
  const rej = ctx.rej;
  rej.sentences++;
  const person = findPerson(sentence, ctx.genderMap);
  if (!person) { rej.noPerson++; return null; }
  const title = findTitle(sentence, ctx.titlePhrases);
  if (!title) { rej.noTitle++; if (rej.samples.title.length < 6) rej.samples.title.push(sentence.slice(0, 120)); return null; }
  const company = findCompany(sentence, person, title, ctx.titlePhrases);
  if (!company) { rej.noCompany++; if (rej.samples.company.length < 6) rej.samples.company.push(sentence.slice(0, 120)); return null; }
  if (company.text.toLowerCase() === person.text.toLowerCase()) { rej.other++; return null; }
  return { person: person.text, title: title.text, company: company.text, strong: STRONG.test(title.text) };
}

// ---------------------------------------------------------------- main
(async () => {
  const d = new Date(Date.now() - 24 * 3600 * 1000);
  const defDate = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`;
  const date = String(arg('date', defDate)).replace(/[^0-9]/g, '');
  const maxFiles = Number(arg('files', '0')) || 0;
  const conc = Math.max(1, Number(arg('conc', '4')) || 4);
  const rangeMb = Number(arg('range-mb', '0')) || 0;
  const outPath = arg('out', `cc-news-hits-${date}.jsonl`);
  const verbose = has('verbose');
  const resolveCos = has('resolve');
  const dumpN = Number(arg('dump-sentences', '0')) || 0;
  let dumped = 0;
  if (!/^\d{8}$/.test(date)) { console.error('--date must be YYYYMMDD'); process.exit(1); }

  const genderMap = loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const titlePhrases = loadTitlePhrases();
  console.error(`CC-NEWS spike — day ${date} | ${Object.keys(genderMap).length.toLocaleString()} known first names, ${titlePhrases.size.toLocaleString()} dictionary titles`);

  // Tune the extractor against one sentence, no network: --test-sentence "…"
  const testSentence = arg('test-sentence', '');
  if (testSentence) {
    const trej = { sentences: 0, noPerson: 0, noTitle: 0, noCompany: 0, other: 0, samples: { title: [], company: [] } };
    console.error(`\n  person : ${JSON.stringify(findPerson(testSentence, genderMap))}`);
    console.error(`  title  : ${JSON.stringify(findTitle(testSentence, titlePhrases))}`);
    console.error(`  company: ${JSON.stringify(findCompany(testSentence, findPerson(testSentence, genderMap), findTitle(testSentence, titlePhrases), titlePhrases))}`);
    console.error(`  RESULT : ${JSON.stringify(extractFromSentence(testSentence, { genderMap, titlePhrases, rej: trej }))}`);
    process.exit(0);
  }

  // 1) The day's WARC list, from the month's warc.paths.gz.
  const ym = `${date.slice(0, 4)}/${date.slice(4, 6)}`;
  const paths = zlib.gunzipSync(await withRetry(() => httpsGet(`https://${HOST}/crawl-data/CC-NEWS/${ym}/warc.paths.gz`), 3, 'warc.paths'))
    .toString('utf8').split('\n').filter((l) => l.includes(`CC-NEWS-${date}`));
  if (!paths.length) { console.error(`No CC-NEWS WARCs for ${date}.`); process.exit(1); }
  const files = maxFiles ? paths.slice(0, maxFiles) : paths;
  console.error(`${paths.length} WARC(s) that day; scanning ${files.length}${rangeMb ? ` (first ${rangeMb} MB each)` : ''} with conc ${conc}\n`);

  // 2) Stream each WARC, scan article text, collect validated hits.
  const t0 = Date.now();
  const tally = { warcs: 0, records: 0, pages: 0, bytes: 0, hits: 0, strong: 0, fresh: 0, dated: 0, errors: 0, prefiltered: 0, transcripts: 0 };
  const rej = { sentences: 0, noPerson: 0, noTitle: 0, noCompany: 0, other: 0, samples: { title: [], company: [] } };
  const seen = new Set();
  const hits = [];
  const out = fs.createWriteStream(outPath, { flags: 'w' });
  const cutoff = new Date(Date.parse(`${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`) - 30 * 86400000).toISOString().slice(0, 10);

  async function doWarc(p) {
    const url = `https://${HOST}/${p}`;
    const before = tally.hits;
    try {
      const res = await withRetry(() => httpsGet(url, { rangeBytes: rangeMb ? rangeMb * 1024 * 1024 : 0, stream: true }), 2, p);
      await streamWarc(res, {
        ranged: !!rangeMb,
        onRecord: (h, block) => {
          tally.records++;
          const ctype = (h['warc-identified-payload-type'] || '').toLowerCase();
          if (ctype && !ctype.includes('html')) return;
          const { headers, body } = httpBody(block);
          if (!/content-type:\s*text\/html/i.test(headers) && !ctype.includes('html')) return;
          tally.pages++; tally.bytes += body.length;
          const text = htmlToText(body.slice(0, 120000)).slice(0, 30000);
          if (!HIRE_VERB.test(text)) return;
          if (TRANSCRIPT.test(text)) { tally.transcripts++; return; }   // earnings calls are speaker lists, not hires
          tally.prefiltered++;
          const u = h['warc-target-uri'] || '';
          let host = '';
          try { host = new URL(u).hostname.replace(/^www\./, ''); } catch (e) { /* */ }
          const published = articleDate(body);

          let found = 0;
          for (const s of text.split(/(?<=[.!?])\s+/)) {
            if (s.length < 30 || s.length > 400 || !HIRE_VERB.test(s)) continue;
            if (dumpN && dumped < dumpN) { console.error(`   « ${s.trim()}`); dumped++; }
            const hit = extractFromSentence(s, { genderMap, titlePhrases, rej });
            if (!hit) continue;
            const key = `${normName(hit.person)}|${hit.company.toLowerCase().replace(/[^a-z0-9]/g, '')}`;
            if (seen.has(key)) continue;
            seen.add(key);
            const rec = { ...hit, published, url: u, host, snippet: s.trim().slice(0, 240) };
            tally.hits++;
            if (hit.strong) tally.strong++;
            if (published) { tally.dated++; if (published >= cutoff) tally.fresh++; }
            hits.push(rec);
            out.write(JSON.stringify(rec) + '\n');
            if (verbose) console.error(`   + ${rec.person} — ${rec.title} @ ${rec.company}  [${host}${published ? ' ' + published : ''}]`);
            if (++found >= 4) break;                     // one article, a handful of announcements
          }
        },
      });
    } catch (e) { tally.errors++; console.error(`  ! ${p.split('/').pop()}: ${e.message}`); }
    tally.warcs++;
    console.error(`  [${tally.warcs}/${files.length}] ${p.split('/').pop()} — pages ${tally.pages.toLocaleString()} · hits ${tally.hits} (+${tally.hits - before}) · ${((Date.now() - t0) / 60000).toFixed(1)}m`);
  }

  let next = 0;
  await Promise.all(Array.from({ length: Math.min(conc, files.length) }, async () => {
    while (next < files.length) { const i = next++; await doWarc(files[i]); }
  }));
  out.end();

  // 3) Optional: can we tie the company to a domain we already know?
  let resolved = 0;
  if (resolveCos && process.env.OPENSEARCH_ENDPOINT) {
    const companies = require('./companies');
    const client = companies.makeClient(process.env.OPENSEARCH_ENDPOINT);
    const names = [...new Set(hits.map((h) => h.company))];
    console.error(`\nResolving ${names.length} company name(s) against the companies index…`);
    const domainOf = {};
    for (const n of names) {
      try { const r = await companies.search(client, { name: n }, { size: 1 }); if (r.rows[0] && r.rows[0].domain) { domainOf[n] = r.rows[0].domain; resolved++; } }
      catch (e) { /* best-effort */ }
    }
    fs.writeFileSync(outPath.replace(/\.jsonl$/, '') + '-resolved.jsonl',
      hits.map((h) => JSON.stringify({ ...h, domain: domainOf[h.company] || '' })).join('\n') + '\n');
    console.error(`  ${resolved}/${names.length} company name(s) matched a known domain.`);
  } else if (resolveCos) {
    console.error('\n--resolve needs OPENSEARCH_ENDPOINT; skipped.');
  }

  // 4) Summary — the numbers that decide whether to build the pipeline.
  const byCompany = {};
  for (const h of hits) byCompany[h.company] = (byCompany[h.company] || 0) + 1;
  const mins = ((Date.now() - t0) / 60000).toFixed(1);
  console.error(`\n──────── CC-NEWS ${date} ────────`);
  console.error(`WARCs ${tally.warcs}/${files.length}${tally.errors ? ` (${tally.errors} failed)` : ''} · records ${tally.records.toLocaleString()} · HTML pages ${tally.pages.toLocaleString()} · ${(tally.bytes / 1e9).toFixed(1)} GB text · ${mins} min`);
  console.error(`Skipped ${tally.transcripts.toLocaleString()} earnings-call/transcript page(s).`);
  console.error(`Funnel: ${tally.prefiltered.toLocaleString()} article(s) with a hire verb -> ${rej.sentences.toLocaleString()} candidate sentence(s) -> dropped: no person ${rej.noPerson.toLocaleString()}, no title ${rej.noTitle.toLocaleString()}, no company ${rej.noCompany.toLocaleString()}`);
  console.error(`HITS: ${tally.hits.toLocaleString()} unique person+company · ${tally.strong.toLocaleString()} senior-title · ${tally.dated.toLocaleString()} with a published date · ${tally.fresh.toLocaleString()} published in the last 30 days`);
  if (tally.pages) console.error(`      = ${(tally.hits / tally.pages * 1000).toFixed(1)} hits per 1,000 articles`);
  if (maxFiles && paths.length > files.length) console.error(`      NOTE: sampled ${files.length}/${paths.length} WARCs — full day ≈ ${Math.round(tally.hits * paths.length / files.length).toLocaleString()} hits (${Math.round(tally.fresh * paths.length / files.length).toLocaleString()} fresh)`);
  if (rangeMb) console.error(`      NOTE: --range-mb ${rangeMb} read only a prefix of each WARC; this is a floor, not the full day.`);
  if (resolveCos) console.error(`      ${resolved} company name(s) matched a domain already in the companies index`);
  if (rej.samples.company.length) console.error(`\nNo-company examples: \n  ${rej.samples.company.slice(0, 3).join('\n  ')}`);
  console.error(`\nTop companies: ${Object.entries(byCompany).sort((a, b) => b[1] - a[1]).slice(0, 12).map(([c, n]) => `${c} (${n})`).join(', ') || '—'}`);
  console.error('\nSample:');
  for (const h of hits.slice(0, 15)) console.error(`  ${h.person} — ${h.title} @ ${h.company}   [${h.host}${h.published ? ' ' + h.published : ''}]`);
  console.error(`\nWrote ${hits.length.toLocaleString()} hit(s) -> ${outPath}`);
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
