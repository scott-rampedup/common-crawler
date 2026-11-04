// Minimal xlsx -> csv. Reads an UNZIPPED .xlsx directory; no dependency, nothing executed from the data.
//   node xlsx2csv.js <unzipped-dir> <out.csv>
const fs = require('fs');
const path = require('path');

const DIR = process.argv[2];
const OUT = process.argv[3];
if (!DIR || !OUT) { console.error('need <unzipped-dir> <out.csv>'); process.exit(1); }

const unesc = (s) => String(s)
  .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'")
  .replace(/&#(\d+);/g, (m, d) => String.fromCodePoint(Number(d)))
  .replace(/&#x([0-9a-f]+);/gi, (m, h) => String.fromCodePoint(parseInt(h, 16)))
  .replace(/&amp;/g, '&');   // last, so an escaped &amp;lt; does not become a tag

// shared strings: each <si> may hold one <t> or several inside rich-text <r> runs — concatenate them all.
const ssPath = path.join(DIR, 'xl', 'sharedStrings.xml');
const shared = [];
if (fs.existsSync(ssPath)) {
  const xml = fs.readFileSync(ssPath, 'utf8');
  for (const si of xml.match(/<si>[\s\S]*?<\/si>/g) || []) {
    let s = '';
    for (const m of si.matchAll(/<t[^>]*>([\s\S]*?)<\/t>/g)) s += unesc(m[1]);
    shared.push(s);
  }
}

const colOf = (ref) => {
  const letters = (ref.match(/^[A-Z]+/) || [''])[0];
  let n = 0;
  for (const ch of letters) n = n * 26 + (ch.charCodeAt(0) - 64);
  return n - 1;
};

const sheet = fs.readFileSync(path.join(DIR, 'xl', 'worksheets', 'sheet1.xml'), 'utf8');
const rows = [];
let width = 0;
for (const rm of sheet.matchAll(/<row[^>]*>([\s\S]*?)<\/row>/g)) {
  const cells = [];
  for (const cm of rm[1].matchAll(/<c\s([^>]*)>([\s\S]*?)<\/c>/g)) {
    const attrs = cm[1], inner = cm[2];
    const ref = (attrs.match(/r="([A-Z]+\d+)"/) || [])[1] || '';
    const t = (attrs.match(/t="([^"]+)"/) || [])[1] || '';
    let v = '';
    if (t === 'inlineStr') { for (const m of inner.matchAll(/<t[^>]*>([\s\S]*?)<\/t>/g)) v += unesc(m[1]); }
    else {
      const raw = (inner.match(/<v>([\s\S]*?)<\/v>/) || [])[1];
      if (raw != null) v = t === 's' ? (shared[Number(raw)] ?? '') : unesc(raw);
    }
    const i = ref ? colOf(ref) : cells.length;
    cells[i] = v;
  }
  for (let i = 0; i < cells.length; i++) if (cells[i] === undefined) cells[i] = '';
  width = Math.max(width, cells.length);
  rows.push(cells);
}

const q = (s) => (/[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s);
const out = rows.map((r) => { const c = r.slice(); while (c.length < width) c.push(''); return c.map((x) => q(String(x ?? ''))).join(','); });
fs.writeFileSync(OUT, out.join('\r\n') + '\r\n', 'utf8');
console.log(`${rows.length} row(s) x ${width} column(s) -> ${OUT}`);
