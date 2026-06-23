/**
 * wireless-block-classifier.js
 * -------------------------------------------------------------
 * One module, one dataset (phone-blocks.csv) for US/Canada (NANP) numbers:
 *
 *   • LINE TYPE  — Toll Free / Mobile / Direct, from the number's NPA+NXX+X block.
 *   • LOCATION   — "City, ST, Country" for that block (more precise than libphonenumber's
 *                  rate-center, which often returns only the state).
 *
 * Data source: `phone-blocks.csv` — columns: Country Code, Area Code, 7 Block, City, State,
 * Country, Type. The **7 Block** is NPA+NXX+X (7 digits). **Type** is the carrier-allocation
 * class: "Mobile"/"WRSL" => wireless, "Office" => wireline (landline). The legacy
 * WIRELESS_BLOCKS.TXT format (NPA,NXX,X,CATEGORY,...) with CATEGORY "PCS"/"WRSL"=wireless,
 * "WIRE"=landline is also understood, so old callers still work.
 *
 * HISTORY: the previous loader added EVERY block to the wireless set regardless of class, so
 * the 694k WIRE/"Office" (landline) blocks were misclassified as Mobile. This loader honors the
 * class column: only Mobile/PCS/WRSL/wireless blocks count as wireless.
 *
 * Caveat: block class reflects the ORIGINAL carrier-type assignment. A number ported
 * landline<->wireless won't be caught by block data alone (that needs an LRN feed).
 */

const fs = require("fs");

const TOLL_FREE = new Set(["800", "888", "877", "866", "855", "844", "833", "822"]);

// Carrier-allocation classes that mean WIRELESS (=> Mobile). Everything else (WIRE / Office /
// wireline / landline / blank) is treated as a geographic landline (=> Direct).
const WIRELESS_CLASS = new Set(["MOBILE", "WIRELESS", "PCS", "WRSL", "CELL", "CELLULAR", "CL"]);
const isWirelessClass = (s) => WIRELESS_CLASS.has(String(s || "").toUpperCase().trim());

// Some datasets list "Quebec" in the Country column for QC blocks; normalize to the country.
const normCountry = (c) => (/^quebec$/i.test(String(c || "").trim()) ? "Canada" : String(c || "").trim());

// Memoize parsed datasets by path — the CSV is ~42MB / 837k rows; parse it once per process and
// share the structure across every job and every caller (loadWirelessBlocks, blockLocation, …).
const _cache = new Map();

/**
 * Load a phone-block dataset (CSV or legacy TXT, auto-detected by header).
 * @returns {{ wireless: Set<string>, location: Map<string,string> }}
 *   wireless = set of 7-blocks (NPA+NXX+X) whose class is wireless.
 *   location = 7-block -> "City, ST, Country" (empty for the legacy TXT, which has no city).
 */
function loadPhoneBlocks(path) {
  if (_cache.has(path)) return _cache.get(path);
  const wireless = new Set();
  const location = new Map();
  let text = "";
  try { text = fs.readFileSync(path, "utf8"); }
  catch (e) { const empty = { wireless, location }; _cache.set(path, empty); return empty; }

  const lines = text.split(/\r?\n/);
  const header = (lines[0] || "").toLowerCase();
  const isCsv = header.includes("7 block") || header.includes("country code");

  for (let i = 1; i < lines.length; i++) {
    const ln = lines[i];
    if (!ln) continue;
    const c = ln.split(",");
    let block, cls, loc = "";
    if (isCsv) {
      // Country Code, Area Code, 7 Block, City, State, Country, Type
      if (c.length < 7) continue;
      block = c[2].replace(/"/g, "").trim();
      cls = c[6].replace(/"/g, "").trim();
      const city = (c[3] || "").trim();
      const state = (c[4] || "").trim();
      const country = normCountry(c[5]);
      loc = [city, state, country].filter(Boolean).join(", ");
    } else {
      // NPA, NXX, X, CATEGORY, FUTURE
      if (c.length < 4) continue;
      const npa = c[0].replace(/"/g, "").trim();
      const nxx = c[1].replace(/"/g, "").trim();
      const x = c[2].replace(/"/g, "").trim();
      if (npa.length !== 3 || nxx.length !== 3 || x.length !== 1) continue;
      block = npa + nxx + x;
      cls = c[3].replace(/"/g, "").trim();
    }
    if (!/^\d{7}$/.test(block)) continue;
    if (isWirelessClass(cls)) wireless.add(block);
    if (loc) location.set(block, loc);
  }
  const out = { wireless, location };
  _cache.set(path, out);
  return out;
}

/** Back-compat: build just the wireless-block Set (now class-filtered, so landlines are excluded). */
function loadWirelessBlocks(path) {
  return loadPhoneBlocks(path).wireless;
}

/** Normalize to 10 NANP digits, or null if it isn't a NANP number. */
function nanpDigits(input) {
  const d = String(input).replace(/\D/g, "");
  if (d.length === 11 && d[0] === "1") return d.slice(1);
  if (d.length === 10) return d;
  return null;
}

/**
 * @param {string} phone     E.164 or loose ("+1 415-555-0142", "4155550142")
 * @param {Set}    wireless  set returned by loadWirelessBlocks()/loadPhoneBlocks().wireless
 * @returns {{type:string, block:?string, wireless:boolean}}
 */
function classifyLineType(phone, wireless) {
  const d = nanpDigits(phone);
  if (!d) return { type: "Unknown", block: null, wireless: false };

  const npa = d.slice(0, 3);
  const block = d.slice(0, 7);

  if (TOLL_FREE.has(npa)) return { type: "Toll Free", block, wireless: false };
  if (wireless && wireless.has(block)) return { type: "Mobile", block, wireless: true };
  return { type: "Direct", block, wireless: false };
}

/**
 * Block-level location for a NANP number.
 * @param {string} phone
 * @param {Map}    location  map from loadPhoneBlocks().location
 * @returns {string} "City, ST, Country" or "" (non-NANP / unknown block).
 */
function blockLocation(phone, location) {
  const d = nanpDigits(phone);
  if (!d || !location) return "";
  return location.get(d.slice(0, 7)) || "";
}

const PHONE_BLOCKS_CSV = __dirname + "/phone-blocks.csv";

module.exports = { loadPhoneBlocks, loadWirelessBlocks, classifyLineType, nanpDigits, blockLocation, PHONE_BLOCKS_CSV };

// --- self-test: `node wireless-block-classifier.js [path]` ---
if (require.main === module) {
  const path = process.argv[2] || PHONE_BLOCKS_CSV;
  const t0 = Date.now();
  const { wireless, location } = loadPhoneBlocks(path);
  console.log(`Loaded ${wireless.size.toLocaleString()} wireless blocks + ${location.size.toLocaleString()} located blocks in ${Date.now() - t0}ms\n`);

  let p = 0, f = 0; const ok = (l, c) => { console.log((c ? "✓" : "✗") + " " + l); c ? p++ : f++; };
  // 2012012 is an "Office"/WIRE (landline) block -> Direct (this was the bug: it returned Mobile)
  ok("WIRE/Office block 201-201-... => Direct (not Mobile)", classifyLineType("+12012012345", wireless).type === "Direct");
  // 2012042 is a PCS/Mobile block -> Mobile
  ok("PCS/Mobile block 201-204-2888 => Mobile", classifyLineType("+12012042888", wireless).type === "Mobile");
  ok("toll-free 800 => Toll Free", classifyLineType("1-800-555-0199", wireless).type === "Toll Free");
  ok("non-NANP => Unknown", classifyLineType("+44 20 7946 0958", wireless).type === "Unknown");
  ok("block location is city-level (Hackensack, NJ)", /Hackensack, NJ/.test(blockLocation("+12012012345", location)));
  ok("non-NANP has no block location", blockLocation("+442079460958", location) === "");

  console.log(`\nblock-classifier self-test: ${p} passed, ${f} failed`);
  process.exit(f ? 1 : 0);
}
