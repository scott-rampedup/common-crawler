/**
 * wireless-block-classifier.js
 * -------------------------------------------------------------
 * Classifies US/Canada (NANP) phone numbers as Toll Free / Mobile / Direct
 * using the IMS Wireless Block Identifier table (NPA-NXX-X thousands blocks).
 *
 *   • Toll Free  -> detected by area code prefix (reliable)
 *   • Mobile     -> the number's NPA+NXX+X block is in the wireless table
 *   • Direct     -> a geographic number whose block is NOT wireless (landline)
 *   • Unknown    -> not a parseable NANP number
 *
 * Geographic LOCATION is intentionally NOT handled here — that comes from
 * libphonenumber's offline geocoder. This module does one job: line type.
 *
 * Caveat: block data reflects the ORIGINAL carrier-type assignment. A number
 * ported landline<->wireless won't be caught by block data alone (that needs
 * a separate ported-number/LRN feed). Block ID is high-accuracy, not perfect.
 */

const fs = require("fs");

const TOLL_FREE = new Set(["800", "888", "877", "866", "855", "844", "833", "822"]);

/** Build the lookup set once at startup. Key = NPA+NXX+X (7 chars). */
function loadWirelessBlocks(path) {
  const text = fs.readFileSync(path, "utf8");
  const set = new Set();
  const lines = text.split(/\r?\n/);
  for (let i = 1; i < lines.length; i++) {      // skip header row
    const line = lines[i];
    if (!line) continue;
    // rows look like:  "201","202","0","WIRE",
    const cells = line.split(",");
    if (cells.length < 3) continue;
    const npa = cells[0].replace(/"/g, "").trim();
    const nxx = cells[1].replace(/"/g, "").trim();
    const x   = cells[2].replace(/"/g, "").trim();
    if (npa.length === 3 && nxx.length === 3 && x.length === 1) {
      set.add(npa + nxx + x);
    }
  }
  return set;
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
 * @param {Set}    wireless  set returned by loadWirelessBlocks()
 * @returns {{type:string, block:?string, wireless:boolean}}
 */
function classifyLineType(phone, wireless) {
  const d = nanpDigits(phone);
  if (!d) return { type: "Unknown", block: null, wireless: false };

  const npa = d.slice(0, 3);
  const nxx = d.slice(3, 6);
  const x   = d.slice(6, 7);
  const block = npa + nxx + x;

  if (TOLL_FREE.has(npa)) return { type: "Toll Free", block, wireless: false };
  if (wireless.has(block)) return { type: "Mobile",    block, wireless: true  };
  return { type: "Direct", block, wireless: false };
}

module.exports = { loadWirelessBlocks, classifyLineType, nanpDigits };

// --- quick self-test when run directly: `node wireless-block-classifier.js <path>` ---
if (require.main === module) {
  const path = process.argv[2] || (__dirname + "/WIRELESS_BLOCKS.TXT");
  const t0 = Date.now();
  const wireless = loadWirelessBlocks(path);
  console.log(`Loaded ${wireless.size.toLocaleString()} wireless blocks in ${Date.now() - t0}ms\n`);
  const samples = [
    "+1 201-201-2345",   // 2012012 -> WIRE in file  -> Mobile
    "+1 201-201-0345",   // 2012010 -> not wireless   -> Direct
    "1-800-555-0199",    // toll free
    "+1 201-204-2888",   // 2012042 -> PCS in file    -> Mobile
    "+44 20 7946 0958",  // non-NANP                  -> Unknown
  ];
  for (const s of samples) {
    const r = classifyLineType(s, wireless);
    console.log(`${s.padEnd(20)} -> ${r.type.padEnd(10)} (block ${r.block || "n/a"})`);
  }
}
