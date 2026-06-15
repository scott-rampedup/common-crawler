/**
 * gsheets.js — read a (private) Google Sheet via a service account, dependency-free.
 *
 * Auth: a service-account JSON key (from env GOOGLE_SERVICE_ACCOUNT_JSON — raw JSON or
 * base64). We mint a Google OAuth access token by signing a JWT with the key's private key
 * (RS256, via node:crypto), then call the Sheets API v4 with a Bearer token. The sheet must
 * be SHARED with the service account's client_email (Viewer). Read-only scope.
 *
 * Test:  GOOGLE_SERVICE_ACCOUNT_JSON='<json or base64>' node gsheets.js <spreadsheetId> [range]
 */
const https = require("https");
const crypto = require("crypto");

const TOKEN_URL = "https://oauth2.googleapis.com/token";
const SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly";

function b64url(buf){ return Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, ""); }

// Parse the service-account key from an env value that is either raw JSON or base64-of-JSON.
function loadServiceAccount(raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON || ""){
  const s = String(raw || "").trim();
  if(!s) return null;
  const tryParse = (t) => { try{ return JSON.parse(t); }catch{ return null; } };
  let sa = tryParse(s);
  if(!sa){ try{ sa = tryParse(Buffer.from(s, "base64").toString("utf8")); }catch{ sa = null; } }
  if(!sa || !sa.client_email || !sa.private_key) return null;
  return sa;
}

// minimal JSON HTTP helper (GET or POST form/bearer). Resolves { status, body }.
function httpJson(url, { method = "GET", headers = {}, body = null } = {}){
  return new Promise((resolve) => {
    let u; try{ u = new URL(url); }catch{ return resolve({ status: 0, body: "" }); }
    const req = https.request(u, { method, headers, timeout: 20000 }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve({ status: res.statusCode || 0, body: Buffer.concat(chunks).toString("utf8") }));
      res.on("error", () => resolve({ status: 0, body: "" }));
    });
    req.on("error", () => resolve({ status: 0, body: "" }));
    req.on("timeout", () => { req.destroy(); resolve({ status: 0, body: "" }); });
    if(body) req.write(body);
    req.end();
  });
}

let _tokenCache = { token: "", exp: 0, key: "" };
async function getAccessToken(sa){
  const now = Math.floor(Date.now() / 1000);
  if(_tokenCache.token && _tokenCache.exp - 60 > now && _tokenCache.key === sa.client_email) return _tokenCache.token;
  const header = b64url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const claims = b64url(JSON.stringify({
    iss: sa.client_email, scope: SCOPE, aud: TOKEN_URL, iat: now, exp: now + 3600,
  }));
  const signingInput = `${header}.${claims}`;
  const signer = crypto.createSign("RSA-SHA256");
  signer.update(signingInput);
  const sig = b64url(signer.sign(sa.private_key));
  const assertion = `${signingInput}.${sig}`;
  const form = `grant_type=${encodeURIComponent("urn:ietf:params:oauth:grant-type:jwt-bearer")}&assertion=${encodeURIComponent(assertion)}`;
  const res = await httpJson(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded", "Content-Length": Buffer.byteLength(form) },
    body: form,
  });
  let j = {}; try{ j = JSON.parse(res.body); }catch{}
  if(res.status !== 200 || !j.access_token){
    throw new Error(`token error ${res.status}: ${(j.error_description || j.error || res.body || "").toString().slice(0, 200)}`);
  }
  _tokenCache = { token: j.access_token, exp: now + (j.expires_in || 3600), key: sa.client_email };
  return j.access_token;
}

async function apiGet(token, url){
  const res = await httpJson(url, { headers: { Authorization: `Bearer ${token}` } });
  let j = null; try{ j = JSON.parse(res.body); }catch{}
  if(res.status !== 200) throw new Error(`sheets api ${res.status}: ${((j && (j.error?.message)) || res.body || "").toString().slice(0, 200)}`);
  return j;
}

// Title of the sheet/tab for a gid (or the first tab if gid is null/not found).
async function tabTitleForGid(token, spreadsheetId, gid = null){
  const meta = await apiGet(token, `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}?fields=sheets.properties`);
  const sheets = (meta && meta.sheets) || [];
  if(gid != null){
    const hit = sheets.find((s) => String(s.properties?.sheetId) === String(gid));
    if(hit) return hit.properties.title;
  }
  return sheets[0]?.properties?.title || "Sheet1";
}

// Read a tab's values as an array of row-arrays. opts: { gid, range, sa }.
async function readSheet(spreadsheetId, opts = {}){
  const sa = opts.sa || loadServiceAccount();
  if(!sa) throw new Error("no service account: set GOOGLE_SERVICE_ACCOUNT_JSON");
  const token = await getAccessToken(sa);
  const title = await tabTitleForGid(token, spreadsheetId, opts.gid != null ? opts.gid : 0);
  const a1 = opts.range || `${title.replace(/'/g, "''")}!A1:ZZ100000`;
  const range = /!/.test(a1) ? a1 : `'${title.replace(/'/g, "''")}'!${a1}`;
  const data = await apiGet(token, `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${encodeURIComponent(range)}?majorDimension=ROWS`);
  return { title, rows: (data && data.values) || [] };
}

// Parse a spreadsheet id + gid out of a full Google Sheets URL (or accept a bare id).
function parseSheetRef(s){
  const str = String(s || "").trim();
  const idm = str.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/) || str.match(/^([a-zA-Z0-9_-]{30,})$/);
  const gidm = str.match(/[?#&]gid=(\d+)/);
  return { spreadsheetId: idm ? idm[1] : "", gid: gidm ? Number(gidm[1]) : 0 };
}

module.exports = { loadServiceAccount, readSheet, parseSheetRef, getAccessToken };

// ---- CLI: dump a sheet's rows (for inspecting columns) ----
if(require.main === module){
  (async () => {
    const ref = parseSheetRef(process.argv[2] || "");
    if(!ref.spreadsheetId){ console.error("usage: GOOGLE_SERVICE_ACCOUNT_JSON=... node gsheets.js <spreadsheetUrlOrId> [A1range]"); process.exit(1); }
    const sa = loadServiceAccount();
    if(!sa){ console.error("Set GOOGLE_SERVICE_ACCOUNT_JSON (the service-account JSON key, raw or base64)."); process.exit(1); }
    console.log(`Service account: ${sa.client_email}`);
    try{
      const { title, rows } = await readSheet(ref.spreadsheetId, { gid: ref.gid, range: process.argv[3] });
      console.log(`Tab: "${title}"   rows: ${rows.length}\n`);
      rows.slice(0, 12).forEach((r, i) => console.log(`${String(i).padStart(2)}: ${r.map((c) => String(c).slice(0, 40)).join(" | ")}`));
      if(rows.length > 12) console.log(`... (${rows.length - 12} more rows)`);
    }catch(e){ console.error("READ FAILED:", e.message); process.exit(1); }
  })();
}
