// Outbound email via SMTP (nodemailer). Configured entirely through env vars / Fly secrets:
//   SMTP_HOST, SMTP_PORT (default 587), SMTP_SECURE (true for 465), SMTP_USER, SMTP_PASS,
//   SMTP_FROM (default "Common Crawler <SMTP_USER>"), ADMIN_EMAIL (signup alerts; default SMTP_USER),
//   APP_BASE_URL (links in emails; default https://common-crawler.com).
// If SMTP isn't configured the module is a safe no-op (logs + returns { skipped:true }) so the
// app runs fine without email. Nothing here ever throws into a request handler.
const nodemailer = require('nodemailer');

const HOST = process.env.SMTP_HOST || '';
const PORT = Number(process.env.SMTP_PORT || 587);
const SECURE = process.env.SMTP_SECURE === 'true' || PORT === 465;   // 465 = implicit TLS, 587 = STARTTLS
const USER = process.env.SMTP_USER || '';
const PASS = process.env.SMTP_PASS || '';
const FROM = process.env.SMTP_FROM || (USER ? `Common Crawler <${USER}>` : '');
const APP_BASE_URL = (process.env.APP_BASE_URL || 'https://common-crawler.com').replace(/\/+$/, '');
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || USER || '';

const configured = !!(HOST && USER && PASS);
let transporter = null;
function getTransport() {
  if (!configured) return null;
  if (!transporter) {
    transporter = nodemailer.createTransport({ host: HOST, port: PORT, secure: SECURE, auth: { user: USER, pass: PASS } });
  }
  return transporter;
}

function mailEnabled() { return configured; }
function adminEmail() { return ADMIN_EMAIL; }
function baseUrl() { return APP_BASE_URL; }

// Minimal, consistent HTML wrapper around a body of <p>/<a> fragments.
function wrap(title, bodyHtml) {
  return `<div style="font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;max-width:520px;margin:0 auto;color:#111827">
    <h2 style="color:#111827;font-size:1.25rem;margin:0 0 12px">${title}</h2>
    ${bodyHtml}
    <p style="color:#9ca3af;font-size:12px;margin-top:24px;border-top:1px solid #eee;padding-top:12px">Common Crawler · <a href="${APP_BASE_URL}" style="color:#6b7280">${APP_BASE_URL.replace(/^https?:\/\//, '')}</a></p>
  </div>`;
}
const esc = (s) => String(s == null ? '' : s).replace(/[<>&]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]));
const name = (u) => (u && (u.first || u.username)) || 'there';

// Message templates -> { subject, text, html }
const templates = {
  signupAdminAlert(u) {
    const subject = `New Common Crawler signup pending: ${u.username}`;
    const lines = [`A new account is awaiting activation.`, ``,
      `Username: ${u.username}`, `Name: ${[u.first, u.last].filter(Boolean).join(' ') || '—'}`,
      `Email: ${u.email || '—'}`, `Company: ${u.company || '—'}`, ``,
      `Activate them in the Admin tab: ${APP_BASE_URL}/admin`];
    return { subject, text: lines.join('\n'),
      html: wrap('New signup pending activation',
        `<p>A new account is awaiting activation:</p>
         <p><strong>Username:</strong> ${esc(u.username)}<br>
            <strong>Name:</strong> ${esc([u.first, u.last].filter(Boolean).join(' ') || '—')}<br>
            <strong>Email:</strong> ${esc(u.email || '—')}<br>
            <strong>Company:</strong> ${esc(u.company || '—')}</p>
         <p><a href="${APP_BASE_URL}/admin">Review &amp; activate in the Admin tab →</a></p>`) };
  },
  signupConfirm(u) {
    return { subject: 'We received your Common Crawler access request',
      text: `Hi ${name(u)},\n\nThanks for requesting access to Common Crawler. Your account is pending administrator approval — we'll email you when it's activated.\n\n${APP_BASE_URL}`,
      html: wrap('Request received',
        `<p>Hi ${esc(name(u))},</p>
         <p>Thanks for requesting access to Common Crawler. Your account is <strong>pending administrator approval</strong> — we'll email you as soon as it's activated.</p>`) };
  },
  accountActivated(u) {
    return { subject: 'Your Common Crawler account is active',
      text: `Hi ${name(u)},\n\nYour Common Crawler account has been activated. You can sign in here:\n${APP_BASE_URL}/login`,
      html: wrap('Your account is active',
        `<p>Hi ${esc(name(u))},</p>
         <p>Your Common Crawler account has been activated.</p>
         <p><a href="${APP_BASE_URL}/login">Sign in →</a></p>`) };
  },
  passwordReset(u, tempPw) {
    return { subject: 'Your Common Crawler temporary password',
      text: `Hi ${name(u)},\n\nWe reset your Common Crawler password. Sign in with this temporary password and change it after:\n\nUsername: ${u.username}\nTemporary password: ${tempPw}\n\n${APP_BASE_URL}/login\n\nIf you didn't request this, you can ignore this email — your old password no longer works, so contact an administrator if needed.`,
      html: wrap('Temporary password',
        `<p>Hi ${esc(name(u))},</p>
         <p>We reset your Common Crawler password. Sign in with this temporary password:</p>
         <p><strong>Username:</strong> ${esc(u.username)}<br>
            <strong>Temporary password:</strong> <code style="background:#f3f4f6;padding:2px 6px;border-radius:4px">${esc(tempPw)}</code></p>
         <p><a href="${APP_BASE_URL}/login">Sign in →</a></p>
         <p style="color:#6b7280;font-size:13px">If you didn't request this, contact an administrator — your previous password no longer works.</p>`) };
  },
  optOutVerify(email, confirmUrl) {
    return { subject: 'Confirm your Common Crawler data-removal request',
      text: `We received a request to remove ${email} from the Common Crawler database.\n\nTo confirm the removal, click the link below. This permanently removes the data and suppresses it from being re-added in the future.\n\n${confirmUrl}\n\nIf you didn't make this request, ignore this email — nothing will be removed.`,
      html: wrap('Confirm data removal',
        `<p>We received a request to remove <strong>${esc(email)}</strong> from the Common Crawler database.</p>
         <p>To confirm and permanently remove this data — and suppress it from being re-added in the future — click below:</p>
         <p><a href="${esc(confirmUrl)}" style="display:inline-block;background:#111827;color:#fff;padding:10px 18px;border-radius:8px;text-decoration:none">Confirm removal →</a></p>
         <p style="color:#6b7280;font-size:13px">If you didn't request this, ignore this email — nothing will be removed.</p>`) };
  },
};

async function sendMail({ to, subject, text, html }) {
  if (!configured) { console.log(`[mail] skipped (SMTP not configured): "${subject}" -> ${to || '(no recipient)'}`); return { skipped: true }; }
  if (!to) { console.log(`[mail] skipped (no recipient): "${subject}"`); return { skipped: true }; }
  try {
    const info = await getTransport().sendMail({ from: FROM, to, subject, text, html });
    console.log(`[mail] sent "${subject}" -> ${to} (${info.messageId || 'ok'})`);
    return { ok: true, messageId: info.messageId };
  } catch (e) {
    console.error(`[mail] FAILED "${subject}" -> ${to}: ${e.message}`);
    return { ok: false, error: e.message };
  }
}

module.exports = { mailEnabled, sendMail, adminEmail, baseUrl, templates };
