/**
 * non-company-domains.js — hosts that must NEVER stand as an employer domain.
 *
 * This list used to live in companies.js, where it guarded the COMPANY index only: the Google Maps /
 * AllThePlaces loaders refuse a business whose "website" is a social or free-host page, so thousands of
 * unrelated businesses are never merged under one junk host. Nothing applied the same rule to CONTACTS,
 * and the contacts path derives `domain` from the host of the bio page it scraped. Measured 2026-11-19
 * that left 1,038,262 contacts carrying a platform as their employer — facebook.com alone accounted for
 * 1,016,897, of which just 586 had an @facebook.com mailbox. Facebook was not their employer; it was
 * merely where the page lived.
 *
 * It is split in two because the two halves need different tests:
 *
 *   FREEMAIL_HOSTS   nobody is ever employed BY their mailbox provider, so these are cleared outright.
 *                    The mailbox test below would otherwise keep them: 20,759 contacts had both
 *                    domain=gmail.com and an @gmail.com address, which "matches" and still means nothing.
 *
 *   PLATFORM_HOSTS   real companies that also host other people's pages. Here the mailbox decides:
 *                    an @facebook.com address really does work at Facebook, an @gmail.com address whose
 *                    page sat on facebook.com does not. Measured on the same date, that test kept 3,330
 *                    genuine @linklaters.com contacts and 48 @wix.com ones while clearing the rest.
 *
 * Exact-match only, after lowercasing and stripping a leading www. — `domain` holds the registrable
 * domain, so a suffix rule would buy nothing and would risk eating real subsidiaries.
 */

// Mailbox providers. Employment is never implied by any of these.
const FREEMAIL_HOSTS = new Set([
  'gmail.com', 'googlemail.com', 'yahoo.com', 'yahoo.co.uk', 'ymail.com', 'hotmail.com', 'hotmail.co.uk',
  'outlook.com', 'live.com', 'msn.com', 'aol.com', 'icloud.com', 'me.com', 'mac.com', 'mail.com',
  'gmx.com', 'gmx.de', 'gmx.net', 'proton.me', 'protonmail.com', 'yandex.com', 'zoho.com',
]);

// Social profiles, video, link-in-bio, directories, lead-gen scrapers, shared site builders and
// domain parkers. Several ARE real employers (Amazon, GitHub, Etsy, Meetup, Patreon, GoDaddy, Wix) —
// which is exactly why the mailbox test exists rather than a blanket ban.
const PLATFORM_HOSTS = new Set([
  'facebook.com', 'm.facebook.com', 'fb.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
  'youtube.com', 'youtu.be', 'pinterest.com', 'tiktok.com', 'snapchat.com', 'yelp.com', 'tripadvisor.com',
  'foursquare.com', 't.me', 'wa.me', 'whatsapp.com', 'linktr.ee', 'about.me', 'crunchbase.com',
  'google.com', 'mail.google.com', 'sites.google.com', 'business.site', 'wixsite.com', 'weebly.com',
  'blogspot.com', 'wordpress.com', 'godaddysites.com', 'webnode.com', 'jimdo.com', 'strikingly.com',
  'netlify.app', 'github.io', 'myshopify.com', 'square.site', 'squareup.com', 'bit.ly',
  // Added 2026-11-19 from Scott's alternate-domain list. linklaters.com was on that list and is
  // deliberately NOT here: 3,330 of its 3,331 contacts hold a real @linklaters.com mailbox, so it is a
  // genuine employer (the law firm), not a host.
  'etsy.com', 'amazon.com', 'wix.com', 'calendly.com', 'angel.co', 'godaddy.com', 'github.com',
  'lnk.bio', 'meetup.com', 'patreon.com', 'tinyurl.com', 'easyleadz.com', 'zumvu.com', 'leadinfo.com',
  'idealist.org', 'complaintsboard.com', 'information.com', 'intensedebate.com', 'leadiq.com',
  'opendorse.com', 'prowly.com', 'indiemart.com', 'hugedomains.com',
]);

const NON_COMPANY_DOMAINS = new Set([...FREEMAIL_HOSTS, ...PLATFORM_HOSTS]);

const normHost = (d) => String(d || '').trim().toLowerCase().replace(/^www\./, '');

// True when the host can never be any company's primary domain. Unchanged in meaning from the original
// companies.js helper, so the Google Maps / ATP / Bing loaders behave exactly as before.
function isBadCompanyDomain(dom) {
  const d = normHost(dom);
  return !d || NON_COMPANY_DOMAINS.has(d);
}

const hostOfEmail = (email) => normHost(String(email || '').split('@')[1] || '');

/**
 * The employer domain this contact should actually carry. Returns '' when the host is standing in for
 * an employer it does not represent; otherwise returns the host unchanged.
 *
 * A freemail host is always wrong. A platform host is wrong unless the person's own mailbox is there,
 * which is the only evidence available that they work for the platform rather than merely on it.
 */
function employerDomainFor(dom, email) {
  const d = normHost(dom);
  if (!d) return '';
  if (FREEMAIL_HOSTS.has(d)) return '';
  if (PLATFORM_HOSTS.has(d) && hostOfEmail(email) !== d) return '';
  return d;
}

module.exports = {
  FREEMAIL_HOSTS, PLATFORM_HOSTS, NON_COMPANY_DOMAINS,
  normHost, hostOfEmail, isBadCompanyDomain, employerDomainFor,
};

if (require.main === module && process.argv.includes('--selftest')) {
  let pass = 0, fail = 0;
  const eq = (label, got, want) => {
    if (got === want) { pass++; return; }
    fail++; console.error('  FAIL ' + label + ': got ' + JSON.stringify(got) + ' want ' + JSON.stringify(want));
  };
  // The case that motivated the whole change.
  eq('facebook page, gmail mailbox', employerDomainFor('facebook.com', 'jane@gmail.com'), '');
  eq('facebook page, facebook mailbox', employerDomainFor('facebook.com', 'jane@facebook.com'), 'facebook.com');
  // Freemail is wrong even when the mailbox "matches".
  eq('gmail as employer', employerDomainFor('gmail.com', 'jane@gmail.com'), '');
  eq('outlook as employer', employerDomainFor('outlook.com', 'jane@outlook.com'), '');
  // Real employers that happen to host pages.
  eq('amazon page, gmail mailbox', employerDomainFor('amazon.com', 'jane@gmail.com'), '');
  eq('amazon staffer', employerDomainFor('amazon.com', 'jane@amazon.com'), 'amazon.com');
  eq('github staffer', employerDomainFor('github.com', 'o@github.com'), 'github.com');
  // Deliberately absent from the list.
  eq('linklaters is a real firm', employerDomainFor('linklaters.com', 'p@linklaters.com'), 'linklaters.com');
  eq('linklaters, foreign mailbox', employerDomainFor('linklaters.com', 'p@gmail.com'), 'linklaters.com');
  // Ordinary employers pass through untouched.
  eq('ordinary employer', employerDomainFor('valencianm.gov', 'm.orona@valencianm.gov'), 'valencianm.gov');
  eq('ordinary employer, other mailbox', employerDomainFor('acme.co.uk', 'jane@gmail.com'), 'acme.co.uk');
  // Normalisation and empties.
  eq('www stripped', employerDomainFor('WWW.Facebook.com', 'jane@gmail.com'), '');
  eq('case folded', employerDomainFor('Acme.COM', 'x@acme.com'), 'acme.com');
  eq('empty domain', employerDomainFor('', 'jane@gmail.com'), '');
  eq('missing email', employerDomainFor('facebook.com', ''), '');
  // The older helper must keep its original meaning for the loaders that depend on it.
  eq('isBad: facebook', isBadCompanyDomain('facebook.com'), true);
  eq('isBad: gmail', isBadCompanyDomain('gmail.com'), true);
  eq('isBad: newly added', isBadCompanyDomain('easyleadz.com'), true);
  eq('isBad: linklaters', isBadCompanyDomain('linklaters.com'), false);
  eq('isBad: empty', isBadCompanyDomain(''), true);
  eq('isBad: real company', isBadCompanyDomain('valencianm.gov'), false);
  console.log('non-company-domains selftest: ' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
}
