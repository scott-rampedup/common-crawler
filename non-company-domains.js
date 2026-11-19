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
//
// This list was .com-only until 2026-11-19, when a 50,000-address sample of the contacts holding no
// employer domain showed the blind spot was most of the problem. EVERY one of the top 25 mailbox hosts
// was a consumer provider the list did not know: libero.it (1,169), orange.fr (1,057), wp.pl (878),
// mail.ru, web.de, yahoo.com.tw, hotmail.fr, seznam.cz, abv.bg, naver.com, t-online.de. Inferring an
// employer from those addresses would have filed a French retiree's Orange mailbox as "employed by
// Orange" on roughly a fifth of the 1.1M rows.
//
// Two mechanisms, because one literal list cannot keep up with ccTLDs:
//   FREEMAIL_FAMILIES  matches on the FIRST LABEL, so every national variant of a global provider is
//                      covered by one entry -- yahoo.es, hotmail.fr, outlook.com.br, live.nl, and the
//                      ones nobody has seen yet. It also catches the typo-squats (gmail.co, gmail.con).
//   FREEMAIL_HOSTS     the national providers and ISPs, which share no naming pattern.
//
// Ambiguous telco/ISP domains (sky.com, telia.com, frontier.com, rogers.com) are deliberately treated
// as consumer mail even though each is also a real employer. company-name-waterfall.js states the
// trade: a blank Company is recoverable and a wrong employer is not.
const FREEMAIL_FAMILIES = new Set([
  'gmail', 'googlemail', 'yahoo', 'myyahoo', 'ymail', 'hotmail', 'outlook', 'live', 'windowslive',
  'msn', 'aol', 'icloud', 'proton', 'protonmail',
  // gmx was enumerated host-by-host until the post-fix audit found gmx.fr still being filed as an
  // employer. It is a family like the rest.
  'gmx',
  // Typo-squats of the above, which are mailboxes in practice and never employers.
  'gamil', 'gmai', 'gmil', 'gnail', 'gmial', 'gmaill', 'gamail', 'hotmai', 'hotmal', 'yahooo', 'yaho',
  'outlok', 'getimel',
]);

const FREEMAIL_HOSTS = new Set([
  // Global providers whose first label is not distinctive enough for a family rule.
  'me.com', 'mac.com', 'mail.com', 'gmx.com', 'gmx.de', 'gmx.net', 'gmx.at', 'gmx.ch',
  'yandex.com', 'yandex.ru', 'ya.ru', 'zoho.com', 'zoho.in', 'fastmail.com', 'hey.com', 'tutanota.com',
  'onmicrosoft.com',                                       // a tenant default, never an employer identity
  // Italy
  'libero.it', 'virgilio.it', 'tiscali.it', 'alice.it', 'tin.it', 'email.it', 'fastwebnet.it',
  // France
  'orange.fr', 'wanadoo.fr', 'free.fr', 'sfr.fr', 'laposte.net', 'neuf.fr', 'bbox.fr', 'aliceadsl.fr',
  // Poland
  'wp.pl', 'o2.pl', 'onet.pl', 'onet.eu', 'op.pl', 'interia.pl', 'interia.eu', 'vp.pl', 'tlen.pl',
  'poczta.fm', 'gazeta.pl',
  // Russia / Ukraine / Baltics
  'mail.ru', 'bk.ru', 'inbox.ru', 'list.ru', 'rambler.ru', 'ukr.net', 'i.ua', 'meta.ua',
  'inbox.lv', 'inbox.eu',
  // Germany / Austria / Switzerland
  'web.de', 't-online.de', 'freenet.de', 'arcor.de', 'online.de', 'mail.de',
  'bluewin.ch', 'sunrise.ch', 'hispeed.ch',
  // Czechia / Slovakia / Hungary / Croatia
  'seznam.cz', 'email.cz', 'centrum.cz', 'atlas.cz', 'post.cz', 'centrum.sk',
  'freemail.hu', 't-online.hu', 'citromail.hu', 't-com.hr', 'net.hr', 'vip.hr',
  // Bulgaria / Greece / Romania / Turkey / Israel
  'abv.bg', 'mail.bg', 'dir.bg', 'otenet.gr', 'in.gr', 'freemail.gr',
  'mynet.com', 'superonline.com', 'walla.com', 'walla.co.il', 'nana10.co.il',
  // Iberia / Portugal
  'sapo.pt', 'iol.pt', 'clix.pt', 'telefonica.net', 'telefonica.es', 'terra.es', 'wanadoo.es',
  // Benelux / Nordics
  'telenet.be', 'skynet.be', 'proximus.be', 'voo.be',
  'ziggo.nl', 'kpnmail.nl', 'home.nl', 'planet.nl', 'hetnet.nl', 'casema.nl',
  'online.no', 'broadpark.no', 'start.no', 'telia.com', 'telia.se', 'comhem.se', 'spray.se',
  'mail.dk', 'stofanet.dk',
  // Latin America
  'bol.com.br', 'uol.com.br', 'terra.com.br', 'ig.com.br', 'globo.com', 'oi.com.br',
  'prodigy.net.mx',
  // Asia-Pacific
  'naver.com', 'hanmail.net', 'daum.net', 'nate.com',
  'qq.com', '163.com', '126.com', 'sina.com', 'sohu.com', 'foxmail.com', 'aliyun.com',
  'rediffmail.com', 'sify.com',
  'hinet.net', 'pchome.com.tw',
  'ocn.ne.jp', 'plala.or.jp', 'nifty.com', 'docomo.ne.jp', 'ezweb.ne.jp', 'ybb.ne.jp', 'softbank.jp',
  'so-net.ne.jp', 'biglobe.ne.jp',
  'bigpond.com', 'bigpond.net.au', 'optusnet.com.au', 'iinet.net.au', 'tpg.com.au',
  'xtra.co.nz', 'clear.net.nz',
  // UK / Ireland
  'btinternet.com', 'btconnect.com', 'sky.com', 'ntlworld.com', 'virginmedia.com', 'talktalk.net',
  'blueyonder.co.uk', 'tiscali.co.uk',
  // North America ISPs
  'comcast.net', 'bellsouth.net', 'att.net', 'verizon.net', 'sbcglobal.net', 'cox.net', 'charter.net',
  'earthlink.net', 'juno.com', 'netzero.net', 'roadrunner.com', 'rr.com', 'optonline.net',
  'windstream.net', 'frontier.com', 'frontiernet.net', 'centurylink.net', 'embarqmail.com', 'aim.com',
  'netscape.net', 'pacbell.net', 'ameritech.net', 'swbell.net', 'prodigy.net', 'suddenlink.net',
  'mindspring.com', 'rogers.com', 'videotron.ca', 'telus.net', 'bellnet.ca', 'sympatico.ca', 'shaw.ca',
  'cogeco.ca',
  // Africa
  'mweb.co.za', 'telkomsa.net', 'vodamail.co.za', 'webmail.co.za',
  // Ireland / Israel, found by the audit that ran after the first fix.
  'eircom.net', 'iol.ie', 'indigo.ie',
  '012.net.il', 'netvision.net.il', 'bezeqint.net', 'zahav.net.il',
  'com.com',                                               // a parked junk host, never an employer
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

// yahoo.com.tw -> 'yahoo'. The registrable domain is what reaches here, so the first label is the
// provider's brand rather than a subdomain.
const firstLabel = (d) => normHost(d).split('.')[0];

// A consumer mailbox, by either mechanism. FREEMAIL_PREFIXES is what makes this hold up against ccTLDs
// the list has never seen.
function isFreemail(dom) {
  const d = normHost(dom);
  if (!d) return false;
  return FREEMAIL_HOSTS.has(d) || FREEMAIL_FAMILIES.has(firstLabel(d));
}

// True when the host can never be any company's primary domain. Same meaning as the original
// companies.js helper for every host it already knew, so the Google Maps / ATP / Bing loaders behave as
// before -- it just knows a great many more consumer hosts now.
function isBadCompanyDomain(dom) {
  const d = normHost(dom);
  return !d || isFreemail(d) || PLATFORM_HOSTS.has(d);
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
  if (isFreemail(d)) return '';
  if (PLATFORM_HOSTS.has(d) && hostOfEmail(email) !== d) return '';
  return d;
}

// Prefix forms of the family rule, for building an OpenSearch query: a terms query can only list the
// hosts that are spelled out, so a scan looking for consumer mailboxes needs these as well.
const FREEMAIL_PREFIXES = [...FREEMAIL_FAMILIES].map((f) => f + '.');

module.exports = {
  FREEMAIL_HOSTS, FREEMAIL_FAMILIES, FREEMAIL_PREFIXES, PLATFORM_HOSTS, NON_COMPANY_DOMAINS,
  normHost, firstLabel, hostOfEmail, isFreemail, isBadCompanyDomain, employerDomainFor,
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
  // The 2026-11-19 blind spot: every one of these was the head of the domainless population and every
  // one would have been filed as an employer.
  for (const d of ['libero.it', 'orange.fr', 'wp.pl', 'mail.ru', 'web.de', 'seznam.cz', 'abv.bg',
    'naver.com', 't-online.de', 'virgilio.it', 'comcast.net', 'btinternet.com', 'bigpond.com',
    'prodigy.net.mx', 'telkomsa.net', 'rediffmail.com', 'ocn.ne.jp', 'ziggo.nl', 'onmicrosoft.com']) {
    eq('consumer host ' + d, employerDomainFor(d, 'someone@' + d), '');
  }
  // The family rule has to cover ccTLD variants the list never enumerates.
  for (const d of ['yahoo.es', 'yahoo.com.tw', 'hotmail.fr', 'outlook.com.br', 'live.nl', 'live.com.mx',
    'hotmail.co.th', 'outlook.dk', 'myyahoo.com', 'windowslive.com']) {
    eq('family rule ' + d, employerDomainFor(d, 'someone@' + d), '');
  }
  // Typo-squats are mailboxes, never employers.
  for (const d of ['gamil.com', 'gmai.com', 'gmil.com', 'gmail.co', 'gmail.con', 'getimel.com']) {
    eq('typo squat ' + d, employerDomainFor(d, 'someone@' + d), '');
  }
  // ...and the long tail of genuine employers must still pass. These all appeared in the same sample.
  for (const d of ['kw.com', 'deped.gov.ph', 'alshaya.com', 'amplifon.com', 'chipotle.com',
    'statefarm.com', 'allianz.de', 'axa.fr', 'tecnocasa.es', 'estes-express.com', 'exprealty.com']) {
    eq('real employer ' + d, employerDomainFor(d, 'x@' + d), d);
  }
  // A company whose name merely starts with a provider's label must not be swept up by the family rule.
  eq('livenation is not live.*', employerDomainFor('livenation.com', 'x@livenation.com'), 'livenation.com');
  eq('aolcorp is not aol.*', employerDomainFor('aolcorp.co.uk', 'x@aolcorp.co.uk'), 'aolcorp.co.uk');
  console.log('non-company-domains selftest: ' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
}
