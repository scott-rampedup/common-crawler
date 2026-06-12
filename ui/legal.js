(async function () {
  const key = window.location.pathname === '/terms' ? 'terms' : 'privacy';
  const titleEl = document.getElementById('legalTitle');
  const bodyEl = document.getElementById('legalBody');
  try {
    const res = await fetch('/api/pages/' + key);
    const out = await res.json();
    const title = out.title || (key === 'terms' ? 'Terms of Use' : 'Privacy Policy');
    titleEl.textContent = title;
    document.title = title + ' · Common Crawler';
    // textContent + CSS white-space:pre-wrap preserves the admin's formatting safely (no HTML injection).
    bodyEl.textContent = out.content || '';
  } catch (e) {
    titleEl.textContent = 'Unavailable';
    bodyEl.textContent = 'Unable to load this page right now.';
  }
})();
