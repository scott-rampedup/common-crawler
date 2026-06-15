document.getElementById('forgotForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const msg = document.getElementById('msg');
  const btn = e.target.querySelector('button[type="submit"]');
  msg.textContent = '';
  msg.className = 'auth-msg';
  const email = document.getElementById('email').value.trim();
  btn.disabled = true;
  try {
    const res = await fetch('/api/auth/forgot', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) {
      msg.textContent = out.error || 'Something went wrong — please try again.';
      msg.className = 'auth-msg err';
      btn.disabled = false;
      return;
    }
    // generic message — we never reveal whether the email matched an account
    msg.textContent = out.message || 'If that email matches an account, we’ve sent reset instructions.';
    msg.className = 'auth-msg ok';
  } catch (err) {
    msg.textContent = 'Network error — please try again.';
    msg.className = 'auth-msg err';
    btn.disabled = false;
  }
});
