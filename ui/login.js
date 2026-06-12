document.getElementById('loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const msg = document.getElementById('msg');
  msg.textContent = '';
  msg.className = 'auth-msg';
  const username = document.getElementById('username').value.trim();
  const password = document.getElementById('password').value;
  try {
    const res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) {
      msg.textContent = out.error || 'Sign in failed.';
      msg.className = 'auth-msg err';
      return;
    }
    // analysts/admins land on the Master Database; plain users on Search.
    window.location.href = out.role === 'user' ? '/search' : '/';
  } catch (err) {
    msg.textContent = 'Network error — please try again.';
    msg.className = 'auth-msg err';
  }
});
