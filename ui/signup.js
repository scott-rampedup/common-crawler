document.getElementById('signupForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const msg = document.getElementById('msg');
  msg.textContent = '';
  msg.className = 'auth-msg';
  const val = (id) => document.getElementById(id).value.trim();

  if (!document.getElementById('agree').checked) {
    msg.textContent = 'Please accept the Privacy Policy and Terms of Use to continue.';
    msg.className = 'auth-msg err';
    return;
  }

  const body = {
    first: val('first'), last: val('last'), company: val('company'), title: val('title'),
    email: val('email'), phone: val('phone'), username: val('username'),
    password: document.getElementById('password').value, agree: true,
  };

  try {
    const res = await fetch('/api/auth/signup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) {
      msg.textContent = out.error || 'Sign up failed.';
      msg.className = 'auth-msg err';
      return;
    }
    document.getElementById('signupForm').style.display = 'none';
    msg.textContent = "Thanks! Your account is pending administrator approval. You'll be able to sign in once it's activated.";
    msg.className = 'auth-msg ok';
  } catch (err) {
    msg.textContent = 'Network error — please try again.';
    msg.className = 'auth-msg err';
  }
});
