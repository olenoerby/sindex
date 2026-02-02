// Shared utilities for all pages

// Format numbers with K/M suffixes
function formatNumber(num) {
  if (!num) return '0';
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
}

// Format date relative to now
function formatDate(dateStr) {
  if (!dateStr) return 'Unknown';
  // Handle Unix timestamps (seconds) - convert to milliseconds
  const timestamp = typeof dateStr === 'number' ? dateStr * 1000 : dateStr;
  const date = new Date(timestamp);
  const now = new Date();
  const days = Math.floor((now - date) / (1000 * 60 * 60 * 24));
  
  if (days === 0) return 'Today';
  if (days === 1) return 'Yesterday';
  if (days < 7) return `${days} days ago`;
  if (days < 30) return `${Math.floor(days / 7)} weeks ago`;
  if (days < 365) return `${Math.floor(days / 30)} months ago`;
  return `${Math.floor(days / 365)} years ago`;
}

// Decode HTML entities
function decodeHtml(html) {
  const txt = document.createElement('textarea');
  txt.innerHTML = html;
  return txt.value;
}

// Escape HTML for safe display
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text || '';
  return div.innerHTML;
}

// Normalize user-entered subreddit queries to a bare subreddit name
// Accepts forms like '/r/name', 'r/name', 'https://reddit.com/r/name', or 'rname'
function normalizeQuery(raw) {
  try {
    const qraw = String(raw || '').trim();
    // strip full reddit URL
    let q = qraw.replace(/^https?:\/\/(?:www\.)?reddit\.com\/r\//i, '');
    // strip leading /r/ or r/ 
    q = q.replace(/^\/?r\//i, '').trim();
    // accept shorthand rname
    const m = qraw.match(/^r([A-Za-z0-9_]{3,21})$/i);
    if (m && m[1]) q = m[1];
    return q;
  } catch(e) {
    return String(raw || '').trim();
  }
}

// Cookie functions
function setCookie(name, value, days) {
  const date = new Date();
  date.setTime(date.getTime() + days * 24 * 60 * 60 * 1000);
  document.cookie = `${name}=${value};expires=${date.toUTCString()};path=/`;
}

function getCookie(name) {
  const nameEQ = name + '=';
  const cookies = document.cookie.split(';');
  for (let c of cookies) {
    c = c.trim();
    if (c.indexOf(nameEQ) === 0) return c.substring(nameEQ.length);
  }
  return null;
}

// Age gate modal
function showAgeGate(onConfirm) {
  const overlay = document.createElement('div');
  overlay.className = 'age-modal-overlay';
  const box = document.createElement('div');
  box.className = 'age-modal-box';
  const h = document.createElement('h2');
  h.textContent = 'Age confirmation';
  const p = document.createElement('p');
  p.textContent = 'This page may contain explicit (18+) content. Please confirm that you are of the required age to visit this website.';
  const actions = document.createElement('div');
  actions.className = 'age-modal-actions';
  const confirmBtn = document.createElement('button');
  confirmBtn.className = 'btn';
  confirmBtn.textContent = 'I am 18+';
  const leaveBtn = document.createElement('button');
  leaveBtn.className = 'btn btn-ghost';
  leaveBtn.textContent = 'Leave';
  actions.appendChild(leaveBtn);
  actions.appendChild(confirmBtn);
  box.appendChild(h);
  box.appendChild(p);
  box.appendChild(actions);
  overlay.appendChild(box);
  document.body.appendChild(overlay);
  document.body.style.overflow = 'hidden';

  confirmBtn.addEventListener('click', () => {
    try { setCookie('sindex_age_confirmed', '1', 365); } catch(e) {}
    overlay.remove();
    document.body.style.overflow = '';
    onConfirm();
  });

  leaveBtn.addEventListener('click', () => {
    try { overlay.remove(); } catch(e) {}
    window.location.href = 'about:blank';
  });

  confirmBtn.focus();
}

// Check age confirmation and run callback
function initWithAgeGate(callback) {
  try {
    const confirmed = getCookie('sindex_age_confirmed');
    if (confirmed === '1') {
      callback();
      return;
    }
  } catch(e) { /* ignore and show gate */ }
  showAgeGate(callback);
}
