// List page (list.html) - Advanced search and table view functionality

let data = [];
let filteredCount = 0; // number matching current filters (from server)
let dbTotal = 0; // total number of subreddits in DB
let currentPage = 1;
let perPage = (window.innerWidth <= 800) ? 25 : 50; // smaller pages on mobile
let currentSort = 'mentions';
let currentSortDir = 'desc'; // 'asc' or 'desc'
let prefs = {};
let randomOrder = null; // array of subreddit names preserving a shuffled order
let paginationDisabled = false; // when true, hide pagination controls (no pages available)
let isLoadingPage = false; // Guard to prevent concurrent loadPage calls

function savePrefs(){
  try{
    const rawQ = (document.getElementById('q') && document.getElementById('q').value) ? String(document.getElementById('q').value).trim() : '';
    const p = {
      q: normalizeQuery(rawQ),
      minMentions: document.getElementById('minMentions') ? document.getElementById('minMentions').value : '',
      maxMentions: document.getElementById('maxMentions') ? document.getElementById('maxMentions').value : '',
      minSubscribers: document.getElementById('minSubscribers') ? document.getElementById('minSubscribers').value : '',
      maxSubscribers: document.getElementById('maxSubscribers') ? document.getElementById('maxSubscribers').value : '',
      firstMentionedPreset: document.getElementById('firstMentionedPreset') ? document.getElementById('firstMentionedPreset').value : '',
      customDays: document.getElementById('customDays') ? document.getElementById('customDays').value : '',
      showAvailable: document.getElementById('showAvailable') ? !!document.getElementById('showAvailable').checked : false,
      showBanned: document.getElementById('showBanned') ? !!document.getElementById('showBanned').checked : false,
      showPending: document.getElementById('showPending') ? !!document.getElementById('showPending').checked : true,
      showNSFW: document.getElementById('showNSFW') ? !!document.getElementById('showNSFW').checked : false,
      showNonNSFW: document.getElementById('showNonNSFW') ? !!document.getElementById('showNonNSFW').checked : false,
      listing: document.getElementById('listing') ? document.getElementById('listing').value : '',
      currentSort, currentSortDir,
      randomOrder: randomOrder || null,
      currentPage: currentPage || 1,
      perPage: perPage || 50
    };
    setCookie('sindex_prefs', encodeURIComponent(JSON.stringify(p)), 365);
    // Mirror search query to a shared cookie so other pages can read it
    try{ setCookie('sindex_search', encodeURIComponent(p.q || ''), 365); }catch(e){}
  }catch(e){ /* ignore */ }
}

function loadPrefs(){
  try{
    const c = getCookie('sindex_prefs');
    if(!c) return;
    const p = JSON.parse(decodeURIComponent(c));
    prefs = p || {};
    if(document.getElementById('minMentions')) document.getElementById('minMentions').value = prefs.minMentions || '';
    if(document.getElementById('maxMentions')) document.getElementById('maxMentions').value = prefs.maxMentions || '';
    if(document.getElementById('minSubscribers')) document.getElementById('minSubscribers').value = prefs.minSubscribers || '';
    if(document.getElementById('maxSubscribers')) document.getElementById('maxSubscribers').value = prefs.maxSubscribers || '';
    if(document.getElementById('firstMentionedPreset')) document.getElementById('firstMentionedPreset').value = prefs.firstMentionedPreset || '';
    if(document.getElementById('customDays')) document.getElementById('customDays').value = prefs.customDays || '';
    if(document.getElementById('showAvailable')) document.getElementById('showAvailable').checked = !!prefs.showAvailable;
    if(document.getElementById('showBanned')) document.getElementById('showBanned').checked = !!prefs.showBanned;
    if(document.getElementById('showPending')) document.getElementById('showPending').checked = (prefs.showPending !== false);
    if(document.getElementById('showNSFW')) document.getElementById('showNSFW').checked = !!prefs.showNSFW;
    if(document.getElementById('showNonNSFW')) document.getElementById('showNonNSFW').checked = !!prefs.showNonNSFW;
    if(document.getElementById('listing')) document.getElementById('listing').value = prefs.listing || '';
    if(document.getElementById('q')) document.getElementById('q').value = prefs.q || '';
    if(prefs.currentSort) currentSort = prefs.currentSort;
    if(prefs.currentSortDir) currentSortDir = prefs.currentSortDir;
    // reflect saved sort selection in dropdown
    if(document.getElementById('sortSelect')) document.getElementById('sortSelect').value = currentSort;
    if(prefs.currentPage) currentPage = Number(prefs.currentPage) || 1;
    if(prefs.perPage) perPage = Number(prefs.perPage) || perPage;
    if(prefs.randomOrder && Array.isArray(prefs.randomOrder)) randomOrder = prefs.randomOrder;
  }catch(e){ /* ignore malformed cookie */ }
  // Prefer shared search cookie if present to populate the list search input
  try{
    const sc = getCookie('sindex_search');
    if(sc !== null && typeof sc !== 'undefined' && String(sc).length>0){
      const qEl = document.getElementById('q');
      if(qEl){ qEl.value = decodeURIComponent(sc); }
    }
  }catch(e){}
}

// Read URL query parameters and apply to UI (overrides cookie prefs when present)
function applyUrlParams(){
  try{
    const params = new URLSearchParams(window.location.search);
    const q = params.get('q');
    const page = params.get('page');
    const per = params.get('per_page') || params.get('per');
    const sort = params.get('sort');
    const sort_dir = params.get('sort_dir');
    if(q !== null && document.getElementById('q')) document.getElementById('q').value = decodeURIComponent(q);
    if(page !== null && Number.isFinite(Number(page))) currentPage = Math.max(1, Number(page));
    if(per !== null && Number.isFinite(Number(per))) perPage = Number(per);
    if(sort) currentSort = sort;
    if(sort_dir) currentSortDir = sort_dir;
    // apply filter params
    const mapBool = (v) => (v === 'true');
    ['show_available','show_banned','show_pending','show_nsfw','show_non_nsfw'].forEach(k=>{
      const v = params.get(k);
      if(v !== null){
        const id = k.replace('show_','show').replace('non_','Non'); // best-effort mapping
        // map to element ids used in page
        if(k === 'show_nsfw' && document.getElementById('showNSFW')) document.getElementById('showNSFW').checked = mapBool(v);
        if(k === 'show_non_nsfw' && document.getElementById('showNonNSFW')) document.getElementById('showNonNSFW').checked = mapBool(v);
        if(k === 'show_available' && document.getElementById('showAvailable')) document.getElementById('showAvailable').checked = mapBool(v);
        if(k === 'show_banned' && document.getElementById('showBanned')) document.getElementById('showBanned').checked = mapBool(v);
        if(k === 'show_pending' && document.getElementById('showPending')) document.getElementById('showPending').checked = mapBool(v);
      }
    });
    // numeric filters
    ['min_mentions','max_mentions','min_subscribers','max_subscribers','first_mentioned_days'].forEach(k=>{
      const v = params.get(k);
      if(v !== null){
        const id = k.replace(/_/g,'');
        try{ const el = document.getElementById(id) || document.getElementById(k); if(el) el.value = v; }catch(e){}
      }
    });
  }catch(e){ /* ignore */ }
}

function updateColumnVisibility(list){
  try{
    // Always show columns. Do not hide table columns even if no rows have data
    // for that column; this preserves stable table layout for users.
    const cols = ['title','subscribers','description','first_mentioned','last_checked'];
    cols.forEach(col => {
      document.querySelectorAll('.col-' + col).forEach(el => el.classList.remove('hidden'));
      const th = document.querySelector('th.col-' + col);
      if(th) th.classList.remove('hidden');
    });
  }catch(e){ console.warn('updateColumnVisibility failed', e); }
}

// Decode HTML entities in titles so escaped codes like &amp; display as &.
function decodeHtmlEntities(str){
  if(str === null || str === undefined) return '';
  const d = document.createElement('div');
  // Assign to innerHTML so entities are parsed, then read textContent.
  d.innerHTML = String(str);
  return d.textContent || d.innerText || '';
}

// Load a single page from the API (server-side pagination)
async function loadPage(page = 1){
  console.log('loadPage called with page:', page);
  if(isLoadingPage){
    console.log('loadPage already in progress, ignoring call');
    return;
  }
  isLoadingPage = true;
  document.getElementById('count').textContent = 'Loading...';
  const loadingDiv = document.getElementById('loading');
  // Replace the Reload button with a spinner to avoid adding an extra inline element
  const reloadBtn = document.getElementById('reload');
  let _origReload = null;
  if(reloadBtn){
    // persist original content so we can restore it
    if(!reloadBtn.dataset.origContent) reloadBtn.dataset.origContent = reloadBtn.innerHTML || '';
    _origReload = reloadBtn.dataset.origContent;
    reloadBtn.disabled = true;
    reloadBtn.innerHTML = '<span class="spinner" aria-hidden="true"></span>';
  } else {
    // fallback to the small loading div
    loadingDiv.innerHTML = '<span class="spinner" aria-hidden="true"></span>';
  }
  setControlsDisabled(true);
  try{
    const qparams = buildFilterQueryParams();
    const url = `/subreddits?page=${page}&per_page=${perPage}&sort=${encodeURIComponent(currentSort)}&sort_dir=${encodeURIComponent(currentSortDir)}${qparams}`;
    console.log('Fetching:', url);
    const res = await fetch(url);
    console.log('Response status:', res.status);
    if(!res.ok) throw new Error('HTTP '+res.status);
    const json = await res.json();
    console.log('Received data:', json);
    data = json.items || [];
    filteredCount = json.total || 0;
    dbTotal = json.db_total || dbTotal;
    currentPage = page;
    console.log('Calling render with', data.length, 'items');
    // restore reload button content
    if(reloadBtn){ try{ reloadBtn.innerHTML = reloadBtn.dataset.origContent || '↻'; reloadBtn.disabled = false; }catch(e){console.error('Error restoring reload button:', e);} }
    else { loadingDiv.innerHTML = ''; }
    render();
    console.log('Render complete');
    renderPaginationControls();
    // Update URL to reflect current state (without navigating)
    try{ syncUrlWithState(); }catch(e){}
    setControlsDisabled(false);
    isLoadingPage = false;
  }catch(e){
    console.error('loadPage error:', e);
    document.getElementById('count').textContent = 'Load failed: ' + e;
    if(reloadBtn){ try{ reloadBtn.innerHTML = reloadBtn.dataset.origContent || '↻'; reloadBtn.disabled = false; }catch(e){} }
    else { loadingDiv.innerHTML = ''; }
    setControlsDisabled(false);
    isLoadingPage = false;
  }
}

// Build a query object of current UI state
function getCurrentStateParams(){
  const params = new URLSearchParams();
  try{
    if(currentPage) params.set('page', String(currentPage));
    if(perPage) params.set('per_page', String(perPage));
    if(currentSort) params.set('sort', currentSort);
    if(currentSortDir) params.set('sort_dir', currentSortDir);
    const qraw = (document.getElementById('q') && document.getElementById('q').value) ? String(document.getElementById('q').value).trim() : '';
    if(qraw) params.set('q', normalizeQuery(qraw));
    const mm = document.getElementById('minMentions') ? document.getElementById('minMentions').value : '';
    if(mm) params.set('min_mentions', mm);
    const mM = document.getElementById('maxMentions') ? document.getElementById('maxMentions').value : '';
    if(mM) params.set('max_mentions', mM);
    const ms = document.getElementById('minSubscribers') ? document.getElementById('minSubscribers').value : '';
    if(ms) params.set('min_subscribers', ms);
    const mS = document.getElementById('maxSubscribers') ? document.getElementById('maxSubscribers').value : '';
    if(mS) params.set('max_subscribers', mS);
    // show flags
    if(document.getElementById('showAvailable')) params.set('show_available', document.getElementById('showAvailable').checked ? 'true' : 'false');
    if(document.getElementById('showBanned')) params.set('show_banned', document.getElementById('showBanned').checked ? 'true' : 'false');
    if(document.getElementById('showPending')) params.set('show_pending', document.getElementById('showPending').checked ? 'true' : 'false');
    if(document.getElementById('showNSFW')) params.set('show_nsfw', document.getElementById('showNSFW').checked ? 'true' : 'false');
    if(document.getElementById('showNonNSFW')) params.set('show_non_nsfw', document.getElementById('showNonNSFW').checked ? 'true' : 'false');
  }catch(e){}
  return params;
}

// Replace the URL in the address bar (no navigation) to reflect current state
function syncUrlWithState(){
  try{
    const params = getCurrentStateParams();
    const newUrl = window.location.pathname + '?' + params.toString();
    window.history.replaceState({}, '', newUrl);
  }catch(e){/* ignore */}
}

function buildFilterQueryParams(){
  try{
    const parts = [];
    const qraw = (document.getElementById('q') && document.getElementById('q').value) ? String(document.getElementById('q').value).trim() : '';
    const q = normalizeQuery(qraw);
    if(q) parts.push('q=' + encodeURIComponent(q));
    const mm = document.getElementById('minMentions') ? document.getElementById('minMentions').value : '';
    if(mm) parts.push('min_mentions=' + encodeURIComponent(mm));
    const mM = document.getElementById('maxMentions') ? document.getElementById('maxMentions').value : '';
    if(mM) parts.push('max_mentions=' + encodeURIComponent(mM));
    const ms = document.getElementById('minSubscribers') ? document.getElementById('minSubscribers').value : '';
    if(ms) parts.push('min_subscribers=' + encodeURIComponent(ms));
    const mS = document.getElementById('maxSubscribers') ? document.getElementById('maxSubscribers').value : '';
    if(mS) parts.push('max_subscribers=' + encodeURIComponent(mS));
    const firstMentionedPreset = document.getElementById('firstMentionedPreset') ? document.getElementById('firstMentionedPreset').value : '';
    const customDays = document.getElementById('customDays') ? document.getElementById('customDays').value : '';
    // Apply discovery date filter
    if(firstMentionedPreset === 'custom' && customDays){
      parts.push('first_mentioned_days=' + encodeURIComponent(customDays));
    } else if(firstMentionedPreset && firstMentionedPreset !== 'custom' && firstMentionedPreset !== ''){
      parts.push('first_mentioned_days=' + encodeURIComponent(firstMentionedPreset));
    }
    const showAvailable = document.getElementById('showAvailable') ? !!document.getElementById('showAvailable').checked : null;
    const showBanned = document.getElementById('showBanned') ? !!document.getElementById('showBanned').checked : null;
    const showPending = document.getElementById('showPending') ? !!document.getElementById('showPending').checked : null;
    const showNSFW = document.getElementById('showNSFW') ? !!document.getElementById('showNSFW').checked : null;
    const showNonNSFW = document.getElementById('showNonNSFW') ? !!document.getElementById('showNonNSFW').checked : null;
    if(showAvailable !== null) parts.push('show_available=' + (showAvailable ? 'true' : 'false'));
    if(showBanned !== null) parts.push('show_banned=' + (showBanned ? 'true' : 'false'));
    if(showPending !== null) parts.push('show_pending=' + (showPending ? 'true' : 'false'));
    if(showNSFW !== null) parts.push('show_nsfw=' + (showNSFW ? 'true' : 'false'));
    if(showNonNSFW !== null) parts.push('show_non_nsfw=' + (showNonNSFW ? 'true' : 'false'));
    return parts.length ? '&' + parts.join('&') : '';
  }catch(e){ return ''; }
}

// Disable/enable interactive controls while a server request is in progress
function setControlsDisabled(disabled){
  try{
    // Only disable pagination controls and the reload/sort UI while loading.
    // Keep search and filter inputs enabled so the user can adjust filters
    // even if the table data hasn't loaded yet.
    document.querySelectorAll('#paginationControls button, #paginationControls select, #paginationControls input, #paginationControlsBottom button, #paginationControlsBottom select, #paginationControlsBottom input').forEach(el => el.disabled = disabled);
    const maybe = ['reload','sortDir'];
    maybe.forEach(id => { const el = document.getElementById(id); if(el) el.disabled = disabled; });
  }catch(e){/* ignore */}
}

function renderPaginationControls(){
  console.log('renderPaginationControls called, currentPage=', currentPage);
  const top = document.getElementById('paginationControls');
  const bottom = document.getElementById('paginationControlsBottom');
  if(!top && !bottom) return;
  if(paginationDisabled){
    if(top) top.classList.add('hidden');
    if(bottom) bottom.classList.add('hidden');
    return;
  } else {
    if(top) top.classList.remove('hidden');
    if(bottom) bottom.classList.remove('hidden');
  }

  const totalPages = Math.max(1, Math.ceil((filteredCount || 0) / perPage));
  // helper to build page URL preserving filter params
  const buildPageUrl = (pageNum) => {
    try{
      const params = getCurrentStateParams();
      params.set('page', String(pageNum));
      return window.location.pathname + '?' + params.toString();
    }catch(e){ return window.location.pathname + '?page=' + String(pageNum); }
  };

  function makeControls(){
  const ctr = document.createElement('div');
  ctr.className = 'pagination-row';

    const makeLink = (text, aria, disabled, pageNum) => {
      if(disabled){ const s = document.createElement('span'); s.className = 'btn btn-ghost disabled'; s.setAttribute('aria-label', aria); s.textContent = text; return s; }
      const a = document.createElement('a'); a.className = 'btn btn-ghost'; a.textContent = text; a.href = buildPageUrl(pageNum); a.setAttribute('aria-label', aria);
      // Use AJAX navigation: prevent full page load and request via loadPage
      a.addEventListener('click', (e)=>{
        e.preventDefault();
        if(isLoadingPage) return;
        if(Number(currentPage) === Number(pageNum)) return;
        // push state so URL reflects requested page immediately
        try{ window.history.pushState({}, '', buildPageUrl(pageNum)); }catch(e){}
        loadPage(pageNum);
      });
      return a;
    };
    const first = makeLink('«', 'First page', currentPage <= 1, 1);

    const prev = makeLink('◀', 'Previous page', currentPage <= 1, Math.max(1, currentPage-1));

    const pageInput = document.createElement('input');
    pageInput.type = 'number';
    pageInput.min = 1;
    pageInput.max = totalPages;
    pageInput.className = 'muted input-small';
    pageInput.value = String(currentPage);
    pageInput.addEventListener('change', ()=>{ 
      const v = Number(pageInput.value||0); 
      if(v>=1 && v<=totalPages){ 
        if(isLoadingPage) { pageInput.value = String(currentPage); return; }
        try{ window.history.pushState({}, '', buildPageUrl(v)); }catch(e){}
        loadPage(v);
      } else { 
        pageInput.value = String(currentPage); 
      } 
    });
    const pageTotal = document.createElement('span'); pageTotal.className = 'muted ml-6'; pageTotal.textContent = '/ ' + String(totalPages);

    const next = makeLink('▶', 'Next page', currentPage >= totalPages, Math.min(totalPages, currentPage+1));
    const last = makeLink('»', 'Last page', currentPage >= totalPages, totalPages);

    ctr.appendChild(first);
    ctr.appendChild(prev);
    ctr.appendChild(pageInput);
    ctr.appendChild(pageTotal);
    ctr.appendChild(next);
    ctr.appendChild(last);
    return ctr;
  }

  if(top){ 
    console.log('Creating top pagination controls, currentPage=', currentPage);
    top.innerHTML = ''; 
    top.appendChild(makeControls()); 
  }
  if(bottom){ 
    console.log('Creating bottom pagination controls, currentPage=', currentPage);
    bottom.innerHTML = ''; 
    bottom.appendChild(makeControls()); 
  }
}

function updateSortedHeader(){
  try{
    document.querySelectorAll('th[data-sort]').forEach(th=>{
      if(th.getAttribute('data-sort') === currentSort) th.classList.add('sorted'); else th.classList.remove('sorted');
    });
    const sortLabel = (currentSortDir === 'random') ? 'Random' : (currentSortDir === 'desc' ? 'Desc' : 'Asc');
    document.getElementById('sortDir').textContent = 'Sort: ' + sortLabel;
    if(document.getElementById('sortSelect')) document.getElementById('sortSelect').value = currentSort;
  }catch(e){/* ignore */}
}

function render(){
  console.log('render() called with data:', data);
  // Server already handles all filtering and sorting - just render the data as-is
  const list = data;
  console.log('Rendering', list.length, 'items');

  const tbody = document.querySelector('#tbl tbody');
  tbody.innerHTML = '';
  for(const s of list){
    const tr = document.createElement('tr');
    tr.tabIndex = 0; // make focusable for keyboard navigation
    const nameTd = document.createElement('td');
    nameTd.classList.add('col-name');
    const a = document.createElement('a');
    const listingEl = document.getElementById('listing');
    const listing = listingEl ? listingEl.value : '';
    if(listing){
      a.href = `https://www.reddit.com/r/${encodeURIComponent(s.name)}/${listing}`;
    } else {
      a.href = `https://www.reddit.com/r/${encodeURIComponent(s.name)}`;
    }
    a.dataset.name = s.name;
    a.target = '_blank';
    a.rel = 'noopener noreferrer';
    // Check subreddit_found first - if false, it doesn't exist on Reddit (404)
    if(s.subreddit_found === false){
      // Subreddit doesn't exist - show as unavailable with "Not found"
      a.classList.add('not-found');
      const nf = document.createElement('span');
      nf.textContent = s.display_name_prefixed || ('/r/' + s.name);
      a.appendChild(nf);
      const nfSpan = document.createElement('span');
      nfSpan.textContent = ' (Not found)';
      nfSpan.className = 'muted';
      a.appendChild(nfSpan);
    } else if(s.is_banned){
      // show struck-through subreddit name for banned/private subreddits
      const nameSpan = document.createElement('span');
      nameSpan.textContent = s.display_name_prefixed || ('/r/' + s.name);
      nameSpan.classList.add('banned');
      a.appendChild(nameSpan);
    } else {
      a.textContent = s.display_name_prefixed || ('/r/' + s.name);
    }
    nameTd.appendChild(a);
    // Add status badges (NEW, NSFW) next to the subreddit link in the list view
    try {
      const badgesSpan = document.createElement('span');
      badgesSpan.className = 'inline-badges';
      const nowTs = Math.floor(Date.now() / 1000);
      const firstMention = s.first_mentioned ? Number(s.first_mentioned) : (s.created_utc ? Number(s.created_utc) : 0);
      const THIRTY_DAYS_SEC = 30 * 24 * 60 * 60;
      // NEW badge: appeared within last 30 days
      if (firstMention && (nowTs - firstMention) <= THIRTY_DAYS_SEC) {
        const nb = document.createElement('span'); nb.className = 'new-badge'; nb.textContent = 'NEW'; badgesSpan.appendChild(nb);
      }
      // NSFW badge: when flagged as over18 (support both field names)
      if (s.over18 === true || s.is_over18 === true || String(s.over18) === 'true' || String(s.is_over18) === 'true') {
        const ns = document.createElement('span'); ns.className = 'nsfw-badge'; ns.textContent = 'NSFW'; badgesSpan.appendChild(ns);
      }
      if (badgesSpan.children.length > 0) nameTd.appendChild(badgesSpan);
    } catch(e) { /* ignore DOM errors */ }
    tr.appendChild(nameTd);

    const titleTd = document.createElement('td');
    titleTd.classList.add('col-title');
    const isUnprocessed = (s.title === null || s.title === undefined) && !s.is_banned && s.subreddit_found !== false;
    if (isUnprocessed) {
      const sp = document.createElement('span'); sp.className = 'muted'; sp.textContent = 'Pending metadata update';
      titleTd.appendChild(sp);
    } else {
      const rawTitle = (s.title === null || s.title === undefined) ? '' : (s.title || '—');
      titleTd.textContent = rawTitle === '' ? '' : decodeHtmlEntities(rawTitle);
    }
    tr.appendChild(titleTd);
    const mk = (v)=> v===null||v===undefined? '—': v.toString();
    const mentionsTd = document.createElement('td');
    mentionsTd.classList.add('col-mentions');
    if (isUnprocessed) {
      // hide zero mentions for unprocessed subreddits
      if (s.mentions && Number(s.mentions) > 0) {
        mentionsTd.textContent = String(s.mentions);
      } else {
        mentionsTd.textContent = '';
      }
    } else {
      if (s.mentions === null || s.mentions === undefined) {
        mentionsTd.textContent = '';
      } else {
        mentionsTd.textContent = String(s.mentions);
      }
    }
    tr.appendChild(mentionsTd);
    const subsTd = document.createElement('td');
    subsTd.classList.add('col-subscribers');
    // For banned subreddits we show an em-dash to indicate unavailable subscriber count
    if (s.is_banned) {
      subsTd.textContent = '—';
    } else if (isUnprocessed) {
      // Do not show any placeholder for unprocessed subreddits to avoid affecting table sorting
      subsTd.textContent = '';
    } else {
      // When subscriber count is unknown, leave cell empty (do not show 'N/A')
      if (s.subscribers === null || s.subscribers === undefined) {
        subsTd.textContent = '';
      } else {
        subsTd.textContent = String(s.subscribers);
      }
    }
    tr.appendChild(subsTd);
    const firstTd = document.createElement('td');
    firstTd.classList.add('col-first_mentioned','muted');
    if (s.first_mentioned) {
      const mentionDate = new Date(s.first_mentioned*1000);
      const dateStr = mentionDate.toLocaleDateString();
      // Add golden star if mentioned within last 7 days
      const nowTs = Math.floor(Date.now() / 1000);
      const sevenDaysAgo = nowTs - (7 * 24 * 60 * 60);
      if (s.first_mentioned >= sevenDaysAgo) {
        firstTd.textContent = dateStr + ' ⭐';
      } else {
        firstTd.textContent = dateStr;
      }
    } else {
      firstTd.textContent = '';
    }
    tr.appendChild(firstTd);
    const descTd = document.createElement('td'); descTd.classList.add('col-description','muted');
    const descText = (s.description === null || s.description === undefined) ? '' : String(s.description);
    const publicDescHtml = (s.public_description_html === null || s.public_description_html === undefined) ? '' : String(s.public_description_html);
    const hasDescription = (publicDescHtml && publicDescHtml.trim().length > 0) || (descText && descText.trim().length > 0);
    const isScanned = (s.last_checked !== null && s.last_checked !== undefined);

    if(isUnprocessed){
      descTd.textContent = '—';
    } else {
      // Determine if subreddit was scanned but has no description text
      const isNoDescription = isScanned && !hasDescription;
      // Always render a button that opens the description modal so users
      // can view the full description regardless of its length.
      const btn = document.createElement('button');
      btn.className = 'btn btn-ghost';
      if(isNoDescription){
        btn.textContent = 'No description';
      } else if((publicDescHtml || descText) && (publicDescHtml || descText).length > 32){
        btn.textContent = (publicDescHtml || descText).slice(0,32) + '...';
      } else {
        btn.textContent = (publicDescHtml || descText) || '—';
      }
      btn.addEventListener('click', ()=>{
        if(isNoDescription){
          openDescriptionModal('This subreddit does not have a descriptive text', s.last_checked);
        } else {
          openDescriptionModal(publicDescHtml || descText || '', s.last_checked);
        }
      });
      descTd.appendChild(btn);
    }
    tr.appendChild(descTd);
    tbody.appendChild(tr);
    // keyboard navigation: Enter opens description; Arrow keys move
    tr.addEventListener('keydown', (e)=>{
      if(e.key === 'Enter'){
        // open description modal for this row
        const desc = (s.public_description_html && s.public_description_html.length>0) ? s.public_description_html : (s.description || '');
        openDescriptionModal(desc || 'No description available', s.last_checked);
      } else if(e.key === 'ArrowDown'){
        e.preventDefault();
        const next = tr.nextElementSibling;
        if(next) next.focus();
      } else if(e.key === 'ArrowUp'){
        e.preventDefault();
        const prev = tr.previousElementSibling;
        if(prev) prev.focus();
      }
    });
  }
  // If list is empty, show a full-width row with a Reset button so users can quickly restore filters
  if(list.length === 0){
    const trEmpty = document.createElement('tr');
    const tdEmpty = document.createElement('td');
    tdEmpty.colSpan = 6;
    tdEmpty.className = 'empty-cell';
    const btn = document.createElement('button');
    btn.className = 'btn btn-ghost';
    btn.textContent = 'Reset all filters';
    btn.addEventListener('click', ()=>{
      try{ const hdr = document.getElementById('resetFilters'); if(hdr) hdr.click(); }catch(e){}
    });
    tdEmpty.appendChild(btn);
    trEmpty.appendChild(tdEmpty);
    tbody.appendChild(trEmpty);
  }
  try{
    if(Array.isArray(list) && list.length === 0){
      document.getElementById('count').textContent = `Showing 0 out of ${dbTotal} in database`;
    } else {
      document.getElementById('count').textContent = `Showing ${filteredCount} out of ${dbTotal} in database`;
    }
  }catch(e){/* ignore DOM errors */}
  // update header count as well (show overall DB total)
  updateHeaderCount(dbTotal);
  updateColumnVisibility(list);
  // Persist current filter/sort/listing preferences so back-navigation preserves state
  try{ savePrefs(); }catch(e){}
  // Show a helpful hint when no results are present to explain why
  try{
    const hintEl = document.getElementById('emptyHint');
    if(!hintEl) throw 0;
    if(list.length === 0){
      hintEl.textContent = 'No results — try widening your filters (search, mentions, subscribers, availability, NSFW).';
      paginationDisabled = false;
    } else {
      hintEl.textContent = '';
      paginationDisabled = false;
    }
  }catch(e){/* ignore */}
}

// Periodically refresh the total subreddit count without reloading the page.
async function refreshTotalCount(){
  try{
    // Get overall DB totals from /stats so we can show "filtered out of X in database"
    const res = await fetch('/stats');
    if(!res.ok) return;
    const json = await res.json();
    const newDbTotal = json.total_subreddits || 0;
    if(newDbTotal !== dbTotal){
      dbTotal = newDbTotal;
    }
    // update displayed counts; if paginationDisabled (client-side filters hide everything)
    // show explicit zero count instead of using server-side `filteredCount` which may be non-zero.
    try{
      if(paginationDisabled){
        document.getElementById('count').textContent = `Showing 0 out of ${dbTotal} in database`;
      } else {
        document.getElementById('count').textContent = `Showing ${filteredCount} out of ${dbTotal} in database`;
      }
    }catch(e){/* ignore DOM errors */}
    updateHeaderCount(dbTotal);
  }catch(e){ /* ignore errors */ }
}

// refresh every 10 seconds and once immediately
setInterval(refreshTotalCount, 10000);
refreshTotalCount();

// debounce utility for input-driven requests
function debounce(fn, wait){
  let t = null;
  return function(...args){
    if(t) clearTimeout(t);
    t = setTimeout(()=> fn.apply(this, args), wait);
  };
}

// Debounced loader used for frequent input changes (typing)
const debouncedLoadPage = debounce(()=>{ try{ savePrefs(); }catch(e){} loadPage(1); }, 350);

const qEl = document.getElementById('q');
const clearQueryBtn = document.getElementById('clearQuery');
if(qEl){
  qEl.addEventListener('input', (e)=>{
    try{ 
      if(clearQueryBtn) clearQueryBtn.classList.toggle('hidden', !(qEl.value && qEl.value.length>0)); 
    }catch(err){ console.error('Clear button error:', err); }
    try{ setCookie('sindex_search', encodeURIComponent((qEl.value||'').trim()), 365); }catch(e){}
    try{
      debouncedLoadPage();
    }catch(err){ console.error('Search error:', err); }
  });
}
if(clearQueryBtn){
  clearQueryBtn.addEventListener('click', ()=>{ 
    if(qEl) qEl.value = ''; 
    if(clearQueryBtn) clearQueryBtn.classList.add('hidden'); 
    try{ setCookie('sindex_search', encodeURIComponent(''), 365); }catch(e){}
    try{ savePrefs(); }catch(e){} 
    loadPage(1); 
  });
}

// About button: opens modal; move between header and filter popout on resize
(function(){
  const explainId = 'explainBtn';
  const infoText = 'How data is collected:\n\n- The scanner periodically reads recent Reddit posts and extracts references to subreddits.\n- Mentioned subreddits are recorded when a subreddit URL is named in a post or comment the scanner processes.\n\nWhat is a mention?\n\n- This is the count of distinct subreddits detected in the collected data, attempting to only counting a mention once per user who submitted it.\n\nUpdate frequency:\n\n- The scanner runs on a schedule (configured in deployment). The site reflects the last scanner run.';

  function ensureButton(){
    let btn = document.getElementById(explainId);
    if(!btn){
      btn = document.createElement('button');
      btn.id = explainId;
      btn.className = 'info-btn';
      btn.title = 'About';
      btn.setAttribute('aria-label','About');
      btn.textContent = 'About';
      // default place in header if possible
      const h = document.querySelector('h1');
      if(h) h.insertBefore(btn, document.getElementById('headerSubCount'));
    }
    btn.onclick = ()=> openDescriptionModal(infoText, null);
    return btn;
  }

  function placeExplainButton(){
    const btn = ensureButton();
    const filterPop = document.getElementById('filterPop');
    // mobile: move into filter popout (menu); desktop: place in header and push right
    if(window.innerWidth <= 800 && filterPop){
      if(filterPop.contains(btn)) return;
      filterPop.insertBefore(btn, filterPop.firstChild);
      btn.classList.remove('ml-auto');
    } else {
      const h = document.querySelector('h1');
      if(!h) return;
      if(h.contains(btn)){
        // ensure pushed to right
        btn.classList.add('ml-auto');
        return;
      }
      h.insertBefore(btn, document.getElementById('headerSubCount'));
      btn.classList.add('ml-auto');
    }
  }

  // initial placement and react to resize
  placeExplainButton();
  window.addEventListener('resize', ()=>{ placeExplainButton(); });
})();

/*
 Example: enable the RTL animation plugin for Chart.js
 Add `plugins: { rtlAnimation: { enabled: true, step: 30 } }` to your
 chart config. To disable animations entirely set `options.animation = false`.

 Example usage (replace labels/data with your real values):
 const ctx = document.getElementById('myChart').getContext('2d');
 const exampleChart = new Chart(ctx, {
   type: 'line',
   data: {
     labels: ['a','b','c'], // replace
     datasets: [{ label: 'Example', data: [1,2,3] }]
   },
   options: {
     // disable all animations for this chart:
     // animation: false,
     // or enable the RTL plugin to animate new points from right->left
     plugins: {
       rtlAnimation: { enabled: true, step: 30 }
     }
   }
 });
*/

// Options integrated into filter popout; old Options popout removed.
// header click sorting: set sort key and toggle direction if same key
document.querySelectorAll('th[data-sort]').forEach(th => {
  th.classList.add('clickable');
  th.addEventListener('click', ()=>{
    const key = th.getAttribute('data-sort');
    if(!key) return;
    if(currentSort === key){
      currentSortDir = (currentSortDir === 'desc') ? 'asc' : 'desc';
    }else{
      currentSort = key;
      currentSortDir = 'desc';
    }
    updateSortedHeader();
    // Request the current page from the server using the new sort so pagination reflects it
    // Request current page using server-side sorting
    if(document.getElementById('sortSelect')) document.getElementById('sortSelect').value = currentSort;
    savePrefs();
    loadPage(currentPage);
  });
});
document.getElementById('minMentions').addEventListener('input', (e) => {
  const el = e.target;
  const val = Number(el.value || 0);
  if (!Number.isFinite(val) || val < 0) el.value = 0;
  debouncedLoadPage();
});
const maxEl = document.getElementById('maxMentions');
if(maxEl){
  maxEl.addEventListener('input', (e) => {
    const el = e.target;
    if(el.value === ''){ debouncedLoadPage(); return; }
    const val = Number(el.value || 0);
    if(!Number.isFinite(val) || val < 1) el.value = 1;
    debouncedLoadPage();
  });
}
// subscriber filters: validate and re-render on input
const minSubsEl = document.getElementById('minSubscribers');
  if(minSubsEl){
  minSubsEl.addEventListener('input', (e)=>{
    const el = e.target;
    if(el.value === ''){ debouncedLoadPage(); return; }
    const val = Number(el.value || 0);
    if(!Number.isFinite(val) || val < 0) el.value = 0;
    debouncedLoadPage();
  });
}
const maxSubsEl = document.getElementById('maxSubscribers');
if(maxSubsEl){
  maxSubsEl.addEventListener('input', (e)=>{
    const el = e.target;
    if(el.value === ''){ debouncedLoadPage(); return; }
    const val = Number(el.value || 0);
    if(!Number.isFinite(val) || val < 0) el.value = 0;
    debouncedLoadPage();
  });
}

// Filter popout open/close and clear behavior
const filterBtn = document.getElementById('filterBtn');
const filterPop = document.getElementById('filterPop');
  if(filterBtn && filterPop){
  console.log('list.js: filterBtn and filterPop found');
  filterBtn.addEventListener('click', (ev)=>{
    ev.stopPropagation();
    console.log('list.js: filterBtn clicked, hidden?', filterPop.classList.contains('hidden'));
    // Toggle visibility: when opening, position the popout below the button
    if(!filterPop.classList.contains('hidden')){
      filterPop.classList.add('hidden');
      return;
    }
    try{
      // Move the popout into a positioned ancestor so CSS can place it.
      let anchor = filterBtn.parentElement || document.body;
      if(anchor === filterPop) anchor = filterPop.parentElement || document.body;
      // ensure anchor is positioned
      anchor.classList.add('filter-anchor');
      if(filterPop.parentElement !== anchor) anchor.appendChild(filterPop);
    }catch(e){
      console.warn('list.js: error appending filterPop to anchor', e);
    }
    // Remove hidden class so CSS can show it
    filterPop.classList.remove('hidden');
    // Clear any previous inline positioning so CSS can take over when
    // the popout is appended into a positioned ancestor (anchor).
    filterPop.style.left = '';
    filterPop.style.top = '';
    filterPop.style.position = '';
    filterPop.style.zIndex = '';
    // Fallback: only apply absolute document positioning when the popout
    // is attached directly to document.body. If it's inside a positioned
    // anchor element, let CSS (via .filter-anchor) handle placement.
    try{
      const rect = filterBtn.getBoundingClientRect();
      const parentEl = filterPop.parentElement || document.body;
      if(parentEl === document.body){
        filterPop.style.position = 'absolute';
        filterPop.style.left = (rect.left + window.scrollX) + 'px';
        filterPop.style.top = (rect.bottom + window.scrollY + 8) + 'px';
        filterPop.style.zIndex = '2000';
        console.log('list.js: filterPop positioned at', filterPop.style.left, filterPop.style.top);
      } else {
        console.log('list.js: using CSS positioning inside anchor element', parentEl);
      }
    }catch(e){ console.warn('list.js: positioning fallback failed', e); }
    // filters now contain the Options section directly
  });
  // close when clicking outside
  document.addEventListener('click', (ev)=>{
    if(!filterPop.classList.contains('hidden') && !filterPop.contains(ev.target) && ev.target !== filterBtn){
      filterPop.classList.add('hidden');
    }
  });
  // prevent clicks inside popout from closing
  filterPop.addEventListener('click', (ev)=> ev.stopPropagation());
  const closePop = document.getElementById('closeFilterPop');
  if(closePop) closePop.addEventListener('click', ()=> filterPop.classList.add('hidden'));
  // wire show toggles and the popout Show/Hide-all button
  const showAvailableEl = document.getElementById('showAvailable');
  const showBannedEl = document.getElementById('showBanned');
  const showNSFWEl = document.getElementById('showNSFW');
  const showNonNSFWEl = document.getElementById('showNonNSFW');
  
  const showAllBtn = document.getElementById('showAll');
  function updateShowAllLabel(){
    if(!showAllBtn) return;
    const sa = showAvailableEl ? showAvailableEl.checked : false;
    const sb = showBannedEl ? showBannedEl.checked : false;
    const sn = showNSFWEl ? showNSFWEl.checked : false;
    const snt = showNonNSFWEl ? showNonNSFWEl.checked : false;
    showAllBtn.textContent = (sa && sb && sn && snt) ? 'Hide all' : 'Show all';
  }
  if(showAvailableEl) showAvailableEl.addEventListener('change', (e)=>{
    updateShowAllLabel();
    try{ savePrefs(); }catch(err){}
    loadPage(1);
  });
  if(showBannedEl) showBannedEl.addEventListener('change', (e)=>{
    updateShowAllLabel();
    try{ savePrefs(); }catch(err){}
    loadPage(1);
  });
  const showPendingEl = document.getElementById('showPending');
  if(showPendingEl) showPendingEl.addEventListener('change', (e)=>{
    try{ savePrefs(); }catch(err){}
    loadPage(1);
  });
  if(showNSFWEl) showNSFWEl.addEventListener('change', (e)=>{ updateShowAllLabel(); try{ savePrefs(); }catch(err){} loadPage(1); });
  if(showNonNSFWEl) showNonNSFWEl.addEventListener('change', (e)=>{ updateShowAllLabel(); try{ savePrefs(); }catch(err){} loadPage(1); });
  if(showAllBtn){
    updateShowAllLabel();
    showAllBtn.addEventListener('click', ()=>{
      const sa = showAvailableEl ? showAvailableEl.checked : false;
      const sb = showBannedEl ? showBannedEl.checked : false;
      const sn = showNSFWEl ? showNSFWEl.checked : false;
      const snt = showNonNSFWEl ? showNonNSFWEl.checked : false;
      const allOn = sa && sb && sn && snt;
      const newState = !allOn;
      if(showAvailableEl) showAvailableEl.checked = newState;
      if(showBannedEl) showBannedEl.checked = newState;
      if(showNSFWEl) showNSFWEl.checked = newState;
      if(showNonNSFWEl) showNonNSFWEl.checked = newState;
      updateShowAllLabel();
      try{ savePrefs(); }catch(e){}
      loadPage(1);
    });
    // wire the Reset button inside the popout to reuse the header Reset behavior
    const resetPopBtn = document.getElementById('resetFiltersPop');
    if(resetPopBtn){
      resetPopBtn.addEventListener('click', ()=>{
        try{
          const hdr = document.getElementById('resetFilters');
          if(hdr) hdr.click();
        }catch(e){}
        // close the popout after resetting
        try{ filterPop.classList.add('hidden'); }catch(e){}
      });
    }
  }
}
document.getElementById('reload').addEventListener('click', async ()=>{
  try{
    await Promise.all([refreshTotalCount().catch(()=>{}), loadPage(currentPage).catch(()=>{})]);
  }catch(e){/* ignore */}
});
document.getElementById('sortDir').addEventListener('click', ()=>{
  if(currentSortDir === 'desc') currentSortDir = 'asc';
  else if(currentSortDir === 'asc') currentSortDir = 'random';
  else currentSortDir = 'desc';
  // Request current page using server-side sorting (server handles random ordering too)
  updateSortedHeader();
  savePrefs();
  loadPage(currentPage);
});

  document.getElementById('resetFilters').addEventListener('click', ()=>{
  document.getElementById('q').value = '';
  document.getElementById('minMentions').value = '';
  if(document.getElementById('maxMentions')) document.getElementById('maxMentions').value = '';
  if(document.getElementById('minSubscribers')) document.getElementById('minSubscribers').value = '';
  if(document.getElementById('maxSubscribers')) document.getElementById('maxSubscribers').value = '';
  if(document.getElementById('firstMentionedPreset')) document.getElementById('firstMentionedPreset').value = '';
  if(document.getElementById('customDays')) document.getElementById('customDays').value = '';
  if(document.getElementById('customDaysLabel')) document.getElementById('customDaysLabel').classList.add('hidden');
  if(document.getElementById('showAvailable')) document.getElementById('showAvailable').checked = true;
  document.getElementById('showBanned').checked = false;
  if(document.getElementById('showPending')) document.getElementById('showPending').checked = true;
  if(document.getElementById('showNSFW')) document.getElementById('showNSFW').checked = true;
  if(document.getElementById('showNonNSFW')) document.getElementById('showNonNSFW').checked = false;
  document.getElementById('listing').value = '';
  currentSort = 'mentions';
  currentSortDir = 'desc';
  updateSortedHeader();
  loadPage(1);
  try{
    const ns = document.getElementById('showNSFW');
    const nn = document.getElementById('showNonNSFW');
    if(ns) ns.disabled = false;
    if(nn) nn.disabled = false;
  }catch(e){}
});

// when user changes the listing dropdown, update all subreddit links on the page
document.getElementById('listing').addEventListener('change', ()=>{
  const val = document.getElementById('listing').value;
  document.querySelectorAll('#tbl tbody a[data-name]').forEach(a=>{
    if(val){
      a.href = `https://www.reddit.com/r/${encodeURIComponent(a.dataset.name)}/${val}`;
    } else {
      a.href = `https://www.reddit.com/r/${encodeURIComponent(a.dataset.name)}`;
    }
  });
  try{ savePrefs(); }catch(e){}
});
// wire sort dropdown to request server-side ordered pages
const sortSel = document.getElementById('sortSelect');
if(sortSel){
  sortSel.addEventListener('change', ()=>{
    currentSort = sortSel.value || 'mentions';
    updateSortedHeader();
    savePrefs();
    loadPage(1);
    
    // Track sorting preference with Google Analytics
    if(typeof gtag !== 'undefined'){
      gtag('event', 'sort_changed', {
        'event_category': 'sorting',
        'event_label': currentSort,
        'value': currentSort
      });
    }
  });
}

// wire per-page select in Options popout (moved from pagination controls)
const perPageSelect = document.getElementById('perPageSelect');
if(perPageSelect){
  perPageSelect.value = String(perPage);
  perPageSelect.addEventListener('change', ()=>{
    perPage = Number(perPageSelect.value);
    try{ savePrefs(); }catch(e){}
    loadPage(1);
  });
}

// wire date filter controls
const firstMentionedPreset = document.getElementById('firstMentionedPreset');
const customDaysLabel = document.getElementById('customDaysLabel');
const customDaysInput = document.getElementById('customDays');
  if(firstMentionedPreset){
  // Load initial state from prefs
  if(prefs.firstMentionedPreset === 'custom' && customDaysLabel){
    customDaysLabel.classList.remove('hidden');
  }
  firstMentionedPreset.addEventListener('change', ()=>{
    const val = firstMentionedPreset.value;
    if(val === 'custom' && customDaysLabel){
      customDaysLabel.classList.remove('hidden');
    } else {
      if(customDaysLabel) customDaysLabel.classList.add('hidden');
    }
    try{ savePrefs(); }catch(e){}
    loadPage(1);
  });
}
if(customDaysInput){
  customDaysInput.addEventListener('input', ()=>{
    try{ savePrefs(); }catch(e){}
    debouncedLoadPage();
  });
}

// modal for showing long descriptions
function openDescriptionModal(htmlText, lastChecked){
  let modal = document.getElementById('descModal');
  if(!modal){
    modal = document.createElement('div');
    modal.id = 'descModal';
    modal.className = 'modal-overlay';
    const inner = document.createElement('div');
    inner.className = 'modal-inner';
    inner.id = 'descModalInner';
    const closeBtn = document.createElement('button');
    closeBtn.innerHTML = '✕';
    closeBtn.setAttribute('aria-label', 'Close');
    closeBtn.title = 'Close';
    closeBtn.className = 'btn btn-ghost modal-close';
    closeBtn.addEventListener('click', ()=>{ modal.remove(); document.body.classList.remove('no-scroll'); });
    inner.appendChild(closeBtn);
    const content = document.createElement('div');
    content.id = 'descModalContent';
    content.className = 'modal-content';
    inner.appendChild(content);
    const timestamp = document.createElement('div');
    timestamp.id = 'descModalTimestamp';
    timestamp.className = 'modal-timestamp muted';
    inner.appendChild(timestamp);
    modal.appendChild(inner);
    // close modal when clicking outside the inner content
    modal.addEventListener('click', (e) => {
      if (e.target === modal) { modal.remove(); document.body.classList.remove('no-scroll'); }
    });
    document.body.appendChild(modal);
    document.body.classList.add('no-scroll');
  }
  const content = document.getElementById('descModalContent');
  const timestamp = document.getElementById('descModalTimestamp');
  // escape HTML by using textContent
  content.textContent = htmlText || '';
  if(lastChecked){
    timestamp.textContent = 'Last updated: ' + timeAgo(lastChecked);
  } else {
    timestamp.textContent = '';
  }
}

// initial load: restore preferences, then show age confirmation (if needed) and fetch the saved page
loadPrefs();
updateSortedHeader();

// Handle back/forward navigation to respect URL params for page changes
window.addEventListener('popstate', (e)=>{
  try{
    const params = new URLSearchParams(window.location.search);
    const page = params.get('page');
    const pnum = page ? Number(page) : 1;
    if(Number.isFinite(pnum) && pnum >= 1 && pnum !== currentPage){
      loadPage(pnum);
    }
  }catch(e){/* ignore */}
});

// Apply URL parameters (if present) to override prefs before initial load
applyUrlParams();

// Ensure clear (X) button visibility reflects loaded query from prefs
try{
  if(typeof qEl !== 'undefined' && qEl && typeof clearQueryBtn !== 'undefined' && clearQueryBtn){
    clearQueryBtn.classList.toggle('hidden', !(qEl.value && qEl.value.length>0));
  }
}catch(e){/* ignore */}

// Initialize with age gate
initWithAgeGate(() => loadPage(currentPage));
