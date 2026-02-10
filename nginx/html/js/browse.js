// Browse page (index.html) functionality

// State management
let currentPage = 1;
let totalResults = 0;
let perPage = 24;
let currentSort = 'mentions';
let previousSort = 'mentions'; // Store sort before search
let currentFilter = 'nsfw';
let searchQuery = '';
let isLoading = false;

// Save preferences to cookie
function savePrefs() {
  try {
    const prefs = {
      currentSort,
      previousSort,
      currentFilter,
      searchQuery,
      currentPage,
      perPage
    };
    setCookie('sindex_browse_prefs', encodeURIComponent(JSON.stringify(prefs)), 365);
    // Mirror the search query to a shared cookie so other pages can read it
    try{ setCookie('sindex_search', encodeURIComponent(searchQuery || ''), 365); }catch(e){}
  } catch(e) { /* ignore */ }
}

// Load preferences from cookie
function loadPrefs() {
  try {
    const cookie = getCookie('sindex_browse_prefs');
    if (!cookie) return;
    const prefs = JSON.parse(decodeURIComponent(cookie));
    
    if (prefs.currentSort) currentSort = prefs.currentSort;
    if (prefs.previousSort) previousSort = prefs.previousSort;
    if (prefs.currentFilter) currentFilter = prefs.currentFilter;
    if (prefs.searchQuery) searchQuery = prefs.searchQuery;
    if (prefs.currentPage) currentPage = Number(prefs.currentPage) || 1;
    if (prefs.perPage) perPage = Number(prefs.perPage) || 24;
  } catch(e) { /* ignore malformed cookie */ }
  // If a shared search cookie exists, prefer it to populate the search field
  try{
    const sc = getCookie('sindex_search');
    if(sc !== null && typeof sc !== 'undefined' && String(sc).length>0){
      searchQuery = decodeURIComponent(sc);
    }
  }catch(e){}
}

// DOM elements
const searchInput = document.getElementById('searchInput');
const searchClear = document.getElementById('searchClear');
const refreshBtn = document.getElementById('refreshBtn');
const sortBy = document.getElementById('sortBy');
const filterChips = document.querySelectorAll('.filter-chip');
const filterBtn = document.getElementById('filterBtn');
const subredditGrid = document.getElementById('subredditGrid');
const statusMessage = document.getElementById('statusMessage');
const resultsInfo = document.getElementById('resultsInfo');
const prevPage = document.getElementById('prevPage');
const nextPage = document.getElementById('nextPage');
const pageInfo = document.getElementById('pageInfo');

// Create subreddit card
function createSubredditCard(sub) {
  const card = document.createElement('div');
  card.className = 'sub-card' + (sub.over18 ? ' nsfw' : '');
  
  const isUnavailable = sub.is_banned || sub.subreddit_found === false;
  
  let statusBadge = '';
  if (sub.over18) {
    statusBadge = '<span class="nsfw-badge">NSFW</span>';
  }
  if (isUnavailable) {
    statusBadge += '<span class="unavailable-badge">Unavailable</span>';
  }

  // Show NEW badge for subreddits first mentioned within the last 30 days.
  try {
    const nowSec = Math.floor(Date.now() / 1000);
    const firstMention = sub.first_mentioned ? Number(sub.first_mentioned) : (sub.created_utc ? Number(sub.created_utc) : 0);
    const THIRTY_DAYS_SEC = 30 * 24 * 60 * 60;
    if (firstMention && (nowSec - firstMention) <= THIRTY_DAYS_SEC) {
      // Prepend so NEW appears before other badges
      statusBadge = '<span class="new-badge">NEW</span>' + statusBadge;
    }
  } catch(e) { /* ignore malformed dates */ }

  const title = decodeHtml(sub.title || 'No title');
  const description = decodeHtml(sub.description || 'No description available');
  
  card.innerHTML = `
    <h3 class="sub-name">
      ${sub.display_name_prefixed || sub.name}
      ${statusBadge}
    </h3>
    <p class="sub-title">${title}</p>
    <p class="sub-description">${description}</p>
    <div class="sub-stats">
      <div class="stat-item">
        <span>💬</span>
        <span class="stat-value">${formatNumber(sub.mentions || 0)}</span>
        <span>mentions</span>
      </div>
      <div class="stat-item">
        <span>👥</span>
        <span class="stat-value">${formatNumber(sub.subscribers || 0)}</span>
        <span>members</span>
      </div>
      <div class="stat-item">
        <span>🔎</span>
        <span class="stat-value">${formatDate(sub.first_mentioned)}</span>
      </div>
    </div>
  `;

  // Click to open subreddit
  card.addEventListener('click', () => {
    const subName = sub.display_name || sub.name || '';
    if (subName) {
      window.open(`https://reddit.com/r/${subName}`, '_blank', 'noopener,noreferrer');
    }
  });

  return card;
}

// Build API URL with filters
function buildApiUrl() {
  const params = new URLSearchParams({
    page: currentPage,
    per_page: perPage,
    sort: currentSort,
    sort_dir: 'desc'
  });

  if (searchQuery) {
    params.append('q', normalizeQuery(searchQuery));
  }

  // Filter by NSFW/SFW - using show_nsfw and show_non_nsfw parameters
  if (currentFilter === 'nsfw') {
    params.append('show_nsfw', 'true');
    params.append('show_non_nsfw', 'false');
  } else if (currentFilter === 'sfw') {
    params.append('show_nsfw', 'false');
    params.append('show_non_nsfw', 'true');
  } else {
    // Show all - include both NSFW and SFW
    params.append('show_nsfw', 'true');
    params.append('show_non_nsfw', 'true');
  }

  // Only show available subreddits by default
  params.append('show_available', 'true');
  params.append('show_banned', 'false');
  // Exclude subreddits that are "pending" (missing metadata)
  params.append('show_pending', 'false');

  return `/subreddits?${params.toString()}`;
}

// Load subreddits from API
async function loadSubreddits() {
  if (isLoading) return;
  
  isLoading = true;
  // preserve current scroll position so updating the grid doesn't jump the page
  const _scrollX = (window.scrollX !== undefined) ? window.scrollX : (window.pageXOffset || 0);
  const _scrollY = (window.scrollY !== undefined) ? window.scrollY : (window.pageYOffset || 0);
  subredditGrid.classList.add('hidden');
  statusMessage.classList.remove('hidden');
  statusMessage.innerHTML = `
    <div class="loading-spinner"></div>
    <p class="mt-8">Loading subreddits...</p>
  `;

  try {
    const response = await fetch(buildApiUrl());
    if (!response.ok) throw new Error('Failed to fetch');

    const data = await response.json();
    
    totalResults = data.total || 0;
    const subreddits = data.items || [];
    
    // Update header count with total DB count
    if (data.db_total) {
      updateHeaderCount(data.db_total);
    }

    // Update results info
    const start = (currentPage - 1) * perPage + 1;
    const end = Math.min(currentPage * perPage, totalResults);
    let resultsHtml = `Showing <strong>${start}-${end}</strong> of <strong>${formatNumber(totalResults)}</strong> subreddits`;
    // If the API indicates there are pending/hidden matches (e.g. metadata not yet fetched), show a note
    if (typeof data.pending_matches !== 'undefined' && Number(data.pending_matches) > 0) {
      resultsHtml += ` <span class="muted">(${formatNumber(Number(data.pending_matches))} not yet updated)</span>`;
    }
    resultsInfo.innerHTML = resultsHtml;

    // Clear grid and render cards
    subredditGrid.innerHTML = '';
    
    if (subreddits.length === 0) {
      statusMessage.innerHTML = '<p>No subreddits found. Try adjusting your filters.</p>';
      statusMessage.classList.remove('hidden');
      subredditGrid.classList.add('hidden');
    } else {
      subreddits.forEach(sub => {
        subredditGrid.appendChild(createSubredditCard(sub));
      });
      statusMessage.classList.add('hidden');
      subredditGrid.classList.remove('hidden');
    }

    // Update pagination
    updatePagination();

    // restore preserved scroll position (use a microtask to ensure layout applied)
    try{ setTimeout(()=>{ window.scrollTo(_scrollX, _scrollY); }, 0); }catch(e){}

  } catch (error) {
    console.error('Error loading subreddits:', error);
    statusMessage.innerHTML = '<p>Error loading subreddits. Please try again.</p>';
    statusMessage.classList.remove('hidden');
    subredditGrid.classList.add('hidden');
  } finally {
    isLoading = false;
    savePrefs();
  }
}

// Update pagination buttons
function updatePagination() {
  const totalPages = Math.max(1, Math.ceil(totalResults / perPage));
  const targets = [document.getElementById('pagination'), document.getElementById('paginationTop')].filter(Boolean);
  if (!targets.length) return;

  // Build a fresh set of controls for each target so event handlers bind correctly
  const buildControls = () => {
    const ctr = document.createElement('div');
    ctr.className = 'pagination-row';

    const buildPageUrl = (pageNum) => {
      try {
        const params = new URLSearchParams(window.location.search);
        params.set('page', String(pageNum));
        return `${window.location.pathname}?${params.toString()}`;
      } catch(e) {
        return `${window.location.pathname}?page=${pageNum}`;
      }
    };

    const makeLink = (text, aria, disabled, pageNum) => {
      if (disabled) {
        const s = document.createElement('span');
        s.className = 'page-btn disabled';
        s.setAttribute('aria-label', aria);
        s.textContent = text;
        return s;
      }
      const a = document.createElement('a');
      a.className = 'page-btn';
      a.href = buildPageUrl(pageNum);
      a.textContent = text;
      a.setAttribute('aria-label', aria);
      return a;
    };

    const first = makeLink('« First', 'First page', currentPage <= 1, 1);
    const prev = makeLink('◀ Prev', 'Previous page', currentPage <= 1, Math.max(1, currentPage-1));

    const pageInput = document.createElement('input');
    pageInput.type = 'number';
    pageInput.min = 1;
    pageInput.max = totalPages;
    pageInput.className = 'muted input-small';
    pageInput.value = String(currentPage);
    pageInput.addEventListener('change', ()=>{
      const v = Number(pageInput.value||0);
      if(Number.isFinite(v) && v>=1 && v<=totalPages){
        window.location.href = buildPageUrl(v);
      } else { pageInput.value = String(currentPage); }
    });
    pageInput.addEventListener('keydown', (e) => {
      if(e.key === 'Enter'){
        e.preventDefault();
        pageInput.dispatchEvent(new Event('change'));
      }
    });

    const pageTotal = document.createElement('span'); pageTotal.className = 'muted ml-6'; pageTotal.textContent = `/ ${totalPages}`;

    const next = makeLink('Next ▶', 'Next page', currentPage >= totalPages, Math.min(totalPages, currentPage+1));
    const last = makeLink('Last »', 'Last page', currentPage >= totalPages, totalPages);

    ctr.appendChild(first);
    ctr.appendChild(prev);
    ctr.appendChild(pageInput);
    ctr.appendChild(pageTotal);
    ctr.appendChild(next);
    ctr.appendChild(last);

    return ctr;
  };

  targets.forEach(t => {
    t.innerHTML = '';
    t.appendChild(buildControls());
  });
  savePrefs();
}

// Event listeners
searchInput.addEventListener('input', (e) => {
  const newQuery = e.target.value.trim();
  const hadQuery = searchQuery.length > 0;
  const hasQuery = newQuery.length > 0;
  searchQuery = newQuery;
  if (searchClear) searchClear.classList.toggle('hidden', !searchQuery);
  try{ setCookie('sindex_search', encodeURIComponent(searchQuery || ''), 365); }catch(e){}
  
  // Auto-switch to alphabetical sort when searching (for relevance)
  if (hasQuery && !hadQuery) {
    // Just started searching - save current sort and switch to A-Z
    previousSort = currentSort;
    currentSort = 'display_name_prefixed';
    sortBy.value = currentSort;
  } else if (!hasQuery && hadQuery) {
    // Just cleared search - restore previous sort
    currentSort = previousSort;
    sortBy.value = currentSort;
  }
  
  // Debounce search
  clearTimeout(searchInput.timeout);
  searchInput.timeout = setTimeout(() => {
    currentPage = 1;
    loadSubreddits();
  }, 500);
});

searchClear.addEventListener('click', () => {
  searchInput.value = '';
  if (searchQuery.length > 0) {
    // Restore previous sort when clearing search
    currentSort = previousSort;
    sortBy.value = currentSort;
  }
  searchQuery = '';
  if (searchClear) searchClear.classList.add('hidden');
  currentPage = 1;
  loadSubreddits();
});

refreshBtn.addEventListener('click', () => {
  currentPage = 1;
  loadSubreddits();
});

sortBy.addEventListener('change', (e) => {
  currentSort = e.target.value;
  // Update previousSort if not currently searching
  if (!searchQuery) {
    previousSort = currentSort;
  }
  currentPage = 1;
  loadSubreddits();
});

if (filterBtn) {
  const _states = ['all','nsfw','sfw'];
  function _updateFilterBtn(){
    const labels = { all: 'All', nsfw: 'NSFW Only', sfw: 'Safe only' };
    try{ filterBtn.textContent = labels[currentFilter] || 'All'; }catch(e){}
    if(currentFilter === 'nsfw') filterBtn.classList.add('active'); else filterBtn.classList.remove('active');
  }
  _updateFilterBtn();
  filterBtn.addEventListener('click', () => {
    const idx = Math.max(0, _states.indexOf(currentFilter));
    const next = _states[(idx + 1) % _states.length];
    currentFilter = next;
    _updateFilterBtn();
    currentPage = 1;
    loadSubreddits();
  });
} else {
  filterChips.forEach(chip => {
    chip.addEventListener('click', () => {
      filterChips.forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      currentFilter = chip.dataset.filter;
      currentPage = 1;
      loadSubreddits();
    });
  });
}

// Pagination buttons are rendered dynamically by updatePagination()

// Initialize preferences and UI on page load
function initializePage() {
  // Load saved preferences
  loadPrefs();
  // If a `page` query parameter is present in the URL, prefer it over saved prefs
  try {
    const params = new URLSearchParams(window.location.search);
    const urlPage = params.get('page');
    if (urlPage) {
      const pnum = Number(urlPage);
      if (Number.isFinite(pnum) && pnum >= 1) {
        currentPage = Math.max(1, Math.floor(pnum));
      }
    }
  } catch (e) { /* ignore */ }
  
  // Apply preferences to UI
  if (sortBy) sortBy.value = currentSort;
    if (searchInput) {
    searchInput.value = searchQuery;
    if (searchClear) searchClear.classList.toggle('hidden', !searchQuery);
  }
  
  // Set active filter UI (single button or chips)
  if (filterBtn) {
    try{
      const labels = { all: 'All', nsfw: 'NSFW Only', sfw: 'Safe only' };
      filterBtn.textContent = labels[currentFilter] || 'All';
      if(currentFilter === 'nsfw') filterBtn.classList.add('active'); else filterBtn.classList.remove('active');
    }catch(e){}
  } else {
    filterChips.forEach(chip => {
      if (chip.dataset.filter === currentFilter) {
        chip.classList.add('active');
      } else {
        chip.classList.remove('active');
      }
    });
  }
  
  // Load data
  loadSubreddits();
}

// Initial load with age gate
initWithAgeGate(() => initializePage());
