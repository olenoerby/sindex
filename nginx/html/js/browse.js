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

// DOM elements
const searchInput = document.getElementById('searchInput');
const searchClear = document.getElementById('searchClear');
const refreshBtn = document.getElementById('refreshBtn');
const sortBy = document.getElementById('sortBy');
const filterChips = document.querySelectorAll('.filter-chip');
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

  return `/subreddits?${params.toString()}`;
}

// Load subreddits from API
async function loadSubreddits() {
  if (isLoading) return;
  
  isLoading = true;
  subredditGrid.style.display = 'none';
  statusMessage.style.display = 'block';
  statusMessage.innerHTML = `
    <div class="loading-spinner"></div>
    <p style="margin-top:12px">Loading subreddits...</p>
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
    resultsInfo.innerHTML = `
      Showing <strong>${start}-${end}</strong> of <strong>${formatNumber(totalResults)}</strong> subreddits
    `;

    // Clear grid and render cards
    subredditGrid.innerHTML = '';
    
    if (subreddits.length === 0) {
      statusMessage.innerHTML = '<p>No subreddits found. Try adjusting your filters.</p>';
      statusMessage.style.display = 'block';
      subredditGrid.style.display = 'none';
    } else {
      subreddits.forEach(sub => {
        subredditGrid.appendChild(createSubredditCard(sub));
      });
      statusMessage.style.display = 'none';
      subredditGrid.style.display = 'grid';
    }

    // Update pagination
    updatePagination();

  } catch (error) {
    console.error('Error loading subreddits:', error);
    statusMessage.innerHTML = '<p>Error loading subreddits. Please try again.</p>';
    statusMessage.style.display = 'block';
    subredditGrid.style.display = 'none';
  } finally {
    isLoading = false;
  }
}

// Update pagination buttons
function updatePagination() {
  const totalPages = Math.ceil(totalResults / perPage);
  
  prevPage.disabled = currentPage <= 1;
  nextPage.disabled = currentPage >= totalPages;
  pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
}

// Update header count from database total
function updateHeaderCount(dbTotal) {
  const el = document.getElementById('headerSubCount');
  if (el && dbTotal > 0) {
    el.textContent = formatNumber(dbTotal) + ' subreddits scanned';
  }
}

// Event listeners
searchInput.addEventListener('input', (e) => {
  const newQuery = e.target.value.trim();
  const hadQuery = searchQuery.length > 0;
  const hasQuery = newQuery.length > 0;
  searchQuery = newQuery;
  searchClear.style.display = searchQuery ? 'block' : 'none';
  
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
  searchClear.style.display = 'none';
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

filterChips.forEach(chip => {
  chip.addEventListener('click', () => {
    filterChips.forEach(c => c.classList.remove('active'));
    chip.classList.add('active');
    currentFilter = chip.dataset.filter;
    currentPage = 1;
    loadSubreddits();
  });
});

prevPage.addEventListener('click', () => {
  if (currentPage > 1) {
    currentPage--;
    loadSubreddits();
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }
});

nextPage.addEventListener('click', () => {
  const totalPages = Math.ceil(totalResults / perPage);
  if (currentPage < totalPages) {
    currentPage++;
    loadSubreddits();
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }
});

// Initial load with age gate
initWithAgeGate(() => loadSubreddits());
