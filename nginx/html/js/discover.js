// Discover page (discover.html) functionality

let currentData = {
  trending: { days: 7, data: null },
  gems: { max_subscribers: 10000, data: null },
  growing: { days: 30, min_recent: 5, min_growth: 1.5, data: null }
};

// Create subreddit card HTML
function createSubredditCard(sub, type = 'default') {
  const badge = type === 'trending' ? `<span class="badge">Hot</span>` :
                type === 'growing' ? `<span class="badge growth">+${sub.growth_ratio}x</span>` : '';
  
  const title = sub.title ? `<div class="subreddit-title">${escapeHtml(sub.title)}</div>` : '';
  
  let stats = [];
  if (sub.recent_mentions) {
    stats.push(`<span class="stat"><span class="stat-value">${formatNumber(sub.recent_mentions)}</span> recent</span>`);
  }
  if (sub.mentions) {
    stats.push(`<span class="stat"><span class="stat-value">${formatNumber(sub.mentions)}</span> mentions</span>`);
  }
  if (sub.total_mentions) {
    stats.push(`<span class="stat"><span class="stat-value">${formatNumber(sub.total_mentions)}</span> total</span>`);
  }
  if (sub.subscribers) {
    stats.push(`<span class="stat"><span class="stat-value">${formatNumber(sub.subscribers)}</span> subs</span>`);
  }
  
  return `
    <div class="subreddit-card">
      ${badge}
      <div class="subreddit-name">
        <a href="https://reddit.com/${sub.display_name_prefixed || 'r/' + sub.name}" target="_blank" rel="noopener noreferrer">
          ${escapeHtml(sub.display_name_prefixed || 'r/' + sub.name)}
        </a>
      </div>
      ${title}
      <div class="subreddit-stats">
        ${stats.join('')}
      </div>
    </div>
  `;
}

// Load data functions
async function loadTrending(days = 7) {
  const container = document.getElementById('trending-list');
  container.innerHTML = '<div class="loading">Loading...</div>';
  
  try {
    const response = await fetch(`/api/discover/trending?days=${days}`);
    const data = await response.json();
    currentData.trending = { days, data };
    
    if (data.items && data.items.length > 0) {
      container.innerHTML = data.items.slice(0, 12).map(sub => createSubredditCard(sub, 'trending')).join('');
    } else {
      container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔍</div><p>No trending subreddits found</p></div>';
    }
  } catch (error) {
    console.error('Error loading trending:', error);
    container.innerHTML = '<div class="empty-state"><p>Error loading data</p></div>';
  }
}

async function loadHiddenGems(maxSubs = 10000) {
  const container = document.getElementById('gems-list');
  container.innerHTML = '<div class="loading">Loading...</div>';
  
  try {
    const response = await fetch(`/api/discover/hidden_gems?max_subscribers=${maxSubs}`);
    const data = await response.json();
    currentData.gems = { max_subscribers: maxSubs, data };
    
    if (data.items && data.items.length > 0) {
      container.innerHTML = data.items.slice(0, 12).map(sub => createSubredditCard(sub, 'gem')).join('');
    } else {
      container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">💎</div><p>No hidden gems found</p></div>';
    }
  } catch (error) {
    console.error('Error loading gems:', error);
    container.innerHTML = '<div class="empty-state"><p>Error loading data</p></div>';
  }
}

async function loadFastestGrowing(days = 30, min_recent = 5, min_growth = 1.5) {
  const container = document.getElementById('growing-list');
  container.innerHTML = '<div class="loading">Loading...</div>';

  try {
    const qs = `days=${days}&min_recent=${min_recent}&min_growth=${min_growth}`;
    const response = await fetch(`/api/discover/fastest_growing?${qs}`);
    const data = await response.json();
    currentData.growing = { days, min_recent, min_growth, data };

    if (data.items && data.items.length > 0) {
      container.innerHTML = data.items.slice(0, 12).map(sub => createSubredditCard(sub, 'growing')).join('');
    } else {
      container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📈</div><p>No growing subreddits found</p></div>';
    }
  } catch (error) {
    console.error('Error loading growing:', error);
    container.innerHTML = '<div class="empty-state"><p>Error loading data</p></div>';
  }
}

// Button handlers
document.querySelectorAll('.time-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    const section = this.dataset.section;
    const days = this.dataset.days;
    const maxSubs = this.dataset.max;
    
    // Update active state
    this.parentElement.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    
    // Load data
    if (section === 'trending' && days) {
      loadTrending(parseInt(days));
    } else if (section === 'gems' && maxSubs) {
      loadHiddenGems(parseInt(maxSubs));
    } else if (section === 'growing' && days) {
      // read radio values for min_recent and min_growth
      const mr = document.querySelector('input[name="min_recent"]:checked');
      const mg = document.querySelector('input[name="min_growth"]:checked');
      const min_recent = mr ? parseInt(mr.value) : currentData.growing.min_recent;
      const min_growth = mg ? parseFloat(mg.value) : currentData.growing.min_growth;
      loadFastestGrowing(parseInt(days), min_recent, min_growth);
    }
  });
});

// Initial load with age gate
initWithAgeGate(() => {
  loadTrending(7);
  loadHiddenGems(10000);
  // pick initial radio values
  const mr0 = document.querySelector('input[name="min_recent"]:checked');
  const mg0 = document.querySelector('input[name="min_growth"]:checked');
  const initialMinRecent = mr0 ? parseInt(mr0.value) : currentData.growing.min_recent;
  const initialMinGrowth = mg0 ? parseFloat(mg0.value) : currentData.growing.min_growth;
  loadFastestGrowing(30, initialMinRecent, initialMinGrowth);

  // reload when radios change
  document.querySelectorAll('input[name="min_recent"]').forEach(r => r.addEventListener('change', () => {
    const mg = document.querySelector('input[name="min_growth"]:checked');
    const min_growth = mg ? parseFloat(mg.value) : currentData.growing.min_growth;
    const mr = document.querySelector('input[name="min_recent"]:checked');
    const min_recent = mr ? parseInt(mr.value) : currentData.growing.min_recent;
    loadFastestGrowing(currentData.growing.days || 30, min_recent, min_growth);
  }));
  document.querySelectorAll('input[name="min_growth"]').forEach(r => r.addEventListener('change', () => {
    const mr = document.querySelector('input[name="min_recent"]:checked');
    const min_recent = mr ? parseInt(mr.value) : currentData.growing.min_recent;
    const mg = document.querySelector('input[name="min_growth"]:checked');
    const min_growth = mg ? parseFloat(mg.value) : currentData.growing.min_growth;
    loadFastestGrowing(currentData.growing.days || 30, min_recent, min_growth);
  }));
});
