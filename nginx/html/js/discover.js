// Discover page (discover.html) functionality

let currentData = {
  trending: { days: 7, data: null },
  gems: { max_subscribers: 10000, data: null },
  growing: { days: 30, min_recent: 5, min_growth: 1.5, data: null }
};

// Global NSFW/SFW filter for all sections: 'all' | 'nsfw' | 'sfw'
let globalFilter = 'nsfw';

function _filterQueryFor() {
  if (globalFilter === 'nsfw') return '&show_nsfw=true&show_non_nsfw=false';
  if (globalFilter === 'sfw') return '&show_nsfw=false&show_non_nsfw=true';
  return '&show_nsfw=true&show_non_nsfw=true';
}

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
    const qs = `days=${days}` + _filterQueryFor();
    const response = await fetch(`/api/discover/trending?${qs}`);
    const data = await response.json();
    currentData.trending = { days, data };

    // Client-side NSFW/SFW filtering because discover endpoints don't accept those params
    let items = data.items || [];
    if (globalFilter === 'nsfw') {
      items = items.filter(s => s.is_over18 === true || s.is_over18 === 'true');
    } else if (globalFilter === 'sfw') {
      items = items.filter(s => s.is_over18 === false || s.is_over18 === 'false');
    }

    if (items.length > 0) {
      container.innerHTML = items.slice(0, 12).map(sub => createSubredditCard(sub, 'trending')).join('');
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
    const qs = `max_subscribers=${maxSubs}` + _filterQueryFor();
    const response = await fetch(`/api/discover/hidden_gems?${qs}`);
    const data = await response.json();
    currentData.gems = { max_subscribers: maxSubs, data };

    let items = data.items || [];
    if (globalFilter === 'nsfw') {
      items = items.filter(s => s.is_over18 === true || s.is_over18 === 'true');
    } else if (globalFilter === 'sfw') {
      items = items.filter(s => s.is_over18 === false || s.is_over18 === 'false');
    }

    if (items.length > 0) {
      container.innerHTML = items.slice(0, 12).map(sub => createSubredditCard(sub, 'gem')).join('');
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
    const fetchFastest = async (d, mr, mg) => {
      const qs = `days=${d}&min_recent=${mr}&min_growth=${mg}` + _filterQueryFor();
      const res = await fetch(`/api/discover/fastest_growing?${qs}`);
      const json = await res.json();
      return { params: { days: d, min_recent: mr, min_growth: mg }, json };
    };

    const result = await fetchFastest(days, min_recent, min_growth);
    const data = result.json;
    currentData.growing = { days, min_recent, min_growth, data };

    let items = data.items || [];
    if (globalFilter === 'nsfw') {
      items = items.filter(s => s.is_over18 === true || s.is_over18 === 'true');
    } else if (globalFilter === 'sfw') {
      items = items.filter(s => s.is_over18 === false || s.is_over18 === 'false');
    }

    if (items.length > 0) {
      container.innerHTML = items.slice(0, 12).map(sub => createSubredditCard(sub, 'growing')).join('');
    } else {
      // Auto-relax filters: try expanding date ranges, then lowering min_recent,
      // then lowering min_growth step-by-step until we find results or exhaust options.
      console.log('discover.js: fastest growing returned 0 items; attempting relaxed queries');
      const daysOptions = [14, 30, 60];
      const minRecentOptions = [10, 5, 3];
      const minGrowthOptions = [1.5, 1.4, 1.3, 1.2, 1.1];

      // Determine start indices from current requested values
      const startDayIdx = Math.max(0, daysOptions.indexOf(days));
      const startMrIdx = Math.max(0, minRecentOptions.indexOf(min_recent));
      const startMgIdx = Math.max(0, minGrowthOptions.indexOf(min_growth));

      let found = false;
      // Iterate days from current index upward
      for (let di = startDayIdx; di < daysOptions.length && !found; di++){
        const d = daysOptions[di];

        // First: attempt stepping down min_recent (less strict) while keeping min_growth fixed
        for (let mri = startMrIdx; mri >= 0 && !found; mri--){
          const mr = minRecentOptions[mri];
          try{
            console.log('discover.js: trying relaxed params (min_recent)', { days: d, min_recent: mr, min_growth: min_growth });
            const r = await fetchFastest(d, mr, min_growth);
            let its = (r.json && r.json.items) ? r.json.items : [];
            if (globalFilter === 'nsfw') its = its.filter(s => s.is_over18 === true || s.is_over18 === 'true');
            else if (globalFilter === 'sfw') its = its.filter(s => s.is_over18 === false || s.is_over18 === 'false');
            if (its.length > 0){
              console.log('discover.js: relaxed (min_recent) query succeeded', { days: d, min_recent: mr, min_growth: min_growth, found: its.length });
              container.innerHTML = its.slice(0,12).map(sub => createSubredditCard(sub, 'growing')).join('');
              currentData.growing = { days: d, min_recent: mr, min_growth: min_growth, data: r.json };
              found = true;
              break;
            }
          }catch(e){ console.warn('discover.js: relaxed fetch failed (min_recent)', e); }
        }

        if (found) break;

        // Second: if still none, attempt stepping down min_growth (less strict) while keeping min_recent fixed
        for (let mgi = startMgIdx; mgi >= 0 && !found; mgi--){
          const mg = minGrowthOptions[mgi];
          try{
            console.log('discover.js: trying relaxed params (min_growth)', { days: d, min_recent: min_recent, min_growth: mg });
            const r = await fetchFastest(d, min_recent, mg);
            let its = (r.json && r.json.items) ? r.json.items : [];
            if (globalFilter === 'nsfw') its = its.filter(s => s.is_over18 === true || s.is_over18 === 'true');
            else if (globalFilter === 'sfw') its = its.filter(s => s.is_over18 === false || s.is_over18 === 'false');
            if (its.length > 0){
              console.log('discover.js: relaxed (min_growth) query succeeded', { days: d, min_recent: min_recent, min_growth: mg, found: its.length });
              container.innerHTML = its.slice(0,12).map(sub => createSubredditCard(sub, 'growing')).join('');
              currentData.growing = { days: d, min_recent: min_recent, min_growth: mg, data: r.json };
              found = true;
              break;
            }
          }catch(e){ console.warn('discover.js: relaxed fetch failed (min_growth)', e); }
        }
      }

      if(!found){
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📈</div><p>No growing subreddits found</p></div>';
      }
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
      // read toggle button values for min_recent and min_growth
      const mrBtn = document.querySelector('.toggle-group[data-name="min_recent"] .toggle-btn.active');
      const mgBtn = document.querySelector('.toggle-group[data-name="min_growth"] .toggle-btn.active');
      const min_recent = mrBtn ? parseInt(mrBtn.dataset.value) : currentData.growing.min_recent;
      const min_growth = mgBtn ? parseFloat(mgBtn.dataset.value) : currentData.growing.min_growth;
      loadFastestGrowing(parseInt(days), min_recent, min_growth);
    }
  });
});

// Initial load with age gate
initWithAgeGate(() => {
  loadTrending(7);
  loadHiddenGems(10000);

  // pick initial toggle values
  const mrBtn0 = document.querySelector('.toggle-group[data-name="min_recent"] .toggle-btn.active');
  const mgBtn0 = document.querySelector('.toggle-group[data-name="min_growth"] .toggle-btn.active');
  const initialMinRecent = mrBtn0 ? parseInt(mrBtn0.dataset.value) : currentData.growing.min_recent;
  const initialMinGrowth = mgBtn0 ? parseFloat(mgBtn0.dataset.value) : currentData.growing.min_growth;
  loadFastestGrowing(30, initialMinRecent, initialMinGrowth);

  // Toggle button handlers for fastest growing controls
  document.querySelectorAll('.toggle-group').forEach(group => {
    group.querySelectorAll('.toggle-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        // ensure only one active per group
        group.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        // reload growing section with new values
        const mr = document.querySelector('.toggle-group[data-name="min_recent"] .toggle-btn.active');
        const mg = document.querySelector('.toggle-group[data-name="min_growth"] .toggle-btn.active');
        const min_recent = mr ? parseInt(mr.dataset.value) : currentData.growing.min_recent;
        const min_growth = mg ? parseFloat(mg.dataset.value) : currentData.growing.min_growth;
        loadFastestGrowing(currentData.growing.days || 30, min_recent, min_growth);
      });
    });
  });

  // Global filter button behavior: clicking any discover-filter cycles All/NSFW/SFW
  const states = ['all','nsfw','sfw'];
  function updateAllFilterButtons() {
    const labels = { all: 'All', nsfw: 'NSFW Only', sfw: 'Safe only' };
    document.querySelectorAll('.discover-filter').forEach(b => {
      b.textContent = labels[globalFilter] || 'All';
      if (globalFilter === 'nsfw') b.classList.add('active'); else b.classList.remove('active');
    });
  }

  updateAllFilterButtons();

  document.querySelectorAll('.discover-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      const idx = Math.max(0, states.indexOf(globalFilter));
      globalFilter = states[(idx + 1) % states.length];
      updateAllFilterButtons();
      // reload all sections with current parameters
      loadTrending(currentData.trending.days || 7);
      loadHiddenGems(currentData.gems.max_subscribers || 10000);
      const mr = document.querySelector('.toggle-group[data-name="min_recent"] .toggle-btn.active');
      const mg = document.querySelector('.toggle-group[data-name="min_growth"] .toggle-btn.active');
      const min_recent = mr ? parseInt(mr.dataset.value) : currentData.growing.min_recent;
      const min_growth = mg ? parseFloat(mg.dataset.value) : currentData.growing.min_growth;
      loadFastestGrowing(currentData.growing.days || 30, min_recent, min_growth);
    });
  });
});
