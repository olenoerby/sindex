// Analytics page (analytics.html) - Charts and statistics functionality

const buttons = Array.from(document.querySelectorAll('.controls .btn'));
let currentDays = 90;
let timelineChart, subsChart;
const textColor = '#d9e4f5';

// Cache for change detection
const cache = {
  stats: {},
  daily: {},
  topBlocks: {}
};

// Config cache (fetched from API)
let appConfig = { metadata_stale_hours: 24 }; // Default fallback

const fmt = (n, opts={}) => (n ?? 0).toLocaleString('en-US', {maximumFractionDigits: 1, ...opts});
// Use shared helpers: expect epoch seconds for timestamps
const fmtDate = (epoch) => epoch ? epochToLocalString(epoch) : '—';

// Fetch app configuration from API
async function fetchConfig(){
  try {
    const res = await fetch('/config');
    if(res.ok) {
      appConfig = await res.json();
    }
  } catch(err) {
    console.warn('Failed to fetch config, using defaults', err);
  }
}

// Initialize currentDays from cookie or default to 30
function initializeDateRange(){
  const savedDays = getCookie('sindex_analytics_date_range');
  if(savedDays && !isNaN(savedDays)){
    currentDays = Number(savedDays);
  } else {
    currentDays = 90;
    setCookie('sindex_analytics_date_range', 90, 365);
  }
  
  // Update button states based on currentDays
  buttons.forEach(btn => {
    const btnDays = Number(btn.dataset.days || 90);
    if(btnDays === currentDays){
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });
  
  // Update window display
  const displayText = currentDays >= 999999 ? 'Window: All time' : `Window: ${currentDays} days`;
  document.getElementById('window').textContent = displayText;
  updateCardTitles();
  updateTableRangeIndicators();
}

// Update card titles to show the selected date range
function updateCardTitles(){
  const rangeText = currentDays >= 999999 ? 'All time' : `${currentDays}d`;
  document.getElementById('totalMentionsTitle').textContent = `Total subreddit mentions (${rangeText})`;
  document.getElementById('totalSubsTitle').textContent = `Total unique subreddits (${rangeText})`;
  document.getElementById('totalPostsTitle').textContent = `Total posts (${rangeText})`;
  document.getElementById('totalCommentsTitle').textContent = `Total comments (${rangeText})`;
  document.getElementById('peakMentionTitle').textContent = `Peak mention day (${rangeText})`;
  // Update description under Total subreddits to reflect the selected window
  const newSubsDescEl = document.getElementById('newSubsDesc');
  if(newSubsDescEl) newSubsDescEl.textContent = `First-time mentions in the last ${currentDays >= 999999 ? 'all days' : currentDays + ' days'}.`;
  document.getElementById('topCommenterTitle').textContent = `Top commenter (${rangeText})`;
    const tmEl = document.getElementById('topMentionerTitle');
    if(tmEl) tmEl.textContent = `Top mentioner (${rangeText})`;
}

// Update small subtitles for analytic tables to show active date range
function updateTableRangeIndicators(){
  const rangeText = currentDays >= 999999 ? 'All time' : `${currentDays}d`;
  const tables = [
    'topSubredditsTable',
    'topCommentersTable',
    'topPostsTable',
    'topMentionersTable'
  ];
  tables.forEach(id => {
    try{
      const table = document.getElementById(id);
      if(!table) return;
      const card = table.closest('.card');
      if(!card) return;
      const small = card.querySelector('.row-title small');
      if(!small) return;
      if(!small.dataset.base) small.dataset.base = small.textContent || '';
      const base = small.dataset.base;
      small.textContent = base ? `${base} · ${rangeText}` : `${rangeText}`;
    }catch(e){/* ignore */}
  });
}

function bindControls(){
  buttons.forEach(btn => {
    btn.addEventListener('click', () => {
      const days = Number(btn.dataset.days);
      if (currentDays === days) return; // Already on this range
      currentDays = days;
      buttons.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      const displayText = days >= 999999 ? 'Window: All time' : `Window: ${days} days`;
      document.getElementById('window').textContent = displayText;
      updateCardTitles();
      updateTableRangeIndicators();
      setCookie('sindex_analytics_date_range', days, 365);
      // Clear cache for this date range to force refresh
      delete cache.daily[days];
      // Reset topBlocks cache object instead of deleting the property
      // (deleting it causes later access to `cache.topBlocks.*` to throw)
      cache.topBlocks = {};
      fetchStats();
      fetchDaily(days);
      fetchTopBlocks();
    });
  });
  
  // No manual refresh button on analytics page; auto-refresh runs instead
}

async function fetchStats(){
  try {
    const statsUrl = (currentDays >= 999999) ? '/stats' : `/stats?days=${currentDays}`;
    const res = await fetch(statsUrl);
    if(!res.ok) throw new Error('HTTP '+res.status);
    const s = await res.json();
    
    // Only update if values changed
    if(cache.stats.total_mentions !== s.total_mentions) {
      const el = document.getElementById('totalMentions');
      if(el) el.textContent = fmt(s.total_mentions);
      cache.stats.total_mentions = s.total_mentions;
    }
    if(cache.stats.total_subreddits !== s.total_subreddits) {
      const el = document.getElementById('totalSubs');
      if(el) el.textContent = fmt(s.total_subreddits);
      cache.stats.total_subreddits = s.total_subreddits;
    }
    if(cache.stats.total_posts !== s.total_posts) {
      const el = document.getElementById('totalPosts');
      if(el) el.textContent = fmt(s.total_posts);
      cache.stats.total_posts = s.total_posts;
    }
    if(cache.stats.total_comments !== s.total_comments) {
      const el = document.getElementById('totalComments');
      if(el) el.textContent = fmt(s.total_comments);
      cache.stats.total_comments = s.total_comments;
    }
    if(cache.stats.last_scanned !== s.last_scanned) {
      const el = document.getElementById('lastScanned');
      if(el) el.textContent = 'Last DB run: ' + timeAgo(s.last_scanned);
      cache.stats.last_scanned = s.last_scanned;
    }
    if(cache.stats.last_scan_new_mentions !== s.last_scan_new_mentions) {
      const el = document.getElementById('lastScanNewMentions');
      if(el) el.textContent = fmt(s.last_scan_new_mentions ?? 0) + ' new mentions';
      cache.stats.last_scan_new_mentions = s.last_scan_new_mentions;
    }
    const dur = s.last_scan_duration ? `${fmt(s.last_scan_duration, {maximumFractionDigits:0})}s` : '—';
    const startedAgo = s.last_scan_started ? timeAgo(s.last_scan_started) : '—';
    const meta = `Started ${startedAgo} · Duration ${dur}`;
    if(cache.stats.lastScanMeta !== meta) {
      const el = document.getElementById('lastScanMeta');
      if(el) el.textContent = meta;
      cache.stats.lastScanMeta = meta;
    }
    const polledEl = document.getElementById('lastPolled');
    if(polledEl){
      // mark when we polled and show relative time
      const polledAt = Math.floor(Date.now() / 1000);
      polledEl.textContent = 'Last polled: ' + timeAgo(polledAt);
    }
  } catch(err) {
    console.warn('stats failed', err);
    if(err.message && err.message.includes('JSON')) {
      console.error('API server may not be running or endpoint returned HTML instead of JSON');
    }
  }
}

async function fetchMetadataStats(){
  try {
    const res = await fetch('/stats/metadata');
    if(!res.ok) throw new Error('HTTP '+res.status);
    const m = await res.json();
    
    const total = m.total_subreddits || 0;
    const pct = (val) => total > 0 ? ` (${((val / total) * 100).toFixed(1)}%)` : '';
    const staleHours = appConfig.metadata_stale_hours || 24;
    
    // Update metadata stats
    document.getElementById('metaTotalSubs').textContent = fmt(total);
    document.getElementById('metaUpToDate').textContent = fmt(m.up_to_date || 0);
    document.getElementById('metaUpToDatePct').textContent = `Checked within ${staleHours}h` + pct(m.up_to_date || 0);
    
    document.getElementById('metaStale').textContent = fmt(m.stale_24h_plus || 0);
    document.getElementById('metaStalePct').textContent = `Older than ${staleHours}h` + pct(m.stale_24h_plus || 0);
    
    document.getElementById('metaNeverChecked').textContent = fmt(m.never_checked || 0);
    document.getElementById('metaNeverCheckedPct').textContent = 'No metadata fetched yet' + pct(m.never_checked || 0);
    
    document.getElementById('metaWithout').textContent = fmt(m.without_metadata || 0);
    document.getElementById('metaWithoutPct').textContent = 'Missing title, subs & desc' + pct(m.without_metadata || 0);
    
    document.getElementById('metaBanned').textContent = fmt(m.banned || 0);
    document.getElementById('metaBannedPct').textContent = 'Banned subreddits' + pct(m.banned || 0);
    
    document.getElementById('metaNotFound').textContent = fmt(m.not_found || 0);
    document.getElementById('metaNotFoundPct').textContent = "Don't exist (404)" + pct(m.not_found || 0);
    
    document.getElementById('metaPendingRetry').textContent = fmt(m.pending_retry || 0);
    document.getElementById('metaPendingRetryPct').textContent = 'Waiting after error' + pct(m.pending_retry || 0);
    
    document.getElementById('metaNsfw').textContent = fmt(m.nsfw_subreddits || 0);
    document.getElementById('metaNsfwPct').textContent = '18+ subreddits' + pct(m.nsfw_subreddits || 0);
    
    document.getElementById('metaWithSubs').textContent = fmt(m.with_subscriber_data || 0);
    document.getElementById('metaWithSubsPct').textContent = 'Have subscriber counts' + pct(m.with_subscriber_data || 0);
    
    document.getElementById('metaWithDesc').textContent = fmt(m.with_descriptions || 0);
    document.getElementById('metaWithDescPct').textContent = 'Have description text' + pct(m.with_descriptions || 0);
  } catch(err) {
    console.warn('metadata stats failed', err);
    if(err.message && err.message.includes('JSON')) {
      console.error('API server may not be running. Please start docker-compose to access analytics data.');
    }
  }
}

async function fetchServiceHealth(){
  try {
    const res = await fetch('/health');
    if(!res.ok) throw new Error('HTTP '+res.status);
    const h = await res.json();

    // API health
    const apiOk = !!h['api-health'];
    const apiEl = document.getElementById('svcApi');
    const apiSub = document.getElementById('svcApiSub');
    if(apiEl) apiEl.textContent = apiOk ? 'Healthy' : 'Unreachable';
    if(apiSub) apiSub.textContent = apiOk ? 'API process responding' : (h.error || 'API error');

    // DB health
    const dbOk = !!h['db-health'];
    const dbEl = document.getElementById('svcDb');
    const dbSubEl = document.getElementById('svcDbSub');
    if(dbEl) dbEl.textContent = dbOk ? 'Connected' : 'Disconnected';
    if(dbSubEl) dbSubEl.textContent = dbOk ? 'Database reachable' : (h.error || 'DB error');

    // Scanner health
    const scannerOk = !!h['scanner-health'];
    const scannerEl = document.getElementById('svcScanner');
    const scannerSub = document.getElementById('svcScannerSub');
    if(scannerEl) scannerEl.textContent = scannerOk ? 'Healthy' : 'Unhealthy';
    if(scannerSub){
              const last = h['scanner-last-scan-started'] || h['scanner_last_scan_started'] || null;
              scannerSub.textContent = `Last scan: ${ last ? timeAgo(last) : '—'}`;
    }
  } catch(err) {
    console.warn('service health fetch failed', err);
    const apiEl = document.getElementById('svcApi'); if(apiEl) apiEl.textContent = 'Unreachable';
    const dbEl = document.getElementById('svcDb'); if(dbEl) dbEl.textContent = 'Unknown';
    const scannerEl = document.getElementById('svcScanner'); if(scannerEl) scannerEl.textContent = 'Unknown';
  }
}

function ensureCharts(){
  if(!timelineChart){
    const ctx = document.getElementById('timelineChart').getContext('2d');
    timelineChart = new Chart(ctx, {
      type: 'line',
      data: { labels: [], datasets: [
        { label: 'Mentions', data: [], borderColor: '#1ec6b3', backgroundColor: 'rgba(30,198,179,0.12)', tension: 0.4, fill: true },
        { label: 'Posts', data: [], borderColor: '#7dd3fc', backgroundColor: 'rgba(125,211,252,0.1)', tension: 0.4, fill: true },
        { label: 'Comments', data: [], borderColor: '#f97316', backgroundColor: 'rgba(249,115,22,0.1)', tension: 0.4, fill: true }
      ]},
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { labels: { color: textColor } } },
        scales: {
          x: { ticks: { color: textColor }, grid: { color: 'rgba(255,255,255,0.05)' } },
          y: { ticks: { color: textColor }, grid: { color: 'rgba(255,255,255,0.05)' }, beginAtZero: true }
        }
      }
    });
  }
  if(!subsChart){
    const ctx = document.getElementById('subsChart').getContext('2d');
    subsChart = new Chart(ctx, {
      type: 'bar',
      data: { labels: [], datasets: [
        { label: 'New subreddits', data: [], backgroundColor: 'rgba(30,198,179,0.5)', borderColor: '#1ec6b3', borderWidth: 1.5 },
        { label: 'Mentions', data: [], backgroundColor: 'rgba(249,115,22,0.35)', borderColor: '#f97316', borderWidth: 1.5 }
      ]},
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { labels: { color: textColor } } },
        scales: {
          x: { ticks: { color: textColor }, grid: { display: false }, barPercentage: 0.8, categoryPercentage: 0.9 },
          y: { ticks: { color: textColor }, grid: { color: 'rgba(255,255,255,0.05)' }, beginAtZero: true }
        }
      }
    });
  }
}

async function fetchDaily(days){
  ensureCharts();
  try {
    const res = await fetch(`/stats/daily?days=${days}`);
    if(!res.ok) throw new Error('HTTP '+res.status);
    const { items=[] } = await res.json();

    // Respect the requested window client-side in case the server returns a larger set
    const visibleItems = (days >= 999999) ? items : items.slice(-Math.max(0, Number(days)));

    // Create a hash of the visible data to detect changes
    const dataKey = JSON.stringify(visibleItems);
    if(cache.daily[days] === dataKey) {
      return; // No changes, skip update
    }
    cache.daily[days] = dataKey;

    const labels = visibleItems.map(it => it.date);
    const mentions = visibleItems.map(it => it.mentions || 0);
    const posts = visibleItems.map(it => it.posts || 0);
    const comments = visibleItems.map(it => it.comments || 0);
    const newSubs = visibleItems.map(it => it.new_subreddits || 0);

    // Update timeline chart
    timelineChart.data.labels = labels;
    timelineChart.data.datasets[0].data = mentions;
    timelineChart.data.datasets[1].data = posts;
    timelineChart.data.datasets[2].data = comments;
    timelineChart.update();

    // Update subs chart (smaller set for readability)
    const sampleEvery = Math.max(1, Math.floor(labels.length / 40));
    const slimLabels = labels.filter((_, idx) => idx % sampleEvery === 0);
    const slimSubs = newSubs.filter((_, idx) => idx % sampleEvery === 0);
    const slimMentions = mentions.filter((_, idx) => idx % sampleEvery === 0);
    subsChart.data.labels = slimLabels;
    subsChart.data.datasets[0].data = slimSubs;
    subsChart.data.datasets[1].data = slimMentions;
    subsChart.update();

    const totalMentions = mentions.reduce((a,b) => a+b, 0);
    const totalPosts = posts.reduce((a,b) => a+b, 0);
    const totalComments = comments.reduce((a,b) => a+b, 0);
    const avgMentions = mentions.length ? totalMentions / mentions.length : 0;
    // Show average for last 7 days within the selected (visible) range
    const daysToShow = Math.min(7, mentions.length);
    const last7 = mentions.slice(-daysToShow);
    const avg7 = last7.length ? last7.reduce((a,b)=>a+b,0) / last7.length : 0;
    document.getElementById('mentionsPerDay').textContent = `${fmt(avgMentions)} avg/day · ${fmt(avg7)} last ${daysToShow}d`;
    document.getElementById('mentionsPerPost').textContent = totalPosts ? `${fmt(totalMentions / totalPosts)} mentions per post` : '—';
    document.getElementById('commentsPerPost').textContent = totalPosts ? `${fmt(totalComments / totalPosts)} comments per post` : '—';

    // Peak day
    let peakIdx = 0; let peakVal = 0;
    mentions.forEach((v, i) => { if(v > peakVal){ peakVal = v; peakIdx = i; } });
    document.getElementById('peakDay').textContent = labels[peakIdx] || '—';
    document.getElementById('peakValue').textContent = `${fmt(peakVal)} mentions on peak day`;

    // New subs within the selected date range
    // newSubs already reflects the visible window, so sum directly
    const sumNewSubs = newSubs.reduce((a,b)=>a+b,0);
    document.getElementById('newSubs30').textContent = fmt(sumNewSubs);
    
    // Summaries
    const lastLabel = labels[labels.length-1];
    document.getElementById('timelineSummary').textContent = `${fmt(totalMentions)} mentions, ${fmt(totalPosts)} posts, ${fmt(totalComments)} comments across ${labels.length} days`;
    const peakSubs = Math.max(...newSubs, 0);
    document.getElementById('subsSummary').textContent = `${fmt(sumNewSubs)} new subs (${currentDays >= 999999 ? 'all time' : currentDays + 'd'}) · peak ${fmt(peakSubs)} in a day`;
  } catch(err) {
    console.warn('daily failed', err);
    if(err.message && err.message.includes('JSON')) {
      console.error('API server may not be running. Please start docker-compose to access analytics data.');
    }
  }
}

async function fetchTopBlocks(){
  try {
    // Top subreddits
    const ts = await fetch(`/stats/top?limit=20&days=${currentDays}`);
    if(ts.ok){
      const data = await ts.json();
      // Keep full set but only show 5 by default with a Show more button to expand to 15
      const full = (data || []).slice(0, 15);
      const dataKey = JSON.stringify(full);
      if(cache.topBlocks.subreddits !== dataKey) {
        cache.topBlocks.subreddits = dataKey;
        const tbody = document.querySelector('#topSubredditsTable tbody');
        tbody.innerHTML = '';

        const renderCount = (count) => {
          tbody.innerHTML = '';
          full.slice(0, count).forEach((row, idx) => {
            const tr = document.createElement('tr');
            const subHref = `https://www.reddit.com/r/${encodeURIComponent(row.name)}`;
            tr.innerHTML = `<td>${idx+1}</td><td><a href="${subHref}" target="_blank" rel="noopener noreferrer">/r/${row.name}</a></td><td>${fmt(row.mentions)}</td>`;
            tbody.appendChild(tr);
          });
        };

        // Default to 5 rows, expandable to 15
        const defaultCount = Math.min(5, full.length);
        const expandedCount = Math.min(15, full.length);
        renderCount(defaultCount);

        // Remove existing show-more if any
        const existingBtn = document.getElementById('topSubredditsTable-showmore');
        if (existingBtn) existingBtn.remove();

        if (full.length > defaultCount) {
          const btn = document.createElement('button');
          btn.id = 'topSubredditsTable-showmore';
          btn.className = 'show-more-btn';
          btn.dataset.expanded = 'false';
          btn.textContent = `Show more (${expandedCount})`;
          btn.addEventListener('click', () => {
            const expanded = btn.dataset.expanded === 'true';
            if (!expanded) {
              renderCount(expandedCount);
              btn.textContent = 'Show less';
              btn.dataset.expanded = 'true';
            } else {
              renderCount(defaultCount);
              btn.textContent = `Show more (${expandedCount})`;
              btn.dataset.expanded = 'false';
            }
          });
          // Insert after the table
          const table = document.getElementById('topSubredditsTable');
          table.insertAdjacentElement('afterend', btn);
        }
      }
    }

    // Top commenters
    const tc = await fetch(`/stats/top_commenters?limit=15&days=${currentDays}`);
    if(tc.ok){
      const data = await tc.json();
      const items = (data.items || []).slice(0,15);
      const dataKey = JSON.stringify(items);
      if(cache.topBlocks.commenters !== dataKey) {
        cache.topBlocks.commenters = dataKey;
        const tbody = document.querySelector('#topCommentersTable tbody');
        tbody.innerHTML = '';

        const renderCount = (count) => {
          tbody.innerHTML = '';
          let rowNum = 1;
          let topName = '—', topCount = '—';
          items.slice(0, count).forEach((row) => {
            if(!row.user_id){ return; }
            const label = row.user_id;
            let userCell = '';
            if(label.toLowerCase() === '[deleted]'){
              userCell = '[deleted]';
            } else {
              const href = `https://www.reddit.com/user/${encodeURIComponent(label)}`;
              userCell = `<a href="${href}" target="_blank">${label}</a>`;
            }
            if(topName === '—'){ topName = label === '[deleted]' ? '[deleted]' : label; topCount = fmt(row.comments); }
            // Prefer explicit comment counts; fall back to mentions if present for older payloads
            const commentCount = (row.comments ?? row.mentions ?? row.count ?? 0);
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${rowNum}</td><td>${userCell}</td><td>${fmt(commentCount)}</td>`;
            tbody.appendChild(tr);
            rowNum++;
          });
          const first = items[0];
          if(first && first.user_id){
            const tn = first.user_id.toLowerCase() === '[deleted]' ? '[deleted]' : first.user_id;
            const nameEl = document.getElementById('topCommenterName');
            if(tn === '[deleted]'){
              nameEl.textContent = tn;
            } else {
              // Link to the user's Reddit comments page sorted by top. Opens in a new tab.
              nameEl.innerHTML = `<a href="https://www.reddit.com/user/${encodeURIComponent(tn)}/comments?sort=top" target="_blank" rel="noopener noreferrer">${tn}</a>`;
            }
            const topCount = (first.comments ?? first.mentions ?? first.count ?? 0);
            document.getElementById('topCommenterCount').textContent = `${fmt(topCount)} comments logged`;
          } else {
            document.getElementById('topCommenterName').textContent = '—';
            document.getElementById('topCommenterCount').textContent = '—';
          }
        };

        const defaultCount = Math.min(5, items.length);
        const expandedCount = Math.min(15, items.length);
        renderCount(defaultCount);

        const existingBtn = document.getElementById('topCommentersTable-showmore');
        if (existingBtn) existingBtn.remove();
        if (items.length > defaultCount) {
          const btn = document.createElement('button');
          btn.id = 'topCommentersTable-showmore';
          btn.className = 'show-more-btn';
          btn.dataset.expanded = 'false';
          btn.textContent = `Show more (${expandedCount})`;
          btn.addEventListener('click', () => {
            const expanded = btn.dataset.expanded === 'true';
            if (!expanded) {
              renderCount(expandedCount);
              btn.textContent = 'Show less';
              btn.dataset.expanded = 'true';
            } else {
              renderCount(defaultCount);
              btn.textContent = `Show more (${expandedCount})`;
              btn.dataset.expanded = 'false';
            }
          });
          const table = document.getElementById('topCommentersTable');
          table.insertAdjacentElement('afterend', btn);
        }
      }
    }

    // Top posts (by mentions)
    const tp = await fetch(`/stats/top_posts?limit=15&days=${currentDays}`);
    if(tp.ok){
      const data = await tp.json();
      const items = (data.items || []).slice(0,15);
      const dataKey = JSON.stringify(items);
      if(cache.topBlocks.posts !== dataKey) {
        cache.topBlocks.posts = dataKey;
        const tbody = document.querySelector('#topPostsTable tbody');

        const renderCount = (count) => {
          tbody.innerHTML = '';
          items.slice(0, count).forEach((row, idx) => {
            const title = (row.title || row.reddit_post_id || '').slice(0,120) || '(untitled)';
            const url = `https://www.reddit.com/comments/${encodeURIComponent(row.reddit_post_id)}`;
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${idx+1}</td><td><a href="${url}" target="_blank">${title}</a></td><td>${fmt(row.mentions)}</td>`;
            tbody.appendChild(tr);
          });
        };

        const defaultCount = Math.min(5, items.length);
        const expandedCount = Math.min(15, items.length);
        renderCount(defaultCount);

        const existingBtn = document.getElementById('topPostsTable-showmore');
        if (existingBtn) existingBtn.remove();
        if (items.length > defaultCount) {
          const btn = document.createElement('button');
          btn.id = 'topPostsTable-showmore';
          btn.className = 'show-more-btn';
          btn.dataset.expanded = 'false';
          btn.textContent = `Show more (${expandedCount})`;
          btn.addEventListener('click', () => {
            const expanded = btn.dataset.expanded === 'true';
            if (!expanded) {
              renderCount(expandedCount);
              btn.textContent = 'Show less';
              btn.dataset.expanded = 'true';
            } else {
              renderCount(defaultCount);
              btn.textContent = `Show more (${expandedCount})`;
              btn.dataset.expanded = 'false';
            }
          });
          const table = document.getElementById('topPostsTable');
          table.insertAdjacentElement('afterend', btn);
        }
      }
    }

    // Top mentioners (users who mentioned the most distinct subreddits)
    const tm = await fetch(`/stats/top_mentioners?limit=15&days=${currentDays}`);
    if(tm.ok){
      const data = await tm.json();
      const items = (data.items || []).slice(0,15);
      const dataKey = JSON.stringify(items);
      if(cache.topBlocks.mentioners !== dataKey) {
        cache.topBlocks.mentioners = dataKey;
        const tbody = document.querySelector('#topMentionersTable tbody');

        const renderCount = (count) => {
          tbody.innerHTML = '';
          items.slice(0, count).forEach((row, idx) => {
            if(!row.user_id) return;
            const href = `https://www.reddit.com/user/${encodeURIComponent(row.user_id)}`;
            const userCell = `<a href="${href}" target="_blank">${row.user_id}</a>`;
            const tr = document.createElement('tr');
            tr.innerHTML = `<td>${idx+1}</td><td>${userCell}</td><td>${fmt(row.unique_subreddits)}</td>`;
            tbody.appendChild(tr);
          });
        };

        const defaultCount = Math.min(5, items.length);
        const expandedCount = Math.min(15, items.length);
        renderCount(defaultCount);

        const existingBtn = document.getElementById('topMentionersTable-showmore');
        if (existingBtn) existingBtn.remove();
        if (items.length > defaultCount) {
          const btn = document.createElement('button');
          btn.id = 'topMentionersTable-showmore';
          btn.className = 'show-more-btn';
          btn.dataset.expanded = 'false';
          btn.textContent = `Show more (${expandedCount})`;
          btn.addEventListener('click', () => {
            const expanded = btn.dataset.expanded === 'true';
            if (!expanded) {
              renderCount(expandedCount);
              btn.textContent = 'Show less';
              btn.dataset.expanded = 'true';
            } else {
              renderCount(defaultCount);
              btn.textContent = `Show more (${expandedCount})`;
              btn.dataset.expanded = 'false';
            }
          });
          const table = document.getElementById('topMentionersTable');
          if(table) table.insertAdjacentElement('afterend', btn);
        }
        // Update top mentioner card with first item
        const firstMentioner = items[0];
        const topMentionerNameEl = document.getElementById('topMentionerName');
        const topMentionerCountEl = document.getElementById('topMentionerCount');
        if(firstMentioner && firstMentioner.user_id){
          const tn = firstMentioner.user_id.toLowerCase() === '[deleted]' ? '[deleted]' : firstMentioner.user_id;
          if(topMentionerNameEl){
            if(tn === '[deleted]'){
              topMentionerNameEl.textContent = tn;
            } else {
              topMentionerNameEl.innerHTML = `<a href="https://www.reddit.com/user/${encodeURIComponent(tn)}/comments?sort=top" target="_blank" rel="noopener noreferrer">${tn}</a>`;
            }
          }
          if(topMentionerCountEl) topMentionerCountEl.textContent = `${fmt(firstMentioner.unique_subreddits ?? firstMentioner.count ?? 0)} subreddits mentioned`;
        } else {
          if(topMentionerNameEl) topMentionerNameEl.textContent = '—';
          if(topMentionerCountEl) topMentionerCountEl.textContent = '—';
        }
      }
    }
  } catch(err) {
    console.warn('top blocks failed', err);
    if(err.message && err.message.includes('JSON')) {
      console.error('API server may not be running. Please start docker-compose to access analytics data.');
    }
  }
}

bindControls();
initializeDateRange();

// Initialize with age gate
async function init() {
  // Fetch config first, then load data
  await fetchConfig();
  fetchStats();
  fetchMetadataStats();
  fetchDaily(currentDays);
  fetchTopBlocks();
  
  // Auto-refresh every 30 seconds
  setInterval(() => { 
    fetchStats();
    fetchMetadataStats();
    fetchDaily(currentDays);
    fetchTopBlocks();
    fetchServiceHealth();
  }, 30000);
}

initWithAgeGate(init);

// Fetch service health immediately and periodically
fetchServiceHealth();
setInterval(fetchServiceHealth, 30000);
