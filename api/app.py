import os
from datetime import datetime, timedelta
import httpx
import os
import logging

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, select, desc, func
from sqlalchemy.orm import Session
from . import models

# Logging setup: use Docker/container logs (stdout)
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
api_logger = logging.getLogger('api')
api_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
sh = logging.StreamHandler()
sh.setFormatter(fmt)
api_logger.addHandler(sh)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple")
META_CACHE_DAYS = int(os.getenv('META_CACHE_DAYS', '7'))
API_RATE_DELAY = float(os.getenv('API_RATE_DELAY', '6.5'))


def fetch_sub_about(name: str):
    url = f"https://www.reddit.com/r/{name}/about.json"
    headers = {"User-Agent": "PineappleIndexAPI/0.1"}
    r = httpx.get(url, headers=headers)
    time_sleep = API_RATE_DELAY
    try:
        # keep a small delay to avoid hammering Reddit when API is used interactively
        import time as _time
        _time.sleep(time_sleep)
    except Exception:
        pass
    return r

engine = create_engine(DATABASE_URL, echo=False, future=True)

app = FastAPI(title="Pineapple Index API")


@app.get("/", response_class=HTMLResponse)
def home():
        html = '''<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Pineapple Index — Subreddits</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
        body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial;margin:20px}
        input,select{margin-right:8px;padding:6px}
        table{width:100%;border-collapse:collapse;margin-top:12px}
        th,td{padding:8px;border-bottom:1px solid #e6e6e6;text-align:left}
        th{cursor:pointer}
        tr:hover{background:#fafafa}
        .muted{color:#666;font-size:0.9em}
    </style>
</head>
<body>
    <h1>Pineapple Index — Subreddits</h1>
    <div>
        <input id="q" placeholder="Search name or description" style="width:36%" />
        <select id="sort">
            <option value="mentions">Top mentions</option>
            <option value="subscribers">Subscribers</option>
            <option value="active_users">Active users</option>
            <option value="created_utc">Created</option>
        </select>
        <label>Min mentions <input id="minMentions" type="number" value="0" style="width:90px"/></label>
        <label style="margin-left:8px"><input id="showBanned" type="checkbox"/> Show banned</label>
        <button id="reload">Reload</button>
    </div>

    <div id="count" class="muted"></div>
    <table id="tbl">
        <thead>
            <tr>
                <th>Name</th>
                <th>Mentions</th>
                <th>Subscribers</th>
                <th>Active Users</th>
                <th>Last Checked</th>
                <th>Description</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>

    <script>
        let data = [];
        async function load(){
            document.getElementById('count').textContent = 'Loading...';
            try{
                const per = 1000;
                const res = await fetch(`/subreddits?per_page=${per}&sort=mentions`);
                data = await res.json();
                render();
            }catch(e){
                document.getElementById('count').textContent = 'Load failed: ' + e;
            }
        }

        function render(){
            const q = document.getElementById('q').value.toLowerCase().trim();
            const minM = Number(document.getElementById('minMentions').value || 0);
            const showB = document.getElementById('showBanned').checked;
            const sort = document.getElementById('sort').value;
            let list = data.slice();
            if(q){
                list = list.filter(s => (s.name||'').toLowerCase().includes(q) || (s.description||'').toLowerCase().includes(q));
            }
            if(!showB){
                list = list.filter(s => !s.is_banned);
            }
            list = list.filter(s => (s.mentions||0) >= minM);

            list.sort((a,b)=>{
                const key = sort;
                const va = (a[key]===null||a[key]===undefined)?0:a[key];
                const vb = (b[key]===null||b[key]===undefined)?0:b[key];
                return vb - va;
            });

            const tbody = document.querySelector('#tbl tbody');
            tbody.innerHTML = '';
            for(const s of list){
                const tr = document.createElement('tr');
                const nameTd = document.createElement('td');
                const a = document.createElement('a');
                a.href = `/subreddits/${encodeURIComponent(s.name)}`;
                a.textContent = s.name;
                nameTd.appendChild(a);
                tr.appendChild(nameTd);

                const mk = (v)=> v===null||v===undefined? '—': v.toString();
                tr.innerHTML += `<td>${mk(s.mentions)}</td><td>${mk(s.subscribers)}</td><td>${mk(s.active_users)}</td><td>${s.last_checked? new Date(s.last_checked).toLocaleString() : '—'}</td><td class="muted">${(s.description||'').slice(0,200)}</td>`;
                tbody.appendChild(tr);
            }
            document.getElementById('count').textContent = `${list.length} shown (from ${data.length} fetched)`;
        }

        document.getElementById('q').addEventListener('input', render);
        document.getElementById('sort').addEventListener('change', render);
        document.getElementById('minMentions').addEventListener('input', render);
        document.getElementById('showBanned').addEventListener('change', render);
        document.getElementById('reload').addEventListener('click', load);

        load();
    </script>
</body>
</html>
'''
        return HTMLResponse(html)



@app.get("/api", response_class=HTMLResponse)
def api_index():
    """Simple HTML page listing available routes for quick browsing."""
    routes = []
    for r in app.routes:
        path = getattr(r, 'path', None)
        methods = getattr(r, 'methods', None)
        summary = getattr(r, 'summary', '') or getattr(r, 'name', '')
        if not path or not methods:
            continue
        # hide internal OpenAPI/docs endpoints if desired
        if path.startswith('/openapi') or path.startswith('/docs') or path.startswith('/redoc'):
            continue
        # format methods
        try:
            m = ','.join(sorted([x for x in methods if x not in ('HEAD', 'OPTIONS')]))
        except Exception:
            m = ''
        routes.append((path, m, summary))
    # dedupe and sort
    routes = sorted(list({(p, m, s) for p, m, s in routes}), key=lambda x: x[0])
    html = ['<html><head><meta charset="utf-8"><title>API Endpoints</title></head><body>']
    html.append('<h1>API Endpoints</h1>')
    html.append('<ul>')
    for path, methods, summary in routes:
        html.append(f"<li><strong>{methods}</strong> <a href=\"{path}\">{path}</a> - {summary}</li>")
    html.append('</ul>')
    html.append('</body></html>')
    return HTMLResponse('\n'.join(html))


class SubredditOut(BaseModel):
    name: str
    display_name: Optional[str]
    display_name_prefixed: Optional[str]
    title: Optional[str]
    created_utc: Optional[int]
    first_mentioned: Optional[int]
    subscribers: Optional[int]
    active_users: Optional[int]
    description: Optional[str]
    public_description_html: Optional[str]
    is_banned: Optional[bool]
    over18: Optional[bool]
    last_checked: Optional[datetime]
    mentions: Optional[int]


@app.get("/subreddits")
def list_subreddits(page: int = 1, per_page: int = 50, sort: str = Query('mentions', regex='^(mentions|subscribers|active_users|created_utc|first_mentioned|name)$')):
    # enforce sensible limits to avoid huge responses
    max_per = 500
    per_page = min(max_per, max(1, int(per_page)))
    page = max(1, int(page))
    offset = (page - 1) * per_page
    with Session(engine) as session:
        # total count for pagination metadata: prefer analytics table if present
        try:
            analytics = session.query(models.Analytics).first()
            if analytics and getattr(analytics, 'total_subreddits', None) is not None:
                total = int(analytics.total_subreddits or 0)
            else:
                total = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)
        except Exception:
            total = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)

        subq = session.query(models.Subreddit, func.count(models.Mention.id).label('mentions'))\
            .join(models.Mention, models.Mention.subreddit_id == models.Subreddit.id, isouter=True)\
            .group_by(models.Subreddit.id)
        if sort == 'mentions':
            subq = subq.order_by(desc('mentions'))
        else:
            subq = subq.order_by(desc(getattr(models.Subreddit, sort)))

        rows = subq.offset(offset).limit(per_page).all()
        items = []
        for row in rows:
            s, mentions = row
            # update metadata on-demand if missing or stale
            now = datetime.utcnow()
            needs_update = False
            if not s.last_checked:
                needs_update = True
            else:
                try:
                    if (now - s.last_checked) > timedelta(days=META_CACHE_DAYS):
                        needs_update = True
                except Exception:
                    needs_update = True

            if needs_update:
                try:
                    r = fetch_sub_about(s.name)
                    if r.status_code == 200:
                        payload = r.json()
                        # if Reddit returns a top-level reason (e.g. banned), record it
                        if isinstance(payload, dict) and payload.get('reason'):
                            s.is_banned = True
                            s.ban_reason = str(payload.get('reason'))
                        data = payload.get('data', {}) if isinstance(payload, dict) else {}

                        def safe_int(v):
                            try:
                                return int(v) if v is not None else None
                            except Exception:
                                return None

                        # general fields
                        s.display_name = data.get('display_name') or s.display_name
                        s.display_name_prefixed = data.get('display_name_prefixed') or s.display_name_prefixed
                        s.title = data.get('title') or s.title

                        created = safe_int(data.get('created_utc'))
                        if created:
                            s.created_utc = created
                        subs = safe_int(data.get('subscribers'))
                        if subs is not None:
                            s.subscribers = subs
                        active = safe_int(data.get('accounts_active') or data.get('active_user_count') or data.get('active_accounts'))
                        if active is not None:
                            s.active_users = active
                        public_desc = data.get('public_description')
                        if public_desc:
                            s.description = public_desc

                            try:
                                s.public_description_html = data.get('public_description_html') or s.public_description_html
                            except Exception:
                                pass

                        # booleans and misc
                        try:
                            s.allow_videogifs = bool(data.get('allow_videogifs')) if data.get('allow_videogifs') is not None else s.allow_videogifs
                        except Exception:
                            pass
                        try:
                            s.allow_videos = bool(data.get('allow_videos')) if data.get('allow_videos') is not None else s.allow_videos
                        except Exception:
                            pass
                        s.subreddit_type = data.get('subreddit_type') or s.subreddit_type
                        s.lang = data.get('lang') or s.lang
                        s.url = data.get('url') or s.url
                        try:
                            ov = data.get('over18') if 'over18' in data else data.get('over_18')
                            if ov is not None:
                                s.over18 = bool(ov)
                        except Exception:
                            pass
                        s.is_banned = s.is_banned or False
                    elif r.status_code in (403, 404):
                        s.is_banned = True
                        api_logger.info(f"/r/{s.name} returned {r.status_code}; marking as banned")
                        try:
                            payload = r.json()
                            if isinstance(payload, dict) and payload.get('reason'):
                                s.ban_reason = str(payload.get('reason'))
                        except Exception:
                            pass
                    else:
                        api_logger.debug(f"/r/{s.name} metadata fetch returned status {r.status_code}")
                    # mark last_checked and commit using the current session
                    s.last_checked = datetime.utcnow()
                    session.add(s)
                    session.commit()
                except Exception:
                    api_logger.exception(f"Failed to update metadata for /r/{s.name}")

            items.append(SubredditOut(name=s.name, display_name=s.display_name, display_name_prefixed=s.display_name_prefixed, title=s.title, created_utc=s.created_utc, first_mentioned=s.first_mentioned, subscribers=s.subscribers, active_users=s.active_users, description=s.description, public_description_html=getattr(s, 'public_description_html', None), is_banned=s.is_banned, over18=s.over18, last_checked=s.last_checked, mentions=mentions).dict())

        has_more = (offset + len(items)) < total
        return {"items": items, "total": total, "page": page, "per_page": per_page, "has_more": has_more}


@app.get("/subreddits/{name}")
def get_subreddit(name: str):
    with Session(engine) as session:
        # lookup by name column since the PK is an integer id
        s = session.query(models.Subreddit).filter(models.Subreddit.name == name.lower()).first()
        if not s:
            raise HTTPException(status_code=404, detail="Subreddit not found")
        mentions = session.query(func.count(models.Mention.id)).filter(models.Mention.subreddit_id == s.id).scalar()
        return {"name": s.name, "created_utc": s.created_utc, "subscribers": s.subscribers, "active_users": s.active_users, "description": s.description, "is_banned": s.is_banned, "last_checked": s.last_checked, "mentions": mentions}


@app.get("/mentions")
def list_mentions(page: int = 1, per_page: int = 50, subreddit: Optional[str] = None):
    offset = (page - 1) * per_page
    with Session(engine) as session:
        q = session.query(models.Mention).order_by(desc(models.Mention.timestamp))
        if subreddit:
            q = q.join(models.Subreddit).filter(models.Subreddit.name == subreddit.lower())
        rows = q.offset(offset).limit(per_page).all()
        out = []
        for m in rows:
            out.append({"subreddit": m.subreddit.name, "comment_id": m.comment_id, "post_id": m.post_id, "timestamp": m.timestamp})
        return out


@app.get("/stats/top")
def stats_top(limit: int = 20):
    with Session(engine) as session:
        rows = session.query(models.Subreddit.name, func.count(models.Mention.id).label('mentions'))\
            .join(models.Mention, models.Mention.subreddit_id == models.Subreddit.id)\
            .group_by(models.Subreddit.name)\
            .order_by(desc('mentions'))\
            .limit(limit).all()
        return [{"name": r[0], "mentions": r[1]} for r in rows]


@app.get("/subreddits/count")
def subreddits_count():
    with Session(engine) as session:
        try:
            analytics = session.query(models.Analytics).first()
            if analytics and getattr(analytics, 'total_subreddits', None) is not None:
                return {"total": int(analytics.total_subreddits or 0)}
        except Exception:
            pass
        total = session.query(func.count(models.Subreddit.id)).scalar()
        return {"total": int(total or 0)}
