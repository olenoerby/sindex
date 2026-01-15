import os
from datetime import datetime, timedelta
import httpx
import os
import logging

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, select, desc, func, text, literal_column
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
def list_subreddits(
    page: int = 1,
    per_page: int = 50,
    sort: str = 'mentions',
    sort_dir: str = 'desc',
    random_seed: Optional[str] = None,
    q: Optional[str] = None,
    min_mentions: Optional[int] = None,
    max_mentions: Optional[int] = None,
    min_subscribers: Optional[int] = None,
    max_subscribers: Optional[int] = None,
    show_available: Optional[bool] = None,
    show_banned: Optional[bool] = None,
    show_nsfw: Optional[bool] = None,
    show_non_nsfw: Optional[bool] = None,
):
    # enforce sensible limits to avoid huge responses
    max_per = 500
    per_page = min(max_per, max(1, int(per_page)))
    page = max(1, int(page))
    offset = (page - 1) * per_page
    # validate sort and sort_dir here to avoid FastAPI raising a 422
    allowed_sorts = {'mentions','subscribers','active_users','created_utc','first_mentioned','name','display_name_prefixed','title','description','random'}
    if not sort or sort not in allowed_sorts:
        sort = 'mentions'
    allowed_dirs = {'asc','desc','random'}
    if not sort_dir or sort_dir not in allowed_dirs:
        sort_dir = 'desc'
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

        # Apply text search filter if provided
        if q:
            q_lower = f"%{q.lower()}%"
            subq = subq.filter(
                func.lower(models.Subreddit.name).like(q_lower) |
                func.lower(models.Subreddit.display_name_prefixed).like(q_lower) |
                func.lower(models.Subreddit.title).like(q_lower) |
                func.lower(models.Subreddit.description).like(q_lower)
            )

        # Apply subscriber filters
        if min_subscribers is not None:
            subq = subq.filter((models.Subreddit.subscribers == None) | (models.Subreddit.subscribers >= int(min_subscribers)))
        if max_subscribers is not None:
            subq = subq.filter((models.Subreddit.subscribers == None) | (models.Subreddit.subscribers <= int(max_subscribers)))

        # NSFW filters
        # If client requests only NSFW: include rows where over18 is explicitly True OR NULL
        # (treat untagged subreddits as NSFW by default).
        if (show_nsfw is True) and (show_non_nsfw is not True):
            subq = subq.filter((models.Subreddit.over18 == True) | (models.Subreddit.over18 == None))
        # If client requests only non-NSFW: include only rows where over18 is explicitly False
        if (show_non_nsfw is True) and (show_nsfw is not True):
            subq = subq.filter(models.Subreddit.over18 == False)

        # Availability filters
        # Show available: include rows where not_found is not True (i.e. False or NULL)
        # Show unavailable: include rows where not_found == True OR is_banned == True
        if (show_available is True) and (show_banned is not True):
            try:
                subq = subq.filter((models.Subreddit.not_found == False) | (models.Subreddit.not_found == None))
            except Exception:
                subq = subq.filter(models.Subreddit.not_found != True)
        if (show_banned is True) and (show_available is not True):
            try:
                subq = subq.filter((models.Subreddit.not_found == True) | (models.Subreddit.is_banned == True))
            except Exception:
                subq = subq.filter((models.Subreddit.not_found == True) | (models.Subreddit.is_banned == True))
        # Apply mentions filters via HAVING (since mentions is an aggregate)
        if min_mentions is not None:
            subq = subq.having(func.count(models.Mention.id) >= int(min_mentions))
        if max_mentions is not None:
            subq = subq.having(func.count(models.Mention.id) <= int(max_mentions))

        # Compute total matching rows before applying ordering/limit
        try:
            subq_count = subq.with_labels().subquery()
            total = int(session.query(func.count()).select_from(subq_count).scalar() or 0)
        except Exception:
            # Fallback to full count
            total = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)

        # Apply server-side ordering. Support random ordering and asc/desc direction.
        try:
            if sort_dir == 'random' or sort == 'random':
                    # Support stable random ordering when a client-supplied seed is provided.
                    # If `random_seed` is present, order deterministically by md5(name || seed),
                    # otherwise fall back to non-deterministic func.random().
                    if random_seed:
                        try:
                            subq = subq.order_by(func.md5(func.concat(models.Subreddit.name, literal_column("'" + str(random_seed).replace("'","''") + "'"))))
                        except Exception:
                            subq = subq.order_by(func.random())
                    else:
                        subq = subq.order_by(func.random())
            else:
                if sort == 'mentions':
                    col = literal_column('mentions')
                    subq = subq.order_by(desc(col) if sort_dir == 'desc' else col.asc())
                else:
                    col = getattr(models.Subreddit, sort)
                    # For numeric columns where NULL means "unknown" (subscribers, active_users,
                    # timestamps), place NULLs at the end so descending sort shows highest numbers first.
                    nulls_last_cols = {'subscribers', 'active_users', 'created_utc', 'first_mentioned'}
                    if sort in nulls_last_cols:
                        if sort_dir == 'desc':
                            subq = subq.order_by(desc(col).nulls_last())
                        else:
                            subq = subq.order_by(col.asc().nulls_last())
                    else:
                        subq = subq.order_by(desc(col) if sort_dir == 'desc' else col.asc())
        except Exception:
            subq = subq.order_by(desc('mentions'))

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


@app.get("/health")
def health():
    """Liveness and DB connectivity check."""
    with Session(engine) as session:
        try:
            # simple DB op
            _ = session.query(func.count(models.Subreddit.id)).limit(1).scalar()
            return {"ok": True, "db": True}
        except Exception as e:
            api_logger.exception("DB health check failed")
            return {"ok": True, "db": False, "error": str(e)}


@app.get("/stats")
def stats():
    """Aggregate statistics about the dataset.

    Returns analytics row if present, otherwise computes counts.
    """
    with Session(engine) as session:
        out = {}
        try:
            analytics = session.query(models.Analytics).first()
            if analytics:
                out.update({
                    "total_subreddits": int(analytics.total_subreddits or 0),
                    "total_posts": int(analytics.total_posts or 0),
                    "total_comments": int(analytics.total_comments or 0),
                    "total_mentions": int(analytics.total_mentions or 0),
                    "analytics_updated_at": getattr(analytics, 'updated_at', None)
                })
            # ensure we always include current last_scanned
            last_scanned = session.query(func.max(models.Subreddit.last_checked)).scalar()
            out["last_scanned"] = last_scanned
        except Exception:
            api_logger.exception("Failed to compute stats")
        # fallback to individual counts if analytics missing
        try:
            if 'total_subreddits' not in out:
                out['total_subreddits'] = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)
            if 'total_mentions' not in out:
                out['total_mentions'] = int(session.query(func.count(models.Mention.id)).scalar() or 0)
            if 'total_posts' not in out:
                out['total_posts'] = int(session.query(func.count(models.Post.id)).scalar() or 0)
            if 'total_comments' not in out:
                out['total_comments'] = int(session.query(func.count(models.Comment.id)).scalar() or 0)
        except Exception:
            api_logger.exception("Failed to compute fallback stats")
        return out


@app.get("/subreddits/{name}/mentions")
def subreddit_mentions(name: str, page: int = 1, per_page: int = 50):
    """List mentions for a given subreddit (paginated)."""
    per_page = max(1, min(500, int(per_page)))
    page = max(1, int(page))
    offset = (page - 1) * per_page
    with Session(engine) as session:
        s = session.query(models.Subreddit).filter(models.Subreddit.name == name.lower()).first()
        if not s:
            raise HTTPException(status_code=404, detail="Subreddit not found")
        q = session.query(models.Mention).filter(models.Mention.subreddit_id == s.id).order_by(desc(models.Mention.timestamp))
        total = int(session.query(func.count(models.Mention.id)).filter(models.Mention.subreddit_id == s.id).scalar() or 0)
        rows = q.offset(offset).limit(per_page).all()
        items = []
        for m in rows:
            items.append({
                "id": m.id,
                "comment_id": m.comment_id,
                "post_id": m.post_id,
                "user_id": m.user_id,
                "timestamp": m.timestamp
            })
        return {"items": items, "total": total, "page": page, "per_page": per_page}


@app.get("/random_sample")
def random_sample(n: int = 10, seed: Optional[str] = None):
    """Return `n` random subreddits. If `seed` is provided ordering is deterministic."""
    n = max(1, min(500, int(n)))
    with Session(engine) as session:
        subq = session.query(models.Subreddit, func.count(models.Mention.id).label('mentions'))\
            .join(models.Mention, models.Mention.subreddit_id == models.Subreddit.id, isouter=True)\
            .group_by(models.Subreddit.id)
        try:
            if seed:
                subq = subq.order_by(func.md5(func.concat(models.Subreddit.name, literal_column("'" + str(seed).replace("'","''") + "'"))))
            else:
                subq = subq.order_by(func.random())
        except Exception:
            subq = subq.order_by(func.random())
        rows = subq.limit(n).all()
        items = []
        for row in rows:
            s, mentions = row
            items.append({
                "name": s.name,
                "display_name_prefixed": s.display_name_prefixed,
                "mentions": int(mentions or 0)
            })
        return {"items": items}


# `POST /subreddits/refresh` endpoint removed per request.


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


@app.get("/stats/top_posts")
def stats_top_posts(limit: int = 20):
    """Top posts ordered by total mention count."""
    limit = max(1, min(500, int(limit)))
    with Session(engine) as session:
        rows = session.query(
            models.Post.reddit_post_id,
            models.Post.title,
            func.count(models.Mention.id).label('mentions')
        ).join(models.Mention, models.Mention.post_id == models.Post.id, isouter=True)
        rows = rows.group_by(models.Post.id).order_by(desc('mentions')).limit(limit).all()
        out = []
        for r in rows:
            out.append({
                'reddit_post_id': r[0],
                'title': r[1],
                'mentions': int(r[2] or 0)
            })
        return {"items": out}


@app.get("/stats/top_unique_posts")
def stats_top_unique_posts(limit: int = 20):
    """Posts ordered by number of distinct subreddits mentioned in the post's comments."""
    limit = max(1, min(500, int(limit)))
    with Session(engine) as session:
        # Count distinct subreddit_id per post via mentions
        rows = session.query(
            models.Post.reddit_post_id,
            models.Post.title,
            func.count(func.distinct(models.Mention.subreddit_id)).label('unique_subreddits')
        ).join(models.Mention, models.Mention.post_id == models.Post.id, isouter=True)
        rows = rows.group_by(models.Post.id).order_by(desc('unique_subreddits')).limit(limit).all()
        out = []
        for r in rows:
            out.append({
                'reddit_post_id': r[0],
                'title': r[1],
                'unique_subreddits': int(r[2] or 0)
            })
        return {"items": out}


@app.get("/stats/top_commenters")
def stats_top_commenters(limit: int = 20):
    """Top users by number of comments (user_id)."""
    limit = max(1, min(500, int(limit)))
    with Session(engine) as session:
        # Prefer counting users from the `mentions` table since the scanner
        # records the author/id there when a subreddit is mentioned. Fall
        # back to counting `comments.user_id` if no mention-based data exists.
        out = []
        try:
            mrows = session.query(
                models.Mention.user_id,
                func.count(models.Mention.id).label('mentions')
            ).filter(models.Mention.user_id != None)
            mrows = mrows.group_by(models.Mention.user_id).order_by(desc('mentions')).limit(limit).all()
            if mrows:
                for r in mrows:
                    out.append({'user_id': r[0], 'comments': int(r[1] or 0)})
                return {"items": out}
        except Exception:
            api_logger.exception('Failed to compute top commenters from mentions')

        # Fallback: count Comment.user_id if mentions are not available
        try:
            crows = session.query(
                models.Comment.user_id,
                func.count(models.Comment.id).label('comments')
            ).filter(models.Comment.user_id != None)
            crows = crows.group_by(models.Comment.user_id).order_by(desc('comments')).limit(limit).all()
            for r in crows:
                out.append({'user_id': r[0], 'comments': int(r[1] or 0)})
        except Exception:
            api_logger.exception('Failed to compute top commenters from comments')

        return {"items": out}


@app.get("/stats/daily")
def stats_daily(days: int = 90):
    """Return daily aggregated counts for posts, comments, mentions and new subreddits.

    The response is a list of {date: 'YYYY-MM-DD', posts: n, comments: n, mentions: n, new_subreddits: n}
    ordered from oldest to newest for the requested `days` window.
    """
    days = max(1, min(3650, int(days)))
    start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    with Session(engine) as session:
        out_map = {}
        # posts by day
        try:
            rows = session.query(
                func.to_char(func.to_timestamp(models.Post.created_utc), 'YYYY-MM-DD').label('day'),
                func.count(models.Post.id)
            ).filter(models.Post.created_utc >= start_ts).group_by('day').order_by('day').all()
            for day, cnt in rows:
                out_map.setdefault(day, {})['posts'] = int(cnt or 0)
        except Exception:
            api_logger.exception('Failed to compute daily posts')

        # comments by day
        try:
            rows = session.query(
                func.to_char(func.to_timestamp(models.Comment.created_utc), 'YYYY-MM-DD').label('day'),
                func.count(models.Comment.id)
            ).filter(models.Comment.created_utc >= start_ts).group_by('day').order_by('day').all()
            for day, cnt in rows:
                out_map.setdefault(day, {})['comments'] = int(cnt or 0)
        except Exception:
            api_logger.exception('Failed to compute daily comments')

        # mentions by day (use Mention.timestamp)
        try:
            rows = session.query(
                func.to_char(func.to_timestamp(models.Mention.timestamp), 'YYYY-MM-DD').label('day'),
                func.count(models.Mention.id)
            ).filter(models.Mention.timestamp >= start_ts).group_by('day').order_by('day').all()
            for day, cnt in rows:
                out_map.setdefault(day, {})['mentions'] = int(cnt or 0)
        except Exception:
            api_logger.exception('Failed to compute daily mentions')

        # new subreddits first_mentioned by day
        try:
            rows = session.query(
                func.to_char(func.to_timestamp(models.Subreddit.first_mentioned), 'YYYY-MM-DD').label('day'),
                func.count(models.Subreddit.id)
            ).filter(models.Subreddit.first_mentioned != None).filter(models.Subreddit.first_mentioned >= start_ts).group_by('day').order_by('day').all()
            for day, cnt in rows:
                out_map.setdefault(day, {})['new_subreddits'] = int(cnt or 0)
        except Exception:
            api_logger.exception('Failed to compute daily new subreddits')

        # produce a sorted list of dates between start and today where we have data (or zeroes)
        try:
            # build continuous date list from start to now
            start_date = (datetime.utcnow() - timedelta(days=days)).date()
            dates = [(start_date + timedelta(days=i)) for i in range(days+1)]
            items = []
            for d in dates:
                key = d.strftime('%Y-%m-%d')
                v = out_map.get(key, {})
                items.append({
                    'date': key,
                    'posts': v.get('posts', 0),
                    'comments': v.get('comments', 0),
                    'mentions': v.get('mentions', 0),
                    'new_subreddits': v.get('new_subreddits', 0)
                })
        except Exception:
            api_logger.exception('Failed to assemble daily timeline')
            items = []

        return { 'items': items }


# Removed endpoint: GET /subreddits/count — use GET /stats for aggregated counts instead.
