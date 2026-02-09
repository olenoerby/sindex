import os
from datetime import datetime, timedelta
import time
import httpx
import os
import logging
import json
import hashlib
from functools import wraps
from api.distributed_rate_limiter import DistributedRateLimiter
from api.phase import attach_phase_filter, temp_phase

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Request, Header
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, select, desc, func, text, literal_column, or_
from sqlalchemy.orm import Session
from . import models

# Logging setup: use Docker/container logs (stdout) with ISO 8601 format (UTC)
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
api_logger = logging.getLogger('api')
api_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s [%(phase)s]: %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
fmt.converter = time.gmtime  # Use UTC instead of local time
sh = logging.StreamHandler()
sh.setFormatter(fmt)
attach_phase_filter(sh)
api_logger.addHandler(sh)

# Configuration and DB
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple")
META_CACHE_DAYS = int(os.getenv('META_CACHE_DAYS', '7'))
METADATA_STALE_HOURS = int(os.getenv('METADATA_STALE_HOURS', '24'))
API_RATE_DELAY = float(os.getenv('API_RATE_DELAY', '6.5'))
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')

# Initialize distributed rate limiter (best-effort)
try:
    API_RATE_DELAY_SECONDS = float(os.getenv('API_RATE_DELAY_SECONDS', API_RATE_DELAY))
    API_MAX_CALLS_MINUTE = int(os.getenv('API_MAX_CALLS_MINUTE', os.getenv('API_MAX_CALLS_MIN', '30')))
    distributed_rate_limiter = DistributedRateLimiter(redis_url=REDIS_URL, min_delay_seconds=API_RATE_DELAY_SECONDS, max_calls_per_minute=API_MAX_CALLS_MINUTE)
    distributed_rate_limiter.set_container_name('api')
except Exception:
    distributed_rate_limiter = None

from sqlalchemy import create_engine
engine = create_engine(DATABASE_URL, echo=False, future=True)

# FastAPI app
app = FastAPI(title="Sindex API")

# Redis cache client (separate from rate limiter)
try:
    from redis import Redis
    cache_redis = Redis.from_url(REDIS_URL, decode_responses=True)
    cache_redis.ping()  # Test connection
    api_logger.info("Cache Redis connected")
except Exception as e:
    api_logger.warning(f"Cache Redis unavailable: {e}")
    cache_redis = None

# JSON encoder that handles datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Cache decorator for stats endpoints
def cache_response(ttl_seconds: int = 30):
    """Cache the JSON response in Redis with the given TTL."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not cache_redis:
                return await func(*args, **kwargs)
            
            # Generate cache key from function name and arguments
            key_data = f"{func.__name__}:{json.dumps(kwargs, sort_keys=True)}"
            cache_key = f"api_cache:{hashlib.md5(key_data.encode()).hexdigest()}"
            
            # Try to get from cache
            try:
                cached = cache_redis.get(cache_key)
                if cached:
                    return JSONResponse(content=json.loads(cached))
            except Exception as e:
                api_logger.warning(f"Cache read error: {e}")
            
            # Call the function and cache result
            result = await func(*args, **kwargs)
            try:
                # Handle different response types
                if isinstance(result, (dict, list)):
                    cache_redis.setex(cache_key, ttl_seconds, json.dumps(result, cls=DateTimeEncoder))
                elif hasattr(result, 'body'):
                    cache_redis.setex(cache_key, ttl_seconds, result.body.decode())
            except Exception as e:
                api_logger.warning(f"Cache write error: {e}")
            
            return result
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not cache_redis:
                return func(*args, **kwargs)
            
            # Generate cache key from function name and arguments
            key_data = f"{func.__name__}:{json.dumps(kwargs, sort_keys=True)}"
            cache_key = f"api_cache:{hashlib.md5(key_data.encode()).hexdigest()}"
            
            # Try to get from cache
            try:
                cached = cache_redis.get(cache_key)
                if cached:
                    return json.loads(cached)
            except Exception as e:
                api_logger.warning(f"Cache read error: {e}")
            
            # Call the function and cache result
            result = func(*args, **kwargs)
            try:
                cache_redis.setex(cache_key, ttl_seconds, json.dumps(result, cls=DateTimeEncoder))
            except Exception as e:
                api_logger.warning(f"Cache write error: {e}")
            
            return result
        
        # Return appropriate wrapper based on whether function is async
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator


def fetch_sub_about(name: str):
    url = f"https://www.reddit.com/r/{name}/about.json"
    headers = {"User-Agent": "SindexAPI/0.1"}
    # Respect distributed/local limiter before making request
    try:
        if distributed_rate_limiter:
            distributed_rate_limiter.wait_if_needed()
        else:
            import time as _time
            with temp_phase('Rate Limiting + Retries'):
                _time.sleep(API_RATE_DELAY)
    except Exception:
        pass

    r = httpx.get(url, headers=headers)
    # Record API call for distributed limiter
    try:
        if distributed_rate_limiter:
            distributed_rate_limiter.record_api_call()
    except Exception:
        pass

    return r
@app.post("/subreddits/{name}/refresh")
def refresh_subreddit(name: str, x_api_key: Optional[str] = Header(None)):
    """Enqueue a background job to refresh subreddit metadata.

    Requires `X-API-Key` header when `API_KEY` is set in the environment.
    Enforces a per-subreddit cooldown and a simple global rate limit using Redis.
    Returns 202 Accepted with job info when queued.
    """
    # API key check: allow requests when no API_KEY configured
    ENV_API_KEY = os.getenv('API_KEY')
    
    # If API key is configured, require it via header only
    is_authenticated = False
    if ENV_API_KEY:
        if not x_api_key or x_api_key != ENV_API_KEY:
            raise HTTPException(status_code=403, detail='Invalid or missing API key')
        is_authenticated = True

    lname = name.lower().strip()
    # cooldown and rate limiting
    REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    from redis import Redis
    redis = Redis.from_url(REDIS_URL)
    COOLDOWN = int(os.getenv('REFRESH_COOLDOWN_SECONDS', '900'))
    GLOBAL_LIMIT = int(os.getenv('REFRESH_GLOBAL_PER_MIN', '60'))

    with Session(engine) as session:
        # Skip cooldown check for authenticated requests
        if not is_authenticated:
            s = session.query(models.Subreddit).filter(models.Subreddit.name == lname).first()
            if s and s.last_checked:
                delta = (datetime.utcnow() - s.last_checked).total_seconds()
                if delta < COOLDOWN:
                    retry_after = int(COOLDOWN - delta)
                    raise HTTPException(status_code=429, detail=f'Subreddit recently refreshed; retry after {retry_after} seconds')

        # simple global rate limit per minute
        try:
            key = 'pineapple:refresh:global'
            cnt = redis.incr(key)
            if cnt == 1:
                redis.expire(key, 60)
            if cnt > GLOBAL_LIMIT:
                raise HTTPException(status_code=429, detail='Global refresh rate limit exceeded')
        except Exception:
            # if Redis unavailable, continue but log
            api_logger.warning('Redis unavailable for rate limiting; proceeding')

        # enqueue job using RQ
        try:
            from rq import Queue
            from redis import Redis as _Redis
            REDIS = _Redis.from_url(REDIS_URL)
            q = Queue(connection=REDIS)
            # reference the callable in api.tasks
            import api.tasks as tasks
            job = q.enqueue(tasks.refresh_subreddit_job, lname, job_timeout=300)
            return {"ok": True, "job_id": job.id, "message": "Refresh enqueued"}, 202
        except Exception as e:
            api_logger.exception('Failed to enqueue refresh job')
            raise HTTPException(status_code=500, detail='Failed to enqueue refresh job')


@app.post("/subreddits/refresh-pending")
def refresh_pending_subreddits(x_api_key: Optional[str] = Header(None)):
    """Enqueue refresh jobs for all pending subreddits (title IS NULL).
    
    Requires API key authentication via X-API-Key header.
    Returns count of jobs enqueued.
    """
    # API key check
    ENV_API_KEY = os.getenv('API_KEY')
    
    if ENV_API_KEY:
        if not x_api_key or x_api_key != ENV_API_KEY:
            raise HTTPException(status_code=403, detail='Invalid or missing API key')
    
    REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    
    with Session(engine) as session:
        # Find all pending subreddits
        pending = session.query(models.Subreddit).filter(
            models.Subreddit.title == None
        ).all()
        
        if not pending:
            return {"ok": True, "enqueued": 0, "message": "No pending subreddits found"}
        
        # Enqueue jobs
        try:
            from rq import Queue
            from redis import Redis as _Redis
            REDIS = _Redis.from_url(REDIS_URL)
            q = Queue(connection=REDIS)
            import api.tasks as tasks
            
            job_ids = []
            for sub in pending:
                job = q.enqueue(tasks.refresh_subreddit_job, sub.name, job_timeout=300)
                job_ids.append(job.id)
            
            api_logger.info(f"Enqueued {len(job_ids)} refresh jobs for pending subreddits")
            return {
                "ok": True,
                "enqueued": len(job_ids),
                "total_pending": len(pending),
                "message": f"Enqueued {len(job_ids)} refresh jobs"
            }, 202
            
        except Exception as e:
            api_logger.exception('Failed to enqueue pending refresh jobs')
            raise HTTPException(status_code=500, detail=f'Failed to enqueue jobs: {str(e)}')


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
    html = [
        '<html><head><meta charset="utf-8"><title>API Endpoints</title>',
        '  <script async src="https://www.googletagmanager.com/gtag/js?id=G-X5E2L44KYG"></script>',
        '  <script>',
        "    window.dataLayer = window.dataLayer || [];",
        "    function gtag(){dataLayer.push(arguments);}",
        "    gtag('js', new Date());",
        "    gtag('config', 'G-X5E2L44KYG');",
        '  </script>',
        '</head><body>'
    ]
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
    display_name_prefixed: Optional[str]  # Add prefixed version like r/subreddit
    title: Optional[str]
    created_utc: Optional[int]
    first_mentioned: Optional[int]
    subscribers: Optional[int]
    active_users: Optional[int]
    description: Optional[str]
    is_banned: Optional[bool]
    subreddit_found: Optional[bool]  # False if subreddit doesn't exist (404)
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
    show_pending: Optional[bool] = None,
    show_nsfw: Optional[bool] = None,
    show_non_nsfw: Optional[bool] = None,
    first_mentioned_days: Optional[int] = None,
    tags: Optional[str] = None,
    tag_mode: str = 'any',
):
    # enforce sensible limits to avoid huge responses
    max_per = 500
    try:
        per_page = min(max_per, max(1, int(per_page)))
        page = max(1, int(page))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid page or per_page parameter")
    
    offset = (page - 1) * per_page
    
    # Validate numeric filter parameters
    try:
        if min_mentions is not None:
            min_mentions = max(0, int(min_mentions))
        if max_mentions is not None:
            max_mentions = max(0, min(1000000, int(max_mentions)))
        if min_subscribers is not None:
            min_subscribers = max(0, int(min_subscribers))
        if max_subscribers is not None:
            max_subscribers = max(0, min(1000000000, int(max_subscribers)))
        if first_mentioned_days is not None:
            first_mentioned_days = max(1, min(3650, int(first_mentioned_days)))
    except (ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid numeric filter parameter: {str(e)}")
    
    # Validate and sanitize random_seed if provided
    if random_seed:
        random_seed = str(random_seed)[:100]  # Limit to 100 chars
    
    # Debug logging for filter parameters
    api_logger.debug(f"Filter params: show_available={show_available}, show_banned={show_banned}, show_pending={show_pending}, show_nsfw={show_nsfw}, show_non_nsfw={show_non_nsfw}")
    
    # validate sort and sort_dir here to avoid FastAPI raising a 422
    allowed_sorts = {'mentions','subscribers','active_users','created_utc','first_mentioned','last_checked','name','display_name_prefixed','title','description','random'}
    if not sort or sort not in allowed_sorts:
        sort = 'mentions'
    allowed_dirs = {'asc','desc','random'}
    if not sort_dir or sort_dir not in allowed_dirs:
        sort_dir = 'desc'
    with Session(engine) as session:
        # Total count reflects all subreddits (including those with 0 mentions)
        try:
            analytics = session.query(models.Analytics).first()
            if analytics and getattr(analytics, 'total_subreddits', None) is not None:
                db_total = int(analytics.total_subreddits or 0)
            else:
                db_total = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)
        except Exception:
            db_total = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)

        subq = session.query(models.Subreddit, func.count(models.Mention.id).label('mentions'))\
            .join(models.Mention, models.Mention.subreddit_id == models.Subreddit.id, isouter=True)\
            .group_by(models.Subreddit.id)

        # Apply category tag filters if provided
        if tags:
            try:
                tag_ids = [int(tid.strip()) for tid in tags.split(',') if tid.strip()]
                if tag_ids:
                    if tag_mode == 'all':
                        # AND mode: subreddit must have ALL specified tags
                        tag_count_subq = session.query(
                            models.SubredditCategoryTag.subreddit_id,
                            func.count(models.SubredditCategoryTag.id).label('tag_count')
                        ).filter(
                            models.SubredditCategoryTag.category_tag_id.in_(tag_ids)
                        ).group_by(
                            models.SubredditCategoryTag.subreddit_id
                        ).having(
                            func.count(models.SubredditCategoryTag.id) == len(tag_ids)
                        ).subquery()
                        
                        subq = subq.join(
                            tag_count_subq,
                            tag_count_subq.c.subreddit_id == models.Subreddit.id
                        )
                    else:
                        # OR mode (default): subreddit must have ANY of the specified tags
                        subq = subq.join(
                            models.SubredditCategoryTag,
                            models.SubredditCategoryTag.subreddit_id == models.Subreddit.id
                        ).filter(
                            models.SubredditCategoryTag.category_tag_id.in_(tag_ids)
                        )
            except ValueError:
                # Invalid tag IDs provided, ignore filter
                pass

        # Apply text search filter if provided
        if q:
            q_lower = f"%{q.lower()}%"
            # Use COALESCE to handle NULL values safely
            subq = subq.filter(
                or_(
                    func.lower(models.Subreddit.name).like(q_lower),
                    func.lower(func.coalesce(models.Subreddit.display_name, '')).like(q_lower),
                    func.lower(func.coalesce(models.Subreddit.title, '')).like(q_lower),
                    func.lower(func.coalesce(models.Subreddit.description, '')).like(q_lower)
                )
            )

        # Apply subscriber filters
        if min_subscribers is not None:
            subq = subq.filter((models.Subreddit.subscribers == None) | (models.Subreddit.subscribers >= int(min_subscribers)))
        if max_subscribers is not None:
            subq = subq.filter((models.Subreddit.subscribers == None) | (models.Subreddit.subscribers <= int(max_subscribers)))

        # NSFW filters - work as AND conditions
        # show_nsfw=True means "include NSFW", =False means "exclude NSFW"
        # show_non_nsfw=True means "include SFW", =False means "exclude SFW"
        # Build OR conditions for what to include, then filter
        nsfw_conditions = []
        if show_nsfw is True:
            # Include NSFW and unknown (NULL treated as potentially NSFW)
            nsfw_conditions.append((models.Subreddit.is_over18 == True) | (models.Subreddit.is_over18 == None))
        if show_non_nsfw is True:
            # Include non-NSFW
            nsfw_conditions.append(models.Subreddit.is_over18 == False)
        
        if nsfw_conditions:
            # At least one is enabled - combine with OR
            if len(nsfw_conditions) == 1:
                subq = subq.filter(nsfw_conditions[0])
            else:
                subq = subq.filter(or_(*nsfw_conditions))
        elif show_nsfw is False and show_non_nsfw is False:
            # Both explicitly disabled - return empty result
            subq = subq.filter(models.Subreddit.id == None)
        # else: both are None (not specified) - default to showing all (no filter)

        # Availability filters - work as AND conditions
        # show_available=True means "include available", =False means "exclude available"
        # show_banned=True means "include banned", =False means "exclude banned"
        
        # Build OR conditions for what to include
        avail_conditions = []
        if show_available is True:
            # Include available subreddits (not banned and subreddit exists)
            try:
                avail_conditions.append(
                    ((models.Subreddit.is_banned == False) | (models.Subreddit.is_banned == None)) &
                    ((models.Subreddit.subreddit_found == True) | (models.Subreddit.subreddit_found == None))
                )
            except Exception:
                avail_conditions.append(
                    (models.Subreddit.is_banned != True) & (models.Subreddit.subreddit_found == True)
                )
        if show_banned is True:
            # Include banned/unavailable subreddits (is_banned=True OR subreddit_found=False)
            try:
                avail_conditions.append(
                    (models.Subreddit.is_banned == True) | (models.Subreddit.subreddit_found == False)
                )
            except Exception:
                avail_conditions.append(models.Subreddit.is_banned == True)
        
        if avail_conditions:
            # At least one is enabled - combine with OR
            if len(avail_conditions) == 1:
                subq = subq.filter(avail_conditions[0])
            else:
                subq = subq.filter(or_(*avail_conditions))
        elif show_available is False and show_banned is False and show_pending is not True:
            # Both availability filters explicitly disabled and not filtering by pending - return empty result
            subq = subq.filter(models.Subreddit.id == None)
        # else: both are None (not specified) or show_pending will handle filtering - default to showing all (no filter)
        
        # Handle pending filter
        # When show_pending=True and availability filters are disabled, show only available pending subreddits
        # When show_pending=False, exclude pending subreddits
        if show_pending is True and show_available is False and show_banned is False:
            # Only available pending subreddits (not banned/not found, and title is None/NULL)
            try:
                subq = subq.filter(
                    ((models.Subreddit.is_banned == False) | (models.Subreddit.is_banned == None)) &
                    ((models.Subreddit.subreddit_found == True) | (models.Subreddit.subreddit_found == None)) &
                    (models.Subreddit.title == None)
                )
            except Exception:
                subq = subq.filter(
                    (models.Subreddit.is_banned != True) &
                    (models.Subreddit.subreddit_found == True) &
                    (models.Subreddit.title == None)
                )
        elif show_pending is False and (show_available is True or show_available is None):
            # Exclude pending subreddits (title is not None) only when showing available
            subq = subq.filter(models.Subreddit.title != None)
        # If show_pending is True with other filters: include all (both pending and non-pending)
        # If show_pending is None: default behavior (include all)
        # Note: When show_banned=True, banned subreddits often have NULL metadata,
        # so we don't filter by pending status to avoid excluding them
        
        # Apply mentions filters via HAVING (since mentions is an aggregate)
        # Do not force a minimum mention count; include subreddits with 0 mentions
        if min_mentions is not None:
            subq = subq.having(func.count(models.Mention.id) >= int(min_mentions))
        if max_mentions is not None:
            subq = subq.having(func.count(models.Mention.id) <= int(max_mentions))
        
        # Apply first_mentioned date filter
        if first_mentioned_days is not None:
            from datetime import timezone
            now_ts = int(datetime.now(timezone.utc).timestamp())
            cutoff_ts = now_ts - (int(first_mentioned_days) * 24 * 60 * 60)
            subq = subq.filter(models.Subreddit.first_mentioned != None)
            subq = subq.filter(models.Subreddit.first_mentioned >= cutoff_ts)

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
                            # Use bind parameter to prevent SQL injection
                            from sqlalchemy import bindparam
                            seed_param = bindparam('seed_value', value=str(random_seed)[:100])  # limit length
                            subq = subq.order_by(func.md5(func.concat(models.Subreddit.name, seed_param)))
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
                    nulls_last_cols = {'subscribers', 'active_users', 'created_utc', 'first_mentioned', 'last_checked'}
                    if sort in nulls_last_cols:
                        if sort_dir == 'desc':
                            subq = subq.order_by(desc(col).nulls_last())
                        else:
                            subq = subq.order_by(col.asc().nulls_last())
                    else:
                        subq = subq.order_by(desc(col) if sort_dir == 'desc' else col.asc())
        except Exception:
            subq = subq.order_by(desc('mentions'))

        try:
            rows = subq.offset(offset).limit(per_page).all()
        except Exception as e:
            api_logger.exception(f"Query execution failed with q={q}")
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
            
        items = []
        for row in rows:
            s, mentions = row
            # API is read-only - just return database data. Scanner handles all metadata updates.
            # Construct display_name_prefixed from display_name or name
            display_name_prefixed = None
            if s.display_name:
                display_name_prefixed = f"r/{s.display_name}"
            elif s.name:
                display_name_prefixed = f"r/{s.name}"
            
            items.append(SubredditOut(
                name=s.name,
                display_name=s.display_name,
                display_name_prefixed=display_name_prefixed,
                title=s.title,
                created_utc=s.created_utc,
                first_mentioned=s.first_mentioned,
                subscribers=s.subscribers,
                active_users=s.active_users,
                description=s.description,
                is_banned=s.is_banned,
                subreddit_found=s.subreddit_found if hasattr(s, 'subreddit_found') else True,
                over18=s.is_over18,
                last_checked=s.last_checked,
                mentions=mentions
            ).dict())

        has_more = (offset + len(items)) < total
        return {"items": items, "total": total, "page": page, "per_page": per_page, "has_more": has_more, "db_total": db_total}


@app.get("/health")
def health():
    """Liveness and DB connectivity check."""
    with Session(engine) as session:
        try:
            # simple DB op
            _ = session.query(func.count(models.Subreddit.id)).limit(1).scalar()
            # Prefer checking scanner via its HTTP health endpoint (safe, low-privilege).
            scanner_ok = False
            scanner_last = None
            scanner_url = os.getenv('SCANNER_HEALTH_URL', os.getenv('SCANNER_URL', 'http://scanner:8001/health'))
            try:
                try:
                    r = httpx.get(scanner_url, timeout=float(os.getenv('SCANNER_HEALTH_TIMEOUT_SECONDS', '1.0')))
                    if r.status_code == 200:
                        try:
                            jr = r.json()
                            scanner_ok = bool(jr.get('ok', True))
                            scanner_last = jr.get('last_scan_started') or jr.get('last_scan_started')
                        except Exception:
                            scanner_ok = True
                except Exception:
                    # HTTP check failed; fall back to DB timestamp check
                    api_logger.debug("Scanner HTTP health check failed, falling back to DB timestamp")
                    analytics = session.query(models.Analytics).first()
                    if analytics and getattr(analytics, 'last_scan_started', None):
                        scanner_last = getattr(analytics, 'last_scan_started')
                        threshold_min = int(os.getenv('SCANNER_HEALTH_THRESHOLD_MINUTES', '10'))
                        try:
                            if isinstance(scanner_last, datetime):
                                age = datetime.utcnow() - (scanner_last.replace(tzinfo=None) if scanner_last.tzinfo else scanner_last)
                            else:
                                age = timedelta.max
                            scanner_ok = age <= timedelta(minutes=threshold_min)
                        except Exception:
                            scanner_ok = False
            except Exception:
                api_logger.exception("Scanner health check failed")

            out = {"api-health": True, "db-health": True, "scanner-health": scanner_ok}
            if scanner_last:
                out["scanner-last-scan-started"] = scanner_last

            return out
        except Exception as e:
            api_logger.exception("DB health check failed")
            return {"api-health": True, "db-health": False, "error": str(e)}


@app.get("/stats")
@cache_response(ttl_seconds=30)
def stats(days: int = None):
    """Aggregate statistics about the dataset.

    If days is provided, returns counts for that date range.
    Otherwise returns analytics row if present, or all-time counts.
    """
    with Session(engine) as session:
        out = {}
        
        # If days specified, compute counts for that window only
        if days is not None:
            days = max(1, min(3650, int(days)))
            start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
            try:
                out['total_mentions'] = int(session.query(func.count(models.Mention.id)).filter(models.Mention.timestamp >= start_ts).scalar() or 0)
                out['total_posts'] = int(session.query(func.count(models.Post.id)).filter(models.Post.created_utc >= start_ts).scalar() or 0)
                out['total_comments'] = int(session.query(func.count(models.Comment.id)).filter(models.Comment.created_utc >= start_ts).scalar() or 0)
                # For subreddits, count those first mentioned in the window
                out['total_subreddits'] = int(session.query(func.count(models.Subreddit.id)).filter(models.Subreddit.first_mentioned >= start_ts).scalar() or 0)
            except Exception:
                api_logger.exception("Failed to compute window stats")
            # ensure we always include current last_scanned
            try:
                last_scanned = session.query(func.max(models.Subreddit.last_checked)).scalar()
                out["last_scanned"] = last_scanned
            except Exception:
                pass
            # Include scanner metadata from analytics table (independent of date range)
            try:
                analytics = session.query(models.Analytics).first()
                if analytics:
                    out["last_scan_started"] = getattr(analytics, 'last_scan_started', None)
                    out["last_scan_duration"] = getattr(analytics, 'last_scan_duration', None)
                    out["last_scan_new_mentions"] = getattr(analytics, 'last_scan_new_mentions', None)
            except Exception:
                pass
            return out
        
        # Otherwise, return all-time stats from analytics or counts
        try:
            analytics = session.query(models.Analytics).first()
            if analytics:
                out.update({
                    "total_subreddits": int(analytics.total_subreddits or 0),
                    "total_posts": int(analytics.total_posts or 0),
                    "total_comments": int(analytics.total_comments or 0),
                    "total_mentions": int(analytics.total_mentions or 0),
                    "analytics_updated_at": getattr(analytics, 'updated_at', None),
                    "last_scan_started": getattr(analytics, 'last_scan_started', None),
                    "last_scan_duration": getattr(analytics, 'last_scan_duration', None),
                    "last_scan_new_mentions": getattr(analytics, 'last_scan_new_mentions', None)
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


@app.get("/config")
@cache_response(ttl_seconds=300)
def get_config():
    """Public configuration values for the frontend."""
    return {
        "metadata_stale_hours": METADATA_STALE_HOURS
    }


@app.get("/stats/metadata")
@cache_response(ttl_seconds=30)
def metadata_stats():
    """Statistics about subreddit metadata freshness and completeness."""
    with Session(engine) as session:
        out = {}
        now = datetime.utcnow()
        
        try:
            # Total subreddits
            total = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)
            out['total_subreddits'] = total
            
            # Never checked (no metadata fetched yet)
            never_checked = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.last_checked == None
            ).scalar() or 0)
            out['never_checked'] = never_checked
            
            # Metadata age thresholds
            threshold_24h = now - timedelta(hours=METADATA_STALE_HOURS)
            threshold_72h = now - timedelta(hours=72)
            threshold_7d = now - timedelta(days=7)
            
            # Up-to-date (checked within configured METADATA_STALE_HOURS)
            up_to_date = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.last_checked >= threshold_24h
            ).scalar() or 0)
            out['up_to_date'] = up_to_date
            
            # Stale (older than configured METADATA_STALE_HOURS)
            # Only count subreddits that have metadata and are not banned/not_found (matches scanner Priority 3)
            stale_24h = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.title != None,
                models.Subreddit.subscribers != None,
                models.Subreddit.description != None,
                models.Subreddit.last_checked != None,
                models.Subreddit.last_checked < threshold_24h,
                models.Subreddit.is_banned == False,
                models.Subreddit.subreddit_found != False
            ).scalar() or 0)
            out['stale_24h_plus'] = stale_24h
            
            # Metadata age breakdown
            fresh_0_24h = up_to_date
            stale_24_72h = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.last_checked < threshold_24h,
                models.Subreddit.last_checked >= threshold_72h
            ).scalar() or 0)
            old_3_7d = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.last_checked < threshold_72h,
                models.Subreddit.last_checked >= threshold_7d
            ).scalar() or 0)
            very_old_7d_plus = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.last_checked < threshold_7d,
                models.Subreddit.last_checked != None
            ).scalar() or 0)
            
            out['metadata_age_breakdown'] = {
                'fresh_0_24h': fresh_0_24h,
                'stale_24_72h': stale_24_72h,
                'old_3_7d': old_3_7d,
                'very_old_7d_plus': very_old_7d_plus
            }
            
            # Banned subreddits
            banned = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.is_banned == True
            ).scalar() or 0)
            out['banned'] = banned
            
            # Subreddits that don't exist (404)
            not_found = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.subreddit_found == False
            ).scalar() or 0)
            out['not_found'] = not_found
            
            # Pending retry (waiting after rate limit/error)
            pending_retry = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.next_retry_at != None
            ).scalar() or 0)
            out['pending_retry'] = pending_retry
            
            # NSFW subreddits
            nsfw = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.is_over18 == True
            ).scalar() or 0)
            out['nsfw_subreddits'] = nsfw
            
            # With subscriber data
            with_subscribers = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.subscribers != None
            ).scalar() or 0)
            out['with_subscriber_data'] = with_subscribers
            
            # With descriptions (empty strings count as having description - just empty)
            with_descriptions = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.description != None
            ).scalar() or 0)
            out['with_descriptions'] = with_descriptions
            
            # Without metadata (missing ANY of: title, subscribers, or description)
            # Matches scanner Priority 2 logic: ANY NULL field = missing metadata
            # Excludes banned and not-found subreddits (scanner won't fetch metadata for these)
            without_metadata = int(session.query(func.count(models.Subreddit.id)).filter(
                models.Subreddit.is_banned == False,
                models.Subreddit.subreddit_found != False,
                or_(
                    models.Subreddit.title == None,
                    models.Subreddit.subscribers == None,
                    models.Subreddit.description == None
                )
            ).scalar() or 0)
            out['without_metadata'] = without_metadata
            
        except Exception:
            api_logger.exception("Failed to compute metadata stats")
        
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
                # Use bind parameter to prevent SQL injection
                from sqlalchemy import bindparam
                seed_param = bindparam('seed_value', value=str(seed)[:100])  # limit length
                subq = subq.order_by(func.md5(func.concat(models.Subreddit.name, seed_param)))
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


    @app.post("/subreddits/{name}/refresh")
    def refresh_subreddit(name: str, api_key: Optional[str] = Query(None)):
        """Fetch Reddit about.json for a single subreddit, create the DB row if missing,
        update stored metadata, and return the updated subreddit record.
        
        Requires API key authentication when API_KEY is set in environment.
        """
        # API key check
        ENV_API_KEY = os.getenv('API_KEY')
        provided_key = api_key or os.getenv('HTTP_X_API_KEY') or ''
        
        if ENV_API_KEY:
            if not provided_key or provided_key != ENV_API_KEY:
                raise HTTPException(status_code=403, detail='Invalid or missing API key')
        
        # Normalize and prevent refreshing user profiles as subreddits
        from scanner.main import normalize, is_user_profile
        lname = normalize(name)
        if is_user_profile(lname):
            raise HTTPException(status_code=400, detail='User profiles are not subreddits')
        with Session(engine) as session:
            s = session.query(models.Subreddit).filter(models.Subreddit.name == lname).first()
            if not s:
                s = models.Subreddit(name=lname)
                session.add(s)
                try:
                    session.commit()
                except Exception:
                    session.rollback()
            try:
                r = fetch_sub_about(lname)
                if r.status_code == 200:
                    payload = r.json()
                    if isinstance(payload, dict) and payload.get('reason'):
                        s.is_banned = True
                        s.ban_reason = str(payload.get('reason'))
                    data = payload.get('data', {}) if isinstance(payload, dict) else {}

                    def safe_int(v):
                        try:
                            return int(v) if v is not None else None
                        except Exception:
                            return None

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
                    try:
                        ov = data.get('over18') if 'over18' in data else data.get('over_18')
                        if ov is not None:
                            s.is_over18 = bool(ov)
                    except Exception:
                        pass
                    s.is_banned = s.is_banned or False
                    s.subreddit_found = True
                elif r.status_code in (403, 404):
                    if r.status_code == 403:
                        s.is_banned = True
                        s.subreddit_found = True
                    else:
                        s.subreddit_found = False
                        s.is_banned = False
                else:
                    api_logger.debug(f"/r/{s.name} metadata fetch returned status {r.status_code}")

                s.last_checked = datetime.utcnow()
                session.add(s)
                session.commit()
                mentions = session.query(func.count(models.Mention.id)).filter(models.Mention.subreddit_id == s.id).scalar()
                return {"ok": True, "subreddit": {"name": s.name, "display_name": s.display_name, "title": s.title, "subscribers": s.subscribers, "active_users": s.active_users, "description": s.description, "is_banned": s.is_banned, "subreddit_found": s.subreddit_found, "last_checked": s.last_checked, "mentions": mentions}}
            except Exception:
                session.rollback()
                raise HTTPException(status_code=500, detail="Failed to refresh subreddit metadata")


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
@cache_response(ttl_seconds=60)
def stats_top(limit: int = 20, days: int = 90):
    limit = max(1, min(500, int(limit)))
    days = max(1, min(3650, int(days)))
    start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    with Session(engine) as session:
        rows = session.query(models.Subreddit.name, func.count(models.Mention.id).label('mentions'))\
            .join(models.Mention, models.Mention.subreddit_id == models.Subreddit.id)\
            .filter(models.Mention.timestamp >= start_ts)\
            .group_by(models.Subreddit.name)\
            .order_by(desc('mentions'))\
            .limit(limit).all()
        return [{"name": r[0], "mentions": r[1]} for r in rows]


@app.get("/stats/top_posts")
@cache_response(ttl_seconds=60)
def stats_top_posts(limit: int = 20, days: int = 90):
    """Top posts ordered by total mention count."""
    limit = max(1, min(500, int(limit)))
    days = max(1, min(3650, int(days)))
    start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    with Session(engine) as session:
        rows = session.query(
            models.Post.reddit_post_id,
            models.Post.title,
            func.count(models.Mention.id).label('mentions')
        ).join(models.Mention, models.Mention.post_id == models.Post.id, isouter=True)\
        .filter(models.Mention.timestamp >= start_ts)
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
@cache_response(ttl_seconds=60)
def stats_top_unique_posts(limit: int = 20, days: int = 90):
    """Posts ordered by number of distinct subreddits mentioned in the post's comments."""
    limit = max(1, min(500, int(limit)))
    days = max(1, min(3650, int(days)))
    start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    with Session(engine) as session:
        # Count distinct subreddit_id per post via mentions
        rows = session.query(
            models.Post.reddit_post_id,
            models.Post.title,
            func.count(func.distinct(models.Mention.subreddit_id)).label('unique_subreddits'),
            models.Post.url,
        ).join(models.Mention, models.Mention.post_id == models.Post.id, isouter=True)\
        .filter(models.Mention.timestamp >= start_ts)
        rows = rows.group_by(models.Post.id).order_by(desc('unique_subreddits')).limit(limit).all()
        out = []
        for r in rows:
            out.append({
                'reddit_post_id': r[0],
                'title': r[1],
                'unique_subreddits': int(r[2] or 0),
                'url': (r[3] or '')
            })
        return {"items": out}


@app.get("/stats/top_commenters")
@cache_response(ttl_seconds=60)
def stats_top_commenters(limit: int = 20, days: int = 90):
    """Top users by number of comments (user_id)."""
    limit = max(1, min(500, int(limit)))
    days = max(1, min(3650, int(days)))
    start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    with Session(engine) as session:
        # Prefer counting users from the `mentions` table since the scanner
        # records the author/id there when a subreddit is mentioned. Fall
        # back to counting `comments.username` if no mention-based data exists.
        out = []
        try:
            mrows = session.query(
                models.Mention.user_id,
                func.count(models.Mention.id).label('mentions')
            ).filter(models.Mention.user_id != None).filter(models.Mention.timestamp >= start_ts)
            mrows = mrows.group_by(models.Mention.user_id).order_by(desc('mentions')).limit(limit).all()
            if mrows:
                for r in mrows:
                    out.append({'user_id': r[0], 'comments': int(r[1] or 0)})
                return {"items": out}
        except Exception:
            api_logger.exception('Failed to compute top commenters from mentions')

        # Fallback: count Comment.username if mentions are not available
        try:
            crows = session.query(
                models.Comment.username,
                func.count(models.Comment.id).label('comments')
            ).filter(models.Comment.username != None)
            crows = crows.group_by(models.Comment.username).order_by(desc('comments')).limit(limit).all()
            for r in crows:
                out.append({'user_id': r[0], 'comments': int(r[1] or 0)})
        except Exception:
            api_logger.exception('Failed to compute top commenters from comments')

        return {"items": out}


@app.get("/stats/daily")
@cache_response(ttl_seconds=60)
def stats_daily(days: int = 90):
    """Return aggregated counts for posts, comments, mentions and new subreddits.

    For days <= 90: returns daily data {date: 'YYYY-MM-DD', ...}
    For days > 90: returns monthly data {date: 'YYYY-MM', ...}
    ordered from oldest to newest for the requested `days` window.
    """
    days = max(1, min(3650, int(days)))
    start_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    use_monthly = days > 90
    with Session(engine) as session:
        out_map = {}
        if use_monthly:
            # Monthly aggregation
            # posts by month
            try:
                rows = session.query(
                    func.to_char(func.to_timestamp(models.Post.created_utc), 'YYYY-MM').label('month'),
                    func.count(models.Post.id)
                ).filter(models.Post.created_utc >= start_ts).group_by('month').order_by('month').all()
                for month, cnt in rows:
                    out_map.setdefault(month, {})['posts'] = int(cnt or 0)
            except Exception:
                api_logger.exception('Failed to compute monthly posts')

            # comments by month
            try:
                rows = session.query(
                    func.to_char(func.to_timestamp(models.Comment.created_utc), 'YYYY-MM').label('month'),
                    func.count(models.Comment.id)
                ).filter(models.Comment.created_utc >= start_ts).group_by('month').order_by('month').all()
                for month, cnt in rows:
                    out_map.setdefault(month, {})['comments'] = int(cnt or 0)
            except Exception:
                api_logger.exception('Failed to compute monthly comments')

            # mentions by month
            try:
                rows = session.query(
                    func.to_char(func.to_timestamp(models.Mention.timestamp), 'YYYY-MM').label('month'),
                    func.count(models.Mention.id)
                ).filter(models.Mention.timestamp >= start_ts).group_by('month').order_by('month').all()
                for month, cnt in rows:
                    out_map.setdefault(month, {})['mentions'] = int(cnt or 0)
            except Exception:
                api_logger.exception('Failed to compute monthly mentions')

            # new subreddits by month
            try:
                rows = session.query(
                    func.to_char(func.to_timestamp(models.Subreddit.first_mentioned), 'YYYY-MM').label('month'),
                    func.count(models.Subreddit.id)
                ).filter(models.Subreddit.first_mentioned != None).filter(models.Subreddit.first_mentioned >= start_ts).group_by('month').order_by('month').all()
                for month, cnt in rows:
                    out_map.setdefault(month, {})['new_subreddits'] = int(cnt or 0)
            except Exception:
                api_logger.exception('Failed to compute monthly new subreddits')

            # build continuous month list from start to now
            # Skip leading empty periods for cleaner display
            try:
                all_items = []
                start_date = (datetime.utcnow() - timedelta(days=days)).date()
                current = start_date.replace(day=1)
                found_data = False
                while current <= datetime.utcnow().date():
                    key = current.strftime('%Y-%m')
                    v = out_map.get(key, {})
                    posts = v.get('posts', 0)
                    comments = v.get('comments', 0)
                    mentions = v.get('mentions', 0)
                    new_subs = v.get('new_subreddits', 0)
                    # Skip leading empty months
                    if not found_data and posts == 0 and comments == 0 and mentions == 0 and new_subs == 0:
                        # Move to next month without appending
                        if current.month == 12:
                            current = current.replace(year=current.year+1, month=1)
                        else:
                            current = current.replace(month=current.month+1)
                        continue
                    found_data = True
                    all_items.append({
                        'date': key,
                        'posts': posts,
                        'comments': comments,
                        'mentions': mentions,
                        'new_subreddits': new_subs
                    })
                    # Move to next month
                    if current.month == 12:
                        current = current.replace(year=current.year+1, month=1)
                    else:
                        current = current.replace(month=current.month+1)
                items = all_items
            except Exception:
                api_logger.exception('Failed to assemble monthly timeline')
                items = []
        else:
            # Daily aggregation
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
            # Skip leading empty periods for cleaner display
            try:
                # build continuous date list from start to now
                start_date = (datetime.utcnow() - timedelta(days=days)).date()
                dates = [(start_date + timedelta(days=i)) for i in range(days+1)]
                all_items = []
                found_data = False
                for d in dates:
                    key = d.strftime('%Y-%m-%d')
                    v = out_map.get(key, {})
                    posts = v.get('posts', 0)
                    comments = v.get('comments', 0)
                    mentions = v.get('mentions', 0)
                    new_subs = v.get('new_subreddits', 0)
                    # Skip leading empty days (no data across all metrics)
                    if not found_data and posts == 0 and comments == 0 and mentions == 0 and new_subs == 0:
                        continue
                    found_data = True
                    all_items.append({
                        'date': key,
                        'posts': posts,
                        'comments': comments,
                        'mentions': mentions,
                        'new_subreddits': new_subs
                    })
                items = all_items
            except Exception:
                api_logger.exception('Failed to assemble daily timeline')
                items = []

        return { 'items': items }


@app.get("/api/discover/trending")
@cache_response(ttl_seconds=300)  # Cache for 5 minutes
async def get_trending(days: int = Query(default=7, ge=1, le=90)):
    """Get subreddits trending in the last N days (most mentions recently)"""
    with Session(engine) as session:
        cutoff = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        
        # Count mentions per subreddit in the time window
        stmt = (
            select(
                models.Mention.subreddit_id,
                func.count(models.Mention.id).label('recent_mentions')
            )
            .where(models.Mention.timestamp >= cutoff)
            .group_by(models.Mention.subreddit_id)
            .order_by(desc('recent_mentions'))
            .limit(50)
        )
        
        results = session.execute(stmt).all()
        items = []
        
        for sub_id, count in results:
            sub = session.get(models.Subreddit, sub_id)
            if sub and sub.subreddit_found and not sub.is_banned:
                total_mentions = session.query(func.count(models.Mention.id)).filter(
                    models.Mention.subreddit_id == sub_id
                ).scalar()
                items.append({
                    'name': sub.name,
                    'title': sub.title,
                    'subscribers': sub.subscribers,
                    'recent_mentions': int(count),
                    'total_mentions': int(total_mentions or 0),
                    'is_over18': sub.is_over18
                })
        
        return {'days': days, 'items': items}


@app.get("/api/discover/hidden_gems")
@cache_response(ttl_seconds=300)
async def get_hidden_gems(max_subscribers: int = Query(default=10000, ge=100, le=100000)):
    """Find active subreddits with low subscriber counts (hidden gems)"""
    with Session(engine) as session:
        # Find subs with mentions but low subscribers
        stmt = (
            select(
                models.Subreddit,
                func.count(models.Mention.id).label('mentions')
            )
            .join(models.Mention, models.Mention.subreddit_id == models.Subreddit.id)
            .where(
                models.Subreddit.subreddit_found == True,
                models.Subreddit.is_banned == False,
                models.Subreddit.subscribers != None,
                models.Subreddit.subscribers < max_subscribers,
                models.Subreddit.subscribers > 0
            )
            .group_by(models.Subreddit.id)
            .having(func.count(models.Mention.id) >= 3)  # At least 3 mentions
            .order_by(desc('mentions'))
            .limit(50)
        )
        
        results = session.execute(stmt).all()
        items = []
        
        for sub, mentions in results:
            items.append({
                'name': sub.name,
                'title': sub.title,
                'subscribers': sub.subscribers,
                'mentions': int(mentions),
                'is_over18': sub.is_over18
            })
        
        return {'max_subscribers': max_subscribers, 'items': items}


@app.get("/api/discover/fastest_growing")
@cache_response(ttl_seconds=300)
async def get_fastest_growing(
    days: int = Query(default=30, ge=7, le=90),
    min_recent: int = Query(default=5, ge=1, le=100),
    min_growth: float = Query(default=1.5, ge=1.0, le=10.0)
):
    """Find subreddits with the biggest increase in mentions recently.

    Optional query parameters:
    - `min_recent`: minimum recent mentions required (default 5)
    - `min_growth`: minimum growth ratio required (default 1.5)
    """
    with Session(engine) as session:
        cutoff = int((datetime.utcnow() - timedelta(days=days)).timestamp())

        # Get recent vs older mention counts for each subreddit
        recent_counts = (
            select(
                models.Mention.subreddit_id,
                func.count(models.Mention.id).label('recent')
            )
            .where(models.Mention.timestamp >= cutoff)
            .group_by(models.Mention.subreddit_id)
            .subquery()
        )

        older_counts = (
            select(
                models.Mention.subreddit_id,
                func.count(models.Mention.id).label('older')
            )
            .where(models.Mention.timestamp < cutoff)
            .group_by(models.Mention.subreddit_id)
            .subquery()
        )

        # Calculate growth ratio
        stmt = (
            select(
                models.Subreddit,
                func.coalesce(recent_counts.c.recent, 0).label('recent_mentions'),
                func.coalesce(older_counts.c.older, 1).label('older_mentions')
            )
            .outerjoin(recent_counts, models.Subreddit.id == recent_counts.c.subreddit_id)
            .outerjoin(older_counts, models.Subreddit.id == older_counts.c.subreddit_id)
            .where(
                models.Subreddit.subreddit_found == True,
                models.Subreddit.is_banned == False,
                recent_counts.c.recent >= min_recent
            )
        )

        results = session.execute(stmt).all()

        # Calculate growth and sort
        growth_data = []
        for sub, recent, older in results:
            growth_ratio = recent / max(older, 1)
            if growth_ratio > float(min_growth):
                total = session.query(func.count(models.Mention.id)).filter(
                    models.Mention.subreddit_id == sub.id
                ).scalar()
                growth_data.append({
                    'name': sub.name,
                    'title': sub.title,
                    'subscribers': sub.subscribers,
                    'recent_mentions': int(recent),
                    'older_mentions': int(older),
                    'growth_ratio': round(growth_ratio, 2),
                    'total_mentions': int(total or 0),
                    'is_over18': sub.is_over18
                })

        # Sort by growth ratio
        growth_data.sort(key=lambda x: x['growth_ratio'], reverse=True)

        return {'days': days, 'min_recent': min_recent, 'min_growth': float(min_growth), 'items': growth_data[:50]}


# Removed endpoint: GET /subreddits/count — use GET /stats for aggregated counts instead.


# ===== Category System Endpoints =====

@app.get("/api/categories")
def list_categories(include_tags: bool = True, active_only: bool = True):
    """List all categories, optionally including their tags."""
    with Session(engine) as session:
        query = session.query(models.Category)
        
        if active_only:
            query = query.filter(models.Category.active == True)
        
        query = query.order_by(models.Category.sort_order, models.Category.name)
        categories = query.all()
        
        result = []
        for cat in categories:
            cat_data = {
                'id': cat.id,
                'name': cat.name,
                'slug': cat.slug,
                'description': cat.description,
                'sort_order': cat.sort_order,
                'icon': cat.icon,
                'active': cat.active
            }
            
            if include_tags:
                tag_query = session.query(models.CategoryTag).filter(
                    models.CategoryTag.category_id == cat.id
                )
                if active_only:
                    tag_query = tag_query.filter(models.CategoryTag.active == True)
                
                tag_query = tag_query.order_by(models.CategoryTag.sort_order, models.CategoryTag.name)
                tags = tag_query.all()
                
                cat_data['tags'] = [{
                    'id': tag.id,
                    'name': tag.name,
                    'slug': tag.slug,
                    'keywords': tag.keywords,
                    'description': tag.description,
                    'sort_order': tag.sort_order,
                    'icon': tag.icon,
                    'active': tag.active,
                    'subreddit_count': session.query(func.count(models.SubredditCategoryTag.id)).filter(
                        models.SubredditCategoryTag.category_tag_id == tag.id
                    ).scalar() or 0
                } for tag in tags]
            
            result.append(cat_data)
        
        return result


@app.get("/api/categories/{category_slug}")
def get_category(category_slug: str, include_tags: bool = True):
    """Get a single category by slug."""
    with Session(engine) as session:
        category = session.query(models.Category).filter(
            models.Category.slug == category_slug
        ).first()
        
        if not category:
            raise HTTPException(status_code=404, detail="Category not found")
        
        cat_data = {
            'id': category.id,
            'name': category.name,
            'slug': category.slug,
            'description': category.description,
            'sort_order': category.sort_order,
            'icon': category.icon,
            'active': category.active
        }
        
        if include_tags:
            tags = session.query(models.CategoryTag).filter(
                models.CategoryTag.category_id == category.id,
                models.CategoryTag.active == True
            ).order_by(models.CategoryTag.sort_order, models.CategoryTag.name).all()
            
            cat_data['tags'] = [{
                'id': tag.id,
                'name': tag.name,
                'slug': tag.slug,
                'keywords': tag.keywords,
                'description': tag.description,
                'sort_order': tag.sort_order,
                'icon': tag.icon,
                'active': tag.active,
                'subreddit_count': session.query(func.count(models.SubredditCategoryTag.id)).filter(
                    models.SubredditCategoryTag.category_tag_id == tag.id
                ).scalar() or 0
            } for tag in tags]
        
        return cat_data


@app.get("/api/tags/{tag_id}/subreddits")
def get_tag_subreddits(
    tag_id: int,
    page: int = 1,
    per_page: int = 50,
    sort: str = 'mentions',
    sort_dir: str = 'desc'
):
    """Get all subreddits tagged with a specific tag."""
    per_page = min(500, max(1, int(per_page)))
    page = max(1, int(page))
    offset = (page - 1) * per_page
    
    allowed_sorts = {'mentions', 'subscribers', 'name', 'created_utc', 'first_mentioned'}
    if sort not in allowed_sorts:
        sort = 'mentions'
    
    with Session(engine) as session:
        tag = session.query(models.CategoryTag).filter(models.CategoryTag.id == tag_id).first()
        if not tag:
            raise HTTPException(status_code=404, detail="Tag not found")
        
        total = session.query(func.count(models.SubredditCategoryTag.id)).filter(
            models.SubredditCategoryTag.category_tag_id == tag_id
        ).scalar() or 0
        
        subq = session.query(
            models.Subreddit,
            func.count(models.Mention.id).label('mentions')
        ).join(
            models.SubredditCategoryTag,
            models.SubredditCategoryTag.subreddit_id == models.Subreddit.id
        ).join(
            models.Mention,
            models.Mention.subreddit_id == models.Subreddit.id,
            isouter=True
        ).filter(
            models.SubredditCategoryTag.category_tag_id == tag_id
        ).group_by(models.Subreddit.id)
        
        if sort == 'mentions':
            subq = subq.order_by(desc('mentions') if sort_dir == 'desc' else 'mentions')
        elif sort == 'subscribers':
            subq = subq.order_by(
                desc(models.Subreddit.subscribers) if sort_dir == 'desc' else models.Subreddit.subscribers
            )
        elif sort == 'name':
            subq = subq.order_by(
                desc(models.Subreddit.name) if sort_dir == 'desc' else models.Subreddit.name
            )
        elif sort == 'created_utc':
            subq = subq.order_by(
                desc(models.Subreddit.created_utc) if sort_dir == 'desc' else models.Subreddit.created_utc
            )
        elif sort == 'first_mentioned':
            subq = subq.order_by(
                desc(models.Subreddit.first_mentioned) if sort_dir == 'desc' else models.Subreddit.first_mentioned
            )
        
        results = subq.limit(per_page).offset(offset).all()
        
        items = [{
            'id': sub.id,
            'name': sub.name,
            'title': sub.title,
            'display_name': sub.display_name,
            'description': sub.description,
            'subscribers': sub.subscribers,
            'active_users': sub.active_users,
            'created_utc': sub.created_utc,
            'first_mentioned': sub.first_mentioned,
            'is_over18': sub.is_over18,
            'is_banned': sub.is_banned,
            'subreddit_found': sub.subreddit_found,
            'mentions': mentions
        } for sub, mentions in results]
        
        return {
            'tag': {
                'id': tag.id,
                'name': tag.name,
                'slug': tag.slug,
                'category_name': tag.category.name if tag.category else None
            },
            'total': total,
            'page': page,
            'per_page': per_page,
            'items': items
        }


@app.get("/api/subreddits/{name}/categories")
def get_subreddit_categories(name: str):
    """Get all category tags applied to a subreddit."""
    with Session(engine) as session:
        subreddit = session.query(models.Subreddit).filter(
            models.Subreddit.name == name
        ).first()
        
        if not subreddit:
            raise HTTPException(status_code=404, detail="Subreddit not found")
        
        tags_query = session.query(
            models.CategoryTag,
            models.Category,
            models.SubredditCategoryTag
        ).join(
            models.SubredditCategoryTag,
            models.SubredditCategoryTag.category_tag_id == models.CategoryTag.id
        ).join(
            models.Category,
            models.Category.id == models.CategoryTag.category_id
        ).filter(
            models.SubredditCategoryTag.subreddit_id == subreddit.id
        ).order_by(
            models.Category.sort_order,
            models.CategoryTag.sort_order
        ).all()
        
        categories = {}
        for tag, category, association in tags_query:
            if category.id not in categories:
                categories[category.id] = {
                    'id': category.id,
                    'name': category.name,
                    'slug': category.slug,
                    'icon': category.icon,
                    'tags': []
                }
            
            categories[category.id]['tags'].append({
                'id': tag.id,
                'name': tag.name,
                'slug': tag.slug,
                'icon': tag.icon,
                'source': association.source,
                'confidence': association.confidence,
                'created_at': association.created_at.isoformat() if association.created_at else None
            })
        
        return {
            'subreddit': {
                'id': subreddit.id,
                'name': subreddit.name,
                'title': subreddit.title
            },
            'categories': list(categories.values())
        }
