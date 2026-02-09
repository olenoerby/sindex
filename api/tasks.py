import os
import logging
from datetime import datetime, timedelta
import httpx
from sqlalchemy.orm import Session
from . import models
from .utils import parse_retry_after_seconds
from sqlalchemy import create_engine
from redis import Redis
from api.distributed_rate_limiter import DistributedRateLimiter
from api.phase import attach_phase_filter, temp_phase

# Initialize distributed rate limiter (best-effort)
try:
    API_MAX_CALLS_MINUTE = int(os.getenv('API_MAX_CALLS_MINUTE', os.getenv('API_MAX_CALLS_MIN', '30')))
    # Calculate minimum delay from max calls per minute
    API_RATE_DELAY_SECONDS = 60.0 / API_MAX_CALLS_MINUTE
    distributed_rate_limiter = DistributedRateLimiter(redis_url=REDIS_URL, min_delay_seconds=API_RATE_DELAY_SECONDS, max_calls_per_minute=API_MAX_CALLS_MINUTE)
    distributed_rate_limiter.set_container_name('api_tasks')
except Exception:
    distributed_rate_limiter = None

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logger = logging.getLogger('api.tasks')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s [%(phase)s]: %(message)s'))
attach_phase_filter(handler)
logger.addHandler(handler)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')

engine = create_engine(DATABASE_URL, future=True)
redis = Redis.from_url(REDIS_URL)


def _safe_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def refresh_subreddit_job(name: str):
    """Background job: fetch /r/{name}/about.json and update/create DB row."""
    # Normalize and skip user profiles
    from scanner.main import normalize, is_user_profile
    lname = normalize(name)
    if is_user_profile(lname):
        logger.info(f"Skipping background refresh for user profile: /u/{lname[2:]}")
        return
    try:
        with temp_phase('Immediate Discovery Metadata'):
            with Session(engine) as session:
            sub = session.query(models.Subreddit).filter(models.Subreddit.name == lname).first()
            if not sub:
                sub = models.Subreddit(name=lname)
                session.add(sub)
                session.commit()

            url = f"https://www.reddit.com/r/{lname}/about.json"
            headers = {"User-Agent": "PineappleIndexWorker/0.1"}
            # simple request with small timeout; worker can rely on RQ retries
            r = httpx.get(url, headers=headers, timeout=15.0)
            # Record distributed API call so global limiter sees it
            try:
                if distributed_rate_limiter:
                    distributed_rate_limiter.record_api_call()
            except Exception:
                pass
            if r.status_code == 200:
                payload = r.json()
                # Check if Reddit returned an error in the body (e.g., {"detail": "Not Found"})
                if isinstance(payload, dict) and payload.get('detail') == 'Not Found':
                    # Subreddit doesn't exist
                    sub.is_banned = False
                    sub.subreddit_found = False
                elif isinstance(payload, dict) and payload.get('reason'):
                    # Subreddit is banned
                    sub.is_banned = True
                    sub.subreddit_found = True
                else:
                    # Valid subreddit data
                    data = payload.get('data', {}) if isinstance(payload, dict) else {}
                    try:
                        sub.display_name = data.get('display_name') or sub.display_name
                        sub.title = data.get('title') or sub.title
                    except Exception:
                        pass
                    created = _safe_int(data.get('created_utc'))
                    if created:
                        sub.created_utc = created
                    subs = _safe_int(data.get('subscribers'))
                    if subs is not None:
                        sub.subscribers = subs
                    active = _safe_int(data.get('accounts_active') or data.get('active_user_count') or data.get('active_accounts'))
                    if active is not None:
                        sub.active_users = active
                    public = data.get('public_description')
                    if public:
                        sub.description = public
                    try:
                        ov = data.get('over18') if 'over18' in data else data.get('over_18')
                        if ov is not None:
                            sub.is_over18 = bool(ov)
                    except Exception:
                        pass
                    sub.is_banned = sub.is_banned or False
                    sub.subreddit_found = True
                    # successful fetch: clear any retry scheduling
                    sub.next_retry_at = None
            elif r.status_code == 404:
                # 404 means the subreddit does not exist on Reddit
                sub.is_banned = False
                sub.subreddit_found = False
            elif r.status_code == 403:
                # 403 means the subreddit is banned/private
                sub.is_banned = True
                sub.subreddit_found = True
            elif r.status_code == 429:
                # Rate limited: parse Retry-After and schedule a retry
                ra = parse_retry_after_seconds(r.headers.get('Retry-After'))
                if ra is None:
                    ra = 30
                sub.next_retry_at = datetime.utcnow() + timedelta(seconds=ra)
                logger.warning(f"Rate limited on /r/{lname}; retry in {ra}s")
            else:
                logger.warning(f"Unexpected status {r.status_code} fetching /r/{lname}")

            sub.last_checked = datetime.utcnow()
            session.add(sub)
            session.commit()
            logger.info(f"Background refresh complete for /r/{lname}: is_banned={sub.is_banned}, subreddit_found={sub.subreddit_found}")
    except Exception as e:
        logger.exception(f"refresh_subreddit_job failed for /r/{name}: {e}")
        raise
