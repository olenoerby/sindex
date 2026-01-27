import os
import logging
from datetime import datetime, timedelta
import httpx
from sqlalchemy.orm import Session
from . import models
from .utils import parse_retry_after_seconds
from sqlalchemy import create_engine
from redis import Redis

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logger = logging.getLogger('api.tasks')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
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
    lname = name.lower().strip()
    try:
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
            if r.status_code == 200:
                payload = r.json()
                if isinstance(payload, dict) and payload.get('reason'):
                    sub.is_banned = True
                    sub.ban_reason = str(payload.get('reason'))
                data = payload.get('data', {}) if isinstance(payload, dict) else {}
                try:
                    sub.display_name = data.get('display_name') or sub.display_name
                    sub.display_name_prefixed = data.get('display_name_prefixed') or sub.display_name_prefixed
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
                        sub.public_description_html = data.get('public_description_html') or sub.public_description_html
                    except Exception:
                        pass
                try:
                    ov = data.get('over18') if 'over18' in data else data.get('over_18')
                    if ov is not None:
                        sub.is_over18 = bool(ov)
                except Exception:
                    pass
                sub.is_banned = sub.is_banned or False
                sub.is_not_found = False
                # successful fetch: clear any retry scheduling
                sub.retry_priority = 0
                sub.next_retry_at = None
            elif r.status_code in (403, 404):
                if r.status_code == 403:
                    sub.is_banned = True
                    sub.is_not_found = False
                else:
                    sub.is_not_found = True
                    sub.is_banned = False
                try:
                    payload = r.json()
                    if isinstance(payload, dict) and payload.get('reason'):
                        sub.ban_reason = str(payload.get('reason'))
                except Exception:
                    pass
            elif r.status_code == 429:
                # Rate limited: parse Retry-After and schedule a retry
                ra = parse_retry_after_seconds(r.headers.get('Retry-After'))
                if ra is None:
                    ra = 30
                sub.next_retry_at = datetime.utcnow() + timedelta(seconds=ra)
                # increase priority so this subreddit is scheduled earlier once ready
                sub.retry_priority = min((sub.retry_priority or 0) + 1, 10)
                logger.warning(f"Rate limited on /r/{lname}; retry in {ra}s, priority={sub.retry_priority}")
            else:
                logger.warning(f"Unexpected status {r.status_code} fetching /r/{lname}")

            sub.last_checked = datetime.utcnow()
            session.add(sub)
            session.commit()
            logger.info(f"Background refresh complete for /r/{lname}: is_banned={sub.is_banned}, is_not_found={sub.is_not_found}")
    except Exception as e:
        logger.exception(f"refresh_subreddit_job failed for /r/{name}: {e}")
        raise
