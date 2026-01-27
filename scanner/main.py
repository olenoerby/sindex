import os
import re
import time
import json
import logging
from dotenv import load_dotenv
# file-based rotating logs removed; rely on container stdout/stderr
from datetime import datetime, timedelta
import httpx
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import Session
import sys
from concurrent.futures import ThreadPoolExecutor
import threading

# Ensure the project root is on sys.path so `import api` works when running
# inside the scanner container. Previously we appended the `api` folder
# itself which prevented importing `api.*` packages.
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import models
from api.utils import parse_retry_after_seconds

# Load environment variables from .env at repo root so values like
# `POST_COMMENT_LOOKBACK_DAYS` can be set without editing `main.py`.
load_dotenv()

# Note: metadata fetch will be performed synchronously when discovering subreddits

# Logging configuration: emit to stdout/stderr (container logs)
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

logger = logging.getLogger('scanner')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
# Use stdout/stderr (container logs)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(stream_handler)
logger.propagate = False

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
REDDIT_USER = os.getenv('REDDIT_USER', 'WeirdPineapple')
API_RATE_DELAY = float(os.getenv('API_RATE_DELAY', '6.5'))
# How many days back to rescan existing posts for new/edited comments.
# Set `POST_COMMENT_LOOKBACK_DAYS` to 0 to skip rescanning existing posts.
try:
    POST_COMMENT_LOOKBACK_DAYS = int(os.getenv('POST_COMMENT_LOOKBACK_DAYS', '180'))
except Exception:
    POST_COMMENT_LOOKBACK_DAYS = 180
# Number of days to consider subreddit metadata fresh before re-fetching from Reddit.
# Can be set via `SUBREDDIT_META_CACHE_DAYS`; falls back to legacy `META_CACHE_DAYS` if present.
# Max retries for subreddit about fetches and per-request HTTP timeout (seconds)
SUBABOUT_MAX_RETRIES = int(os.getenv('SUBABOUT_MAX_RETRIES', '3'))
HTTP_REQUEST_TIMEOUT = float(os.getenv('HTTP_REQUEST_TIMEOUT', '15'))
try:
    SUBABOUT_CONCURRENCY = int(os.getenv('SUBABOUT_CONCURRENCY', '1'))
except Exception:
    SUBABOUT_CONCURRENCY = 1
# Semaphore to limit concurrent subreddit-about requests (default 1 to serialize)
SUBABOUT_SEMAPHORE = threading.BoundedSemaphore(SUBABOUT_CONCURRENCY)

# Global lock and timestamp for serializing and spacing subreddit about requests
SUBREDDIT_RATE_LOCK = threading.Lock()
LAST_SUBREDDIT_REQUEST = 0.0
# NOTE: metadata freshness is not cached by days; metadata is refreshed
# immediately after discovery during each scan.
# Optional testing controls:
# If set, scanner will only process up to this many Friday posts and then exit.
TEST_POST_LIMIT = int(os.getenv('TEST_POST_LIMIT')) if os.getenv('TEST_POST_LIMIT') else None
TEST_POST_IDS = [p.strip() for p in os.getenv('TEST_POST_IDS', '').split(',') if p.strip()]

engine = create_engine(DATABASE_URL, future=True)

# Patterns for subreddit mentions. Accepts r/name, /r/name and reddit url forms.
RE_SUB = re.compile(r"(?:/r/|\br/|https?://(?:www\.)?reddit\.com/r/)([A-Za-z0-9_]{3,21})")

def normalize(name: str) -> str:
    return name.lower().strip().lstrip('/').lstrip('r/').replace('\n','')


def format_ts(ts: int) -> str:
    """Format a unix timestamp (seconds) as YYYY-MM-DD; return 'none' if falsy."""
    try:
        if not ts:
            return 'none'
        return datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d')
    except Exception:
        return str(ts)


# Comma-separated list of subreddit names to ignore. Defaults include a couple examples.
# Set `IGNORE_SUBREDDITS` in the environment to override (comma-separated).
IGNORE_SUBREDDITS = set(
    normalize(s) for s in os.getenv('IGNORE_SUBREDDITS', 'wowthissubexists,sneakpeekbot').split(',') if s.strip()
)

# Optional: explicit list of subreddits to scan. Comma-separated, normalized.
SUBREDDITS_TO_SCAN = [s for s in (os.getenv('SUBREDDITS_TO_SCAN') or '').split(',') if s.strip()]
SUBREDDITS_TO_SCAN = [normalize(s) for s in SUBREDDITS_TO_SCAN]


def ensure_tables():
    # Wait for the database to be ready before attempting DDL.
    max_retries = int(os.getenv('DB_STARTUP_MAX_RETRIES', '30'))
    delay = float(os.getenv('DB_STARTUP_RETRY_DELAY', '1'))
    attempt = 0
    while True:
        attempt += 1
        try:
            # Try a lightweight connection first
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            break
        except Exception:
            if attempt >= max_retries:
                logger.exception('Database not ready after retries; proceeding and will fail on DDL')
                break
            sleep_for = min(30, delay * (2 ** (attempt - 1)))
            logger.info(f'Database not ready yet (attempt {attempt}/{max_retries}), retrying in {sleep_for}s')
            time.sleep(sleep_for)

    try:
        models.Base.metadata.create_all(engine)
    except Exception:
        # If DDL fails here, log and continue; caller will handle runtime errors.
        logger.exception('Failed to create tables during ensure_tables')
    # Apply idempotent schema migrations for minor schema drift so manual psql
    # is not required on prod deployments where direct DB access is difficult.
    try:
        apply_schema_migrations()
    except Exception:
        logger.exception('apply_schema_migrations failed')
    # Ensure a DB-level uniqueness constraint (index) exists for mentions
    # so repeated inserts across restarts cannot create duplicates.
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_mention_sub_comment_idx ON mentions(subreddit_id, comment_id)"
            ))
            conn.commit()
    except Exception:
        # Non-fatal: if DB user lacks privileges or index already exists differently, continue.
        logger.exception("Could not ensure unique index on mentions (continuing)")


def wait_for_db_startup(initial_delay: float = 10.0, max_retries: int = 5, retry_delay: float = 5.0):
    """Pause briefly, then verify DB connectivity. Exit process if DB unreachable.

    - initial_delay: seconds to wait immediately after container start
    - max_retries: number of connection attempts before giving up
    - retry_delay: seconds between retries
    """
    try:
        initial = float(os.getenv('DB_STARTUP_INITIAL_DELAY', str(initial_delay)))
    except Exception:
        initial = initial_delay
    try:
        retries = int(os.getenv('DB_STARTUP_MAX_CONN_RETRIES', str(max_retries)))
    except Exception:
        retries = max_retries
    try:
        rdelay = float(os.getenv('DB_STARTUP_CONN_RETRY_DELAY', str(retry_delay)))
    except Exception:
        rdelay = retry_delay

    logger.info(f'Waiting {initial}s before testing DB connectivity')
    time.sleep(initial)

    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            logger.info('Database connectivity verified')
            return True
        except Exception as e:
            logger.warning(f'DB connection attempt {attempt}/{retries} failed: {e}')
            if attempt >= retries:
                logger.error('DB not reachable after startup retries; exiting to stop container')
                # Exit with non-zero so container orchestrator marks the container as failed
                sys.exit(1)
            time.sleep(rdelay)



def startup_metadata_prefetch():
    """Refresh missing subreddit metadata at scanner startup.

    This fetch prioritizes the most-mentioned subreddits first (descending
    `mentions`) so high-value communities are refreshed before lesser-known
    ones. It also skips rows scheduled for future retries.
    """
    try:
        limit = int(os.getenv('METADATA_PREFETCH_LIMIT', '200'))
    except Exception:
        limit = 200
    try:
        concurrency = int(os.getenv('METADATA_CONCURRENCY', '2'))
    except Exception:
        concurrency = 2

    logger.info(f"Startup metadata prefetch: limit={limit}, concurrency={concurrency}")
    try:
        with Session(engine) as session:
            from sqlalchemy import or_
            now = datetime.utcnow()
            missing_q = session.query(models.Subreddit).filter(
                or_(models.Subreddit.display_name == None, models.Subreddit.display_name == ''),
                or_(models.Subreddit.title == None, models.Subreddit.title == ''),
                or_(models.Subreddit.description == None, models.Subreddit.description == ''),
                models.Subreddit.subscribers == None,
            )
            # Avoid rows scheduled for a future retry
            try:
                missing_q = missing_q.filter(or_(models.Subreddit.next_retry_at == None, models.Subreddit.next_retry_at <= now))
            except Exception:
                pass

            # Prioritize by mentions (most-mentioned first), then by retry_priority
            # so previously-rate-limited subreddits are retried earlier within the
            # same mentions bucket. Finally, older last_checked values come first.
            try:
                missing_q = missing_q.order_by(models.Subreddit.mentions.desc(), models.Subreddit.retry_priority.desc(), models.Subreddit.last_checked.asc().nullsfirst())
            except Exception:
                try:
                    missing_q = missing_q.order_by(models.Subreddit.mentions.desc(), models.Subreddit.retry_priority.desc(), models.Subreddit.last_checked.asc())
                except Exception:
                    pass

            try:
                candidates = missing_q.limit(limit).all()
            except Exception:
                session.rollback()
                candidates = []

        if not candidates:
            logger.info('No missing-metadata subreddits found at startup')
            return

        def _refresh_worker(sub_id):
            try:
                with Session(engine) as s:
                    sub = s.get(models.Subreddit, sub_id)
                    if not sub:
                        return
                    try:
                        if getattr(sub, 'is_banned', False):
                            logger.info(f"Startup: skipping banned /r/{sub.name}")
                            return
                    except Exception:
                        pass
                    # Only refresh if needed (older than 24 hours or missing key fields)
                    try:
                        logger.info(f"Startup: considering metadata refresh for /r/{sub.name}")
                        if should_refresh_sub(sub):
                            logger.info(f"Startup: refreshing metadata for /r/{sub.name}")
                            try:
                                with SUBABOUT_SEMAPHORE:
                                    update_subreddit_metadata(s, sub)
                            except Exception:
                                s.rollback()
                        else:
                            logger.info(f"Startup: skipping metadata refresh for /r/{sub.name} (recent)")
                    except Exception:
                        s.rollback()
            except Exception:
                logger.exception('Exception in startup metadata worker')

        ids = [s.id for s in candidates if getattr(s, 'id', None) is not None]
        if ids:
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
                list(ex.map(_refresh_worker, ids))
        logger.info('Startup metadata prefetch complete')
    except Exception:
        logger.exception('Error during startup metadata prefetch')


def get_or_create_analytics(session: Session):
    try:
        a = session.query(models.Analytics).first()
        if not a:
            a = models.Analytics(total_subreddits=0, total_posts=0, total_comments=0, total_mentions=0)
            session.add(a)
            session.commit()
        return a
    except Exception:
        session.rollback()
        # try to create safely
        a = models.Analytics(total_subreddits=0, total_posts=0, total_comments=0, total_mentions=0)
        try:
            session.add(a)
            session.commit()
            return a
        except Exception:
            session.rollback()
            return None


def increment_analytics(session: Session, posts: int = 0, comments: int = 0, subreddits: int = 0, mentions: int = 0):
    a = get_or_create_analytics(session)
    if not a:
        return
    changed = False
    if posts:
        try:
            a.total_posts = (a.total_posts or 0) + int(posts)
            changed = True
        except Exception:
            pass
    if comments:
        try:
            a.total_comments = (a.total_comments or 0) + int(comments)
            changed = True
        except Exception:
            pass
    if subreddits:
        try:
            a.total_subreddits = (a.total_subreddits or 0) + int(subreddits)
            changed = True
        except Exception:
            pass
    if mentions:
        try:
            a.total_mentions = (a.total_mentions or 0) + int(mentions)
            changed = True
        except Exception:
            pass
    if changed:
        try:
            session.add(a)
            session.commit()
        except Exception:
            session.rollback()


def record_scan_completion(session: Session, scan_start_time: float, new_mentions: int):
    """Record scan completion metrics in analytics table."""
    try:
        a = get_or_create_analytics(session)
        if a:
            scan_duration = int(time.time() - scan_start_time)
            a.last_scan_duration = scan_duration
            a.last_scan_new_mentions = new_mentions
            session.add(a)
            session.commit()
            logger.info(f"Scan completed: duration={scan_duration}s, new_mentions={new_mentions}")
    except Exception:
        logger.exception('Failed to record scan completion')
        session.rollback()


def fetch_user_posts(after: str = None):
    # Calls Reddit public endpoint for user submissions
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=100"
    if after:
        url += f"&after={after}"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    timeout = HTTP_REQUEST_TIMEOUT
    try:
        r = httpx.get(url, headers=headers, timeout=timeout)
    except httpx.ReadTimeout as e:
        logger.warning(f"Read timeout fetching user posts (after={after}): {e}")
        raise
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching user posts (after={after}): {e}")
        raise
    time.sleep(API_RATE_DELAY)
    r.raise_for_status()
    return r.json()


def fetch_subreddit_posts(subname: str, after: str = None):
    """Fetch recent posts for a subreddit (`/r/{subname}/new.json`). Returns parsed JSON."""
    url = f"https://www.reddit.com/r/{subname}/new.json?limit=100"
    if after:
        url += f"&after={after}"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    timeout = HTTP_REQUEST_TIMEOUT
    try:
        r = httpx.get(url, headers=headers, timeout=timeout)
    except httpx.ReadTimeout as e:
        logger.warning(f"Read timeout fetching /r/{subname} posts (after={after}): {e}")
        raise
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching /r/{subname} posts (after={after}): {e}")
        raise
    
    # Log response status and headers for debugging
    logger.debug(f"fetch_subreddit_posts /r/{subname}: status_code={r.status_code}, headers={dict(r.headers)}")
    
    try:
        time.sleep(API_RATE_DELAY)
    except Exception:
        pass
    
    # Check for error status codes before raising
    if r.status_code == 429:
        retry_after = r.headers.get('Retry-After', 'unknown')
        logger.error(f"Rate limited fetching /r/{subname}: 429 Too Many Requests, Retry-After={retry_after}")
        raise Exception(f"HTTP 429 Rate Limited on /r/{subname}; Retry-After={retry_after}")
    
    r.raise_for_status()
    return r.json()


def _parse_retry_after(header_value: str):
    """Parse a Retry-After header value. Returns seconds (int) or None."""
    if not header_value:
        return None
    header_value = header_value.strip()
    # If it's an integer, return as seconds
    try:
        return int(header_value)
    except Exception:
        pass
    # Otherwise, try to parse HTTP date
    try:
        # Example: Wed, 21 Oct 2015 07:28:00 GMT
        dt = datetime.strptime(header_value, '%a, %d %b %Y %H:%M:%S %Z')
        # Compute seconds until that time
        delta = (dt - datetime.utcnow()).total_seconds()
        return int(delta) if delta > 0 else 0
    except Exception:
        return None


def fetch_post_comments(post_id: str, max_retries: int = 5):
    """Fetch comments JSON for a post, with retry/backoff on 429 responses.

    Honors `Retry-After` header when present. Returns parsed JSON on success
    or raises the last encountered exception after retries are exhausted.
    """
    url = f"https://www.reddit.com/comments/{post_id}.json?limit=500"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    attempt = 0
    base_sleep = 5
    timeout = HTTP_REQUEST_TIMEOUT
    while True:
        attempt += 1
        try:
            r = httpx.get(url, headers=headers, timeout=timeout)
        except Exception as e:
            # Network-level errors: if we have retries left, back off and retry
            if attempt <= max_retries:
                sleep_for = min(60, base_sleep * (2 ** (attempt - 1)))
                logger.warning(f"Network error fetching comments for {post_id}, retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
                continue
            raise

        # Always sleep a bit to respect general API rate limiting
        try:
            time.sleep(API_RATE_DELAY)
        except Exception:
            pass

        # Handle 429 Too Many Requests specially
        if r.status_code == 429:
            # Check Retry-After header
            ra = _parse_retry_after(r.headers.get('Retry-After'))
            if ra is None:
                # exponential backoff if header absent
                backoff = min(60, base_sleep * (2 ** (attempt - 1)))
            else:
                backoff = max(1, ra)
            # Ensure we respect the global API rate delay as a minimum when retrying
            sleep_for = max(API_RATE_DELAY, backoff)
            logger.warning(f"Received 429 for post {post_id}; retry {attempt}/{max_retries} after {sleep_for}s (backoff={backoff})")
            if attempt <= max_retries:
                time.sleep(sleep_for)
                continue
            # exhausted retries
            r.raise_for_status()

        try:
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError:
            # For other HTTP errors, don't retry here; propagate
            raise


def fetch_sub_about(name: str):
    url = f"https://www.reddit.com/r/{name}/about.json"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    max_retries = SUBABOUT_MAX_RETRIES
    timeout = HTTP_REQUEST_TIMEOUT
    attempt = 0
    base_sleep = 2
    while True:
        attempt += 1
        # Ensure only one thread calls Reddit about endpoint at a time and
        # respect the global API_RATE_DELAY spacing between calls.
        try:
            global LAST_SUBREDDIT_REQUEST
            # Acquire short-lived lock to enforce spacing and serialization
            SUBREDDIT_RATE_LOCK.acquire()
            try:
                now_ts = time.time()
                elapsed = now_ts - (LAST_SUBREDDIT_REQUEST or 0.0)
                if elapsed < API_RATE_DELAY:
                    sleep_for = API_RATE_DELAY - elapsed
                    try:
                        time.sleep(sleep_for)
                    except Exception:
                        pass
                # perform the request while holding lock to prevent concurrent calls
                r = httpx.get(url, headers=headers, timeout=timeout)
                LAST_SUBREDDIT_REQUEST = time.time()
            finally:
                try:
                    SUBREDDIT_RATE_LOCK.release()
                except Exception:
                    pass
        except httpx.ReadTimeout as e:
            if attempt <= max_retries:
                sleep_for = min(60, base_sleep * (2 ** (attempt - 1)))
                logger.warning(f"Read timeout fetching /r/{name} (attempt {attempt}/{max_retries}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
                continue
            # re-raise so caller can log/handle
            raise
        except httpx.RequestError as e:
            # network-level errors
            if attempt <= max_retries:
                sleep_for = min(60, base_sleep * (2 ** (attempt - 1)))
                logger.warning(f"Network error fetching /r/{name} (attempt {attempt}/{max_retries}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
                continue
            raise

        # small pause to respect API rate limiting
        try:
            time.sleep(API_RATE_DELAY)
        except Exception:
            pass

        # Handle 429 Too Many Requests specially
        if r.status_code == 429:
            ra = parse_retry_after_seconds(r.headers.get('Retry-After'))
            if ra is None:
                backoff = min(60, base_sleep * (2 ** (attempt - 1)))
            else:
                backoff = max(1, ra)
            # Respect global API_RATE_DELAY as a minimum pause between retries
            sleep_for = max(API_RATE_DELAY, backoff)
            logger.warning(f"Received 429 for /r/{name}; retry {attempt}/{max_retries} after {sleep_for}s (backoff={backoff})")
            if attempt <= max_retries:
                time.sleep(sleep_for)
                continue
            # exhausted retries; return response so caller can record Retry-After and schedule next attempt
            return r

        return r


def apply_schema_migrations():
    """Run small, idempotent schema migrations required by runtime code.

    Currently ensures `mentions.source_subreddit_id` exists and an index
    is present so the scanner can record where mentions were observed.
    Also adds scan tracking columns to the analytics table.
    """
    try:
        with engine.begin() as conn:
            # Add column if not present (Postgres supports IF NOT EXISTS for ADD COLUMN)
            try:
                conn.execute(text("ALTER TABLE mentions ADD COLUMN IF NOT EXISTS source_subreddit_id integer NULL"))
            except Exception:
                # Older Postgres versions or permission errors may raise; ignore
                pass
            # Create index to speed up lookups
            try:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mentions_source_subreddit_id ON mentions(source_subreddit_id)"))
            except Exception:
                pass
            # Add scan tracking columns to analytics table
            try:
                conn.execute(text("ALTER TABLE analytics ADD COLUMN IF NOT EXISTS last_scan_started timestamp NULL"))
                conn.execute(text("ALTER TABLE analytics ADD COLUMN IF NOT EXISTS last_scan_duration integer NULL"))
                conn.execute(text("ALTER TABLE analytics ADD COLUMN IF NOT EXISTS last_scan_new_mentions integer NULL"))
            except Exception:
                pass
    except Exception:
        logger.exception('apply_schema_migrations failure')


def should_refresh_sub(sub: models.Subreddit, now: datetime = None) -> bool:
    """Return True if we should refresh metadata for `sub`.

    Rules:
    - If any key fields are missing (display_name, title, description, subscribers), return True.
    - Otherwise, only refresh if `last_checked` is older than 24 hours.
    """
    try:
        if now is None:
            now = datetime.utcnow()
        # If a future retry is scheduled (from a previous 429), skip until then
        try:
            nra = getattr(sub, 'next_retry_at', None)
            if nra:
                if now is None:
                    now = datetime.utcnow()
                if nra and nra > now:
                    return False
        except Exception:
            pass

        # If missing essential metadata, refresh regardless of last_checked
        missing = (
            not getattr(sub, 'display_name', None) or
            not getattr(sub, 'title', None) or
            not getattr(sub, 'description', None) or
            (getattr(sub, 'subscribers', None) is None)
        )
        if missing:
            return True
        last = getattr(sub, 'last_checked', None)
        if not last:
            return True
        try:
            age = (now - last).total_seconds()
            # 24 hours = 86400 seconds
            return age >= 86400
        except Exception:
            return True
    except Exception:
        return True


def walk_comments(data, found):
    # Reddit comments JSON structure: list with post and comments tree
    if isinstance(data, list):
        if len(data) >= 2 and 'data' in data[1]:
            children = data[1]['data'].get('children', [])
            for c in children:
                walk_comments(c, found)
        return
    kind = data.get('kind')
    d = data.get('data', {})
    if kind == 't1':
        body = d.get('body', '')
        # prefer Reddit internal id (author_fullname) but fall back to username
        author_id = d.get('author_fullname') or d.get('author')
        found.append({'id': d.get('id'), 'body': body, 'created_utc': d.get('created_utc'), 'author_id': author_id})
        # walk replies
        replies = d.get('replies')
        if replies and isinstance(replies, dict):
            for child in replies.get('data', {}).get('children', []):
                walk_comments(child, found)
    elif kind == 'more':
        # ignore for now
        return


def extract_subreddits_from_text(text: str):
    names = set()
    for m in RE_SUB.findall(text or ''):
        nm = normalize(m)
        if 3 <= len(nm) <= 21 and nm not in ('all','random'):
            names.add(nm)
    return names


def process_post(post_item, session: Session, source_subreddit_name: str = None, require_fap_friday: bool = True):
    """Process a single reddit post item.

    Returns a tuple (processed: bool, discovered_subreddits: set).
    `processed` is True when the post was a Fap Friday post and was handled
    (even if skipped because already present). `discovered_subreddits` is the
    set of subreddit names seen in new comments that may need metadata updates.
    """
    data = post_item['data']
    reddit_id = data.get('id')
    title = data.get('title')
    created_utc = int(data.get('created_utc') or 0)
    url = data.get('permalink')
    # If caller requires Fap Friday posts, enforce that; otherwise process any post
    if require_fap_friday:
        if 'fap friday' not in (title or '').lower():
            return (False, set())

    # Resolve or create a Subreddit row for the source subreddit (where this post was found)
    source_sub = None
    try:
        if source_subreddit_name:
            sname = normalize(source_subreddit_name)
            source_sub = session.query(models.Subreddit).filter_by(name=sname).first()
            if not source_sub:
                source_sub = models.Subreddit(name=sname)
                session.add(source_sub)
                session.commit()
    except Exception:
        session.rollback()
    # If post already exists, decide whether to re-scan comments.
    # We re-scan posts that are within the past ~6 months to catch edited
    # comments which may have added new subreddit mentions. Older posts are
    # skipped to avoid reprocessing a large backlog.
    existing = session.query(models.Post).filter_by(reddit_post_id=reddit_id).first()
    now = datetime.utcnow()
    # Use configured lookback days to determine whether to re-scan comments
    six_months_ago_ts = int((now - timedelta(days=POST_COMMENT_LOOKBACK_DAYS)).timestamp())
    if existing:
        # If the post is older than six months, skip re-scanning comments.
        try:
            post_created = int(existing.created_utc or 0)
        except Exception:
            post_created = 0
        if post_created and post_created < six_months_ago_ts:
            logger.info(f"Post {reddit_id} already in DB and older than {POST_COMMENT_LOOKBACK_DAYS} days, skipping comments")
            return (True, set())
        # Otherwise, allow re-scan below to detect new or edited comments

    # fetch comments first so we can determine whether any are new
    try:
        comments_json = fetch_post_comments(reddit_id)
    except Exception as e:
        logger.exception(f"Failed to fetch comments for {reddit_id}: {e}")
        return (True, set())

    found = []
    walk_comments(comments_json, found)

    # If there are no comments at all, create the post record and move on
    if not found:
        post = models.Post(reddit_post_id=reddit_id, title=title, created_utc=created_utc, url=url)
        session.add(post)
        session.commit()
        try:
            date_str = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d') if created_utc else 'unknown-date'
        except Exception:
            date_str = 'unknown-date'
        source_sub = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
        logger.info(f"Saved post {reddit_id} ({date_str}) (no comments found){source_sub}")
        try:
            increment_analytics(session, posts=1)
        except Exception:
            logger.debug('Failed to increment analytics for post')
        return (True, set())

    # Determine which comments are new or have changed since last scan.
    missing = []
    edited = []
    for c in found:
        cm = session.query(models.Comment).filter_by(reddit_comment_id=c['id']).first()
        if not cm:
            missing.append(c)
        else:
            # If the stored comment body differs from the newly fetched body,
            # treat it as edited so we re-extract subreddit mentions from it.
            try:
                stored_body = (cm.body or '').strip()
                fetched_body = (c.get('body') or '').strip()
                if stored_body != fetched_body:
                    edited.append((cm, c))
            except Exception:
                # On any comparison error, conservatively treat as unedited
                pass

    # If there are no new or edited comments, skip this post entirely
    if not missing and not edited and existing:
        logger.info(f"All comments for post {reddit_id} already scanned and unchanged, skipping post")
        return (True, set())

    # Ensure a Post row exists (create if missing)
    if not existing:
        post = models.Post(reddit_post_id=reddit_id, title=title, created_utc=created_utc, url=url)
        session.add(post)
        session.commit()
        try:
            increment_analytics(session, posts=1)
        except Exception:
            logger.debug('Failed to increment analytics for post')
        source_sub = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
        logger.info(f"Saved post {reddit_id} ({format_ts(created_utc)}) - processing {len(missing)} new comments{source_sub}")
    else:
        post = existing
        source_sub = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
        logger.info(f"Rescanning post {reddit_id} ({format_ts(post.created_utc)}) - {len(missing)} new, {len(edited)} edited comments{source_sub}")
    try:
        date_str = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d') if created_utc else 'unknown-date'
    except Exception:
        date_str = 'unknown-date'
    source_sub = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
    logger.info(f"Saved post {reddit_id} ({date_str}) - processing {len(missing)} new comments{source_sub}")
    try:
        increment_analytics(session, posts=1)
    except Exception:
        logger.debug('Failed to increment analytics for post')

    discovered = set()

    # Process newly discovered comments first
    for c in missing:
        # extract subreddits first; only persist comment if at least one subreddit mention exists
        subnames = extract_subreddits_from_text(c['body'])
        if not subnames:
            continue

        # create or get comment (idempotent)
        cm = session.query(models.Comment).filter_by(reddit_comment_id=c['id']).first()
        if not cm:
            cm = models.Comment(reddit_comment_id=c['id'], post_id=post.id, body=c['body'], created_utc=int(c.get('created_utc') or 0), user_id=c.get('author_id'))
            session.add(cm)
            session.commit()
            try:
                increment_analytics(session, comments=1)
            except Exception:
                logger.debug('Failed to increment analytics for comment')

        for sname in subnames:
            # Skip ignored subreddits (configured via IGNORE_SUBREDDITS)
            if sname in IGNORE_SUBREDDITS:
                continue

            # get or create subreddit
            sub = session.query(models.Subreddit).filter_by(name=sname).first()
            if not sub:
                sub = models.Subreddit(name=sname)
                session.add(sub)
                session.commit()
                logger.info(f"New subreddit discovered: /r/{sname}")
                try:
                    increment_analytics(session, subreddits=1)
                except Exception:
                    logger.debug('Failed to increment analytics for new subreddit')
                # mark for metadata fetch later
                discovered.add(sname)
            else:
                # Log existence so operator sees when we encounter already-known subreddits
                try:
                    logger.info(f"Existing subreddit encountered: /r/{sname}")
                except Exception:
                    logger.debug(f"Encountered /r/{sname} (logging failed)")
                # Always refresh metadata on discovery (schedule for immediate update)
                try:
                    discovered.add(sname)
                except Exception:
                    session.rollback()

            # update first_mentioned if this mention is earlier; log known mentions
            try:
                ts = int(c.get('created_utc') or 0)
                updated = False
                old_val = sub.first_mentioned
                if ts:
                    if (not sub.first_mentioned) or ts < int(sub.first_mentioned):
                        sub.first_mentioned = ts
                        session.add(sub)
                        session.commit()
                        updated = True
                # Log that the subreddit was mentioned and whether we changed first_mentioned
                try:
                    if updated:
                        logger.info(f"Known subreddit mentioned: /r/{sname} (comment {c.get('id')}) - first_mentioned updated from {format_ts(old_val)} to {format_ts(sub.first_mentioned)}")
                    else:
                        logger.info(f"Known subreddit mentioned: /r/{sname} (comment {c.get('id')}) - no change to first_mentioned ({format_ts(sub.first_mentioned)})")
                except Exception:
                    # logging should not block processing
                    logger.debug(f"Mention processed for /r/{sname} (comment {c.get('id')})")
            except Exception:
                session.rollback()
                logger.exception(f"Error updating first_mentioned for /r/{sname}")

            # Insert mention only if it doesn't already exist to ensure idempotency
            try:
                # skip if this user already mentioned this subreddit previously
                try:
                    existing_by_user = None
                    if cm.user_id:
                        existing_by_user = session.query(models.Mention).filter_by(subreddit_id=sub.id, user_id=cm.user_id).first()
                    if existing_by_user:
                        # user already has a mention for this subreddit; do not insert duplicate
                        pass
                    else:
                        exists = session.query(models.Mention).filter_by(subreddit_id=sub.id, comment_id=cm.id).first()
                        if not exists:
                            mention = models.Mention(subreddit_id=sub.id, comment_id=cm.id, post_id=post.id, timestamp=int(c.get('created_utc') or 0), user_id=cm.user_id, source_subreddit_id=(source_sub.id if source_sub else None))
                            session.add(mention)
                            session.commit()
                            try:
                                increment_analytics(session, mentions=1)
                            except Exception:
                                logger.debug('Failed to increment analytics for mention')
                except Exception:
                    session.rollback()
            except Exception:
                session.rollback()

    # Process edited comments: update stored body and extract any newly-added subreddit mentions
    for cm, c in edited:
        try:
            fetched_body = c.get('body') or ''
            # Update stored comment body and metadata
            try:
                cm.body = fetched_body
                cm.user_id = c.get('author_id') or cm.user_id
                cm.created_utc = int(c.get('created_utc') or cm.created_utc or 0)
                session.add(cm)
                session.commit()
            except Exception:
                session.rollback()

            subnames = extract_subreddits_from_text(fetched_body)
            if not subnames:
                continue

            for sname in subnames:
                if sname in IGNORE_SUBREDDITS:
                    continue
                # get or create subreddit
                sub = session.query(models.Subreddit).filter_by(name=sname).first()
                if not sub:
                    sub = models.Subreddit(name=sname)
                    session.add(sub)
                    session.commit()
                    logger.info(f"New subreddit discovered (edited comment): /r/{sname}")
                    try:
                        increment_analytics(session, subreddits=1)
                    except Exception:
                        logger.debug('Failed to increment analytics for new subreddit')
                    discovered.add(sname)
                else:
                    try:
                        discovered.add(sname)
                    except Exception:
                        session.rollback()

                # update first_mentioned if this mention is earlier
                try:
                    ts = int(c.get('created_utc') or 0)
                    updated = False
                    old_val = sub.first_mentioned
                    if ts:
                        if (not sub.first_mentioned) or ts < int(sub.first_mentioned):
                            sub.first_mentioned = ts
                            session.add(sub)
                            session.commit()
                            updated = True
                except Exception:
                    session.rollback()

                # Insert mention only if it doesn't already exist
                try:
                    existing_by_user = None
                    if cm.user_id:
                        existing_by_user = session.query(models.Mention).filter_by(subreddit_id=sub.id, user_id=cm.user_id).first()
                    if not existing_by_user:
                        exists = session.query(models.Mention).filter_by(subreddit_id=sub.id, comment_id=cm.id).first()
                        if not exists:
                            mention = models.Mention(subreddit_id=sub.id, comment_id=cm.id, post_id=post.id, timestamp=int(c.get('created_utc') or 0), user_id=cm.user_id, source_subreddit_id=(source_sub.id if source_sub else None))
                            session.add(mention)
                            session.commit()
                            try:
                                increment_analytics(session, mentions=1)
                            except Exception:
                                logger.debug('Failed to increment analytics for mention')
                except Exception:
                    session.rollback()
        except Exception as e:
            session.rollback()
            logger.exception(f"Error processing edited comments for post {reddit_id}: {e}")

    # After processing new and edited comments, update the post's unique_subreddits
    try:
        try:
            uniq = int(session.query(func.count(func.distinct(models.Mention.subreddit_id))).filter(models.Mention.post_id == post.id).scalar() or 0)
        except Exception:
            # fallback: count distinct subreddit ids manually
            uniq = 0
            try:
                rows = session.query(models.Mention.subreddit_id).filter(models.Mention.post_id == post.id).distinct().all()
                uniq = len(rows)
            except Exception:
                pass
        try:
            post.unique_subreddits = uniq
            session.add(post)
            session.commit()
            logger.info(f"Updated post {reddit_id} unique_subreddits={uniq}")
        except Exception:
            session.rollback()
    except Exception:
        # non-fatal
        pass

    return (True, discovered)


def update_subreddit_metadata(session: Session, sub: models.Subreddit):
    # Always attempt to refresh metadata when called. This scanner configuration
    # updates subreddit metadata immediately after discovery, so do not rely on
    # any time-based caching logic here.
    try:
        r = fetch_sub_about(sub.name)
        if r.status_code == 200:
            # successful fetch — clear not_found if previously set
            sub.not_found = False
            payload = r.json()
            # some responses may return top-level 'reason' for banned communities
            if isinstance(payload, dict) and payload.get('reason'):
                sub.is_banned = True
                sub.ban_reason = str(payload.get('reason'))
            data = payload.get('data', {}) if isinstance(payload, dict) else {}
            # save a broad set of metadata fields
            try:
                sub.display_name = data.get('display_name') or sub.display_name
                sub.display_name_prefixed = data.get('display_name_prefixed') or sub.display_name_prefixed
                sub.title = data.get('title') or sub.title
            except Exception:
                pass

            try:
                sub.created_utc = int(data.get('created_utc')) if data.get('created_utc') else sub.created_utc
            except Exception:
                pass

            try:
                sub.subscribers = int(data.get('subscribers')) if data.get('subscribers') is not None else sub.subscribers
            except Exception:
                pass

            try:
                sub.active_users = int(data.get('accounts_active') or data.get('active_user_count') or data.get('active_accounts')) if (data.get('accounts_active') or data.get('active_user_count') or data.get('active_accounts')) is not None else sub.active_users
            except Exception:
                pass

            # public_description is what we previously used for description
            try:
                sub.description = data.get('public_description') or sub.description
            except Exception:
                pass

            # booleans and small strings
            try:
                sub.allow_videogifs = bool(data.get('allow_videogifs')) if data.get('allow_videogifs') is not None else sub.allow_videogifs
            except Exception:
                pass
            try:
                sub.allow_videos = bool(data.get('allow_videos')) if data.get('allow_videos') is not None else sub.allow_videos
            except Exception:
                pass
            try:
                sub.subreddit_type = data.get('subreddit_type') or sub.subreddit_type
            except Exception:
                pass
            try:
                sub.lang = data.get('lang') or sub.lang
            except Exception:
                pass
            try:
                sub.url = data.get('url') or sub.url
            except Exception:
                pass
            try:
                # reddit may return over18 or over_18
                ov = data.get('over18') if 'over18' in data else data.get('over_18')
                if ov is not None:
                    sub.over18 = bool(ov)
            except Exception:
                pass
            sub.is_banned = sub.is_banned or False
            try:
                logger.info(f"Updated metadata for /r/{sub.name}: display_name='{sub.display_name}', subscribers={sub.subscribers}")
            except Exception:
                logger.debug(f"Metadata updated for /r/{sub.name} (logging failed)")
        elif 300 <= r.status_code < 400:
            # treat redirects as 'not found' for our purposes
            sub.not_found = True
            sub.is_banned = False
            try:
                payload = r.json()
                if isinstance(payload, dict) and payload.get('reason'):
                    sub.ban_reason = str(payload.get('reason'))
            except Exception:
                pass
            try:
                logger.info(f"/r/{sub.name} returned redirect ({r.status_code}); marked not_found")
            except Exception:
                pass
        elif r.status_code in (403, 404):
            # Distinguish between forbidden (403) and not found (404).
            if r.status_code == 403:
                sub.is_banned = True
                sub.not_found = False
            else:
                # 404 -> subreddit does not exist; mark as not_found so UI can hide it
                sub.not_found = True
                sub.is_banned = False
            # if response body includes reason, save it
            try:
                payload = r.json()
                if isinstance(payload, dict) and payload.get('reason'):
                    sub.ban_reason = str(payload.get('reason'))
            except Exception:
                pass
            try:
                logger.info(f"/r/{sub.name} returned {r.status_code}; is_banned={sub.is_banned}, not_found={sub.not_found}")
            except Exception:
                pass
        elif r.status_code == 429:
            # Rate limited: schedule next retry based on Retry-After header
            try:
                ra = parse_retry_after_seconds(r.headers.get('Retry-After'))
            except Exception:
                ra = None
            try:
                if ra and ra > 0:
                    sub.next_retry_at = datetime.utcnow() + timedelta(seconds=int(ra))
                else:
                    # fallback: schedule a conservative retry window (e.g., 10 * API_RATE_DELAY)
                    sub.next_retry_at = datetime.utcnow() + timedelta(seconds=max(30, int(API_RATE_DELAY * 10)))
                # Increase retry_priority so top-listed subreddits are retried earlier
                try:
                    sub.retry_priority = int(sub.retry_priority or 0) + 1
                except Exception:
                    sub.retry_priority = 1
                logger.warning(f"/r/{sub.name} rate-limited; scheduling next_retry_at={sub.next_retry_at} (Retry-After={ra})")
            except Exception:
                logger.exception(f"Failed to schedule retry for /r/{sub.name} after 429")
        else:
            logger.warning(f"Unexpected status {r.status_code} for /r/{sub.name}")
            try:
                logger.info(f"/r/{sub.name} unexpected status {r.status_code}")
            except Exception:
                pass
    except Exception as e:
        logger.exception(f"Error fetching about for /r/{sub.name}: {e}")
    finally:
        try:
            # Record when we last attempted to check this subreddit so idle
            # sweeps will not repeatedly try the same rows immediately.
            sub.last_checked = datetime.utcnow()
        except Exception:
            pass
        session.add(sub)
        try:
            session.commit()
            try:
                logger.info(f"Recorded last_checked and committed metadata for /r/{sub.name}")
            except Exception:
                pass
        except Exception:
            session.rollback()


# metadata_worker removed: metadata is fetched synchronously during discovery


def main_loop():
    wait_for_db_startup()
    ensure_tables()
    # Skip startup metadata prefetch; begin scanning immediately
    logger.info("Starting scanner main loop")
    after = None
    processed_count = 0
    while True:
        scan_start_time = time.time()
        mentions_before = 0
        try:
            # If SUBREDDITS_TO_SCAN configured, iterate each subreddit and fetch its recent posts.
            if SUBREDDITS_TO_SCAN:
                discovered_overall = set()
                exit_after_batch = False
                with Session(engine) as session:
                    # Record scan start time and initial mention count
                    try:
                        analytics = session.query(models.Analytics).first()
                        if analytics:
                            analytics.last_scan_started = datetime.utcnow()
                            mentions_before = analytics.total_mentions or 0
                            session.commit()
                    except Exception:
                        logger.debug('Failed to record scan start time')
                    
                    for subname in SUBREDDITS_TO_SCAN:
                        after_sub = None
                        while True:
                            try:
                                data = fetch_subreddit_posts(subname, after_sub)
                            except Exception as e:
                                # Check if it's a 429 rate limit error
                                error_str = str(e)
                                error_type = type(e).__name__
                                if '429' in error_str:
                                    logger.warning(f"Rate limited on /r/{subname}: {error_type}: {error_str} - will retry in next loop iteration")
                                else:
                                    logger.warning(f"Exception fetching /r/{subname} (type={error_type}): {error_str}")
                                    logger.exception(f"Full traceback for /r/{subname}")
                                break
                            children = data.get('data', {}).get('children', [])
                            if not children:
                                break
                            if not after_sub:
                                logger.info(f"Scanning new posts from /r/{subname}")
                            for p in children:
                                pid = p.get('data', {}).get('id')
                                if TEST_POST_IDS and pid not in TEST_POST_IDS:
                                    continue
                                pdata = p.get('data', {})
                                # Only process NSFW posts
                                over18 = bool(pdata.get('over_18') or pdata.get('over18'))
                                if not over18:
                                    continue
                                # For wowthissubexists, only posts by REDDIT_USER
                                if subname == normalize('wowthissubexists'):
                                    author = (pdata.get('author') or '').strip()
                                    if author.lower() != (REDDIT_USER or '').lower():
                                        continue
                                processed, discovered = process_post(p, session, source_subreddit_name=subname, require_fap_friday=False)
                                if processed:
                                    processed_count += 1
                                if discovered:
                                    discovered_overall.update(discovered)
                                if TEST_POST_LIMIT and processed_count >= TEST_POST_LIMIT:
                                    logger.info(f"Reached TEST_POST_LIMIT={TEST_POST_LIMIT}, stopping after this batch to refresh metadata.")
                                    exit_after_batch = True
                                    break
                            after_sub = data.get('data', {}).get('after')
                            if not after_sub or exit_after_batch:
                                break
                        if exit_after_batch:
                            break
                # If we processed any discovered subs in SUBREDDITS_TO_SCAN mode, refresh metadata
                if discovered_overall:
                    logger.info(f"Refreshing metadata for {len(discovered_overall)} subreddits discovered in this scan")
                    for sname in discovered_overall:
                        try:
                            sub = session.query(models.Subreddit).filter_by(name=sname).first()
                            if sub and should_refresh_sub(sub):
                                with SUBABOUT_SEMAPHORE:
                                    update_subreddit_metadata(session, sub)
                        except Exception:
                            logger.exception(f"Failed to refresh metadata for /r/{sname}")
                # Record scan completion metrics
                with Session(engine) as session:
                    try:
                        analytics = session.query(models.Analytics).first()
                        if analytics:
                            new_mentions = (analytics.total_mentions or 0) - mentions_before
                            record_scan_completion(session, scan_start_time, new_mentions)
                    except Exception:
                        logger.debug('Failed to record scan completion')
                # Sleep before next scan iteration
                logger.info('Completed SUBREDDITS_TO_SCAN pass, sleeping 5 minutes before next scan')
                time.sleep(300)
                continue
            else:
                data = fetch_user_posts(after)
                children = data.get('data', {}).get('children', [])
                if not children:
                    logger.info('No posts found, sleeping for 10 minutes')
                    time.sleep(600)
                    continue
                with Session(engine) as session:
                    discovered_overall = set()
                    exit_after_batch = False
                    for p in children:
                        # If TEST_POST_IDS is set, skip posts not in the list
                        pid = p.get('data', {}).get('id')
                        if TEST_POST_IDS and pid not in TEST_POST_IDS:
                            continue
                        processed, discovered = process_post(p, session)
                        if processed:
                            processed_count += 1
                        if discovered:
                            discovered_overall.update(discovered)
                        # If TEST_POST_LIMIT is set, exit once we've processed that many Friday posts
                        if TEST_POST_LIMIT and processed_count >= TEST_POST_LIMIT:
                            logger.info(f"Reached TEST_POST_LIMIT={TEST_POST_LIMIT}, stopping after this batch to refresh metadata.")
                            exit_after_batch = True
                            break

                # After processing posts in this batch, refresh subreddit metadata for
                # all discovered or-stale subreddits in one pass to reduce frequent
                # synchronous per-mention requests.
                if discovered_overall:
                    logger.info(f"Refreshing metadata for {len(discovered_overall)} subreddits discovered in this batch")
                    for sname in discovered_overall:
                        try:
                            logger.info(f"Refreshing metadata for /r/{sname}")
                            sub = session.query(models.Subreddit).filter_by(name=sname).first()
                            if sub:
                                # Skip banned subreddits
                                try:
                                    if getattr(sub, 'is_banned', False):
                                        logger.info(f"Skipping metadata refresh for banned /r/{sname}")
                                        continue
                                except Exception:
                                    pass
                                try:
                                    if should_refresh_sub(sub):
                                        with SUBABOUT_SEMAPHORE:
                                            update_subreddit_metadata(session, sub)
                                    else:
                                        logger.info(f"Skipping metadata refresh for /r/{sname} (checked within 24h)")
                                except Exception:
                                    session.rollback()
                        except Exception:
                            logger.exception(f"Failed to refresh metadata for /r/{sname}")
                if exit_after_batch:
                    logger.info('TEST_POST_LIMIT reached; performing missing-metadata refresh before exiting')
                    try:
                        try:
                            from sqlalchemy import or_
                            missing_q = session.query(models.Subreddit).filter(
                                or_(models.Subreddit.display_name == None, models.Subreddit.display_name == ''),
                                or_(models.Subreddit.title == None, models.Subreddit.title == ''),
                                or_(models.Subreddit.description == None, models.Subreddit.description == ''),
                                models.Subreddit.subscribers == None
                            )
                            # Order by last_checked ascending so oldest (and NULL) are refreshed first
                            try:
                                missing = missing_q.order_by(models.Subreddit.last_checked.asc().nullsfirst()).all()
                            except Exception:
                                # Fallback if nullsfirst() isn't available in this SQLAlchemy version
                                missing = missing_q.order_by(models.Subreddit.last_checked.asc()).all()
                        except Exception:
                            session.rollback()
                            missing = []

                        if missing:
                            logger.info(f"Found {len(missing)} subreddits missing metadata; refreshing now (post-limit path)")
                            for sub in missing:
                                try:
                                    # Skip banned subreddits
                                    try:
                                        if getattr(sub, 'is_banned', False):
                                            logger.info(f"Skipping post-limit idle-refresh for banned /r/{sub.name}")
                                            continue
                                    except Exception:
                                        pass
                                    logger.info(f"Post-limit idle-refresh: refreshing /r/{sub.name}")
                                    with SUBABOUT_SEMAPHORE:
                                        update_subreddit_metadata(session, sub)
                                except Exception:
                                    session.rollback()
                    except Exception:
                        logger.exception('Error during post-limit missing metadata refresh')

                    logger.info('Exiting after metadata refresh (TEST_POST_LIMIT path)')
                    return
            
            # pagination (user posts mode only)
            after = data.get('data', {}).get('after')
            if not after:
                # No more pages of user posts. Perform idle metadata refresh:
                # 1. First refresh subreddits missing metadata, prioritized by mentions
                # 2. Then refresh subreddits stale (>24 hours), prioritized by mentions
                try:
                    logger.info('No more user posts; performing idle metadata refresh')
                    with Session(engine) as session:
                        from sqlalchemy import or_
                        now = datetime.utcnow()
                        twenty_four_hours_ago = now - timedelta(hours=24)
                        
                        # Phase 1: Subreddits missing metadata, prioritized by mentions DESC
                        try:
                            missing_q = session.query(models.Subreddit).filter(
                                or_(models.Subreddit.display_name == None, models.Subreddit.display_name == ''),
                                or_(models.Subreddit.title == None, models.Subreddit.title == ''),
                                or_(models.Subreddit.description == None, models.Subreddit.description == ''),
                                models.Subreddit.subscribers == None
                            ).order_by(models.Subreddit.mentions.desc())
                            missing = missing_q.all()
                        except Exception:
                            session.rollback()
                            missing = []

                        if missing:
                            logger.info(f"Found {len(missing)} subreddits missing metadata; refreshing by mention count")
                            for sub in missing:
                                try:
                                    if getattr(sub, 'is_banned', False):
                                        logger.info(f"Skipping idle-refresh for banned /r/{sub.name}")
                                        continue
                                    logger.info(f"Idle-refresh (missing metadata): refreshing /r/{sub.name} ({sub.mentions} mentions)")
                                    with SUBABOUT_SEMAPHORE:
                                        update_subreddit_metadata(session, sub)
                                except Exception:
                                    logger.exception(f"Failed to refresh metadata for /r/{sub.name}")
                                    session.rollback()
                        
                        # Phase 2: Subreddits stale (>24 hours), prioritized by mentions DESC
                        try:
                            stale_q = session.query(models.Subreddit).filter(
                                models.Subreddit.last_checked < twenty_four_hours_ago
                            ).order_by(models.Subreddit.mentions.desc())
                            stale = stale_q.all()
                        except Exception:
                            session.rollback()
                            stale = []
                        
                        if stale:
                            logger.info(f"Found {len(stale)} subreddits stale (>24h); refreshing by mention count")
                            for sub in stale:
                                try:
                                    if getattr(sub, 'is_banned', False):
                                        logger.info(f"Skipping idle-refresh for banned /r/{sub.name}")
                                        continue
                                    logger.info(f"Idle-refresh (stale metadata): refreshing /r/{sub.name} ({sub.mentions} mentions)")
                                    with SUBABOUT_SEMAPHORE:
                                        update_subreddit_metadata(session, sub)
                                except Exception:
                                    logger.exception(f"Failed to refresh metadata for /r/{sub.name}")
                                    session.rollback()
                        
                        if missing or stale:
                            logger.info(f"Idle metadata refresh complete; sleeping 6 hours")
                        else:
                            logger.info(f"All metadata current; sleeping 6 hours")
                except Exception:
                    logger.exception('Error during idle metadata refresh')
                time.sleep(6 * 3600)
        except Exception as e:
            logger.exception(f"Scanner main loop error: {e}")
            time.sleep(60)


if __name__ == '__main__':
    main_loop()
