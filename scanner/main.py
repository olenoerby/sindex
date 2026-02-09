import os
import re
import time
import json
import logging
from dotenv import load_dotenv
# file-based rotating logs removed; rely on container stdout/stderr
from datetime import datetime, timedelta
from contextlib import contextmanager
import httpx
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import Session
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
try:
    from redis import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Ensure the project root is on sys.path so `import api` works when running
# inside the scanner container. Previously we appended the `api` folder
# itself which prevented importing `api.*` packages.
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import models
from api.utils import parse_retry_after_seconds
from api.distributed_rate_limiter import DistributedRateLimiter

# Load environment variables from .env at repo root so values like
# `POST_COMMENT_LOOKBACK_DAYS` can be set without editing `main.py`.
load_dotenv()

# Note: metadata fetch will be performed synchronously when discovering subreddits

# Logging configuration: emit to stdout/stderr (container logs)
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

logger = logging.getLogger('scanner')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
# Use stdout/stderr (container logs) with ISO 8601 timestamp format (UTC)
# Inject a dynamic `phase` field into every log record so console output
# shows which phase the scanner is currently in.
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s [%(phase)s]: %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
formatter.converter = time.gmtime  # Use UTC instead of local time
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
CURRENT_PHASE = 'Startup'


class PhaseFilter(logging.Filter):
    def filter(self, record):
        # Ensure every record has a `phase` attribute for the formatter.
        try:
            record.phase = CURRENT_PHASE
        except Exception:
            record.phase = 'unknown'
        return True


@contextmanager
def temp_phase(name: str):
    """Temporarily set `CURRENT_PHASE` for the duration of the context."""
    global CURRENT_PHASE
    prev = CURRENT_PHASE
    CURRENT_PHASE = name
    logger.info(f"=== PHASE: {name} ===")
    try:
        yield
    finally:
        CURRENT_PHASE = prev
        logger.info(f"=== PHASE: {prev} ===")

if not logger.handlers:
    stream_handler.addFilter(PhaseFilter())
    logger.addHandler(stream_handler)
logger.propagate = False

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
API_MAX_CALLS_MINUTE = int(os.getenv('API_MAX_CALLS_MINUTE', '30'))
# Calculate minimum delay from max calls per minute (60 seconds / max calls)
API_RATE_DELAY_SECONDS = 60.0 / API_MAX_CALLS_MINUTE
# METADATA_REFRESH_SECONDS: time in seconds to spend refreshing metadata
METADATA_REFRESH_SECONDS = float(os.getenv('METADATA_REFRESH_SECONDS', '7200'))
# If true, scanner starts by refreshing metadata before scanning for new mentions
SCAN_FOR_METADATA_FIRST = os.getenv('SCAN_FOR_METADATA_FIRST', 'false').lower() in ('true', '1', 'yes')

# How far back to initially scan posts from source subreddits (posts older than this are skipped entirely)
# If not set or empty, will scan ALL posts with no age limit.
post_initial_env = os.getenv('POST_INITIAL_SCAN_DAYS', '').strip()
if post_initial_env:
    try:
        POST_INITIAL_SCAN_DAYS = int(post_initial_env)
    except Exception:
        POST_INITIAL_SCAN_DAYS = None
else:
    POST_INITIAL_SCAN_DAYS = None

# How many days back to rescan existing posts for new/edited comments.
# Set to 0 to skip rescanning existing posts. If not set or empty, will rescan ALL existing posts.
post_rescan_env = os.getenv('POST_RESCAN_DAYS', '').strip()
if post_rescan_env:
    try:
        POST_RESCAN_DAYS = int(post_rescan_env)
    except Exception:
        POST_RESCAN_DAYS = None
else:
    POST_RESCAN_DAYS = None

# Skip posts that were scanned within the last X hours (useful for container restarts)
# Set to 0 to disable this feature and always scan posts.
skip_recently_scanned_env = os.getenv('SKIP_RECENTLY_SCANNED_HOURS', '').strip()
if skip_recently_scanned_env:
    try:
        SKIP_RECENTLY_SCANNED_HOURS = int(skip_recently_scanned_env)
    except Exception:
        SKIP_RECENTLY_SCANNED_HOURS = 0
else:
    SKIP_RECENTLY_SCANNED_HOURS = 0

# How many seconds to sleep between scan iterations (after metadata refresh completes)
SCAN_SLEEP_SECONDS = int(os.getenv('SCAN_SLEEP_SECONDS', '300'))

# How many seconds to spend rescanning posts from the DB each iteration
POST_RESCAN_DURATION = int(os.getenv('POST_RESCAN_DURATION', '300'))  # default: 5 minutes

# Number of days to consider subreddit metadata fresh before re-fetching from Reddit.
# Can be set via `SUBREDDIT_META_CACHE_DAYS`; falls back to legacy `META_CACHE_DAYS` if present.
# Max retries for subreddit about fetches and per-request HTTP timeout (seconds)
SUBABOUT_MAX_RETRIES = int(os.getenv('SUBABOUT_MAX_RETRIES', '3'))
HTTP_REQUEST_TIMEOUT = float(os.getenv('HTTP_REQUEST_TIMEOUT', '15'))
# How many hours before metadata is considered stale and needs refreshing
METADATA_STALE_HOURS = int(os.getenv('METADATA_STALE_HOURS', '24'))

# Global lock and timestamp for serializing and spacing subreddit about requests
# NOTE: With RateLimiter in place, these are legacy. RateLimiter handles all API throttling.
SUBREDDIT_RATE_LOCK = threading.Lock()
LAST_SUBREDDIT_REQUEST = 0.0
# NOTE: metadata freshness is not cached by days; metadata is refreshed
# immediately after discovery during each scan.
# Optional testing controls:
# If set, scanner will only process up to this many posts PER SOURCE SUBREDDIT and then exit.
TEST_MAX_POSTS_PER_SUBREDDIT = int(os.getenv('TEST_MAX_POSTS_PER_SUBREDDIT')) if os.getenv('TEST_MAX_POSTS_PER_SUBREDDIT') else None

engine = create_engine(DATABASE_URL, future=True)

# Initialize distributed rate limiter for coordination with metadata_worker
try:
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    distributed_rate_limiter = DistributedRateLimiter(
        redis_url=redis_url,
        min_delay_seconds=API_RATE_DELAY_SECONDS,
        max_calls_per_minute=API_MAX_CALLS_MINUTE
    )
    distributed_rate_limiter.set_container_name("scanner")
    logger.info("Initialized distributed rate limiter")
except Exception as e:
    logger.error(f"Failed to initialize distributed rate limiter: {e}")
    logger.warning("Continuing with local rate limiting only")
    distributed_rate_limiter = None


class RateLimiter:
    """Thread-safe rolling window rate limiter for Reddit API calls.
    
    Enforces TWO constraints:
    1. Per-minute limit: max_calls_per_minute over rolling 60-second window
    2. Minimum delay: Calculated as 60/max_calls_per_minute between consecutive calls
    """
    
    def __init__(self, max_calls_per_minute, min_delay_seconds=None):
        self.max_calls = max_calls_per_minute
        self.min_delay = min_delay_seconds or 0
        self.call_times = []  # List of timestamps (in seconds)
        self.last_call_time = 0.0  # Track the most recent call
        self.lock = threading.Lock()
        logger.info(f"RateLimiter initialized: {self.max_calls} calls per 60 seconds, min delay {self.min_delay}s between calls")
    
    def wait_if_needed(self):
        """Block if necessary to stay within rate limit AND minimum delay, then record this call."""
        with self.lock:
            now = time.time()
            
            # Check minimum delay since last call (trumps everything)
            if self.last_call_time > 0:
                time_since_last = now - self.last_call_time
                if time_since_last < self.min_delay:
                    sleep_time = self.min_delay - time_since_last
                    with temp_phase('Rate Limiting + Retries'):
                        logger.info(f"Enforcing min API delay: {time_since_last:.2f}s elapsed, sleeping {sleep_time:.2f}s more (total min: {self.min_delay}s)")
                        time.sleep(sleep_time)
                        now = time.time()
            
            # Remove calls older than 60 seconds (rolling window)
            self.call_times = [t for t in self.call_times if now - t < 60.0]
            
            current_count = len(self.call_times)
            
            if current_count >= self.max_calls:
                # Must wait until the oldest call expires from the window
                oldest_time = self.call_times[0]
                sleep_time = 60.0 - (now - oldest_time) + 0.1  # +0.1s buffer
                with temp_phase('Rate Limiting + Retries'):
                    logger.info(f"Rate limit: {current_count}/{self.max_calls} calls in last 60s, waiting {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    # Re-clean the window after waiting
                    now = time.time()
                    self.call_times = [t for t in self.call_times if now - t < 60.0]
            
            # Record this API call
            self.call_times.append(now)
            self.last_call_time = now
            new_count = len(self.call_times)
            
            # Log periodically (every 10th call or when approaching limit)
            if new_count % 10 == 0 or new_count >= self.max_calls - 5:
                logger.debug(f"API call tracker: {new_count}/{self.max_calls} calls in last 60 seconds")


# Global rate limiter instance
rate_limiter = RateLimiter(API_MAX_CALLS_MINUTE, min_delay_seconds=API_RATE_DELAY_SECONDS)

# Patterns for subreddit mentions. Accepts r/name, /r/name and reddit url forms.
RE_SUB = re.compile(r"(?:/r/|\br/|https?://(?:www\.)?reddit\.com/r/)([A-Za-z0-9_]{3,21})")
# Pattern for user mentions. Accepts u/name, /u/name and reddit url forms.
RE_USER = re.compile(r"(?:/u/|\bu/|https?://(?:www\.)?reddit\.com/u(?:ser)?/)([A-Za-z0-9_-]{3,20})")

def normalize(name: str) -> str:
    """Normalize subreddit or user reference into storage form.

    Rules:
    - Lowercase and strip whitespace/newlines
    - Accept forms: "r/name", "/r/name", "name" -> "name"
    - Accept forms: "u/name", "/u/name" -> "u_name"
    - Preserve "u_name" form (user profiles) and do NOT strip the underscore
    """
    if not name:
        return ''
    n = str(name).lower().strip().replace('\n', '')
    # remove leading slashes
    while n.startswith('/'):
        n = n[1:]
    # r/ prefix
    if n.startswith('r/'):
        return n[2:]
    # u/ prefix -> convert to u_username
    if n.startswith('u/'):
        return 'u_' + n[2:]
    # already in u_username form
    if n.startswith('u_'):
        return n
    return n

def is_user_profile(name: str) -> bool:
    """Check if a normalized name represents a user profile (u_ prefix)."""
    return name.startswith('u_')


def format_ts(ts: int) -> str:
    """Format a unix timestamp (seconds) as YYYY-MM-DD; return 'none' if falsy."""
    try:
        if not ts:
            return 'none'
        return datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d')
    except Exception:
        return str(ts)


def clean_username(raw):
    """Return a normalized reddit username or None when unavailable/deleted."""
    try:
        if not raw:
            return None
        name = str(raw).strip()
        if not name:
            return None
        if name.lower() in ('[deleted]', 'deleted'):
            return None
        return name
    except Exception:
        return None


# Configuration loaded from database at runtime
# Legacy fallback to .env if database config not available
_LEGACY_IGNORE_SUBREDDITS = set(
    normalize(s) for s in os.getenv('IGNORE_SUBREDDITS', '').split(',') if s.strip()
)
_LEGACY_SUBREDDITS_TO_SCAN = [
    normalize(s) for s in (os.getenv('SUBREDDITS_TO_SCAN') or '').split(',') if s.strip()
]


def load_scan_config_from_db(session):
    """Load active scan configurations from database.
    Returns: (scan_configs_dict, ignored_subreddits_set, ignored_users_set)
    
    scan_configs_dict format: {
        'subreddit_name': {
            'allowed_users': set(['user1', 'user2']) or None (for all users),
            'nsfw_only': True/False,
            'priority': int (1=highest, 2=high, 3=normal, 4=low)
        }
    }
    """
    try:
        from models import SubredditScanConfig, IgnoredSubreddit, IgnoredUser
        
        # Load active scan configs
        scan_configs = {}
        configs = session.query(SubredditScanConfig).filter_by(active=True).all()
        for cfg in configs:
            allowed_users = None
            if cfg.allowed_users:
                # Parse comma-separated list and ensure usernames are prefixed with 'u_'
                users = []
                for u in cfg.allowed_users.split(','):
                    uname = u.strip().lower()
                    if uname:
                        if not uname.startswith('u_'):
                            uname = f'u_{uname}'
                        users.append(uname)
                if users:
                    allowed_users = set(users)
            
            # Normalize subreddit_name: always lowercase, and preserve u_ prefix for user profiles
            subname = normalize(cfg.subreddit_name)
            scan_configs[subname] = {
                'allowed_users': allowed_users,
                'nsfw_only': cfg.nsfw_only,
                'priority': getattr(cfg, 'priority', 3)  # Default to 3 if not set
            }
        
        # Load ignored subreddits
        ignored_subs = set()
        ignored_sub_rows = session.query(IgnoredSubreddit).filter_by(active=True).all()
        for ign in ignored_sub_rows:
            ignored_subs.add(ign.subreddit_name)
        
        # Load ignored users
        ignored_users = set()
        ignored_user_rows = session.query(IgnoredUser).filter_by(active=True).all()
        for ign in ignored_user_rows:
            ignored_users.add(ign.username.lower())
        
        return scan_configs, ignored_subs, ignored_users
        
    except Exception as e:
        logger.warning(f"Failed to load scan config from database: {e}. Using legacy .env config.")
        # Fallback to legacy config
        scan_configs = {}
        for sub in _LEGACY_SUBREDDITS_TO_SCAN:
            scan_configs[sub] = {
                'allowed_users': None,
                'nsfw_only': True,
                'priority': 3  # Default priority for legacy configs
            }
        return scan_configs, _LEGACY_IGNORE_SUBREDDITS, set()


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
        logger.info(f'Creating tables: {list(models.Base.metadata.tables.keys())}')
        models.Base.metadata.create_all(engine)
        logger.info('Schema tables created successfully')
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
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_mention_sub_comment_idx ON mention(subreddit_id, comment_id)"
            ))
            conn.commit()
    except Exception:
        # Non-fatal: if DB user lacks privileges or index already exists differently, continue.
        logger.exception("Could not ensure unique index on mention (continuing)")


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

    # Try to connect immediately first
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        logger.info('Database connectivity verified (immediate)')
        return True
    except Exception as e:
        logger.warning(f'Initial DB connection failed: {e}. Waiting {initial}s before retrying...')
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


def sync_analytics_counts(session: Session):
    """Sync analytics table with actual database counts."""
    try:
        a = get_or_create_analytics(session)
        if a:
            # Update all counts with actual DB totals
            a.total_subreddits = int(session.query(func.count(models.Subreddit.id)).scalar() or 0)
            a.total_mentions = int(session.query(func.count(models.Mention.id)).scalar() or 0)
            a.total_posts = int(session.query(func.count(models.Post.id)).scalar() or 0)
            a.total_comments = int(session.query(func.count(models.Comment.id)).scalar() or 0)
            session.add(a)
            session.commit()
            logger.debug(f"Analytics synced: subreddits={a.total_subreddits}, mentions={a.total_mentions}, posts={a.total_posts}, comments={a.total_comments}")
    except Exception:
        logger.exception('Failed to sync analytics counts')
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
            # Sync all counts with actual DB totals
            sync_analytics_counts(session)
    except Exception:
        logger.exception('Failed to record scan completion')
        session.rollback()


def fetch_subreddit_posts(subname: str, after: str = None):
    """Fetch recent posts for a subreddit or user.
    
    For regular subreddits: /r/{subname}/new.json
    For user profiles (u_username): /user/{username}/submitted.json
    
    Returns parsed JSON.
    """
    # Check if this is a user profile
    if is_user_profile(subname):
        # Extract username (remove u_ prefix)
        username = subname[2:]  # Remove 'u_' prefix
        url = f"https://www.reddit.com/user/{username}/submitted.json?limit=100&sort=new"
        entity_label = f"/u/{username}"
    else:
        url = f"https://www.reddit.com/r/{subname}/new.json?limit=100"
        entity_label = f"/r/{subname}"
    
    if after:
        url += f"&after={after}"
    
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    timeout = HTTP_REQUEST_TIMEOUT
    # Respect the distributed/local rate limiter BEFORE making the request
    try:
        if distributed_rate_limiter:
            distributed_rate_limiter.wait_if_needed()
        else:
            rate_limiter.wait_if_needed()
    except Exception:
        pass

    try:
        r = httpx.get(url, headers=headers, timeout=timeout)
    except httpx.ReadTimeout as e:
        logger.warning(f"Read timeout fetching {entity_label} posts (after={after}): {e}")
        raise
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching {entity_label} posts (after={after}): {e}")
        raise

    # Record this API call in distributed limiter so other containers see it
    try:
        if distributed_rate_limiter:
            distributed_rate_limiter.record_api_call()
    except Exception:
        pass

    # Log response status and headers for debugging
    logger.debug(f"fetch_subreddit_posts {entity_label}: status_code={r.status_code}, headers={dict(r.headers)}")
    
    # Check for error status codes before raising
    if r.status_code == 429:
        retry_after = r.headers.get('Retry-After', 'unknown')
        retry_seconds = _parse_retry_after(retry_after) if retry_after != 'unknown' else None
        if retry_seconds:
            logger.warning(f"Rate limited fetching {entity_label}: 429 Too Many Requests, Retry-After={retry_after} ({retry_seconds}s). Waiting...")
            time.sleep(retry_seconds + 1)  # Add 1 second buffer
        else:
            # No valid Retry-After, use exponential backoff starting at 60s
            wait_time = 60
            logger.warning(f"Rate limited fetching {entity_label}: 429 Too Many Requests, Retry-After={retry_after}. Waiting {wait_time}s...")
            time.sleep(wait_time)
        raise Exception(f"HTTP 429 Rate Limited on {entity_label}; Retry-After={retry_after}")
    
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
        # Respect the distributed/local rate limiter BEFORE making the request
        try:
            if distributed_rate_limiter:
                distributed_rate_limiter.wait_if_needed()
            else:
                rate_limiter.wait_if_needed()
        except Exception:
            pass

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

        # Record this API call for distributed rate limiting coordination
        try:
            if distributed_rate_limiter:
                distributed_rate_limiter.record_api_call()
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
            sleep_for = max(API_RATE_DELAY_SECONDS, backoff)
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
    """Fetch about.json for a subreddit or user profile.
    
    For regular subreddits: /r/{name}/about.json
    For user profiles (u_username): /user/{username}/about.json
    """
    if is_user_profile(name):
        # Extract username (remove u_ prefix)
        username = name[2:]
        url = f"https://www.reddit.com/user/{username}/about.json"
        entity_label = f"/u/{username}"
    else:
        url = f"https://www.reddit.com/r/{name}/about.json"
        entity_label = f"/r/{name}"
    
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    max_retries = SUBABOUT_MAX_RETRIES
    timeout = HTTP_REQUEST_TIMEOUT
    attempt = 0
    base_sleep = 2
    while True:
        attempt += 1
        # Use distributed rate limiter if available for coordination
        try:
            global LAST_SUBREDDIT_REQUEST
            if distributed_rate_limiter:
                distributed_rate_limiter.wait_if_needed()
            else:
                # Acquire short-lived lock to enforce spacing and serialization
                SUBREDDIT_RATE_LOCK.acquire()
                try:
                    now_ts = time.time()
                    elapsed = now_ts - (LAST_SUBREDDIT_REQUEST or 0.0)
                    if elapsed < API_RATE_DELAY_SECONDS:
                        sleep_for = API_RATE_DELAY_SECONDS - elapsed
                        try:
                            time.sleep(sleep_for)
                        except Exception:
                            pass
                    LAST_SUBREDDIT_REQUEST = now_ts
                finally:
                    try:
                        SUBREDDIT_RATE_LOCK.release()
                    except Exception:
                        pass
            
            # perform the request
            r = httpx.get(url, headers=headers, timeout=timeout)
            
            # Record this API call
            if distributed_rate_limiter:
                distributed_rate_limiter.record_api_call()
            else:
                LAST_SUBREDDIT_REQUEST = time.time()
        except httpx.ReadTimeout as e:
            if attempt <= max_retries:
                sleep_for = min(60, base_sleep * (2 ** (attempt - 1)))
                logger.warning(f"Read timeout fetching {entity_label} (attempt {attempt}/{max_retries}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
                continue
            # re-raise so caller can log/handle
            raise
        except httpx.RequestError as e:
            # network-level errors
            if attempt <= max_retries:
                sleep_for = min(60, base_sleep * (2 ** (attempt - 1)))
                logger.warning(f"Network error fetching {entity_label} (attempt {attempt}/{max_retries}), retrying in {sleep_for}s: {e}")
                time.sleep(sleep_for)
                continue
            raise

        # small pause to respect API rate limiting
        try:
            if distributed_rate_limiter:
                distributed_rate_limiter.wait_if_needed()
            else:
                rate_limiter.wait_if_needed()
        except Exception:
            pass

        # Handle 429 Too Many Requests specially
        if r.status_code == 429:
            ra = parse_retry_after_seconds(r.headers.get('Retry-After'))
            if ra is None:
                backoff = min(60, base_sleep * (2 ** (attempt - 1)))
            else:
                backoff = max(1, ra)
            # Respect global API_RATE_DELAY_SECONDS as a minimum pause between retries
            sleep_for = max(API_RATE_DELAY_SECONDS, backoff)
            logger.warning(f"Received 429 for {entity_label}; retry {attempt}/{max_retries} after {sleep_for}s (backoff={backoff})")
            if attempt <= max_retries:
                time.sleep(sleep_for)
                continue
            # exhausted retries; return response so caller can record Retry-After and schedule next attempt
            return r

        return r


def apply_schema_migrations():
    """Run small, idempotent schema migrations required by runtime code.

    NOTE: These are now handled by Alembic migrations in migrations/versions/,
    so this function is a no-op to maintain backward compatibility.
    """
    pass


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
        # prefer username; keep id fallback for uniqueness when username missing
        author_name = clean_username(d.get('author'))
        author_id = d.get('author_fullname') or d.get('author')
        found.append({
            'id': d.get('id'),
            'body': body,
            'created_utc': d.get('created_utc'),
            'author_id': author_id,
            'author': author_name
        })
        # walk replies
        replies = d.get('replies')
        if replies and isinstance(replies, dict):
            for child in replies.get('data', {}).get('children', []):
                walk_comments(child, found)
    elif kind == 'more':
        # ignore for now
        return


def extract_subreddits_from_text(text: str):
    """Extract subreddit and user mentions from text.
    Returns a dict: {normalized_name: (raw_text, context_snippet, is_user)}
    
    Handles:
    - /r/subreddit or r/subreddit (regular subreddits)
    - /r/u_username (user profile subreddits)
    - /u/username or u/username (user mentions)
    """
    results = {}
    
    # Extract subreddit mentions (including /r/u_ user profiles)
    for m in RE_SUB.findall(text or ''):
        nm = normalize(m)
        # If this is a user profile subreddit (starts with u_ or u-), always store as u_username
        if nm.startswith('u_') or nm.startswith('u-'):
            usernm = 'u_' + nm[2:] if not nm.startswith('u_') else nm
            is_user = True
        else:
            usernm = nm
            is_user = False
        # Skip special subreddits
        if usernm in ('all', 'random'):
            continue
        if 3 <= len(usernm) <= 21 or (is_user and 5 <= len(usernm) <= 23):
            # Extract context around this mention (±50 chars)
            match_idx = (text or '').lower().find(m.lower())
            if match_idx >= 0:
                start = max(0, match_idx - 50)
                end = min(len(text), match_idx + len(m) + 50)
                context = text[start:end].strip()
            else:
                context = m
            results[usernm] = (m, context[:200], is_user)
    
    # Extract direct user mentions (/u/username)
    for m in RE_USER.findall(text or ''):
        nm = 'u_' + normalize(m)  # Store as u_username for consistency
        if 5 <= len(nm) <= 23:  # u_ + 3-20 char username
            # Always overwrite or add user mention as u_username
            # Extract context around this mention (±50 chars)
            match_idx = (text or '').lower().find(m.lower())
            if match_idx >= 0:
                start = max(0, match_idx - 50)
                end = min(len(text), match_idx + len(m) + 50)
                context = text[start:end].strip()
            else:
                context = m
            results[nm] = (m, context[:200], True)  # store raw text, context, and user flag
    
    return results


def resolve_comment_user(comment: dict):
    """Prefer username; fall back to author_id; drop deleted users."""
    name = clean_username(comment.get('author') or comment.get('author_name'))
    if name:
        return name
    # retain the raw id if present to keep de-duplication working, unless deleted
    return clean_username(comment.get('author_id'))


def process_post(post_item, session: Session, source_subreddit_name: str = None, require_fap_friday: bool = True, ignored_subreddits: set = None, ignored_users: set = None):
    """Process a single reddit post item.

    Returns a tuple (processed: bool, discovered_subreddits: set).
    `processed` is True when the post was a Fap Friday post and was handled
    (even if skipped because already present). `discovered_subreddits` is the
    set of subreddit names seen in new comments that may need metadata updates.
    
    Args:
        ignored_subreddits: Set of subreddit names to skip when recording mentions
        ignored_users: Set of usernames whose mentions should not be recorded
    """
    if ignored_subreddits is None:
        ignored_subreddits = set()
    if ignored_users is None:
        ignored_users = set()
    data = post_item['data']
    reddit_id = data.get('id')
    title = data.get('title')
    created_utc = int(data.get('created_utc') or 0)
    url = data.get('permalink')
    author = data.get('author')
    # If caller requires Fap Friday posts, enforce that; otherwise process any post
    if require_fap_friday:
        if 'fap friday' not in (title or '').lower():
            return (False, set())

    # If keywords are set for this subreddit, filter by them
    if source_subreddit_name:
        scan_cfg = session.query(models.SubredditScanConfig).filter_by(subreddit_name=source_subreddit_name.lower()).first()
        if scan_cfg and scan_cfg.keywords:
            keywords = [k.strip().lower() for k in scan_cfg.keywords.split(',') if k.strip()]
            if not any(kw in (title or '').lower() for kw in keywords):
                return (False, set())

    # Resolve or create a Subreddit row for the source subreddit (where this post was found)
    source_sub = None
    try:
        if source_subreddit_name:
            sname = normalize(source_subreddit_name)
            # If the original name is a user profile, ensure prefix is u_
            if source_subreddit_name.strip().lower().startswith(('u/', '/u/')):
                if not sname.startswith('u_'):
                    sname = 'u_' + sname.lstrip('_')
            # Do NOT add user profiles (u_) to the subreddit table
            if is_user_profile(sname):
                source_sub = None
            else:
                source_sub = session.query(models.Subreddit).filter_by(name=sname).first()
                if not source_sub:
                    source_sub = models.Subreddit(name=sname)
                    session.add(source_sub)
                    session.commit()
                    # Refresh object to ensure ID is populated after insert
                    session.refresh(source_sub)
    except Exception:
        session.rollback()
    # If post already exists, decide whether to re-scan comments.
    # We always process posts the first time to capture their mentions.
    # On subsequent runs, we only re-scan posts within the configured lookback window
    # to catch edited comments. Older posts are skipped to avoid reprocessing a large
    # backlog (comments older than ~180 days are archived anyway).
    existing = session.query(models.Post).filter_by(reddit_post_id=reddit_id).first()
    now = datetime.utcnow()
    
    # Skip posts that are too old to initially scan (not in database yet)
    if not existing and POST_INITIAL_SCAN_DAYS is not None:
        try:
            post_created = int(data.get('created_utc') or 0)
        except Exception:
            post_created = 0
        if post_created:
            cutoff_ts = int((now - timedelta(days=POST_INITIAL_SCAN_DAYS)).timestamp())
            if post_created < cutoff_ts:
                logger.debug(f"Post {reddit_id} is older than {POST_INITIAL_SCAN_DAYS} days (initial scan limit), skipping")
                return (True, set())
    
    # Skip posts that were recently scanned (helps avoid reprocessing on container restart)
    if existing and SKIP_RECENTLY_SCANNED_HOURS > 0:
        if hasattr(existing, 'last_scanned') and existing.last_scanned:
            time_since_scan = now - existing.last_scanned
            if time_since_scan < timedelta(hours=SKIP_RECENTLY_SCANNED_HOURS):
                hours_ago = time_since_scan.total_seconds() / 3600
                logger.debug(f"Post {reddit_id} was scanned {hours_ago:.1f}h ago (within {SKIP_RECENTLY_SCANNED_HOURS}h window), skipping")
                return (True, set())
    
    # Skip re-scanning existing posts that are too old
    if existing and POST_RESCAN_DAYS is not None:
        try:
            post_created = int(existing.created_utc or 0)
        except Exception:
            post_created = 0
        if post_created:
            cutoff_ts = int((now - timedelta(days=POST_RESCAN_DAYS)).timestamp())
            if post_created < cutoff_ts:
                logger.info(f"Post {reddit_id} already in DB and older than {POST_RESCAN_DAYS} days (rescan limit), skipping")
                return (True, set())
    # Otherwise, process/rescan the post below to capture mentions and detect new/edited comments

    # fetch comments first so we can determine whether any are new
    try:
        # Use distributed rate limiter for coordination
        if distributed_rate_limiter:
            distributed_rate_limiter.wait_if_needed()
        else:
            rate_limiter.wait_if_needed()
        comments_json = fetch_post_comments(reddit_id)
    except Exception as e:
        logger.exception(f"Failed to fetch comments for {reddit_id}: {e}")
        return (True, set())

    found = []
    walk_comments(comments_json, found)

    # If there are no comments at all, create the post record and move on
    if not found:
        if not existing:
            try:
                post = models.Post(reddit_post_id=reddit_id, title=title, created_utc=created_utc, url=url, original_poster=author)
                if source_sub:
                    post.subreddit_id = source_sub.id
                post.last_scanned = now
                session.add(post)
                session.commit()
                try:
                    increment_analytics(session, posts=1)
                except Exception:
                    logger.debug('Failed to increment analytics for post')
            except Exception as e:
                session.rollback()
                logger.debug(f"Post {reddit_id} already exists or insert failed: {e}")
        try:
            date_str = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d') if created_utc else 'unknown-date'
        except Exception:
            date_str = 'unknown-date'
        source_sub_str = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
        logger.info(f"Post {reddit_id} ({date_str}) (no comments found){source_sub_str}")
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
        try:
            post = models.Post(reddit_post_id=reddit_id, title=title, created_utc=created_utc, url=url, original_poster=author)
            if source_sub:
                post.subreddit_id = source_sub.id
            post.last_scanned = now
            session.add(post)
            session.commit()
            try:
                increment_analytics(session, posts=1)
            except Exception:
                logger.debug('Failed to increment analytics for post')
            source_sub_str = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
            logger.info(f"Saved post {reddit_id} ({format_ts(created_utc)}) - processing {len(missing)} new comments{source_sub_str}")
        except Exception as e:
            session.rollback()
            logger.debug(f"Post {reddit_id} already exists or insert failed: {e}")
            source_sub_str = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
            logger.info(f"Processing post {reddit_id} ({format_ts(created_utc)}) - {len(missing)} new comments{source_sub_str}")
    else:
        post = existing
        # Update subreddit_id, original_poster, and last_scanned
        if source_sub:
            post.subreddit_id = source_sub.id
        post.original_poster = author
        post.last_scanned = now
        session.add(post)
        session.commit()
        source_sub_str = f" from /r/{source_subreddit_name}" if source_subreddit_name else ""
        logger.info(f"Rescanning post {reddit_id} ({format_ts(post.created_utc)}) - {len(missing)} new, {len(edited)} edited comments{source_sub_str}")

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
            cm = models.Comment(
                reddit_comment_id=c['id'],
                post_id=post.id,
                body=c['body'],
                created_utc=int(c.get('created_utc') or 0),
                username=resolve_comment_user(c)
            )
            session.add(cm)
            session.commit()
            try:
                increment_analytics(session, comments=1)
            except Exception:
                logger.debug('Failed to increment analytics for comment')


        for sname, (raw_text, context, is_user) in subnames.items():
            # Skip user profiles and do not add them to subreddit table
            if is_user:
                logger.debug(f"Skipping user profile: /u/{sname[2:] if sname.startswith('u_') else sname}")
                continue
            if sname in ignored_subreddits:
                logger.debug(f"Skipping ignored subreddit: /r/{sname}")
                continue

            entity_label = f"/r/{sname}"
            logger.debug(f"Processing mention: {entity_label} (raw={raw_text})")

            # get or create subreddit (only for real subreddits)
            sub = session.query(models.Subreddit).filter_by(name=sname).first()
            is_new_subreddit = (sub is None)
            if not sub:
                sub = models.Subreddit(name=sname)
                session.add(sub)
                session.commit()
                logger.info(f"New subreddit discovered and added to subreddit table: /r/{sname}")
                logger.debug(f"New subreddit discovered: {entity_label}")
                try:
                    increment_analytics(session, subreddits=1)
                except Exception:
                    logger.debug('Failed to increment analytics for new subreddit')
                # mark for async metadata fetch
                discovered.add(sname)
            else:
                # Log at debug level for already-known entities to reduce spam
                logger.debug(f"Subreddit encountered: {entity_label}")
                # Always refresh metadata on discovery (schedule for immediate update)
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
                # Only log detailed mention info for existing entities (not newly discovered ones)
                if not is_new_subreddit:
                    try:
                        if updated:
                            logger.info(f"Known {'user' if is_user else 'subreddit'} mentioned: {entity_label} (comment {c.get('id')}) - first_mentioned updated from {format_ts(old_val)} to {format_ts(sub.first_mentioned)}")
                        else:
                            logger.info(f"Known {'user' if is_user else 'subreddit'} mentioned: {entity_label} (comment {c.get('id')}) - no change to first_mentioned ({format_ts(sub.first_mentioned)})")
                    except Exception:
                        # logging should not block processing
                        logger.debug(f"Mention processed for {entity_label} (comment {c.get('id')})")
            except Exception:
                session.rollback()
                logger.exception(f"Error updating first_mentioned for {entity_label}")

            # Insert mention only if it doesn't already exist
            # Check both: same comment shouldn't mention same subreddit twice
            # AND same user shouldn't mention same subreddit more than once in entire DB
            try:
                # Combine both checks into a single query for efficiency
                existing_mention = None
                if cm.username:
                    # Check both comment and user constraints in one query
                    existing_mention = session.query(models.Mention).filter(
                        (models.Mention.subreddit_id == sub.id) & (
                            (models.Mention.comment_id == cm.id) | (models.Mention.user_id == cm.username)
                        )
                    ).first()
                else:
                    # Only check comment constraint if no username
                    existing_mention = session.query(models.Mention).filter_by(subreddit_id=sub.id, comment_id=cm.id).first()
                
                if not existing_mention:
                    mention = models.Mention(
                        subreddit_id=sub.id,
                        comment_id=cm.id,
                        post_id=post.id,
                        timestamp=int(c.get('created_utc') or 0),
                        user_id=cm.username
                    )
                    session.add(mention)
                    session.commit()
                    logger.debug(f"Inserted mention: {entity_label} by {cm.username} in comment {cm.reddit_comment_id}")
                    try:
                        increment_analytics(session, mentions=1)
                    except Exception:
                        logger.debug('Failed to increment analytics for mention')
                else:
                    logger.debug(f"Skipped duplicate mention: {entity_label} comment {cm.reddit_comment_id}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting mention for {entity_label}: {e}")

    # Process edited comments: update stored body and extract any newly-added subreddit mentions
    for cm, c in edited:
        try:
            fetched_body = c.get('body') or ''
            # Update stored comment body and metadata
            try:
                cm.body = fetched_body
                cm.username = resolve_comment_user(c) or cm.username
                cm.created_utc = int(c.get('created_utc') or cm.created_utc or 0)
                session.add(cm)
                session.commit()
            except Exception:
                session.rollback()

            subnames = extract_subreddits_from_text(fetched_body)
            if not subnames:
                continue

            for sname, (raw_text, context, is_user) in subnames.items():
                # Skip user profiles and do not add them to subreddit table
                if is_user:
                    logger.debug(f"Skipping user profile: /u/{sname[2:] if sname.startswith('u_') else sname}")
                    continue
                if sname in ignored_subreddits:
                    continue
                entity_label = f"/r/{sname}"
                # get or create subreddit (only for real subreddits)
                sub = session.query(models.Subreddit).filter_by(name=sname).first()
                if not sub:
                    sub = models.Subreddit(name=sname)
                    session.add(sub)
                    session.commit()
                    logger.info(f"New subreddit discovered (edited comment): {entity_label}")
                    try:
                        increment_analytics(session, subreddits=1)
                    except Exception:
                        logger.debug(f"Failed to increment analytics for new subreddit")
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
                # Check both: same comment shouldn't mention same subreddit twice
                # AND same user shouldn't mention same subreddit more than once in entire DB
                try:
                    # Combine both checks into a single query for efficiency
                    existing_mention = None
                    if cm.username:
                        existing_mention = session.query(models.Mention).filter(
                            (models.Mention.subreddit_id == sub.id) & (
                                (models.Mention.comment_id == cm.id) | (models.Mention.user_id == cm.username)
                            )
                        ).first()
                    else:
                        existing_mention = session.query(models.Mention).filter_by(subreddit_id=sub.id, comment_id=cm.id).first()
                    
                    if not existing_mention:
                        mention = models.Mention(
                            subreddit_id=sub.id,
                            comment_id=cm.id,
                            post_id=post.id,
                            timestamp=int(c.get('created_utc') or 0),
                            user_id=cm.username
                        )
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
            # Update last_scanned timestamp to track when this post was last processed
            if hasattr(post, 'last_scanned'):
                post.last_scanned = now
            session.add(post)
            session.commit()
            logger.info(f"Updated post {reddit_id} unique_subreddits={uniq}")
        except Exception:
            session.rollback()
    except Exception:
        # non-fatal
        pass

    return (True, discovered)


def rescan_posts_phase(duration_seconds):
    """Rescan existing posts from the `post` table.

    Ordering: prioritize posts with `last_scanned IS NULL` first, then by
    `last_scanned` ascending (oldest first). Each selected post is passed to
    `process_post` which will fetch comments and update `last_scanned`.
    """
    logger.info(f"=== Starting Post Rescan Phase ({duration_seconds} seconds) ===")
    end_time = time.time() + float(duration_seconds or 0)
    processed_count = 0

    while time.time() < end_time:
        with Session(engine) as session:
            try:
                # Build base query for posts. If POST_RESCAN_DAYS is set, only
                # consider posts newer than that cutoff.
                posts_q = session.query(models.Post)
                if POST_RESCAN_DAYS is not None:
                    try:
                        cutoff_ts = int((datetime.utcnow() - timedelta(days=POST_RESCAN_DAYS)).timestamp())
                        posts_q = posts_q.filter(models.Post.created_utc >= cutoff_ts)
                    except Exception:
                        pass

                # Order so that never-scanned (last_scanned IS NULL) come first,
                # then older last_scanned values before newer ones.
                try:
                    candidate = posts_q.order_by(models.Post.last_scanned.asc().nullsfirst()).first()
                except Exception:
                    candidate = posts_q.order_by(models.Post.last_scanned.asc()).first()

                if not candidate:
                    logger.info('No posts found to rescan. Post rescan phase complete.')
                    break

                # Prepare a minimal reddit-style post_item for process_post
                post_item = {
                    'data': {
                        'id': candidate.reddit_post_id,
                        'title': candidate.title,
                        'created_utc': candidate.created_utc,
                        'permalink': candidate.url,
                        'author': candidate.original_poster
                    }
                }

                # Load ignored lists so we can pass them through
                _, ignored_subreddits, ignored_users = load_scan_config_from_db(session)

                try:
                    processed, discovered = process_post(post_item, session, source_subreddit_name=None, require_fap_friday=False, ignored_subreddits=ignored_subreddits, ignored_users=ignored_users)
                    if processed:
                        processed_count += 1
                except Exception as e:
                    session.rollback()
                    logger.exception(f'Error rescanning post {candidate.reddit_post_id}: {e}')

            except Exception:
                session.rollback()
                logger.exception('Error selecting candidate post for rescan')

    logger.info(f'Post rescan phase complete; processed {processed_count} posts')
    return


def update_subreddit_metadata(session: Session, sub: models.Subreddit):
    # Always attempt to refresh metadata when called. This scanner configuration
    # updates subreddit metadata immediately after discovery, so do not rely on
    # any time-based caching logic here.
    
    # Determine display label for logging
    is_user = is_user_profile(sub.name)
    if is_user:
        username = sub.name[2:] if sub.name.startswith('u_') else sub.name
        entity_label = f"/u/{username}"
    else:
        entity_label = f"/r/{sub.name}"
    
    try:
        r = fetch_sub_about(sub.name)
        if r.status_code == 200:
            # successful fetch — mark subreddit as found
            sub.subreddit_found = True
            payload = r.json()
            # some responses may return top-level 'reason' for banned communities
            if isinstance(payload, dict) and payload.get('reason'):
                sub.is_banned = True
                sub.ban_reason = str(payload.get('reason'))
            data = payload.get('data', {}) if isinstance(payload, dict) else {}
            # Check subreddit_type for private/restricted/gold_restricted
            subtype = data.get('subreddit_type', '')
            if subtype in ('private', 'gold_restricted', 'employees_only', 'gold_only'):
                sub.is_banned = True
            # save a broad set of metadata fields
            try:
                display_name = data.get('display_name')
                if display_name is not None:
                    sub.display_name = display_name
                elif sub.display_name is None:
                    sub.display_name = ''
                    
                display_name_prefixed = data.get('display_name_prefixed')
                if display_name_prefixed is not None:
                    sub.display_name_prefixed = display_name_prefixed
                elif sub.display_name_prefixed is None:
                    sub.display_name_prefixed = ''
                    
                title = data.get('title')
                if title is not None:
                    sub.title = title
                elif sub.title is None:
                    sub.title = ''
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

            # Store Reddit's public_description field (use empty string if None to avoid infinite retry loop)
            try:
                public_desc = data.get('public_description')
                if public_desc is not None:
                    sub.description = public_desc
                elif sub.description is None:
                    # Set to empty string if Reddit returns None and we don't have a value
                    sub.description = ''
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
                    sub.is_over18 = bool(ov)
            except Exception:
                pass
            # Only set is_banned=False if we haven't detected it as banned above
            if not sub.is_banned:
                sub.is_banned = False
            try:
                logger.info(f"Updated metadata for {entity_label}: display_name='{sub.display_name}', subscribers={sub.subscribers}")
            except Exception:
                logger.debug(f"Metadata updated for {entity_label} (logging failed)")
        elif 300 <= r.status_code < 400:
            # treat redirects as 'not found' for our purposes
            sub.subreddit_found = False
            sub.is_banned = False
            try:
                payload = r.json()
                if isinstance(payload, dict) and payload.get('reason'):
                    sub.ban_reason = str(payload.get('reason'))
            except Exception:
                pass
            try:
                logger.info(f"{entity_label} returned redirect ({r.status_code}); marked not_found")
            except Exception:
                pass
        elif r.status_code in (403, 404):
            # Distinguish between forbidden (403) and not found (404).
            if r.status_code == 403:
                sub.is_banned = True
                sub.subreddit_found = True
            else:
                # 404 -> subreddit does not exist; mark subreddit_found=False so UI can hide it
                sub.subreddit_found = False
                sub.is_banned = False
            # if response body includes reason, save it
            try:
                payload = r.json()
                if isinstance(payload, dict) and payload.get('reason'):
                    sub.ban_reason = str(payload.get('reason'))
            except Exception:
                pass
            try:
                logger.info(f"{entity_label} returned {r.status_code}; is_banned={sub.is_banned}, subreddit_found={sub.subreddit_found}")
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
                    # fallback: schedule a conservative retry window (e.g., 10 * API_RATE_DELAY_SECONDS)
                    sub.next_retry_at = datetime.utcnow() + timedelta(seconds=max(30, int(API_RATE_DELAY_SECONDS * 10)))
                # Increase retry_priority so top-listed subreddits are retried earlier
                try:
                    sub.retry_priority = int(sub.retry_priority or 0) + 1
                except Exception:
                    sub.retry_priority = 1
                logger.warning(f"{entity_label} rate-limited; scheduling next_retry_at={sub.next_retry_at} (Retry-After={ra})")
            except Exception:
                logger.exception(f"Failed to schedule retry for {entity_label} after 429")
        else:
            logger.warning(f"Unexpected status {r.status_code} for {entity_label}")
            try:
                logger.info(f"{entity_label} unexpected status {r.status_code}")
            except Exception:
                pass
    except Exception as e:
        logger.exception(f"Error fetching about for {entity_label}: {e}")
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
                logger.info(f"Recorded last_checked and committed metadata for {entity_label}")
            except Exception:
                pass
        except Exception:
            session.rollback()


# metadata_worker removed: metadata is fetched synchronously during discovery


def refresh_metadata_phase(duration_seconds):
    """
    Run metadata refresh for up to duration_seconds.
    Priority 1: Subreddits NEVER scanned (last_checked is null), ordered by first_mentioned (oldest first)
    Priority 2: Subreddits missing ANY metadata (title, subscribers, or description), ordered by first_mentioned (oldest first)
    Priority 3: Subreddits with stale metadata (configured by METADATA_STALE_HOURS), ordered by last_checked (oldest first)
    Priority 4: Not-found subreddits re-checked every 7 days (they may have been created since)
    Note: Banned subreddits are never re-checked as bans are permanent.
    Updates last_checked timestamp after each refresh.
    """
    logger.info(f"=== Starting Metadata Refresh Phase ({duration_seconds} seconds) ===")
    logger.info(f"Priority: 1) Never scanned, 2) Missing any metadata, 3) Stale metadata >{METADATA_STALE_HOURS}h, 4) Not-found subreddits every 7d")
    
    # Count subreddits in each priority at start
    with Session(engine) as session:
        cutoff_24h = datetime.utcnow() - timedelta(hours=24)
        
        never_scanned = session.query(models.Subreddit).filter(
            models.Subreddit.last_checked == None,
            models.Subreddit.is_banned == False,
            models.Subreddit.subreddit_found != False
        ).count()
        
        missing_metadata = session.query(models.Subreddit).filter(
            models.Subreddit.last_checked != None,
            models.Subreddit.is_banned == False,
            models.Subreddit.subreddit_found != False,
            (
                (models.Subreddit.title == None) |
                (models.Subreddit.subscribers == None) |
                (models.Subreddit.description == None)
            )
        ).count()
        
        logger.info(f"Never scanned: {never_scanned}, Missing metadata: {missing_metadata}")
    
    start_time = time.time()
    end_time = start_time + duration_seconds
    refreshed_count = 0
    
    while time.time() < end_time:
        with Session(engine) as session:
            cutoff_24h = datetime.utcnow() - timedelta(hours=METADATA_STALE_HOURS)
            
            # Priority 1: Subreddits NEVER scanned (last_checked is null)
            # Ordered by first_mentioned (oldest discoveries first)
            subreddit_to_refresh = session.query(models.Subreddit).filter(
                models.Subreddit.last_checked == None,
                models.Subreddit.is_banned == False,
                models.Subreddit.subreddit_found != False
            ).order_by(models.Subreddit.first_mentioned.asc()).first()
            
            priority_level = None
            priority_desc = ""
            if subreddit_to_refresh:
                priority_level = 1
                priority_desc = "Never scanned"
            
            # Priority 2: Subreddits missing ANY metadata (title, subscribers, or description)
            # Only check for NULL - empty strings mean we successfully fetched the data
            if not subreddit_to_refresh:
                subreddit_to_refresh = session.query(models.Subreddit).filter(
                    models.Subreddit.last_checked != None,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.subreddit_found != False,
                    (
                        (models.Subreddit.title == None) |
                        (models.Subreddit.subscribers == None) |
                        (models.Subreddit.description == None)
                    )
                ).order_by(models.Subreddit.first_mentioned.asc()).first()
                if subreddit_to_refresh:
                    priority_level = 2
                    priority_desc = "Missing metadata"
            
            # Priority 3: Subreddits with stale metadata (configured by METADATA_STALE_HOURS)
            # Only for subreddits that have metadata (non-NULL, empty strings are ok)
            if not subreddit_to_refresh:
                subreddit_to_refresh = session.query(models.Subreddit).filter(
                    models.Subreddit.title != None,
                    models.Subreddit.subscribers != None,
                    models.Subreddit.description != None,
                    models.Subreddit.last_checked != None,
                    models.Subreddit.last_checked < cutoff_24h,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.subreddit_found != False
                ).order_by(models.Subreddit.last_checked.asc()).first()
                if subreddit_to_refresh:
                    priority_level = 3
                    priority_desc = f"Stale metadata >{METADATA_STALE_HOURS}h"
            
            # Priority 4: Re-check "not found" subreddits every 7 days (they may have been created)
            # Banned subreddits are never re-checked as bans are permanent
            if not subreddit_to_refresh:
                cutoff_7d = datetime.utcnow() - timedelta(days=7)
                subreddit_to_refresh = session.query(models.Subreddit).filter(
                    models.Subreddit.subreddit_found == False,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.last_checked != None,
                    models.Subreddit.last_checked < cutoff_7d
                ).order_by(models.Subreddit.last_checked.asc()).first()
                if subreddit_to_refresh:
                    priority_level = 4
                    priority_desc = "Not found recheck"
            
            # If no subreddits need refresh, we're done
            if not subreddit_to_refresh:
                logger.info("No subreddits require metadata refresh. Metadata phase complete.")
                break
            
            # Refresh this subreddit's metadata
            sub_name = subreddit_to_refresh.name
            priority_msg = f" [Priority {priority_level}: {priority_desc}]" if priority_level else ""
            
            # Show remaining counts for Priority 1 and 2
            remaining_msg = ""
            if priority_level == 1:
                remaining_count = session.query(models.Subreddit).filter(
                    models.Subreddit.last_checked == None,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.subreddit_found != False
                ).count()
                remaining_msg = f" [{remaining_count} never scanned remaining]"
            elif priority_level == 2:
                remaining_count = session.query(models.Subreddit).filter(
                    models.Subreddit.last_checked != None,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.subreddit_found != False,
                    (
                        (models.Subreddit.title == None) |
                        (models.Subreddit.subscribers == None) |
                        (models.Subreddit.description == None)
                    )
                ).count()
                remaining_msg = f" [{remaining_count} missing metadata remaining]"
            elif priority_level == 3:
                remaining_count = session.query(models.Subreddit).filter(
                    models.Subreddit.title != None,
                    models.Subreddit.subscribers != None,
                    models.Subreddit.description != None,
                    models.Subreddit.last_checked != None,
                    models.Subreddit.last_checked < cutoff_24h,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.subreddit_found != False
                ).count()
                remaining_msg = f" [{remaining_count} stale metadata remaining]"
            elif priority_level == 4:
                cutoff_7d = datetime.utcnow() - timedelta(days=7)
                remaining_count = session.query(models.Subreddit).filter(
                    models.Subreddit.subreddit_found == False,
                    models.Subreddit.is_banned == False,
                    models.Subreddit.last_checked != None,
                    models.Subreddit.last_checked < cutoff_7d
                ).count()
                remaining_msg = f" [{remaining_count} not found recheck remaining]"
            
            logger.info(f"Refreshing metadata for /r/{sub_name}{priority_msg}{remaining_msg} ({refreshed_count + 1} processed)")
            
            # Use the rate limiter before making the API call
            if distributed_rate_limiter:
                distributed_rate_limiter.wait_if_needed()
            else:
                rate_limiter.wait_if_needed()
            
            # Fetch metadata using the existing update_subreddit_metadata function
            update_subreddit_metadata(session, subreddit_to_refresh)
            
            # Record the API call
            if distributed_rate_limiter:
                distributed_rate_limiter.record_api_call()
            else:
                rate_limiter.record_call()
            
            refreshed_count += 1
            
            # Commit after each refresh
            try:
                session.commit()
            except Exception:
                session.rollback()
                logger.exception(f"Failed to commit metadata for /r/{sub_name}")
    
    elapsed = time.time() - start_time
    
    # Final counts for Priority 1 and 2
    with Session(engine) as session:
        final_never_scanned = session.query(models.Subreddit).filter(
            models.Subreddit.last_checked == None,
            models.Subreddit.is_banned == False,
            models.Subreddit.subreddit_found != False
        ).count()
        
        final_missing = session.query(models.Subreddit).filter(
            models.Subreddit.last_checked != None,
            models.Subreddit.is_banned == False,
            models.Subreddit.subreddit_found != False,
            (
                (models.Subreddit.title == None) |
                (models.Subreddit.subscribers == None) |
                (models.Subreddit.description == None)
            )
        ).count()
        
        logger.info(f"=== Metadata Refresh Phase Complete: {refreshed_count} subreddits refreshed in {elapsed/3600:.2f} hours ===")
        if final_never_scanned > 0 or final_missing > 0:
            logger.info(f"Remaining: {final_never_scanned} never scanned, {final_missing} missing metadata (will be prioritized in next refresh)")
        else:
            logger.info("All subreddits scanned with complete metadata!")


def check_scan_subreddits_availability():
    """Check that all subreddits in scan configuration are available (not banned/not found).
    Updates metadata for scan subreddits if they don't exist in database yet.
    """
    logger.info("Checking availability of scan configuration subreddits...")
    
    with Session(engine) as session:
        scan_configs, _, _ = load_scan_config_from_db(session)
        
        if not scan_configs:
            logger.warning("No scan configuration found. Skipping availability check.")
            return
        
        unavailable = []
        
        for subname in scan_configs.keys():
            is_user = is_user_profile(subname)
            if is_user:
                username = subname[2:] if subname.startswith('u_') else subname
                entity_label = f"/u/{username}"
                # Do not create or update Subreddit rows for user profiles
                logger.info(f"Skipping user profile scan config: {entity_label} (not a real subreddit)")
                continue
            else:
                entity_label = f"/r/{subname}"

            # Get or create subreddit record
            sub = session.query(models.Subreddit).filter(
                func.lower(models.Subreddit.name) == subname.lower()
            ).first()
            if not sub:
                logger.info(f"Scan subreddit {entity_label} not in database, creating and fetching metadata...")
                sub = models.Subreddit(name=subname.lower())
                session.add(sub)
                session.flush()

            # Check if we need to fetch/update metadata
            if sub.title is None or sub.subreddit_found is None:
                logger.info(f"Fetching metadata for scan subreddit {entity_label}...")
                if distributed_rate_limiter:
                    distributed_rate_limiter.wait_if_needed()
                else:
                    rate_limiter.wait_if_needed()

                update_subreddit_metadata(session, sub)

                if distributed_rate_limiter:
                    distributed_rate_limiter.record_api_call()
                else:
                    rate_limiter.record_call()

                try:
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception(f"Failed to commit metadata for {entity_label}")

            # Check availability
            if sub.is_banned or sub.subreddit_found == False:
                status = "banned" if sub.is_banned else "not found"
                unavailable.append(f"{entity_label} ({status})")
                logger.warning(f"Scan subreddit {entity_label} is {status}!")
        
        if unavailable:
            logger.error(f"WARNING: {len(unavailable)} scan entity/entities are unavailable: {', '.join(unavailable)}")
            logger.error("Scanner will continue but may not discover new mentions from these entities.")
        else:
            logger.info(f"All {len(scan_configs)} scan entities (subreddits/users) are available.")


def main_loop():
    logger.info("main_loop() called")
    with temp_phase('Startup'):
        wait_for_db_startup()
        logger.info("wait_for_db_startup() complete")
        ensure_tables()
        logger.info("ensure_tables() complete")
    # Phase 1: Startup (DB, rate limiter, initial configs, optional prefetch)
    logger.info("Initializing rate limiter and loading initial scan configs")
    try:
        with Session(engine) as session:
            initial_scan_configs, initial_ignored_subreddits, initial_ignored_users = load_scan_config_from_db(session)
    except Exception:
        initial_scan_configs = {}
        initial_ignored_subreddits = set()
        initial_ignored_users = set()

    # Optionally prefetch high-value metadata at startup
    if SCAN_FOR_METADATA_FIRST:
        logger.info(f"SCAN_FOR_METADATA_FIRST enabled. Running startup metadata prefetch for {METADATA_REFRESH_SECONDS} seconds...")
        startup_metadata_prefetch()
        logger.info("Startup metadata prefetch complete")
    else:
        logger.info("Skipping startup metadata prefetch")

    # Phase 2: Check availability of scan subreddits configured in DB
    check_scan_subreddits_availability()

    # Enter main loop that follows the optimal phase ordering
    while True:
        scan_start_time = time.time()
        mentions_before = 0
        try:
            # Phase 2 (repeated each iteration): Load Scan Configs
            with temp_phase('Load Subreddit Scan Configs'):
                with Session(engine) as session:
                    scan_configs, ignored_subreddits, ignored_users = load_scan_config_from_db(session)

            # Phase 3: Primary Scan Loop (Scan Targets)
            if scan_configs:
                discovered_overall = set()
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

                    sorted_configs = sorted(scan_configs.items(), key=lambda x: x[1].get('priority', 3))

                    for subname, config in sorted_configs:
                        allowed_users = config['allowed_users']
                        nsfw_only = config['nsfw_only']
                        priority = config.get('priority', 3)

                        is_user = is_user_profile(subname)
                        if is_user:
                            username = subname[2:] if subname.startswith('u_') else subname
                            entity_label = f"/u/{username}"
                        else:
                            entity_label = f"/r/{subname}"

                        subreddit_processed_count = 0
                        after_sub = None
                        prev_after_sub = None
                        # Scan this target with its priority shown in phase
                        with temp_phase(f"Scan Targets (priority {priority})"):
                            while True:
                                try:
                                    # Rate limiting applies globally across phases
                                    logger.debug(f"Paging state before fetch: prev_after={prev_after_sub}, after_sub={after_sub}")
                                    logger.info(f"Preparing to fetch posts for {entity_label} (after={after_sub})")
                                    if distributed_rate_limiter:
                                        distributed_rate_limiter.wait_if_needed()
                                    else:
                                        rate_limiter.wait_if_needed()
                                    logger.info(f"Calling Reddit to fetch posts for {entity_label}")
                                    data = fetch_subreddit_posts(subname, after_sub)
                                    logger.info(f"Fetch complete for {entity_label}")
                                    # Log number of children and after cursor for visibility
                                    try:
                                        children_count = len(data.get('data', {}).get('children', []) or [])
                                    except Exception:
                                        children_count = 'unknown'
                                        current_after = data.get('data', {}).get('after')
                                    logger.info(f"Fetched {children_count} posts; after={current_after}")
                                    logger.debug(f"Paging state after fetch: prev_after={prev_after_sub}, current_after={current_after}")
                                        # Update paging cursor immediately so next iteration uses it
                                        if current_after == prev_after_sub:
                                            logger.warning(f"No progress paging {entity_label}; after cursor unchanged ({current_after}). Breaking to avoid loop.")
                                            break
                                        prev_after_sub = current_after
                                        after_sub = current_after
                                except Exception as e:
                                    error_str = str(e)
                                    error_type = type(e).__name__
                                    if '429' in error_str:
                                        logger.warning(f"Rate limited on {entity_label}: {error_type}: {error_str} - retrying after wait")
                                        continue
                                    else:
                                        logger.warning(f"Exception fetching {entity_label} (type={error_type}): {error_str}")
                                        logger.exception(f"Full traceback for {entity_label}")
                                    break

                            children = data.get('data', {}).get('children', [])
                            if not children:
                                break
                            if not after_sub:
                                logger.info(f"Scanning new posts from {entity_label} (priority {priority})")

                            for p in children:
                                pdata = p.get('data', {})
                                if nsfw_only:
                                    over18 = bool(pdata.get('over_18') or pdata.get('over18'))
                                    if not over18:
                                        continue
                                if allowed_users is not None:
                                    author = (pdata.get('author') or '').strip().lower()
                                    if author not in allowed_users:
                                        continue

                                # Mark we're processing an individual post
                                with temp_phase('Process Post'):
                                    processed, discovered = process_post(p, session, source_subreddit_name=subname, require_fap_friday=False, ignored_subreddits=ignored_subreddits, ignored_users=ignored_users)
                                if processed:
                                    subreddit_processed_count += 1
                                if discovered:
                                    discovered_overall.update(discovered)
                                if TEST_MAX_POSTS_PER_SUBREDDIT and subreddit_processed_count >= TEST_MAX_POSTS_PER_SUBREDDIT:
                                    logger.info(f"Reached TEST_MAX_POSTS_PER_SUBREDDIT={TEST_MAX_POSTS_PER_SUBREDDIT} for {entity_label}, moving to next subreddit.")
                                    break

                            # after_sub already updated after fetch; break if no more pages
                            if not after_sub or (TEST_MAX_POSTS_PER_SUBREDDIT and subreddit_processed_count >= TEST_MAX_POSTS_PER_SUBREDDIT):
                                break

                # Phase 5: Immediate Metadata Discovery (fetch metadata for discovered subs)
                if discovered_overall:
                    with temp_phase('Immediate Discovery Metadata'):
                        logger.info(f"Discovered {len(discovered_overall)} new subreddits during scan")
                        for sname in discovered_overall:
                            with Session(engine) as meta_session:
                                sub = meta_session.query(models.Subreddit).filter(models.Subreddit.name == sname.lower()).first()
                                if sub:
                                    if distributed_rate_limiter:
                                        distributed_rate_limiter.wait_if_needed()
                                    else:
                                        rate_limiter.wait_if_needed()
                                    update_subreddit_metadata(meta_session, sub)
                                    try:
                                        if distributed_rate_limiter:
                                            distributed_rate_limiter.record_api_call()
                                        else:
                                            rate_limiter.record_call()
                                    except Exception:
                                        pass
                                    try:
                                        meta_session.commit()
                                    except Exception:
                                        meta_session.rollback()

                # Phase 6: Metadata Refresh Phase
                with temp_phase('Metadata Refresh Phase'):
                    logger.info(f'Scan complete. Starting metadata refresh phase for {METADATA_REFRESH_SECONDS} seconds...')
                    refresh_metadata_phase(METADATA_REFRESH_SECONDS)

                # Phase 7: Post Rescan Phase
                try:
                    with temp_phase('Post Rescan Phase'):
                        logger.info(f'Metadata refresh complete. Starting post rescan phase for {POST_RESCAN_DURATION} seconds...')
                        rescan_posts_phase(POST_RESCAN_DURATION)
                except Exception:
                    logger.exception('Post rescan phase failed')

                # Record scan completion metrics
                with Session(engine) as session:
                    try:
                        analytics = session.query(models.Analytics).first()
                        if analytics:
                            new_mentions = (analytics.total_mentions or 0) - mentions_before
                            record_scan_completion(session, scan_start_time, new_mentions)
                    except Exception:
                        logger.debug('Failed to record scan completion')

                # Pause before next iteration
                logger.info(f'Sleeping {SCAN_SLEEP_SECONDS} seconds before next scan iteration.')
                time.sleep(SCAN_SLEEP_SECONDS)
            else:
                # Phase 8: Idle Mode - continuous metadata refresh when no scan configs
                with temp_phase('Idle Mode'):
                    logger.warning('No subreddit scan configs found in database. Running continuous metadata refresh.')
                    refresh_metadata_phase(METADATA_REFRESH_SECONDS)
                    logger.info('Idle metadata refresh complete. Sleeping 10 minutes before checking for scan configs again.')
                    time.sleep(600)
        except Exception as e:
            logger.exception(f"Scanner main loop error: {e}")
            time.sleep(60)


if __name__ == '__main__':
    main_loop()
