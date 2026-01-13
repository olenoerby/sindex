import os
import re
import time
import json
import logging
# file-based rotating logs removed; rely on container stdout/stderr
from datetime import datetime, timedelta
import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'api'))
import models

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
# Number of days to consider subreddit metadata fresh before re-fetching from Reddit.
# Can be set via `SUBREDDIT_META_CACHE_DAYS`; falls back to legacy `META_CACHE_DAYS` if present.
SUBREDDIT_META_CACHE_DAYS = int(os.getenv('SUBREDDIT_META_CACHE_DAYS') or os.getenv('META_CACHE_DAYS') or '7')
# Optional testing controls:
# If set, scanner will only process up to this many Friday posts and then exit.
TEST_POST_LIMIT = int(os.getenv('TEST_POST_LIMIT')) if os.getenv('TEST_POST_LIMIT') else None
TEST_POST_IDS = [p.strip() for p in os.getenv('TEST_POST_IDS', '').split(',') if p.strip()]

engine = create_engine(DATABASE_URL, future=True)

# Patterns for subreddit mentions. Accepts r/name, /r/name and reddit url forms.
RE_SUB = re.compile(r"(?:/r/|\br/|https?://(?:www\.)?reddit\.com/r/)([A-Za-z0-9_]{3,21})")

def normalize(name: str) -> str:
    return name.lower().strip().lstrip('/').lstrip('r/').replace('\n','')


# Comma-separated list of subreddit names to ignore. Defaults include a couple examples.
# Set `IGNORE_SUBREDDITS` in the environment to override (comma-separated).
IGNORE_SUBREDDITS = set(
    normalize(s) for s in os.getenv('IGNORE_SUBREDDITS', 'wowthissubexists,sneakpeekbot').split(',') if s.strip()
)


def ensure_tables():
    models.Base.metadata.create_all(engine)
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


def fetch_user_posts(after: str = None):
    # Calls Reddit public endpoint for user submissions
    url = f"https://www.reddit.com/user/{REDDIT_USER}/submitted.json?limit=100"
    if after:
        url += f"&after={after}"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    r = httpx.get(url, headers=headers)
    time.sleep(API_RATE_DELAY)
    r.raise_for_status()
    return r.json()


def fetch_post_comments(post_id: str):
    url = f"https://www.reddit.com/comments/{post_id}.json?limit=500"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    r = httpx.get(url, headers=headers)
    time.sleep(API_RATE_DELAY)
    r.raise_for_status()
    return r.json()


def fetch_sub_about(name: str):
    url = f"https://www.reddit.com/r/{name}/about.json"
    headers = {"User-Agent": "PineappleIndexBot/0.1 (by /u/yourbot)"}
    r = httpx.get(url, headers=headers)
    time.sleep(API_RATE_DELAY)
    return r


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


def process_post(post_item, session: Session):
    """Process a single reddit post item. Returns True if a Fap Friday post was processed (saved and scanned), False otherwise."""
    data = post_item['data']
    reddit_id = data.get('id')
    title = data.get('title')
    created_utc = int(data.get('created_utc') or 0)
    url = data.get('permalink')
    # Only consider Fap Friday posts by title
    if 'fap friday' not in (title or '').lower():
        return False
    # If post already exists, don't re-scan it here
    existing = session.query(models.Post).filter_by(reddit_post_id=reddit_id).first()
    if existing:
        logger.info(f"Post {reddit_id} already in DB, skipping comments")
        return True

    # fetch comments first so we can determine whether any are new
    try:
        comments_json = fetch_post_comments(reddit_id)
    except Exception as e:
        logger.exception(f"Failed to fetch comments for {reddit_id}: {e}")
        return True

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
        logger.info(f"Saved post {reddit_id} ({date_str}) (no comments found)")
        try:
            increment_analytics(session, posts=1)
        except Exception:
            logger.debug('Failed to increment analytics for post')
        return True

    # Determine which comments are not yet stored
    missing = []
    for c in found:
        if not session.query(models.Comment).filter_by(reddit_comment_id=c['id']).first():
            missing.append(c)

    # If all comments are already scanned, skip this post entirely
    if not missing:
        logger.info(f"All comments for post {reddit_id} already scanned, skipping post")
        return True

    # At least one new comment exists — create the post and persist only missing comments
    post = models.Post(reddit_post_id=reddit_id, title=title, created_utc=created_utc, url=url)
    session.add(post)
    session.commit()
    try:
        date_str = datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d') if created_utc else 'unknown-date'
    except Exception:
        date_str = 'unknown-date'
    logger.info(f"Saved post {reddit_id} ({date_str}) - processing {len(missing)} new comments")
    try:
        increment_analytics(session, posts=1)
    except Exception:
        logger.debug('Failed to increment analytics for post')

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
                # fetch metadata synchronously for discovery (rate-limited respected inside fetch)
                try:
                    update_subreddit_metadata(session, sub)
                except Exception:
                    session.rollback()
            else:
                # Log existence so operator sees when we encounter already-known subreddits
                try:
                    logger.info(f"Existing subreddit encountered: /r/{sname}")
                except Exception:
                    logger.debug(f"Encountered /r/{sname} (logging failed)")
                # ensure metadata is refreshed on discovery if stale
                try:
                    update_subreddit_metadata(session, sub)
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
                        logger.info(f"Known subreddit mentioned: /r/{sname} (comment {c.get('id')}) - first_mentioned updated from {old_val} to {sub.first_mentioned}")
                    else:
                        logger.info(f"Known subreddit mentioned: /r/{sname} (comment {c.get('id')}) - no change to first_mentioned")
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
                            mention = models.Mention(subreddit_id=sub.id, comment_id=cm.id, post_id=post.id, timestamp=int(c.get('created_utc') or 0), user_id=cm.user_id)
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
    return True


def update_subreddit_metadata(session: Session, sub: models.Subreddit):
    # Only refresh if last_checked older than SUBREDDIT_META_CACHE_DAYS
    # Note: DB may set `last_checked` via server_default on insert even though
    # we haven't fetched metadata yet. If the subreddit record lacks any
    # meaningful metadata, proceed to fetch regardless of a recent last_checked.
    now = datetime.utcnow()
    if sub.last_checked and (now - sub.last_checked) < timedelta(days=SUBREDDIT_META_CACHE_DAYS):
        if sub.display_name or sub.display_name_prefixed or sub.title or sub.public_description_html or sub.subscribers:
            return
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
        elif r.status_code in (403, 404):
            sub.is_banned = True
            sub.not_found = False
            # if response body includes reason, save it
            try:
                payload = r.json()
                if isinstance(payload, dict) and payload.get('reason'):
                    sub.ban_reason = str(payload.get('reason'))
            except Exception:
                pass
        else:
            logger.warning(f"Unexpected status {r.status_code} for /r/{sub.name}")
    except Exception as e:
        logger.exception(f"Error fetching about for /r/{sub.name}: {e}")
    finally:
        session.add(sub)
        session.commit()


# metadata_worker removed: metadata is fetched synchronously during discovery


def main_loop():
    ensure_tables()
    logger.info("Starting scanner main loop")
    after = None
    processed_count = 0
    while True:
        try:
            data = fetch_user_posts(after)
            children = data.get('data', {}).get('children', [])
            if not children:
                logger.info('No posts found, sleeping for 10 minutes')
                time.sleep(600)
                continue
            with Session(engine) as session:
                for p in children:
                    # If TEST_POST_IDS is set, skip posts not in the list
                    pid = p.get('data', {}).get('id')
                    if TEST_POST_IDS and pid not in TEST_POST_IDS:
                        continue
                    processed = process_post(p, session)
                    if processed:
                        processed_count += 1
                    # If TEST_POST_LIMIT is set, exit once we've processed that many Friday posts
                    if TEST_POST_LIMIT and processed_count >= TEST_POST_LIMIT:
                        logger.info(f"Reached TEST_POST_LIMIT={TEST_POST_LIMIT}, exiting.")
                        return
                # metadata updates are handled by the background metadata worker
            # pagination
            after = data.get('data', {}).get('after')
            if not after:
                logger.info('Reached end of user posts. Sleeping for 6 hours.')
                time.sleep(6 * 3600)
        except Exception as e:
            logger.exception(f"Scanner main loop error: {e}")
            time.sleep(60)


if __name__ == '__main__':
    main_loop()
