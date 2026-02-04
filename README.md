# Pineapple Subreddit Index

A read-only index of NSFW subreddit mentions. It watches a small set of Reddit sources (a user account and a few subreddits), parses public comments for referenced subreddit names, refreshes metadata from Reddit, and serves a searchable web UI/API. No posting, voting, or private data access.

## Features
- Continuous scanning of target posts/subreddits for subreddit mentions
- Mention aggregation with timestamps and de-duplication
- Metadata refresh (title, subscribers, description, over18/banned/not_found) with retry scheduling on rate limits
- Search/sort/filter UI (mentions, subscribers, first-seen windows, availability, NSFW flags)
- Read-only FastAPI backend for the frontend
- Dockerized: scanner, API, PostgreSQL, and static Nginx frontend

## Architecture
- **scanner/**: Python worker that fetches posts/comments, extracts subreddit mentions, stores mentions, and refreshes subreddit metadata on a schedule.
- **api/**: FastAPI service exposing read-only endpoints (`/subreddits`, `/stats`, etc.).
- **nginx/html/**: Static UI consuming the API.
- **db**: PostgreSQL (docker-compose service) holding subreddits, mentions, posts, and analytics.

## Data Model (summary)
- **posts**: reddit_post_id, title, created_utc, url
- **comments**: reddit_comment_id, post_id, created_utc, user_id
- **subreddits**: name, title, description, subscribers, active_users, created_utc, first_mentioned, last_checked, is_banned/not_found/over18
- **mentions**: subreddit_id, comment_id, post_id, timestamp, source_subreddit_id, user_id (dedupe), unique constraints on (subreddit_id, comment_id) and (subreddit_id, user_id)
- **analytics**: totals plus last_scan_started/duration/new_mentions

## Scanner Behavior
- Targets: specific user posts (e.g., /u/WeirdPineapple) and configured subreddits (e.g., wowthissubexists).
- Parses comments for `/r/name` patterns (3–21 chars, strips invalid/banned patterns).
- Inserts mentions if not already recorded; updates first_mentioned; schedules metadata refresh.
- Respects low-rate usage (non-OAuth public endpoints) and handles 429 Retry-After.
- Idle loop refreshes metadata for missing fields first, then stale (>24h), ordered by mention count.

## Running Locally
Prereqs: Docker and docker-compose.

```sh
docker-compose build
docker-compose up -d
```
Services: `api`, `scanner`, `db`, `nginx` (static UI).

### Environment
Basic settings in `.env`:
- `DATABASE_URL` (Postgres DSN)
- `API_RATE_DELAY` (delay between Reddit API calls, default 7s)
- `HTTP_REQUEST_TIMEOUT`, `SUBABOUT_CONCURRENCY`, etc.
- `POST_INITIAL_SCAN_DAYS` (how far back to initially scan posts; older posts are skipped; empty = no limit)
- `POST_RESCAN_DAYS` (how far back to re-check existing posts for new comments; 0 = no rescanning, empty = rescan all)
- `SKIP_RECENTLY_SCANNED_HOURS` (skip posts scanned within X hours; useful for container restarts; 0 = disabled, default: 0)
- `SCAN_SLEEP_SECONDS` (how many seconds to sleep between scan iterations; default: 300)

**Scan configuration is now database-driven** (no .env editing needed):
- Use the `subreddit_scan_configs`, `ignored_subreddits`, and `ignored_users` tables
- Scanner reloads configuration automatically each scan cycle
- See [Configuration](#configuration) section below

## API (brief)
- `GET /subreddits` – paginated, filters (mentions/subscribers ranges, availability, NSFW), sorting.
- `GET /stats` – totals plus last_scan_started/duration/new_mentions.
- Additional params in code (`api/app.py`). All endpoints are read-only.

## Frontend
- Static UI under `nginx/html/` with multiple views:
  - **Browse** (`/browse`) - Simplified, mobile-friendly card view for casual browsing with basic filters
  - **Advanced** (`/`) - Full-featured table view with comprehensive search, sort, filters (mentions, subscribers, first-mentioned presets, availability, NSFW/SFW), and pagination
  - **Discover** (`/discover`) - Trending and notable subreddit collections
  - **Analytics** (`/analytics`) - Statistics and charts showing platform metrics
- Links open to Reddit (configurable listing target in Advanced view)

## Configuration

### Database-Driven Scan Configuration
The scanner uses database tables for configuration instead of `.env` variables. Changes take effect on the next scan cycle (no container restart needed).

**Three configuration tables:**

1. **`subreddit_scan_configs`** - Which subreddits to scan and how
2. **`ignored_subreddits`** - Subreddits to never record mentions for
3. **`ignored_users`** - Users whose mentions should not be recorded

### Managing Scan Targets

**Add a new subreddit to scan:**
```sql
-- Scan all posts from all users (NSFW only)
INSERT INTO subreddit_scan_configs (subreddit_name, allowed_users, nsfw_only, active)
VALUES ('newsubreddit', NULL, TRUE, TRUE);

-- Scan posts from specific users only
INSERT INTO subreddit_scan_configs (subreddit_name, allowed_users, nsfw_only, active)
VALUES ('anothersubreddit', 'user1,user2,user3', FALSE, TRUE);
```

**Ignore a subreddit** (mentions from this subreddit won't be recorded):
```sql
INSERT INTO ignored_subreddits (subreddit_name, active)
VALUES ('spamsubreddit', TRUE);
```

**Ignore a user** (this user's mentions won't be recorded):
```sql
INSERT INTO ignored_users (username, active)
VALUES ('spamuser', TRUE);
```

**Enable/disable configs without deleting:**
```sql
-- Temporarily disable a scan target
UPDATE subreddit_scan_configs SET active = FALSE WHERE subreddit_name = 'oldsubreddit';

-- Re-enable it later
UPDATE subreddit_scan_configs SET active = TRUE WHERE subreddit_name = 'oldsubreddit';
```

**View current configuration:**
```sql
-- Active scan targets
SELECT subreddit_name, allowed_users, nsfw_only 
FROM subreddit_scan_configs 
WHERE active = TRUE;

-- Active ignored subreddits
SELECT subreddit_name FROM ignored_subreddits WHERE active = TRUE;

-- Active ignored users
SELECT username FROM ignored_users WHERE active = TRUE;
```

### Configuration Fields Explained

**`subreddit_scan_configs` table:**
- `subreddit_name`: Name of the subreddit to scan (without /r/ prefix)
- `allowed_users`: Comma-separated usernames (e.g., `'user1,user2'`) or `NULL` for all users
- `nsfw_only`: If `TRUE`, only process posts marked NSFW; if `FALSE`, process all posts
- `active`: If `FALSE`, this config is ignored (allows temporary disable without deletion)

**`ignored_subreddits` table:**
- Mentions referencing these subreddits will NOT be recorded
- Useful for meta-subreddits or spam sources

**`ignored_users` table:**
- Mentions from these users will NOT be recorded
- Useful for bots or spam accounts

### Initial Setup

Default configuration is initialized automatically on first run. To manually initialize or reset:

```sh
docker cp initialize_scan_config.py pineapple-index-api-1:/app/
docker exec pineapple-index-api-1 python /app/initialize_scan_config.py
```

This creates default configs:
- Scan `/r/wowthissubexists` (user: WeirdPineapple, NSFW only)
- Scan `/r/nsfw411` (all users, NSFW only)
- Ignore mentions from: wowthissubexists, sneakpeekbot, nsfw411

## Database Migrations

The database schema is managed with **Alembic**. Schema changes no longer require dropping the database!

### Making Schema Changes

1. **Modify** `api/models.py` with your schema changes
2. **Generate migration** inside API container:
   ```sh
   docker exec pineapple-index-api-1 alembic revision --autogenerate -m "your description"
   ```
3. **Review** the generated file in `migrations/versions/`
4. **Deploy** - migrations run automatically when containers start:
   ```sh
   docker-compose up -d api
   ```

See [docs/MIGRATIONS.md](docs/MIGRATIONS.md) for detailed migration workflows.

## Development
- Code: Python 3.11, FastAPI, SQLAlchemy, httpx.
- Format/lint: not enforced here; keep changes minimal and clear.
- Restart services after code edits: `docker-compose build api scanner && docker-compose up -d api scanner`.
- Database schema changes use Alembic migrations (see above).

## Notes
- Uses public Reddit JSON endpoints without OAuth (keep requests modest; 429 handled with backoff).
- Designed as read-only indexing; no posting or account actions.
