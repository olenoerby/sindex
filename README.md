# Pineapple Subreddit Index

A simple, read-only index of NSFW subreddit mentions. The system watches a small set of Reddit sources (a user account and a few subreddits), finds mentions of other subreddits in public comments, keeps some basic subreddit info up to date, and provides a search UI and a read-only API. It does not post or access private data.

## What it does (short)
- Continuously watches configured Reddit sources for mentions of other subreddits.
- Collects and deduplicates mentions with timestamps.
- Keeps basic subreddit metadata (title, description, subscribers) reasonably fresh.
- Serves a simple static UI and a read-only API for browsing results.

## How it's organized
- `scanner/` — worker that discovers and records mentions.
- `api/` — read-only web service serving the UI and API.
- `nginx/html/` — static frontend files.
- `db` — PostgreSQL stores posts, comments, subreddits, and mentions.

## Quick start (local)
Prereqs: Docker and docker-compose.

```sh
docker-compose build
docker-compose up -d
```

This starts the `api`, `scanner`, `db`, and `nginx` services.

### Important settings
Most useful options are set via environment variables in `.env` or in your compose setup. Key ones:
- `DATABASE_URL` — database connection string
- `POST_INITIAL_SCAN_DAYS` — how far back to initially scan posts (empty = no limit)
- `POST_RESCAN_DAYS` — how far back to rescan existing posts (empty = rescan all)
- `SKIP_RECENTLY_SCANNED_HOURS` — skip posts scanned very recently

Configuration is primarily database-driven: the scanner reads `subreddit_scan_configs`, `ignored_subreddits`, and `ignored_users` from the DB and applies changes automatically.

## API (brief)
- `GET /subreddits` — list and filter subreddits
- `GET /stats` — basic totals and last scan stats

## Frontend
The static UI provides simple browsing and discovery views (Browse, Advanced, Discover, Analytics). Links open to Reddit.

## Scanner Phases

A brief, user-friendly overview of what the scanner does while running.

- **Startup** — set up DB connectivity and check configured scan sources. Optionally fetch some subreddit metadata to warm the cache.
- **Load scan list** — the scanner reads active scan targets from the database. Each target may restrict posts by author or NSFW flag.
- **Scan posts** — for each target the scanner fetches recent posts, filters them by configuration (NSFW, allowed users, optional keywords), and inspects comments for subreddit mentions.
- **Process posts** — new and edited comments are recorded; mentions are added unless they already exist. Posts get a `last_scanned` timestamp so the scanner knows when they were checked.
- **Metadata updates** — newly discovered subreddits are looked up and basic metadata is saved. Older metadata is refreshed on a schedule.
- **Post rescan** — the scanner can also rescan posts stored in the database; posts never-scanned are handled first, then older scans are checked before newer ones.
- **Idle behavior** — if no scan targets are configured the scanner focuses on keeping subreddit metadata fresh.

## Configuration (database-driven)
Changes to which sources are scanned are done in the database and take effect without restarting services. There are three simple tables for this:

1. `subreddit_scan_configs` — which sources to scan and how
2. `ignored_subreddits` — subreddits to ignore when recording mentions
3. `ignored_users` — users whose mentions should be ignored

You can add or modify entries with simple `INSERT`/`UPDATE` SQL commands. Examples appear below in the original README if you need them.

## Scanner Phases

A concise overview of the scanner's runtime phases and ordering decisions.

- **Startup**: initialize DB/tables, rate limiter, and perform an availability check for configured scan targets. Optionally run a short metadata prefetch to prioritise high-value subreddits.
- **Load Scan Configs**: read `subreddit_scan_configs`, `ignored_subreddits`, and `ignored_users` from the DB; each scan config includes `priority`, `allowed_users`, `nsfw_only`, and optional `keywords`.
- **Scan Targets**: iterate active scan configs ordered by `priority` (lower number = higher priority). For each target fetch recent posts (`fetch_subreddit_posts()`), apply `nsfw_only`, `allowed_users`, and per-config `keywords`, then pass posts to `process_post()`.
- **Process Post (`process_post`)**: enforces `POST_INITIAL_SCAN_DAYS`, `POST_RESCAN_DAYS`, and `SKIP_RECENTLY_SCANNED_HOURS`; fetches comments, detects new/edited comments, inserts/updates `Post`/`Comment`/`Mention` rows, updates `post.unique_subreddits`, and sets `post.last_scanned`.
- **Immediate Discovery Metadata**: when new subreddits are seen in comments, create `Subreddit` rows and update metadata immediately via `update_subreddit_metadata()` (rate-limited); `last_checked`, `next_retry_at`, and `retry_priority` are used for retry scheduling on 429s.
- **Metadata Refresh Phase**: runs after scanning and processes subreddits in this order — 1) never-scanned (`last_checked IS NULL`), 2) missing metadata (NULL fields), 3) stale metadata (`last_checked` older than `METADATA_STALE_HOURS`), 4) re-check not-found subreddits every 7 days. Each refresh updates `last_checked`.
- **Post Rescan Phase**: periodically rescan posts from the DB using `rescan_posts_phase()` (controlled by `POST_RESCAN_DURATION`). Posting order is `last_scanned IS NULL` first, then by `last_scanned` ascending (oldest first); posts are processed via `process_post()` so `last_scanned` is updated.
- **Idle Mode**: if there are no active scan configs, the scanner runs the metadata refresh loop continuously instead of fetching posts.
- **Rate Limiting & Retries**: all Reddit API access goes through `RateLimiter` or `DistributedRateLimiter`. On 429 responses the scanner schedules `next_retry_at` and increments `retry_priority` to give previously-rate-limited rows higher retry precedence.


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
