# Pineapple Subreddit Index

A simple, read-only index of NSFW subreddit mentions. It watches a few Reddit sources, finds mentions of other subreddits in public comments, keeps basic subreddit info current, and provides a small web UI and read-only API.

Quick summary:
- Watches configured Reddit sources and records subreddit mentions.
- Keeps simple metadata (title, subscribers, description) reasonably fresh.
- Exposes a small static UI and read-only API for browsing results.

Quick start (local):

```sh
docker-compose build
docker-compose up -d
```

This runs the `api`, `scanner`, `db`, and `nginx` services.

Main notes:
- Configuration is database-driven via `subreddit_scan_configs`, `ignored_subreddits`, and `ignored_users` (changes take effect without restarting).
- Useful env vars: `DATABASE_URL`, `POST_INITIAL_SCAN_DAYS`, `POST_RESCAN_DAYS`, `SKIP_RECENTLY_SCANNED_HOURS`.

Scanner phases (short):
- Startup: initialize and optionally prefetch metadata.
- Load scan list: read active scan targets from DB.
- Scan posts: fetch recent posts, apply filters, inspect comments.
- Process posts: record new/edited comments and mentions; update `last_scanned`.
- Metadata updates: fetch and refresh subreddit info.
- Post rescan: rescan stored posts (never-scanned first, then oldest scans).
- Idle: if no scan targets, keep metadata refreshed.

API endpoints (examples):
- `GET /subreddits` — list and filter subreddits
- `GET /stats` — counts and last-scan info

For CLI and SQL examples, migration details, and advanced setup, see the rest of this README below.

## Scanner Phases

A concise overview of the scanner's runtime phases and ordering decisions.

- **Startup**: initialize DB/tables, rate limiter, and perform an availability check for configured scan targets. Optionally run a short metadata prefetch to prioritise high-value subreddits.
- **Load Scan Configs**: read `subreddit_scan_configs`, `ignored_subreddits`, and `ignored_users` from the DB; each scan config includes `priority`, `allowed_users`, `nsfw_only`, and optional `keywords`.
- **Scan Targets**: iterate active scan configs ordered by `priority` (lower number = higher priority). For each target fetch recent posts (`fetch_subreddit_posts()`), apply `nsfw_only`, `allowed_users`, and per-config `keywords`, then pass posts to `process_post()`.
- **Process Post (`process_post`)**: enforces `POST_INITIAL_SCAN_DAYS`, `POST_RESCAN_DAYS`, and `SKIP_RECENTLY_SCANNED_HOURS`; fetches comments, detects new/edited comments, inserts/updates `Post`/`Comment`/`Mention` rows, updates `post.unique_subreddits`, and sets `post.last_scanned`.
- **Immediate Discovery Metadata**: when new subreddits are seen in comments, create `Subreddit` rows and update metadata immediately via `update_subreddit_metadata()` (rate-limited); `last_checked`, `next_retry_at`, and `retry_priority` are used for retry scheduling on 429s.
- **Metadata Refresh Phase**: runs after scanning and processes subreddits in this order:
— 1) never-scanned (`last_checked IS NULL`), 
- 2) missing metadata (NULL fields), 
- 3) stale metadata (`last_checked` older than `METADATA_STALE_HOURS`), 
- 4) re-check not-found subreddits every 7 days. Each refresh updates `last_checked`.
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
