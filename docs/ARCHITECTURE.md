# Pineapple-Index Architecture and Design

This document describes the DB schema, API routes, caching strategy, sequence diagram, and Dockerized architecture.

**Database Schema**
- Table `posts`:
  - `id` PK
  - `reddit_post_id` TEXT UNIQUE INDEX
  - `title` TEXT
  - `created_utc` BIGINT INDEX
  - `url` TEXT
- Table `comments`:
  - `id` PK
  - `reddit_comment_id` TEXT UNIQUE INDEX
  - `post_id` FK -> posts.id
  - `body` TEXT
  - `created_utc` BIGINT INDEX
- Table `subreddits`:
  - `id` PK
  - `name` TEXT UNIQUE INDEX
  - `created_utc` BIGINT
  - `subscribers` INT
  - `active_users` INT
  - `description` TEXT
  - `is_banned` BOOLEAN
  - `last_checked` TIMESTAMP
- Table `mentions`:
  - `id` PK
  - `subreddit_id` FK -> subreddits.id
  - `comment_id` FK -> comments.id
  - `post_id` FK -> posts.id
  - `timestamp` BIGINT INDEX
  - `user_id` TEXT (username who made the mention)
  - UNIQUE constraints: (subreddit_id, comment_id) AND (subreddit_id, user_id)
- Table `subreddit_scan_configs` (controls which subreddits to scan):
  - `id` PK
  - `subreddit_name` TEXT UNIQUE (subreddit to scan)
  - `allowed_users` TEXT NULL (comma-separated usernames, or NULL for all users)
  - `nsfw_only` BOOLEAN (if TRUE, only process NSFW posts)
  - `active` BOOLEAN (if FALSE, config is ignored)
  - `created_at` TIMESTAMP
- Table `ignored_subreddits` (mentions from these subreddits are not recorded):
  - `id` PK
  - `subreddit_name` TEXT UNIQUE
  - `active` BOOLEAN
  - `created_at` TIMESTAMP
- Table `ignored_users` (mentions from these users are not recorded):
  - `id` PK
  - `username` TEXT UNIQUE
  - `active` BOOLEAN
  - `created_at` TIMESTAMP

Indexes: add indexes on `reddit_post_id`, `reddit_comment_id`, `subreddits.name`, and timestamp fields used for filtering.

**Configuration System**
The scanner loads scan targets dynamically from `subreddit_scan_configs` table on each scan cycle. This allows runtime configuration changes without editing .env or restarting containers. Each scan config can specify:
- Per-subreddit user filtering (scan posts only from specific users)
- NSFW-only filtering (ignore non-NSFW posts)
- Active/inactive toggle for temporary disabling

Mentions are deduplicated at two levels:
1. One mention per comment per subreddit (prevents duplicate extractions)
2. One mention per user per subreddit globally (prevents spam from single user)

**Backend API Routes** (FastAPI - read-only)
- GET `/subreddits?page=&per_page=&sort=`: list subreddits with pagination; `sort` can be `mentions`, `subscribers`, `active_users`, `created_utc`.
- GET `/subreddits/{name}`: full metadata and mention count for `name`.
- GET `/mentions?page=&per_page=&subreddit=`: list mention records, filterable by subreddit.
- GET `/stats/top?limit=`: top mentioned subreddits.

**Caching Strategy**
- Subreddit metadata is cached in `subreddits.last_checked` and refreshed only if older than `META_CACHE_DAYS` (default 7 days).
- The scanner updates metadata in a controlled loop; the scanner respects `API_RATE_DELAY` between all external Reddit requests (>=6.5s).
- For high-scale, add Redis cache and use a prioritized queue to update metadata gradually while remaining under the 10 req/min limit.

**Sequence (Weekly pipeline)**
1. Scanner lists `https://www.reddit.com/user/WeirdPineapple/submitted.json` and finds posts with "Fap Friday" in title.
2. For each new post (by reddit_post_id), scanner fetches `https://www.reddit.com/comments/{postid}.json`.
3. Scanner extracts comments (walks replies) and runs regex to find subreddit mentions.
4. Normalize, validate, and deduplicate subreddit names; insert `posts`, `comments`, `subreddits`, `mentions` records.
5. Periodically (in same loop) update subreddit metadata from `https://www.reddit.com/r/{name}/about.json` only if cache expired.
6. API reads DB (read-only) and serves lists and stats to the web UI.

**Dockerized Architecture**
- `db` - Postgres service (persistent volume)
- `api` - FastAPI app (read-only)
- `scanner` - Background worker collects data
 - `scanner` - Background worker collects data
 - `redis_worker` - RQ worker that processes background jobs (e.g., subreddit metadata refreshes)
- `nginx` - reverse proxy to `api` (optional; you can expose API directly)

All services are on internal docker network; expose `api` port 8000 and nginx port 80. Cloudflare Tunnel runs on host to route to nginx.
