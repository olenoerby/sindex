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

Indexes: add indexes on `reddit_post_id`, `reddit_comment_id`, `subreddits.name`, and timestamp fields used for filtering.

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
- `nginx` - reverse proxy to `api` (optional; you can expose API directly)

All services are on internal docker network; expose `api` port 8000 and nginx port 80. Cloudflare Tunnel runs on host to route to nginx.
