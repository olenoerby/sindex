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
Configure via `.env` (see defaults in `scanner/main.py` and `api/app.py`):
- `DATABASE_URL` (Postgres DSN)
- `REDDIT_USER` (source user, default WeirdPineapple)
- `SUBREDDITS_TO_SCAN` (comma list; default includes wowthissubexists)
- `API_RATE_DELAY`, `HTTP_REQUEST_TIMEOUT`, `SUBABOUT_CONCURRENCY`, etc.
- Frontend served by Nginx; adjust compose if you change ports.

## API (brief)
- `GET /subreddits` – paginated, filters (mentions/subscribers ranges, availability, NSFW), sorting.
- `GET /stats` – totals plus last_scan_started/duration/new_mentions.
- Additional params in code (`api/app.py`). All endpoints are read-only.

## Frontend
- Static UI under `nginx/html/` with search, sort, filters (mentions, subscribers, first-mentioned presets, availability, NSFW/SFW), pagination, and column sorting.
- Links open to Reddit (configurable listing target).

## Development
- Code: Python 3.11, FastAPI, SQLAlchemy, httpx.
- Format/lint: not enforced here; keep changes minimal and clear.
- Restart services after code edits: `docker-compose build api scanner && docker-compose up -d api scanner`.

## Notes
- Uses public Reddit JSON endpoints without OAuth (keep requests modest; 429 handled with backoff).
- Designed as read-only indexing; no posting or account actions.
