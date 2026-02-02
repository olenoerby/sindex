# Backfill First Mentioned Timestamps

## Problem

The `first_mentioned` field on subreddits should reflect the earliest comment timestamp that mentioned each subreddit, but it was only being set for newly discovered mentions. This means old comments in the database weren't reflected in the `first_mentioned` dates.

## Solution

The `scripts/backfill_first_mentioned.py` script queries all existing mentions in the database and updates each subreddit's `first_mentioned` to the earliest mention timestamp.

## Usage

### Local Development (Docker)

```bash
docker-compose run --rm scanner python /app/scripts/backfill_first_mentioned.py
```

### Production Database (10.0.0.10)

From your local machine with network access to production:

```bash
# Option 1: Using environment variable
DATABASE_URL="postgresql+psycopg2://username:password@10.0.0.10:5432/pineapple" \
  python scripts/backfill_first_mentioned.py

# Option 2: Using command-line argument
python scripts/backfill_first_mentioned.py \
  "postgresql+psycopg2://username:password@10.0.0.10:5432/pineapple"
```

From a production server (if SSH'd in):

```bash
# If running from the production server itself
DATABASE_URL="postgresql+psycopg2://username:password@localhost:5432/pineapple" \
  python scripts/backfill_first_mentioned.py
```

### After Database Reset

After any database reset or migration, run this script to ensure all `first_mentioned` dates are correct based on existing mention data.

## What It Does

1. Queries all mentions in the database grouped by subreddit
2. Finds the minimum (earliest) timestamp for each subreddit
3. Updates each subreddit's `first_mentioned` field if different
4. Reports statistics and changes made

## Output

The script will display:
- Database connection info (with masked password)
- Database statistics (total subreddits, posts, comments, mentions)
- Each subreddit being updated with old → new dates
- Summary of updates made

## Safety

- Read-only operations except for updating `first_mentioned` field
- Uses transactions (will rollback on error)
- Shows all changes before committing
- Can be run multiple times safely (idempotent)
