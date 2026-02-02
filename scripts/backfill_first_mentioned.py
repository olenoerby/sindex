#!/usr/bin/env python3
"""
Backfill first_mentioned timestamps for all subreddits based on earliest mention in database.

This script scans all COMMENTS in the database (not just the mention table) and updates 
each subreddit's first_mentioned to the timestamp of the earliest comment that mentioned it.

Usage:
    # Local dev (Docker):
    docker-compose run --rm scanner python /app/scripts/backfill_first_mentioned.py
    
    # Production database:
    DATABASE_URL="postgresql+psycopg2://user:pass@10.0.0.10:5432/dbname" python scripts/backfill_first_mentioned.py
    
    # Or pass as argument:
    python scripts/backfill_first_mentioned.py "postgresql+psycopg2://user:pass@10.0.0.10:5432/dbname"
"""
import os
import sys
import re
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session

# Add parent directory to path so we can import models
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from api import models

# Regex pattern to find subreddit mentions in comments
RE_SUB = re.compile(r"(?:/r/|\br/|https?://(?:www\.)?reddit\.com/r/)([A-Za-z0-9_]{3,21})", re.IGNORECASE)

# Support passing DATABASE_URL as command-line argument or environment variable
if len(sys.argv) > 1:
    DATABASE_URL = sys.argv[1]
else:
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')

engine = create_engine(DATABASE_URL, future=True)


def normalize_subreddit_name(name):
    """Normalize subreddit name to lowercase."""
    return name.lower().strip()


def backfill_first_mentioned():
    """Update first_mentioned for all subreddits based on earliest mention in comments."""
    
    with Session(engine) as session:
        # First, get some database stats
        total_subreddits = session.query(func.count(models.Subreddit.id)).scalar() or 0
        total_posts = session.query(func.count(models.Post.id)).scalar() or 0
        total_comments = session.query(func.count(models.Comment.id)).scalar() or 0
        
        print(f"\nDatabase statistics:")
        print(f"  Total subreddits: {total_subreddits}")
        print(f"  Total posts: {total_posts}")
        print(f"  Total comments: {total_comments}")
        print()
        
        if total_comments == 0:
            print("No comments found in database. Nothing to backfill.")
            return
        
        # Get all subreddits as a lookup dictionary
        all_subreddits = {s.name: s for s in session.query(models.Subreddit).all()}
        print(f"Loaded {len(all_subreddits)} subreddits from database")
        
        # Track earliest mention for each subreddit
        earliest_mentions = {}  # {subreddit_name: timestamp}
        
        print(f"\nScanning {total_comments} comments for subreddit mentions...")
        print("This may take a while...\n")
        
        # Process comments in batches
        batch_size = 1000
        processed = 0
        
        for offset in range(0, total_comments, batch_size):
            comments = session.query(models.Comment).order_by(models.Comment.created_utc).offset(offset).limit(batch_size).all()
            
            for comment in comments:
                if not comment.body:
                    continue
                
                # Find all subreddit mentions in the comment
                matches = RE_SUB.findall(comment.body)
                
                for match in matches:
                    sub_name = normalize_subreddit_name(match)
                    
                    # Skip special subreddits
                    if sub_name in ('all', 'random'):
                        continue
                    
                    # Only track if this subreddit exists in our database
                    if sub_name in all_subreddits:
                        timestamp = comment.created_utc or 0
                        if timestamp:
                            if sub_name not in earliest_mentions or timestamp < earliest_mentions[sub_name]:
                                earliest_mentions[sub_name] = timestamp
                
                processed += 1
                if processed % 10000 == 0:
                    print(f"  Processed {processed:,} / {total_comments:,} comments ({processed*100//total_comments}%)")
        
        print(f"\nFound mentions for {len(earliest_mentions)} subreddits in comments")
        print(f"\nUpdating first_mentioned timestamps...\n")
        
        updated_count = 0
        unchanged_count = 0
        
        for sub_name, earliest_timestamp in sorted(earliest_mentions.items(), key=lambda x: x[1]):
            subreddit = all_subreddits.get(sub_name)
            if not subreddit:
                continue
            
            # Update first_mentioned if it's different
            current_first = subreddit.first_mentioned
            
            if current_first != earliest_timestamp:
                old_date = datetime.utcfromtimestamp(current_first).strftime('%Y-%m-%d %H:%M:%S') if current_first else 'None'
                new_date = datetime.utcfromtimestamp(earliest_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                
                subreddit.first_mentioned = earliest_timestamp
                session.add(subreddit)
                updated_count += 1
                
                print(f"Updated /r/{subreddit.name}: {old_date} -> {new_date}")
            else:
                unchanged_count += 1
        
        # Commit all changes
        try:
            session.commit()
            print(f"\n{'='*60}")
            print(f"Backfill complete!")
            print(f"  Updated: {updated_count} subreddits")
            print(f"  Unchanged: {unchanged_count} subreddits")
            print(f"  Total processed: {len(earliest_mentions)} subreddits")
            print(f"{'='*60}")
        except Exception as e:
            session.rollback()
            print(f"Error committing changes: {e}")
            raise


if __name__ == '__main__':
    print("="*60)
    print("BACKFILL FIRST_MENTIONED TIMESTAMPS")
    print("="*60)
    # Mask password in URL for display
    display_url = DATABASE_URL
    if '@' in display_url:
        parts = display_url.split('@')
        if ':' in parts[0]:
            user_pass = parts[0].split('//')[-1]
            user = user_pass.split(':')[0]
            display_url = display_url.replace(user_pass, f"{user}:****")
    print(f"Database: {display_url}")
    print("="*60)
    print()
    
    try:
        backfill_first_mentioned()
    except KeyboardInterrupt:
        print("\n\nBackfill cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
