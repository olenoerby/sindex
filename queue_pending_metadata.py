#!/usr/bin/env python3
"""
Queue all subreddits with missing metadata for refresh.
This script finds subreddits where title is NULL and queues them for metadata refresh.
"""
import os
import sys
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from redis import Redis

# Import models
sys.path.insert(0, os.path.dirname(__file__))
from api import models

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@localhost:5432/pineapple')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

def main():
    print("=== Queue Pending Metadata Refresh ===")
    
    # Connect to database
    engine = create_engine(DATABASE_URL)
    print(f"Connected to database: {DATABASE_URL}")
    
    # Connect to Redis
    try:
        redis_client = Redis.from_url(REDIS_URL)
        redis_client.ping()
        print(f"Connected to Redis: {REDIS_URL}")
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Find all subreddits with NULL title (missing metadata)
    with Session(engine) as session:
        stmt = select(models.Subreddit).where(
            models.Subreddit.title == None,
            models.Subreddit.is_banned == False,
            models.Subreddit.subreddit_found != False
        )
        subreddits = session.execute(stmt).scalars().all()
        
        total = len(subreddits)
        print(f"\nFound {total} subreddits with missing metadata")
        
        if total == 0:
            print("Nothing to queue!")
            return
        
        # Ask for confirmation
        response = input(f"\nQueue {total} subreddits for metadata refresh? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cancelled.")
            return
        
        # Queue them all
        queued = 0
        failed = 0
        for sub in subreddits:
            try:
                redis_client.lpush('metadata_refresh_queue', sub.name)
                queued += 1
                if queued % 100 == 0:
                    print(f"Queued {queued}/{total}...")
            except Exception as e:
                print(f"Failed to queue /r/{sub.name}: {e}")
                failed += 1
        
        print(f"\n✓ Successfully queued {queued} subreddits")
        if failed > 0:
            print(f"✗ Failed to queue {failed} subreddits")
        
        # Show queue length
        queue_len = redis_client.llen('metadata_refresh_queue')
        print(f"\nCurrent queue length: {queue_len}")
        print(f"\nMetadata worker will process these at ~8 calls/minute (rate limited)")
        print(f"Estimated time: ~{queue_len / 8:.1f} minutes")

if __name__ == '__main__':
    main()
