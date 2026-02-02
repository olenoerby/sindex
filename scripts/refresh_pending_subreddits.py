#!/usr/bin/env python3
"""
Refresh all pending subreddits (those with NULL title) using the API refresh endpoint.
This script queries the database for pending subreddits and calls the refresh API for each.
"""
import os
import sys
import time
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api import models

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
API_BASE_URL = os.getenv('API_BASE_URL', 'http://api:8000')
API_KEY = os.getenv('API_KEY', '')
DELAY_BETWEEN_REQUESTS = float(os.getenv('REFRESH_DELAY', '1.0'))  # seconds

def main():
    if not API_KEY:
        print("ERROR: API_KEY environment variable not set!")
        sys.exit(1)
    
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    
    with Session(engine) as session:
        # Find all pending subreddits (title is NULL)
        pending = session.query(models.Subreddit).filter(
            models.Subreddit.title == None
        ).all()
        
        total = len(pending)
        print(f"Found {total} pending subreddits to refresh")
        
        if total == 0:
            print("No pending subreddits found!")
            return
        
        success_count = 0
        error_count = 0
        
        for idx, sub in enumerate(pending, 1):
            print(f"[{idx}/{total}] Refreshing /r/{sub.name}...", end=" ")
            
            try:
                url = f"{API_BASE_URL}/subreddits/{sub.name}/refresh"
                response = requests.post(
                    url,
                    params={"api_key": API_KEY},
                    timeout=10
                )
                
                if response.status_code == 202:
                    print("✓ Enqueued")
                    success_count += 1
                elif response.status_code == 429:
                    print(f"⚠ Rate limited: {response.json().get('detail', '')}")
                    error_count += 1
                else:
                    print(f"✗ Error {response.status_code}: {response.text[:100]}")
                    error_count += 1
                    
            except Exception as e:
                print(f"✗ Exception: {e}")
                error_count += 1
            
            # Delay between requests to avoid overwhelming the worker queue
            if idx < total:
                time.sleep(DELAY_BETWEEN_REQUESTS)
        
        print(f"\nSummary:")
        print(f"  Total: {total}")
        print(f"  Enqueued: {success_count}")
        print(f"  Errors: {error_count}")

if __name__ == '__main__':
    main()
