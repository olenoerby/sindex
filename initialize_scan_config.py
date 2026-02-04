#!/usr/bin/env python3
"""
Initialize scan configuration tables with default data.
Run this once to migrate from .env-based config to database-based config.
"""

import sys
import os
sys.path.insert(0, '/app')

from api.models import Base, SubredditScanConfig, IgnoredSubreddit, IgnoredUser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Connect to database
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def normalize(s: str) -> str:
    """Normalize to lowercase."""
    return (s or '').strip().lower()

def initialize_config():
    session = Session()
    
    try:
        print("=" * 60)
        print("INITIALIZING SCAN CONFIGURATION")
        print("=" * 60)
        
        # Create tables if they don't exist
        Base.metadata.create_all(engine)
        print("\n✓ Tables created/verified")
        
        # Add default subreddit scan configs
        configs = [
            {
                'subreddit_name': normalize('wowthissubexists'),
                'allowed_users': 'WeirdPineapple',  # Only this user's posts
                'nsfw_only': True,
                'active': True,
                'priority': 1  # Highest priority
            },
            {
                'subreddit_name': normalize('NSFW411'),
                'allowed_users': None,  # All users
                'nsfw_only': True,
                'active': True,
                'priority': 2  # High priority
            },
            {
                'subreddit_name': normalize('wowthisNSFWsubexists'),
                'allowed_users': None,  # All users
                'nsfw_only': True,
                'active': True,
                'priority': 2  # High priority
            }
        ]
        
        for cfg in configs:
            existing = session.query(SubredditScanConfig).filter_by(
                subreddit_name=cfg['subreddit_name']
            ).first()
            
            if not existing:
                config = SubredditScanConfig(**cfg)
                session.add(config)
                print(f"  + Added scan config: /r/{cfg['subreddit_name']}")
            else:
                print(f"  - Scan config already exists: /r/{cfg['subreddit_name']}")
        
        session.commit()
        
        # Add ignored subreddits (mentions from these won't be recorded)
        ignored_subs = ['wowthissubexists', 'sneakpeekbot', 'nsfw411']
        
        for sub_name in ignored_subs:
            normalized = normalize(sub_name)
            existing = session.query(IgnoredSubreddit).filter_by(
                subreddit_name=normalized
            ).first()
            
            if not existing:
                ignored = IgnoredSubreddit(subreddit_name=normalized, active=True)
                session.add(ignored)
                print(f"  + Added ignored subreddit: /r/{normalized}")
            else:
                print(f"  - Ignored subreddit already exists: /r/{normalized}")
        
        session.commit()
        
        # Display final configuration
        print("\n" + "=" * 60)
        print("CONFIGURATION SUMMARY")
        print("=" * 60)
        
        print("\nActive Scan Configs:")
        configs = session.query(SubredditScanConfig).filter_by(active=True).all()
        for cfg in configs:
            users = cfg.allowed_users or "ALL USERS"
            nsfw = "NSFW only" if cfg.nsfw_only else "All posts"
            print(f"  • /r/{cfg.subreddit_name}")
            print(f"    Users: {users}")
            print(f"    Filter: {nsfw}")
        
        print("\nIgnored Subreddits (mentions not recorded):")
        ignored = session.query(IgnoredSubreddit).filter_by(active=True).all()
        for ign in ignored:
            print(f"  • /r/{ign.subreddit_name}")
        
        print("\n" + "=" * 60)
        print("✓ Configuration initialized successfully!")
        print("=" * 60)
        
        print("\nNext steps:")
        print("1. Update scanner code to read from database instead of .env")
        print("2. Remove old config from .env file:")
        print("   - SUBREDDITS_TO_SCAN")
        print("   - IGNORE_SUBREDDITS")
        print("   - REDDIT_USER (partially - now per-subreddit)")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    initialize_config()
