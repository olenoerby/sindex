#!/usr/bin/env python3
"""
Backfill mentions for all existing comments in the database.
This script processes every comment, extracts subreddit mentions, and inserts them.
"""

import sys
import os
import re
sys.path.insert(0, '/app')

from api.models import Base, Post, Comment, Subreddit, Mention
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Connect to database
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

RE_SUB = re.compile(r"(?:/r/|\br/|https?://(?:www\.)?reddit\.com/r/)([A-Za-z0-9_]{3,21})")
IGNORE_SUBREDDITS = {'wowthissubexists', 'sneakpeekbot', 'nsfw411'}

def normalize(s: str) -> str:
    """Normalize subreddit name to lowercase."""
    return (s or '').lower()

def extract_subreddits_from_text(text: str):
    """Extract subreddit mentions from text."""
    results = {}
    for m in RE_SUB.findall(text or ''):
        nm = normalize(m)
        if 3 <= len(nm) <= 21 and nm not in ('all', 'random'):
            if nm not in results:  # Only keep first occurrence
                match_idx = (text or '').lower().find(m.lower())
                if match_idx >= 0:
                    start = max(0, match_idx - 50)
                    end = min(len(text), match_idx + len(m) + 50)
                    context = text[start:end].strip()
                else:
                    context = m
                results[nm] = (m, context[:200])
    return results

def backfill_mentions():
    session = Session()
    
    try:
        print("=" * 60)
        print("BACKFILL MENTIONS FOR EXISTING COMMENTS")
        print("=" * 60)
        
        # Get all comments
        comments = session.query(Comment).all()
        print(f"\nProcessing {len(comments)} comments...")
        
        mentions_created = 0
        mentions_skipped = 0
        
        for i, cm in enumerate(comments):
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(comments)}")
            
            # Extract mentions from comment body
            body = cm.body or ''
            subnames = extract_subreddits_from_text(body)
            
            if not subnames:
                continue
            
            # Get the post to find source subreddit
            post = session.query(Post).filter_by(id=cm.post_id).first()
            if not post:
                continue
            
            # Get source subreddit
            source_sub = None
            if post.reddit_post_id:
                # For now, we don't know the source subreddit from reddit_post_id alone
                # This would need to be stored separately
                pass
            
            # Process each mention
            for sname, (raw_text, context) in subnames.items():
                if sname in IGNORE_SUBREDDITS:
                    continue
                
                # Get or create subreddit
                sub = session.query(Subreddit).filter_by(name=sname).first()
                if not sub:
                    sub = Subreddit(name=sname)
                    session.add(sub)
                    session.commit()
                
                # Check if mention already exists
                comment_mention_exists = session.query(Mention).filter_by(subreddit_id=sub.id, comment_id=cm.id).first()
                user_mention_exists = session.query(Mention).filter_by(subreddit_id=sub.id, user_id=cm.user_id).first() if cm.user_id else None
                
                if not comment_mention_exists and not user_mention_exists:
                    # Create mention
                    mention = Mention(
                        subreddit_id=sub.id,
                        comment_id=cm.id,
                        post_id=cm.post_id,
                        timestamp=cm.created_utc or 0,
                        user_id=cm.user_id,
                        source_subreddit_id=(source_sub.id if source_sub else None),
                        mentioned_text=raw_text,
                        context_snippet=context
                    )
                    session.add(mention)
                    session.commit()
                    mentions_created += 1
                else:
                    mentions_skipped += 1
        
        print(f"\n" + "=" * 60)
        print(f"COMPLETE")
        print(f"  Mentions created: {mentions_created}")
        print(f"  Mentions skipped: {mentions_skipped}")
        print(f"=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == '__main__':
    backfill_mentions()
