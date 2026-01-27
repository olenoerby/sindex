#!/usr/bin/env python3
"""
One-off script to extract mentions from existing comments that were
already fetched but never had mentions extracted (e.g., due to constraint issues).
"""
import os
import sys
import re
import logging

# Setup path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
import models

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mention_processor')

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
engine = create_engine(DATABASE_URL, future=True)

# Pattern for subreddit mentions
RE_SUB = re.compile(r"(?:/r/|\br/|https?://(?:www\.)?reddit\.com/r/)([A-Za-z0-9_]{3,21})")

IGNORE_SUBREDDITS = set(s.strip().lower() for s in os.getenv('IGNORE_SUBREDDITS', '').split(',') if s.strip())

def extract_subreddits_from_text(text):
    """Extract subreddit mentions from text."""
    if not text:
        return {}
    
    subnames = {}
    for m in RE_SUB.finditer(text):
        raw = m.group(1)
        sname = raw.lower().strip()
        if sname in IGNORE_SUBREDDITS:
            continue
        if sname not in subnames:
            # Store the matched text and context
            start = max(0, m.start() - 30)
            end = min(len(text), m.end() + 30)
            context = text[start:end]
            subnames[sname] = (raw, context)
    
    return subnames

def process_existing_mentions():
    """Process existing comments to extract and create mentions."""
    with Session(engine) as session:
        # Get all comments
        total_comments = session.query(func.count(models.Comment.id)).scalar() or 0
        logger.info(f"Processing {total_comments} existing comments for mentions")
        
        processed = 0
        mentions_created = 0
        
        comments = session.query(models.Comment).all()
        
        for comment in comments:
            try:
                if not comment.body:
                    continue
                
                subnames = extract_subreddits_from_text(comment.body)
                if not subnames:
                    continue
                
                for sname, (raw_text, context) in subnames.items():
                    # Get or create subreddit
                    sub = session.query(models.Subreddit).filter_by(name=sname).first()
                    if not sub:
                        sub = models.Subreddit(name=sname)
                        session.add(sub)
                        session.commit()
                        logger.info(f"Created new subreddit: /r/{sname}")
                    
                    # Check if mention already exists
                    existing = session.query(models.Mention).filter_by(
                        subreddit_id=sub.id,
                        comment_id=comment.id
                    ).first()
                    
                    if not existing:
                        # Create mention
                        mention = models.Mention(
                            subreddit_id=sub.id,
                            comment_id=comment.id,
                            post_id=comment.post_id,
                            timestamp=comment.created_utc,
                            user_id=comment.user_id,
                            mentioned_text=raw_text,
                            context_snippet=context
                        )
                        session.add(mention)
                        session.commit()
                        mentions_created += 1
                        
                        if mentions_created % 100 == 0:
                            logger.info(f"Created {mentions_created} mentions so far...")
                
                processed += 1
                if processed % 500 == 0:
                    logger.info(f"Processed {processed}/{total_comments} comments...")
            
            except Exception as e:
                logger.exception(f"Error processing comment {comment.id}: {e}")
                session.rollback()
        
        logger.info(f"Completed: processed {processed} comments, created {mentions_created} mentions")

if __name__ == '__main__':
    process_existing_mentions()
