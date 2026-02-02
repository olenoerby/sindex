#!/usr/bin/env python3
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from api import models

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')
engine = create_engine(DATABASE_URL, future=True)

with Session(engine) as session:
    oldest = session.query(models.Comment).order_by(models.Comment.created_utc.asc()).first()
    newest = session.query(models.Comment).order_by(models.Comment.created_utc.desc()).first()
    total = session.query(models.Comment).count()
    
    print(f"\nComment Statistics:")
    print(f"  Total comments: {total:,}")
    print()
    
    if oldest:
        print(f"Oldest comment:")
        print(f"  ID: {oldest.reddit_comment_id}")
        print(f"  Date: {datetime.utcfromtimestamp(oldest.created_utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  Timestamp: {oldest.created_utc}")
        print(f"  Body preview: {(oldest.body or '')[:150]}...")
        print()
    
    if newest:
        print(f"Newest comment:")
        print(f"  ID: {newest.reddit_comment_id}")
        print(f"  Date: {datetime.utcfromtimestamp(newest.created_utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  Timestamp: {newest.created_utc}")
        print()
    
    # Also check posts
    oldest_post = session.query(models.Post).order_by(models.Post.created_utc.asc()).first()
    newest_post = session.query(models.Post).order_by(models.Post.created_utc.desc()).first()
    total_posts = session.query(models.Post).count()
    
    print(f"\nPost Statistics:")
    print(f"  Total posts: {total_posts:,}")
    print()
    
    if oldest_post:
        print(f"Oldest post:")
        print(f"  ID: {oldest_post.reddit_post_id}")
        print(f"  Date: {datetime.utcfromtimestamp(oldest_post.created_utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  Title: {(oldest_post.title or '')[:100]}...")
        print()
    
    if newest_post:
        print(f"Newest post:")
        print(f"  ID: {newest_post.reddit_post_id}")
        print(f"  Date: {datetime.utcfromtimestamp(newest_post.created_utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  Title: {(newest_post.title or '')[:100]}...")
