#!/usr/bin/env python3
"""
Test script to verify mention deduplication logic works correctly.
Tests:
1. Same user can't mention the same subreddit twice (uq_mention_sub_user)
2. Same comment can't mention the same subreddit twice (uq_mention_sub_comment)
3. Different users CAN mention the same subreddit
4. Same user CAN mention different subreddits
"""

import sys
import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'api'))
from models import Base, Post, Comment, Subreddit, Mention

# Setup database connection (using the same Docker PostgreSQL)
DB_URL = "postgresql://postgres:postgres@localhost:5432/pineapple_db"
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

def test_mention_constraints():
    session = Session()
    
    try:
        print("=" * 60)
        print("Testing Mention Constraints")
        print("=" * 60)
        
        # Create test data
        print("\n1. Creating test subreddits...")
        sub1 = Subreddit(name="testsubreddit1", title="Test Sub 1", created_utc=1000000)
        sub2 = Subreddit(name="testsubreddit2", title="Test Sub 2", created_utc=1000000)
        session.add(sub1)
        session.add(sub2)
        session.commit()
        print(f"   ✓ Created subreddits: {sub1.name}, {sub2.name}")
        
        print("\n2. Creating test post...")
        post = Post(reddit_post_id="post_test_1", title="Test Post", created_utc=1000000)
        session.add(post)
        session.commit()
        print(f"   ✓ Created post: {post.reddit_post_id}")
        
        print("\n3. Creating test comments...")
        comment1 = Comment(
            reddit_comment_id="comment_1",
            post_id=post.id,
            user_id="user1",
            body="Test comment",
            created_utc=1000000
        )
        comment2 = Comment(
            reddit_comment_id="comment_2",
            post_id=post.id,
            user_id="user2",
            body="Another comment",
            created_utc=1000000
        )
        session.add(comment1)
        session.add(comment2)
        session.commit()
        print(f"   ✓ Created comments: user1 (comment_1), user2 (comment_2)")
        
        # Test 1: Same user can mention same subreddit from different comments (SHOULD FAIL)
        print("\n4. TEST: User1 mentions sub1 from comment1...")
        mention1 = Mention(
            subreddit_id=sub1.id,
            comment_id=comment1.id,
            post_id=post.id,
            user_id="user1",
            timestamp=1000000,
            mentioned_text="testsubreddit1"
        )
        session.add(mention1)
        session.commit()
        print("   ✓ SUCCESS: First mention created")
        
        print("\n5. TEST: Same user (user1) tries to mention same sub (sub1) again (SHOULD FAIL)...")
        mention2 = Mention(
            subreddit_id=sub1.id,
            comment_id=comment2.id,
            post_id=post.id,
            user_id="user1",
            timestamp=1000000,
            mentioned_text="testsubreddit1"
        )
        session.add(mention2)
        try:
            session.commit()
            print("   ✗ FAILURE: Should have raised IntegrityError!")
        except IntegrityError as e:
            session.rollback()
            print("   ✓ SUCCESS: IntegrityError raised as expected (uq_mention_sub_user constraint)")
            print(f"      Error: {e.orig}")
        
        # Test 2: Different user CAN mention same subreddit
        print("\n6. TEST: Different user (user2) mentions same sub (sub1) (SHOULD SUCCEED)...")
        mention3 = Mention(
            subreddit_id=sub1.id,
            comment_id=comment2.id,
            post_id=post.id,
            user_id="user2",
            timestamp=1000000,
            mentioned_text="testsubreddit1"
        )
        session.add(mention3)
        session.commit()
        print("   ✓ SUCCESS: Different user can mention same subreddit")
        
        # Test 3: Same user CAN mention different subreddit
        print("\n7. TEST: Same user (user1) mentions different subreddit (sub2) (SHOULD SUCCEED)...")
        comment3 = Comment(
            reddit_comment_id="comment_3",
            post_id=post.id,
            user_id="user1",
            body="Third comment",
            created_utc=1000000
        )
        session.add(comment3)
        session.commit()
        
        mention4 = Mention(
            subreddit_id=sub2.id,
            comment_id=comment3.id,
            post_id=post.id,
            user_id="user1",
            timestamp=1000000,
            mentioned_text="testsubreddit2"
        )
        session.add(mention4)
        session.commit()
        print("   ✓ SUCCESS: Same user can mention different subreddit")
        
        # Test 4: Same comment can't mention same subreddit twice
        print("\n8. TEST: Same comment mentions same subreddit twice (SHOULD FAIL)...")
        mention5 = Mention(
            subreddit_id=sub1.id,
            comment_id=comment2.id,
            post_id=post.id,
            user_id="user3",
            timestamp=1000000,
            mentioned_text="testsubreddit1"
        )
        session.add(mention5)
        try:
            session.commit()
            print("   ✗ FAILURE: Should have raised IntegrityError!")
        except IntegrityError as e:
            session.rollback()
            print("   ✓ SUCCESS: IntegrityError raised as expected (uq_mention_sub_comment constraint)")
            print(f"      Error: {e.orig}")
        
        print("\n" + "=" * 60)
        print("All tests completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    test_mention_constraints()
