#!/usr/bin/env python3
"""Fill missing subreddit metadata by fetching /r/<name>/about.json and persisting fields."""
import os
import time
from datetime import datetime
import sys

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import Session

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api import models
from api.app import fetch_sub_about

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@db:5432/pineapple')

def safe_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def update_sub(session, s):
    try:
        r = fetch_sub_about(s.name)
        if r.status_code == 200:
            payload = r.json()
            data = payload.get('data', {}) if isinstance(payload, dict) else {}
            s.display_name = data.get('display_name') or s.display_name
            s.display_name_prefixed = data.get('display_name_prefixed') or s.display_name_prefixed
            s.title = data.get('title') or s.title
            created = safe_int(data.get('created_utc'))
            if created:
                s.created_utc = created
            subs = safe_int(data.get('subscribers'))
            if subs is not None:
                s.subscribers = subs
            active = safe_int(data.get('accounts_active') or data.get('active_user_count') or data.get('active_accounts'))
            if active is not None:
                s.active_users = active
            public_desc = data.get('public_description') or data.get('description')
            if public_desc:
                s.description = public_desc
            # save raw HTML if available
            try:
                s.public_description_html = data.get('public_description_html') or s.public_description_html
            except Exception:
                pass
            try:
                s.allow_videogifs = bool(data.get('allow_videogifs')) if data.get('allow_videogifs') is not None else s.allow_videogifs
            except Exception:
                pass
            try:
                s.allow_videos = bool(data.get('allow_videos')) if data.get('allow_videos') is not None else s.allow_videos
            except Exception:
                pass
            s.subreddit_type = data.get('subreddit_type') or s.subreddit_type
            s.lang = data.get('lang') or s.lang
            s.url = data.get('url') or s.url
            try:
                ov = data.get('over18') if 'over18' in data else data.get('over_18')
                if ov is not None:
                    s.over18 = bool(ov)
            except Exception:
                pass
            if isinstance(payload, dict) and payload.get('reason'):
                s.is_banned = True
                s.ban_reason = str(payload.get('reason'))
        elif r.status_code in (403,404):
            s.is_banned = True
        s.last_checked = datetime.utcnow()
        session.add(s)
        session.commit()
        return True
    except Exception as e:
        print(f"Failed updating /r/{s.name}: {e}")
        try:
            session.rollback()
        except Exception:
            pass
        return False

def main():
    engine = create_engine(DATABASE_URL)
    with Session(engine) as session:
        q = session.query(models.Subreddit).filter(
            or_(models.Subreddit.title == None,
                models.Subreddit.display_name == None,
                models.Subreddit.subscribers == None,
                models.Subreddit.description == None)
        )
        subs = q.all()
        print(f"Found {len(subs)} subreddits with missing metadata")
        updated = 0
        for s in subs:
            ok = update_sub(session, s)
            if ok:
                updated += 1
            # be gentle with Reddit
            time.sleep(1.5)
        print(f"Updated {updated}/{len(subs)} subreddits")

if __name__ == '__main__':
    main()
