import os
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from api import models

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://pineapple:pineapple@localhost:5432/pineapple')
engine = create_engine(DATABASE_URL)

with Session(engine) as session:
    result = session.execute(
        select(
            models.Subreddit.name,
            models.Subreddit.title,
            models.Subreddit.is_banned,
            models.Subreddit.subreddit_found
        ).limit(20)
    )
    
    print("Sample subreddits:")
    print(f"{'Name':<25} {'Title':<30} {'Banned':<10} {'Found'}")
    print("-" * 80)
    for row in result:
        name, title, is_banned, found = row
        print(f"{name:<25} {str(title)[:30]:<30} {str(is_banned):<10} {found}")
