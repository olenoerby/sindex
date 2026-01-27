#!/usr/bin/env python
"""
Run Alembic migrations on database.
Used during container startup to apply pending schema changes without dropping database.
"""
import os
import sys
from alembic.config import Config
from alembic.command import upgrade

def run_migrations():
    """Run all pending Alembic migrations."""
    # Get database URL from environment
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)
    
    # Configure Alembic
    alembic_cfg = Config("/app/alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    
    try:
        print("Running database migrations...")
        upgrade(alembic_cfg, "head")
        print("✓ Migrations completed successfully")
    except Exception as e:
        print(f"ERROR running migrations: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_migrations()
