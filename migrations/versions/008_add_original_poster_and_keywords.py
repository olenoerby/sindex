"""Add original_poster to post and keywords to subreddit_scan_config
Revision ID: 008_add_original_poster_and_keywords
Revises: 007_add_scan_priority
Create Date: 2026-02-08
"""

from alembic import op
import sqlalchemy as sa

revision = '008_add_original_poster_and_keywords'
down_revision = '007_add_scan_priority'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('post', sa.Column('original_poster', sa.String(), nullable=True))
    op.add_column('subreddit_scan_config', sa.Column('keywords', sa.String(), nullable=True))

def downgrade():
    op.drop_column('post', 'original_poster')
    op.drop_column('subreddit_scan_config', 'keywords')
