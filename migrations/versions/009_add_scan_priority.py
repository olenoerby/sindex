"""Add priority column to subreddit_scan_config

Revision ID: 009
Revises: 008
Create Date: 2026-02-04

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '009'
down_revision = '008'
branch_labels = None
depends_on = None


def upgrade():
    # Add priority column with default value of 3
    # Priority levels: 1 (highest), 2 (high), 3 (normal), 4 (low)
    op.add_column('subreddit_scan_config', 
                  sa.Column('priority', sa.Integer(), nullable=False, server_default='3'))


def downgrade():
    # Remove the priority column
    op.drop_column('subreddit_scan_config', 'priority')
