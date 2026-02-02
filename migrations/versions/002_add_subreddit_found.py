"""add subreddit_found column

Revision ID: 002
Revises: 001_initial
Create Date: 2026-01-29

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001_initial'
branch_labels = None
depends_on = None


def upgrade():
    # Add subreddit_found column with default True
    op.add_column('subreddit', sa.Column('subreddit_found', sa.Boolean(), nullable=True, server_default=sa.text('true')))
    
    # Update existing records where is_not_found=True to set subreddit_found=False
    op.execute("UPDATE subreddit SET subreddit_found = false WHERE is_not_found = true")
    
    # Make column non-nullable after setting defaults
    op.alter_column('subreddit', 'subreddit_found', nullable=False, server_default=sa.text('true'))


def downgrade():
    op.drop_column('subreddit', 'subreddit_found')
