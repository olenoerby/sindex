"""remove is_not_found column

Revision ID: 003
Revises: 002
Create Date: 2026-01-29

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade():
    # Drop the redundant is_not_found column (replaced by subreddit_found)
    op.drop_column('subreddit', 'is_not_found')


def downgrade():
    # Restore is_not_found column
    op.add_column('subreddit', sa.Column('is_not_found', sa.Boolean(), nullable=True, server_default=sa.text('false')))
    # Sync with subreddit_found: is_not_found = NOT subreddit_found
    op.execute("UPDATE subreddit SET is_not_found = NOT subreddit_found")
    op.alter_column('subreddit', 'is_not_found', nullable=False, server_default=sa.text('false'))
