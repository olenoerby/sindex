"""make post.subreddit_id FK cascade on delete

Revision ID: 011
Revises: 010
Create Date: 2026-02-10

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '011'
down_revision = '010'
branch_labels = None
depends_on = None


def upgrade():
    # Drop the existing foreign key and recreate it with ON DELETE CASCADE
    op.drop_constraint('post_subreddit_id_fkey', 'post', type_='foreignkey')
    op.create_foreign_key(
        'post_subreddit_id_fkey',
        'post',
        'subreddit',
        ['subreddit_id'],
        ['id'],
        ondelete='CASCADE'
    )


def downgrade():
    # Recreate the original foreign key without cascade
    op.drop_constraint('post_subreddit_id_fkey', 'post', type_='foreignkey')
    op.create_foreign_key(
        'post_subreddit_id_fkey',
        'post',
        'subreddit',
        ['subreddit_id'],
        ['id']
    )
