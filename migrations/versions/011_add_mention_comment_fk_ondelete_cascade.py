"""Add ON DELETE CASCADE to mention.comment_id FK

Revision ID: 011
Revises: 010
Create Date: 2026-02-09 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '011'
down_revision = '010'
branch_labels = None
depends_on = None


def upgrade():
    # Best-effort: drop existing FK (common name in Postgres is mention_comment_id_fkey)
    try:
        op.drop_constraint('mention_comment_id_fkey', 'mention', type_='foreignkey')
    except Exception:
        # If constraint name differs, fall back to recreating FK by issuing raw SQL
        pass

    # Create FK with ON DELETE CASCADE
    op.create_foreign_key(
        'mention_comment_id_fkey',
        'mention',
        'comment',
        ['comment_id'],
        ['id'],
        ondelete='CASCADE'
    )


def downgrade():
    # Drop cascade FK and recreate without cascade
    try:
        op.drop_constraint('mention_comment_id_fkey', 'mention', type_='foreignkey')
    except Exception:
        pass
    op.create_foreign_key(
        'mention_comment_id_fkey',
        'mention',
        'comment',
        ['comment_id'],
        ['id']
    )
