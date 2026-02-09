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
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c['name'] for c in insp.get_columns('mention')]

    # If legacy schema stores reddit_comment_id (string) but no comment_id FK column,
    # create comment_id, populate it from comment.reddit_comment_id, then add FK.
    if 'comment_id' not in cols:
        op.add_column('mention', sa.Column('comment_id', sa.Integer(), nullable=True))
        # Populate comment_id by joining on reddit_comment_id -> comment.reddit_comment_id
        try:
            conn.execute(sa.text(
                """
                UPDATE mention
                SET comment_id = c.id
                FROM comment c
                WHERE mention.reddit_comment_id IS NOT NULL
                AND c.reddit_comment_id = mention.reddit_comment_id
                """
            ))
        except Exception:
            # best-effort population; continue even if some rows fail
            pass

    # Drop any existing FK with the conventional name if present
    try:
        op.drop_constraint('mention_comment_id_fkey', 'mention', type_='foreignkey')
    except Exception:
        pass

    # Create FK on the integer comment_id column with ON DELETE CASCADE
    op.create_foreign_key(
        'mention_comment_id_fkey',
        'mention',
        'comment',
        ['comment_id'],
        ['id'],
        ondelete='CASCADE'
    )


def downgrade():
    try:
        op.drop_constraint('mention_comment_id_fkey', 'mention', type_='foreignkey')
    except Exception:
        pass
    # Optionally remove the comment_id column if it was added by this migration
    try:
        op.drop_column('mention', 'comment_id')
    except Exception:
        pass
