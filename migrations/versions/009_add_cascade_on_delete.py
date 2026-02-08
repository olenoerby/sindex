"""Add ON DELETE CASCADE to comment->post and mention->comment FKs
Revision ID: 009
Revises: 008
Create Date: 2026-02-08
"""

from alembic import op
import sqlalchemy as sa

revision = '009'
down_revision = '008'
branch_labels = None
depends_on = None


def upgrade():
    # mention.comment_id -> comment.id : add ON DELETE CASCADE
    op.drop_constraint('mention_comment_id_fkey', 'mention', type_='foreignkey')
    op.create_foreign_key(
        'mention_comment_id_fkey',
        'mention',
        'comment',
        ['comment_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # comment.post_id -> post.id : add ON DELETE CASCADE
    op.drop_constraint('comment_post_id_fkey', 'comment', type_='foreignkey')
    op.create_foreign_key(
        'comment_post_id_fkey',
        'comment',
        'post',
        ['post_id'],
        ['id'],
        ondelete='CASCADE'
    )


def downgrade():
    # revert mention FK to no ON DELETE CASCADE
    op.drop_constraint('mention_comment_id_fkey', 'mention', type_='foreignkey')
    op.create_foreign_key(
        'mention_comment_id_fkey',
        'mention',
        'comment',
        ['comment_id'],
        ['id']
    )

    # revert comment FK to no ON DELETE CASCADE
    op.drop_constraint('comment_post_id_fkey', 'comment', type_='foreignkey')
    op.create_foreign_key(
        'comment_post_id_fkey',
        'comment',
        'post',
        ['post_id'],
        ['id']
    )
