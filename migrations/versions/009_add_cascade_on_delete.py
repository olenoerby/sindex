"""Add ON DELETE CASCADE to comment->post and mention->comment FKs
Revision ID: 009_add_cascade_on_delete
Revises: 008_add_original_poster_and_keywords
Create Date: 2026-02-08
"""

from alembic import op
import sqlalchemy as sa

revision = '009_add_cascade_on_delete'
down_revision = '008_add_original_poster_and_keywords'
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
