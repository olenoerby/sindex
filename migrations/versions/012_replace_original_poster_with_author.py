"""replace original_poster with author on post

Revision ID: 012_replace_original_poster_with_author
Revises: 011
Create Date: 2026-02-10

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '012'
down_revision = '011'
branch_labels = None
depends_on = None


def upgrade():
    # Add new `author` column, copy data from `original_poster`, then drop `original_poster`.
    op.add_column('post', sa.Column('author', sa.String(), nullable=True))
    # Copy existing values
    op.execute('UPDATE post SET author = original_poster')
    # Drop the old column
    op.drop_column('post', 'original_poster')


def downgrade():
    # Recreate `original_poster`, copy data back from `author`, then drop `author`.
    op.add_column('post', sa.Column('original_poster', sa.String(), nullable=True))
    op.execute('UPDATE post SET original_poster = author')
    op.drop_column('post', 'author')
