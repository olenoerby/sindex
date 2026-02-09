"""add comment last_scanned column

Revision ID: 010
Revises: 009
Create Date: 2026-02-09

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '010'
down_revision = '009'
branch_labels = None
depends_on = None


def upgrade():
    # Add last_scanned column to track when comments were last processed
    op.add_column('comment', sa.Column('last_scanned', sa.DateTime(), nullable=True))
    # Create an index for efficient ordering/queries
    op.create_index('ix_comment_last_scanned', 'comment', ['last_scanned'])


def downgrade():
    op.drop_index('ix_comment_last_scanned', table_name='comment')
    op.drop_column('comment', 'last_scanned')
