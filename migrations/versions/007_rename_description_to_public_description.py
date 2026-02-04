"""Rename subreddit.description to public_description

Revision ID: 007
Revises: 006
Create Date: 2026-02-04

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '007'
down_revision = '006'
branch_labels = None
depends_on = None


def upgrade():
    # Rename column from 'description' to 'public_description'
    op.alter_column('subreddit', 'description',
                    new_column_name='public_description',
                    existing_type=sa.Text(),
                    existing_nullable=True)


def downgrade():
    # Rename column back from 'public_description' to 'description'
    op.alter_column('subreddit', 'public_description',
                    new_column_name='description',
                    existing_type=sa.Text(),
                    existing_nullable=True)
