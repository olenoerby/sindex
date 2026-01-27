"""Initial schema from models.

Revision ID: 001_initial
Revises: 
Create Date: 2026-01-27 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create initial schema."""
    # Create subreddit table
    op.create_table(
        'subreddit',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('display_name', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('subscribers', sa.Integer(), nullable=True),
        sa.Column('active_users', sa.Integer(), nullable=True),
        sa.Column('created_utc', sa.BigInteger(), nullable=True),
        sa.Column('first_mentioned', sa.BigInteger(), nullable=True),
        sa.Column('last_checked', sa.DateTime(), nullable=True),
        sa.Column('is_banned', sa.Boolean(), nullable=True),
        sa.Column('is_not_found', sa.Boolean(), nullable=True),
        sa.Column('is_over18', sa.Boolean(), nullable=True),
        sa.Column('next_retry_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_index(op.f('ix_subreddit_name'), 'subreddit', ['name'], unique=False)
    op.create_index(op.f('ix_subreddit_created_utc'), 'subreddit', ['created_utc'], unique=False)

    # Create post table
    op.create_table(
        'post',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('reddit_post_id', sa.String(), nullable=False),
        sa.Column('subreddit_id', sa.Integer(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('url', sa.String(), nullable=True),
        sa.Column('created_utc', sa.BigInteger(), nullable=True),
        sa.Column('unique_subreddits', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['subreddit_id'], ['subreddit.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('reddit_post_id'),
    )
    op.create_index(op.f('ix_post_reddit_post_id'), 'post', ['reddit_post_id'], unique=False)
    op.create_index(op.f('ix_post_created_utc'), 'post', ['created_utc'], unique=False)

    # Create comment table
    op.create_table(
        'comment',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('reddit_comment_id', sa.String(), nullable=False),
        sa.Column('post_id', sa.Integer(), nullable=False),
        sa.Column('body', sa.String(), nullable=True),
        sa.Column('created_utc', sa.BigInteger(), nullable=True),
        sa.Column('username', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['post_id'], ['post.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('reddit_comment_id'),
    )
    op.create_index(op.f('ix_comment_reddit_comment_id'), 'comment', ['reddit_comment_id'], unique=False)
    op.create_index(op.f('ix_comment_created_utc'), 'comment', ['created_utc'], unique=False)

    # Create mention table
    op.create_table(
        'mention',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('subreddit_id', sa.Integer(), nullable=False),
        sa.Column('comment_id', sa.Integer(), nullable=False),
        sa.Column('post_id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.BigInteger(), nullable=True),
        sa.Column('user_id', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['comment_id'], ['comment.id'], ),
        sa.ForeignKeyConstraint(['post_id'], ['post.id'], ),
        sa.ForeignKeyConstraint(['subreddit_id'], ['subreddit.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('subreddit_id', 'comment_id', name='uq_mention_sub_comment'),
        sa.UniqueConstraint('subreddit_id', 'user_id', name='uq_mention_sub_user'),
    )
    op.create_index(op.f('ix_mention_timestamp'), 'mention', ['timestamp'], unique=False)

    # Create analytics table
    op.create_table(
        'analytics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('total_posts', sa.Integer(), nullable=True),
        sa.Column('total_comments', sa.Integer(), nullable=True),
        sa.Column('total_subreddits', sa.Integer(), nullable=True),
        sa.Column('total_mentions', sa.Integer(), nullable=True),
        sa.Column('last_scan_started', sa.DateTime(), nullable=True),
        sa.Column('last_scan_duration', sa.Float(), nullable=True),
        sa.Column('last_scan_new_mentions', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create subreddit_scan_config table
    op.create_table(
        'subreddit_scan_config',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('subreddit_name', sa.String(), nullable=False),
        sa.Column('allowed_users', sa.String(), nullable=True),
        sa.Column('nsfw_only', sa.Boolean(), nullable=True),
        sa.Column('active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('subreddit_name'),
    )

    # Create ignored_subreddit table
    op.create_table(
        'ignored_subreddit',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('subreddit_name', sa.String(), nullable=False),
        sa.Column('active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('subreddit_name'),
    )

    # Create ignored_user table
    op.create_table(
        'ignored_user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('username', sa.String(), nullable=False),
        sa.Column('active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('username'),
    )


def downgrade() -> None:
    """Drop all tables."""
    op.drop_table('ignored_user')
    op.drop_table('ignored_subreddit')
    op.drop_table('subreddit_scan_config')
    op.drop_table('analytics')
    op.drop_index(op.f('ix_mention_timestamp'), table_name='mention')
    op.drop_table('mention')
    op.drop_index(op.f('ix_comment_created_utc'), table_name='comment')
    op.drop_index(op.f('ix_comment_reddit_comment_id'), table_name='comment')
    op.drop_table('comment')
    op.drop_index(op.f('ix_post_created_utc'), table_name='post')
    op.drop_index(op.f('ix_post_reddit_post_id'), table_name='post')
    op.drop_table('post')
    op.drop_index(op.f('ix_subreddit_created_utc'), table_name='subreddit')
    op.drop_index(op.f('ix_subreddit_name'), table_name='subreddit')
    op.drop_table('subreddit')
