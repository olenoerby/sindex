from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, BigInteger, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    reddit_post_id = Column(String, unique=True, index=True, nullable=False)
    title = Column(Text)
    created_utc = Column(BigInteger, index=True)
    # count of distinct subreddits mentioned in this post's comments
    unique_subreddits = Column(Integer, nullable=False, default=0)
    url = Column(Text)
    # timestamp when this post was last scanned for comments
    last_scanned = Column(DateTime, nullable=True)
    subreddit_id = Column(Integer, ForeignKey('subreddit.id'), nullable=True)
    original_poster = Column(String, nullable=True)
    comments = relationship('Comment', back_populates='post')


class Comment(Base):
    __tablename__ = 'comment'
    id = Column(Integer, primary_key=True)
    reddit_comment_id = Column(String, unique=True, index=True, nullable=False)
    post_id = Column(Integer, ForeignKey('post.id'))
    # store the author username for reference
    username = Column(String(255), nullable=True, index=True)
    body = Column(Text)
    created_utc = Column(BigInteger, index=True)
    mentions = relationship('Mention', back_populates='comment')
    post = relationship('Post', back_populates='comments')


class Subreddit(Base):
    __tablename__ = 'subreddit'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    # human-friendly subreddit title (from Reddit about.title)
    title = Column(String(255), nullable=True)
    created_utc = Column(BigInteger, index=True, nullable=True)
    subscribers = Column(Integer, nullable=True)
    active_users = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    # additional metadata fields from about.json
    display_name = Column(String(255), nullable=True)
    # timestamp (unix seconds) of the first time a subreddit was mentioned
    first_mentioned = Column(BigInteger, nullable=True)
    is_banned = Column(Boolean, default=False)
    subreddit_found = Column(Boolean, default=True)  # False if subreddit doesn't exist on Reddit (404)
    is_over18 = Column(Boolean, nullable=True)
    last_checked = Column(DateTime, server_default=func.now(), onupdate=func.now())
    # Retry/priority fields used when a fetch returned 429 Too Many Requests
    next_retry_at = Column(DateTime, nullable=True)
    # `mentions` relationship configured after `Mention` is defined to avoid
    # ambiguity between multiple foreign keys referencing `subreddit.id`.


class Mention(Base):
    __tablename__ = 'mention'
    id = Column(Integer, primary_key=True)
    subreddit_id = Column(Integer, ForeignKey('subreddit.id'))
    comment_id = Column(Integer, ForeignKey('comment.id'))
    # store user id (author_fullname or username) for reference
    user_id = Column(String(255), nullable=True, index=True)
    post_id = Column(Integer, ForeignKey('post.id'))
    timestamp = Column(BigInteger, index=True)
    __table_args__ = (
        UniqueConstraint('subreddit_id', 'comment_id', name='uq_mention_sub_comment'),
        UniqueConstraint('subreddit_id', 'user_id', name='uq_mention_sub_user'),
    )
    comment = relationship('Comment', back_populates='mentions')


class Analytics(Base):
    __tablename__ = 'analytics'
    id = Column(Integer, primary_key=True)
    # counters
    total_subreddits = Column(Integer, nullable=False, default=0)
    total_posts = Column(Integer, nullable=False, default=0)
    total_comments = Column(Integer, nullable=False, default=0)
    total_mentions = Column(Integer, nullable=False, default=0)
    # scan tracking
    last_scan_started = Column(DateTime, nullable=True)
    last_scan_duration = Column(Integer, nullable=True)  # seconds
    last_scan_new_mentions = Column(Integer, nullable=True)
    # timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class SubredditScanConfig(Base):
    """Configuration for which subreddits to actively scan for posts."""
    __tablename__ = 'subreddit_scan_config'
    id = Column(Integer, primary_key=True)
    # Subreddit name (normalized to lowercase)
    subreddit_name = Column(String(255), unique=True, nullable=False, index=True)
    # Comma-separated list of usernames to scan posts from (null/empty = all users)
    allowed_users = Column(Text, nullable=True)
    # Only scan NSFW posts
    nsfw_only = Column(Boolean, nullable=False, default=True)
    # Whether this config is active
    active = Column(Boolean, nullable=False, default=True)
    # Scan priority: 1 (highest), 2 (high), 3 (normal/default), 4 (low)
    priority = Column(Integer, nullable=False, default=3)
    keywords = Column(Text, nullable=True)
    # timestamps
    created_at = Column(DateTime, server_default=func.now())


class IgnoredSubreddit(Base):
    """Subreddits to never record mentions for."""
    __tablename__ = 'ignored_subreddit'
    id = Column(Integer, primary_key=True)
    subreddit_name = Column(String(255), unique=True, nullable=False, index=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, server_default=func.now())


class IgnoredUser(Base):
    """Users whose mentions should not be recorded."""
    __tablename__ = 'ignored_user'
    id = Column(Integer, primary_key=True)
    username = Column(String(255), unique=True, nullable=False, index=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, server_default=func.now())


class Category(Base):
    """Top-level categories for organizing content (e.g., 'Body Type', 'Sexual Position', 'Kinks')."""
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    slug = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    sort_order = Column(Integer, nullable=False, default=0)
    icon = Column(String(50), nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, server_default=func.now())
    tags = relationship('CategoryTag', back_populates='category', cascade='all, delete-orphan')


class CategoryTag(Base):
    """Sub-categories/tags within a category (e.g., 'BBW' under 'Body Type')."""
    __tablename__ = 'category_tag'
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey('category.id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(255), nullable=False, index=True)
    slug = Column(String(255), nullable=False, index=True)
    keywords = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    sort_order = Column(Integer, nullable=False, default=0)
    icon = Column(String(50), nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, server_default=func.now())
    __table_args__ = (
        UniqueConstraint('category_id', 'slug', name='uq_category_tag_category_slug'),
    )
    category = relationship('Category', back_populates='tags')
    subreddit_associations = relationship('SubredditCategoryTag', back_populates='tag', cascade='all, delete-orphan')


class SubredditCategoryTag(Base):
    """Many-to-many relationship between subreddits and category tags."""
    __tablename__ = 'subreddit_category_tag'
    id = Column(Integer, primary_key=True)
    subreddit_id = Column(Integer, ForeignKey('subreddit.id', ondelete='CASCADE'), nullable=False, index=True)
    category_tag_id = Column(Integer, ForeignKey('category_tag.id', ondelete='CASCADE'), nullable=False, index=True)
    created_at = Column(DateTime, server_default=func.now())
    source = Column(String(50), nullable=True, default='manual')
    confidence = Column(Integer, nullable=True)
    __table_args__ = (
        UniqueConstraint('subreddit_id', 'category_tag_id', name='uq_subreddit_category_tag'),
    )
    subreddit = relationship('Subreddit', backref='category_tags')
    tag = relationship('CategoryTag', back_populates='subreddit_associations')


# Configure relationships explicitly now that all classes are declared.
from sqlalchemy.orm import relationship as _relationship

# Link mentions -> subreddit using the explicit column to disambiguate
try:
    Subreddit.mentions = _relationship('Mention', back_populates='subreddit', foreign_keys=[Mention.subreddit_id])
    Mention.subreddit = _relationship('Subreddit', back_populates='mentions', foreign_keys=[Mention.subreddit_id])
except Exception:
    # If something goes wrong during import-time relationship wiring, fallback
    # to string-based relationships so SQLAlchemy can attempt configuration later.
    pass
