from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, BigInteger, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Post(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True)
    reddit_post_id = Column(String, unique=True, index=True, nullable=False)
    title = Column(Text)
    created_utc = Column(BigInteger, index=True)
    # count of distinct subreddits mentioned in this post's comments
    unique_subreddits = Column(Integer, nullable=False, default=0)
    url = Column(Text)
    comments = relationship('Comment', back_populates='post')


class Comment(Base):
    __tablename__ = 'comments'
    id = Column(Integer, primary_key=True)
    reddit_comment_id = Column(String, unique=True, index=True, nullable=False)
    post_id = Column(Integer, ForeignKey('posts.id'))
    # store the author's id (author_fullname) or fallback to username
    user_id = Column(String(255), nullable=True, index=True)
    body = Column(Text)
    created_utc = Column(BigInteger, index=True)
    mentions = relationship('Mention', back_populates='comment')
    post = relationship('Post', back_populates='comments')


class Subreddit(Base):
    __tablename__ = 'subreddits'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    # human-friendly subreddit title (from Reddit about.title)
    title = Column(String(255), nullable=True)
    created_utc = Column(BigInteger, index=True, nullable=True)
    subscribers = Column(Integer, nullable=True)
    active_users = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    # raw HTML description (public_description_html from Reddit)
    public_description_html = Column(Text, nullable=True)
    # additional metadata fields from about.json
    display_name = Column(String(255), nullable=True)
    display_name_prefixed = Column(String(255), nullable=True)
    allow_videogifs = Column(Boolean, nullable=True)
    allow_videos = Column(Boolean, nullable=True)
    subreddit_type = Column(String(50), nullable=True)
    lang = Column(String(10), nullable=True)
    url = Column(String(255), nullable=True)
    over18 = Column(Boolean, nullable=True)
    ban_reason = Column(String(255), nullable=True)
    # timestamp (unix seconds) of the first time a subreddit was mentioned
    first_mentioned = Column(BigInteger, nullable=True)
    is_banned = Column(Boolean, default=False)
    not_found = Column(Boolean, default=False)
    last_checked = Column(DateTime, server_default=func.now(), onupdate=func.now())
    # Retry/priority fields used when a fetch returned 429 Too Many Requests
    retry_priority = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime, nullable=True)
    # `mentions` relationship configured after `Mention` is defined to avoid
    # ambiguity between multiple foreign keys referencing `subreddits.id`.


class Mention(Base):
    __tablename__ = 'mentions'
    id = Column(Integer, primary_key=True)
    subreddit_id = Column(Integer, ForeignKey('subreddits.id'))
    # the subreddit where the mention was observed (source subreddit)
    source_subreddit_id = Column(Integer, ForeignKey('subreddits.id'), nullable=True)
    comment_id = Column(Integer, ForeignKey('comments.id'))
    # store user id (author_fullname or username) for reference
    user_id = Column(String(255), nullable=True, index=True)
    post_id = Column(Integer, ForeignKey('posts.id'))
    timestamp = Column(BigInteger, index=True)
    # Additional context for analytics
    mentioned_text = Column(String(255), nullable=True)  # the subreddit name as it appeared (e.g., "gaming")
    context_snippet = Column(Text, nullable=True)  # short excerpt from the comment around the mention
    __table_args__ = (
        UniqueConstraint('subreddit_id', 'comment_id', name='uq_mention_sub_comment'),
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


# Configure relationships explicitly now that all classes are declared.
from sqlalchemy.orm import relationship as _relationship

# Link mentions -> subreddit using the explicit column to disambiguate
Subreddit.mentions = _relationship('Mention', back_populates='subreddit', foreign_keys=[None])
# Now set Mention.subreddit and Mention.source_subreddit properly
# We assign using string-based relationships but reference actual FK columns
try:
    # replace placeholder with proper column refs if available
    Subreddit.mentions = _relationship('Mention', back_populates='subreddit', foreign_keys=[globals()['Mention'].subreddit_id])
    globals()['Mention'].subreddit = _relationship('Subreddit', back_populates='mentions', foreign_keys=[globals()['Mention'].subreddit_id])
    globals()['Mention'].source_subreddit = _relationship('Subreddit', foreign_keys=[globals()['Mention'].source_subreddit_id])
except Exception:
    # If something goes wrong during import-time relationship wiring, fallback
    # to string-based relationships so SQLAlchemy can attempt configuration later.
    Subreddit.mentions = _relationship('Mention', back_populates='subreddit', foreign_keys='Mention.subreddit_id')
    globals()['Mention'].subreddit = _relationship('Subreddit', back_populates='mentions', foreign_keys='Mention.subreddit_id')
    globals()['Mention'].source_subreddit = _relationship('Subreddit', foreign_keys='Mention.source_subreddit_id')
