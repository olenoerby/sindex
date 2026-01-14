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
    mentions = relationship('Mention', back_populates='subreddit')


class Mention(Base):
    __tablename__ = 'mentions'
    id = Column(Integer, primary_key=True)
    subreddit_id = Column(Integer, ForeignKey('subreddits.id'))
    comment_id = Column(Integer, ForeignKey('comments.id'))
    # store user id (author_fullname or username) to de-duplicate mentions by user
    user_id = Column(String(255), nullable=True, index=True)
    post_id = Column(Integer, ForeignKey('posts.id'))
    timestamp = Column(BigInteger, index=True)
    __table_args__ = (
        UniqueConstraint('subreddit_id', 'comment_id', name='uq_mention_sub_comment'),
        UniqueConstraint('subreddit_id', 'user_id', name='uq_mention_sub_user'),
    )
    subreddit = relationship('Subreddit', back_populates='mentions')
    comment = relationship('Comment', back_populates='mentions')


class Analytics(Base):
    __tablename__ = 'analytics'
    id = Column(Integer, primary_key=True)
    # counters
    total_subreddits = Column(Integer, nullable=False, default=0)
    total_posts = Column(Integer, nullable=False, default=0)
    total_comments = Column(Integer, nullable=False, default=0)
    total_mentions = Column(Integer, nullable=False, default=0)
    # timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
