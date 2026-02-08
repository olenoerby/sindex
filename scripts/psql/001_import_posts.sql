CREATE TABLE IF NOT EXISTS post (
    reddit_post_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    created_utc BIGINT NOT NULL
);

DROP TABLE IF EXISTS import_stage;

CREATE TABLE import_stage (
    reddit_post_id TEXT,
    title TEXT,
    url TEXT,
    created_utc BIGINT
);

\copy import_stage (reddit_post_id, title, url, created_utc) FROM 'C:\\Users\\ole\\OneDrive\\Dokumenter\\GitHub\\sindex\\backfill_AutoModerator_FapFriday.csv' WITH CSV HEADER;

INSERT INTO post (reddit_post_id, title, url, created_utc)
SELECT reddit_post_id, title, url, created_utc
FROM import_stage
ON CONFLICT (reddit_post_id) DO NOTHING;

DROP TABLE import_stage;
