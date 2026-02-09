-- $CONN="host=localhost port=5432 dbname=pineapple user=pineapple password=pineapple"
-- $CONN="host=10.0.0.10 port=5432 dbname=pineapple user=pineapple password=pineapple"
-- psql $CONN -f 001_import_posts.sql




DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='post' AND column_name='author') THEN
        ALTER TABLE post ADD COLUMN author TEXT;
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS post (
    reddit_post_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    created_utc BIGINT NOT NULL,
    author TEXT
);

DROP TABLE IF EXISTS import_stage;


CREATE TABLE import_stage (
    reddit_post_id TEXT,
    title TEXT,
    url TEXT,
    created_utc TEXT,
    author TEXT
);

\copy import_stage (reddit_post_id, title, url, created_utc, author) FROM 'C:\\Users\\ole\\OneDrive\\Dokumenter\\GitHub\\sindex\\backfill_All_FapFriday_AllYears.csv' WITH CSV HEADER;



INSERT INTO post (reddit_post_id, title, url, created_utc, author)
SELECT reddit_post_id, title, url, CAST(CAST(created_utc AS float) AS bigint), author
FROM import_stage
ON CONFLICT (reddit_post_id) DO NOTHING;

DROP TABLE import_stage;
