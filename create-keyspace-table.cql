CREATE KEYSPACE IF NOT EXISTS fancy_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};


CREATE TABLE IF NOT EXISTS fancy_keyspace.created_pages (
    page_id TEXT,
    user_is_bot BOOLEAN,
    created_at TIMESTAMP,
    domain TEXT,
    PRIMARY KEY ((domain, page_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS fancy_keyspace.for_user (
    page_id TEXT,
    user_id TEXT,
    user_text TEXT,
    created_at TIMESTAMP,
    page_title TEXT,
    PRIMARY KEY ((user_id, page_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);


CREATE TABLE IF NOT EXISTS fancy_keyspace.pages (
    page_id TEXT,
    domain TEXT,
    user_id TEXT,
    page_title TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (domain, page_id)
);

CREATE INDEX IF NOT EXISTS ON fancy_keyspace.pages (user_id);
CREATE INDEX IF NOT EXISTS ON fancy_keyspace.pages (page_id);

CREATE INDEX IF NOT EXISTS ON fancy_keyspace.created_pages (created_at);
