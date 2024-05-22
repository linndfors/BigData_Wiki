CREATE KEYSPACE fancy_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};


CREATE TABLE fancy_keyspace.created_pages (
    page_id TEXT,
    user_is_bot BOOLEAN,
    created_at TIMESTAMP,
    domain TEXT,
    PRIMARY KEY ((domain, page_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE fancy_keyspace.for_user (
    page_id TEXT,
    user_id TEXT,
    user_text TEXT,
    created_at TIMESTAMP,
    page_title TEXT,
    PRIMARY KEY ((user_id, page_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);


