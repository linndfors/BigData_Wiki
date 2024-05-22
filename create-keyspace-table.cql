CREATE KEYSPACE hw12_nahurna WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE hw12_nahurna.main_table (
    user_id text,
    domain text,
    created_at timestamp,
    page_title text,
    PRIMARY KEY (user_id, domain, created_at)
);
