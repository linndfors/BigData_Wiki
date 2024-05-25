docker cp ./create-keyspace-table.cql bigdata_wiki-cassandra-1:/create-keyspace-table.cql
docker exec -it bigdata_wiki-cassandra-1 cqlsh -f create-keyspace-table.cql
