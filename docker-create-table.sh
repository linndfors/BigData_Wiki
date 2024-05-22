docker cp ./create-keyspace-table.cql homework12-cassandra-1:/create-keyspace-table.cql
docker exec -it homework12-cassandra-1 cqlsh -f create-keyspace-table.cql
