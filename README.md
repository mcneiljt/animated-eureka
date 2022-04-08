
A restful json interface for hive metastore columns.

Testing command:
`LISTEN_ADDR=127.0.0.1 LISTEN_PORT=8080 URL_PREFIX="http://localhost:8080" HIVE_URL=thrift://hive.node.consul:9083 java -jar target/hive-controller-1.0.0.jar`

View tables: `<URL_PREFIX>/ui/<db_name>`


## future josh problems
- it should support POST and DELETE
- error handling
- it should have hive metastore access metrics
- it should have request metrics
