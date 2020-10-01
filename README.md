# Kafka to HBase

## Links

1. [Application Configurations](docs/k2hb-configurations.md)
1. [Local Development](docs/local-development.md)
1. [Kafka samples and tutorial](docs/kafka-tutorial-samples.md)

## Summary

Providing a way of migrating data in Kafka topics into tables in Hbase,
preserving versions based on Kafka message timestamps.

Two columns are written to for each message received; one to store the body
of the message and one to store a count and last received date of the
topic. These are configured using the `K2HB_KAFKA_TOPIC_*` and
`K2HB_KAFKA_DATA_*` environment variables.

By default if the kafka topic is `db.database-name.collection.name` the data 
table is `database_name:collection_name` with a column family of `topic`.

The qualifier is the topic name, the body of the cell is the raw message
received from Kafka and the version is the timestamp of the message in
milliseconds.

For example, after receiving a single message on `db.my.data` the data
is as follows:

```
hbase(main):001:0> scan 'my:data'
ROW                                       COLUMN+CELL
 63213667-c5a5-4411-a93b-e2da709c553e     column=topic:my:data, timestamp=1563547895682, value=<message body>
1 row(s) in 0.1090 seconds
```

Kafka2Hbase will attempt to create the required namespaces, tables and
column families on startup. If they already exist, nothing will happen. By
default the data table column family has a maximum of MAXINT versions
(approximately 2.1 billion) and a minimum of 1 version. There is no TTL.
The topic counter column family has no versioning or TTL.
