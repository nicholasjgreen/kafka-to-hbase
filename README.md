# kafka-to-hbase

A simple connector to extract messages from a Kafka topic and inject them into an Hbase table

## Configuration

All configuration is handled through environment variables. All variables are prefixed `K2HB_` for easy identification.

To get a full list of configurable variables and their current values use the `--help` parameter.

```
$ kafka-to-hbase --help
usage: kafka-to-hbase [-h]

Simple Kafka to Hbase shovel utility

optional arguments:
  -h, --help  show this help message and exit

Current environment:
    K2HB_KAFKA_BOOTSTRAP_SERVERS                        localhost:9092
    K2HB_KAFKA_CLIENT_ID                                kafka-to-hbase
    K2HB_KAFKA_GROUP_ID                                 kafka-to-hbase
    K2HB_KAFKA_FETCH_MIN_BYTES                          1
    K2HB_KAFKA_FETCH_MAX_WAIT_MS                        500
    K2HB_KAFKA_FETCH_MAX_BYTES                          52428800
    K2HB_KAFKA_MAX_PARTITION_FETCH_BYTES                1048576
    K2HB_KAFKA_REQUEST_TIMEOUT_MS                       305000
    K2HB_KAFKA_RETRY_BACKOFF_MS                         100
    K2HB_KAFKA_RECONNECT_BACKOFF_MS                     1000
    K2HB_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION    5
    K2HB_KAFKA_CHECK_CRCS                               true
    K2HB_KAFKA_METADATA_MAX_AGE                         300000
    K2HB_KAFKA_SESSION_TIMEOUT_MS                       10000
    K2HB_KAFKA_HEARTBEAT_INTERVAL_MS                    3000
    K2HB_KAFKA_RECEIVE_BUFFER_BYTES                     32768
    K2HB_KAFKA_SEND_BUFFER_BYTES                        131072
    K2HB_KAFKA_CONSUMER_TIMEOUT_MS                      0
    K2HB_KAFKA_CONECTIONS_MAX_IDLE_MS                   540000
    K2HB_KAFKA_SSL                                      false
    K2HB_KAFKA_SSL_CHECK_HOSTNAME                       true
    K2HB_KAFKA_SSL_CAFILE
    K2HB_KAFKA_SSL_CERTFILE
    K2HB_KAFKA_SSL_KEYFILE
    K2HB_KAFKA_SSL_PASSWORD
    K2HB_KAFKA_SSL_CRLFILE
    K2HB_KAFKA_SSL_CIPHERS
    K2HB_KAFKA_SASL_KERBEROS                            false
    K2HB_KAFKA_SASL_KERBEROS_SERVICE_NAME               kafka
    K2HB_KAFKA_SASL_KERBEROS_DOMAIN_NAME
    K2HB_KAFKA_SASL_PLAIN_USERNAME
    K2HB_KAFKA_SASL_PLAIN_PASSWORD
    K2HB_KAFKA_TOPICS
    K2HB_HBASE_HOST                                     localhost
    K2HB_HBASE_PORT                                     9090
    K2HB_HBASE_TIMEOUT                                  0
    K2HB_HBASE_NAMESPACE
    K2HB_HBASE_TABLE_PREFIX
    K2HB_HBASE_COLUMN
```
