# kafka-to-hbase

A simple connector to extract messages from a Kafka topic and inject them into
an Hbase table

## Configuration

All configuration is handled through environment variables. All variables are
prefixed `K2HB_` for easy identification.

To get a full list of configurable variables and their current values use the
`--help` parameter.

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
    K2HB_KAFKA_CONNECTIONS_MAX_IDLE_MS                  540000
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

## Development

All core tasks in kafka-to-hbase use the Python Invoke utility. This is similar
to other build tools and the configuration is written in pure Python.

Invoke can be installed from Pypi using Pip:

```
pip install invoke
```

Once done, all other development tasks can be achieved using the `invoke`
command.

### Installing Dependencies

All dependencies for development an execution can be installed together by
running the `dev.install` task.

```
$ invoke dev.install
```

This installs all runtime and development dependencies as well as using an "egg
link" to allow changes to the code to be reflected immediately in the installed
package.

### Running Attached Services

Kafka To Hbase requires both Kafka and Hbase to be running. These can be started
easily by using the `dev.up` Invoke task.

```
$ invoke dev.up
```

By default this builds the Docker image containing Kafka To Hbase and starts up
both the services.

### Running Kafka To Hbase Locally

The Kafka To Hbase utility can be run locally on the development machine. The
configured defaults are sufficient to get started, with the exception of
`K2HB_KAFKA_TOPICS` which needs setting to at least a single topic. In addition
the tables need to be created in Hbase using the namespace, prefix and topic as
input.

Once this is done, and `dev.install` has been run, executing Kafka To Hbase is a case of running the CLI.

```
$ export K2HB_KAFKA_TOPICS=some_topic
$ kafka-to-hbase
```

### Running Kafka To Hbase In Docker

Running the Kafka To Hbase utility in Docker using the configuration in Docker
Compose is the easiest way to ensure the package builds, installs and runs. This
can be achieved using `dev.run`.

```
$ invoke dev.run
```

This will builds the Kafka To Hbase Docker image and run the process using the
environment defined in the Docker Compose YAML file.

### Changing the Docker Compose Configuration

eThe Docker Compose file defines the logical architecture of the 3 service system
required to run Kafka To Hbase. To change the Docker Compose configuration of
Hbase tables or namespaces, or which Kafka topics will be used, use the
`docker-compose.override.yaml` file. This is read in addition to the base Docker
Compose file but separates infrastructure from runtime configuration.