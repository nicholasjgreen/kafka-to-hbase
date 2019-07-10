# Kafka2Hbase

Providing a way of migrating data in Kafka topics into tables in Hbase,
preserving versions based on Kafka message timestamps.

## Build

Ensure a JVM is installed and run the gradle wrapper.

    ./gradlew build

## Distribute

If a standard zip file is required, just use the assembleDist command.

    ./gradlew assembleDist

Otherwise if a tarball is required use the distTar command.

    ./gradlew distTar
    
## Run full local stack

A full local stack can be run using the provided Dockerfile and Docker
Compose configuration. The Dockerfile uses a multi-stage build so no
pre-compilation isrequired.

    docker-compose up --build -d

The environment can be completely removed.

    docker-compose down

## Run integration tests

Integration tests can be executed inside a Docker container to make use of
the Kafka and Hbase instances running in the local stack. The integration
tests are written in Groovy and use the Spock testing framework.

    docker-compose up --build -d
    docker-compose run --rm integration ./gradlew integration

## Getting logs

The services are listed in the `docker-compose.yaml` file and logs can be
retrieved for all services, or for a subset.

    docker-compose logs hbase

The logs can be followed so new lines are automatically shown.

    docker-compose logs -f hbase

## Getting an HBase shell

To access the HBase shell it's necessary to use a Docker container. This
can be run as a separate container.

    docker-compose run --rm hbase shell

## Configuration

There are a number of environment variables that can be used to configure
the system. Some of them are for configuring Kafka2Hbase itself, and some
are for configuring the built-in ACM PCA client to perform mutual auth.

### Kafka2Hbase Configuration

#### Hbase

By default Kafka2Hbase will connect to Zookeeper at `zookeeper:2181` and
create tables in the `k2hb` namespace. The data will be stored in `cf:data`
with at least `1` version and at most `10` versions and a TTL of 10 days.

* **K2HB_HBASE_ZOOKEEPER_QUORUM**
    Comma separated list of Zookeeper servers
* **K2HB_HBASE_ZOOKEEPER_PORT**
    The listening port of the Zookeeper servers
* **K2HB_HBASE_NAMESPACE**
    The Hbase namespace in which to create topic tables
* **K2HB_HBASE_FAMILY_NAME**
    The name of the column family to store message bodies in
* **K2HB_HBASE_QUALIFIER**
    The name of the column qualifier to store message bodies in
* **K2HB_HBASE_FAMILY_MIN_VERSIONS**
    The min versions value to use when creating the topic table
* **K2HB_HBASE_FAMILY_MAX_VERSIONS**
    The max versions value to use when creating the topic table
* **K2HB_HBASE_FAMILY_TTL**
    The TTL of versions in seconds to use when creating the topic table

#### Kafka

By default Kafka2Hbase will connect to Kafka at `kafka:9092` in the `k2hb`
consumer group. It will poll the `test-topic` topic with a poll timeout of
`10` seconds.

* **K2HB_KAFKA_BOOTSTRAP_SERVERS**
    Comma separated list of Kafka servers and ports
* **K2HB_KAFKA_CONSUMER_GROUP**
    The name of the consumer group to join
* **K2HB_KAFKA_TOPICS**
    The list of topics to listen to
* **K2HB_KAFKA_POLL_TIMEOUT**
    The maximum time to wait for messages in ISO-8601 duration format (e.g. `PT10S`)
* **K2HB_KAFKA_INSECURE**
    Disable SSL entirely (useful for dev / test)

#### SSL Mutual Authentication

By default the SSL is enabled but has no defaults. These must either be
configured in full or disabled entirely via `K2HB_KAFKA_INSECURE`.

* **CERTGEN_KEY_TYPE**
    The type of private key (`RSA` or `DSA`)
* **CERTGEN_KEY_LENGTH**
    The key length in bits (`1024`, `2048` or `4096`)
* **CERTGEN_KEY_DIGEST**
    The key digest algorithm (`sha256`, `sha384`, `sha512`)
* **CERTGEN_SUBJECT_C**
    The subject country
* **CERTGEN_SUBJECT_ST**
    The subject state/province/county
* **CERTGEN_SUBJECT_L**
    The subject locality
* **CERTGEN_SUBJECT_O**
    The subject organisation
* **CERTGEN_SUBJECT_OU**
    The subject organisational unit
* **CERTGEN_SUBJECT_EMAILADDRESS**
    The subject email address
* **CERTGEN_SIGNING_ALGORITHM**
    The certificate signing algorithm used by ACM PCA
    (`SHA256WITHECDSA`, `SHA384WITHECDSA`, `SHA512WITHECDSA`, `SHA256WITHRSA`, `SHA384WITHRSA`, `SHA512WITHRSA`)
* **CERTGEN_VALIDITY_PERIOD**
    The certificate validity period in Go style duration (e.g. `1y2m6d`)
* **CERTGEN_TRUSTSTORE_CERTS**
    Comma delimited list of S3 URIs pointing to certificates to be included in the trust store
* **CERTGEN_TRUSTSTORE_ALIASES**
    Comma delimited list of aliases for the certificate
* **CERTGEN_LOG_LEVEL**
    The log level of the certificate generator (`CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`)
