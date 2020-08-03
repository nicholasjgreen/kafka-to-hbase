# Kafka2Hbase

Providing a way of migrating data in Kafka topics into tables in Hbase,
preserving versions based on Kafka message timestamps.

Two columns are written to for each message received; one to store the body
of the message and one to store a count and last received date of the
topic. These are configured using the `K2HB_KAFKA_TOPIC_*` and
`K2HB_KAFKA_DATA_*` environment variables.

By default the data table is `k2hb:ingest` with a column family of `topic`.
The qualifier is the topic name, the body of the cell is the raw message
received from Kafka and the version is the timestamp of the message in
milliseconds.

Along with the data of the message a counter is kept for each topic to
indicate how many messages have been processed and when. This is useful for
creating a list of topics to process or limiting that list to only topics
that have received new data since a given time. The default table is
`k2hb:ingest-topic` and the default column is `c:msg`.

For example, after receiving a single message on `test-topic` the data
is as follows:

```
hbase(main):001:0> scan 'k2hb:ingest'
ROW                                                          COLUMN+CELL
 63213667-c5a5-4411-a93b-e2da709c553e                        column=topic:test-topic, timestamp=1563547895682, value=<message body>
1 row(s) in 0.1090 seconds

hbase(main):002:0> scan 'k2hb:ingest-topic'
ROW                                                          COLUMN+CELL
 test-topic                                                  column=c:msg, timestamp=1563547895689, value=\x00\x00\x00\x00\x00\x00\x00\x01
1 row(s) in 0.0100 seconds
```

Kafka2Hbase will attempt to create the required namespaces, tables and
column families on startup. If they already exist, nothing will happen. By
default the data table column family has a maximum of MAXINT versions
(approximately 2.1 billion) and a minimum of 1 version. There is no TTL.
The topic counter column family has no versioning or TTL.

# Local development

You will need local installs of Docker, Gradle and Kotlin, and so a JVM on at least 1.8.
The SDK-Man utility is good for package management of these.

## Makefile

A Makefile wraps some of the gradle and docker-compose commands to give a
more unified basic set of operations. These can be checked by running:

   ```
   make help
   ```

## Local Jar Build

Ensure a JVM is installed and run the gradle build.

   ```
   make local-build
   ```

## Run local unit tests

The unit tests use JUnit to run and are written using specification language.
They can be executed with the following command.

   ```
   make local-test
   ```

## Create local Distribution

If a standard zip file is required, just use the assembleDist command.
This produces a zip and a tarball of the latest version.
   ```
   make local-dist
   ```

## Build full local stack

You can build all the local images with
   ```
   make build
   ```

## Push local images into AWS DEV account

You will need to know your AWS account number, have relevant permssions and create a ECR in advance, i.e. "k2hb-test"

Then you can push to dev like this;
   ```
   make push-local-to-ecr aws_dev_account=12345678 temp_image_name=k2hb-test aws_default_region=eu-middle-3
   ```

Which does the following steps for you
   ```
   export AWS_DEV_ACCOUNT=12345678
   export TEMP_IMAGE_NAME=k2hb-test
   export AWS_DEFAULT_REGION=eu-middle-3
   aws ecr get-login-password --region ${AWS_DEFAULT_REGION} --profile dataworks-development | docker login --username AWS --password-stdin ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com
   docker tag kafka2hbase ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}
   docker push ${AWS_DEV_ACCOUNT}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com/${TEMP_IMAGE_NAME}
   ```

## Run full local stack

A full local stack can be run using the provided Dockerfile and Docker
Compose configuration. The Dockerfile uses a multi-stage build so no
pre-compilation is required.

   ```
   make up
   ```

The environment can be stopped without losing any data:

   ```
   make down
   ```

Or completely removed including all data volumes:

   ```
   make destroy
   ```

## Run integration tests

Integration tests can be executed inside a Docker container to make use of
the Kafka and Hbase instances running in the local stack. The integration
tests are written in Kotlin and use the standard `kotlintest` testing framework.

To run from a clean build:

   ```
   make integration-all
   ```

To run just the tests again with everything running

   ```
   make integration-tests
   ```

## Run in an IDE

Both Kafka2HBase and the integration tests can be run in an IDE to facilitate
quicker feedback then a containerized approach. This is useful during active development.

To do this first bring up the hbase, kafka and zookeeper containers:

   ```
   make services
   ```

On the run configuration for Kafka2Hbase set the following environment variables
(nb not system properties)

   ```
   K2HB_HBASE_ZOOKEEPER_QUORUM=localhost;K2HB_KAFKA_POLL_TIMEOUT=PT2S
   ```

And on the run configuration for the integration tests set these:

   ```
   K2HB_KAFKA_BOOTSTRAP_SERVERS=localhost:9092;K2HB_HBASE_ZOOKEEPER_QUORUM=localhost
   ```

Then insert into your local hosts file the names, IP addresses of the kafka and
hbase containers:

   ```
   ./hosts.sh
   ```

## Getting logs

The services are listed in the `docker-compose.yaml` file and logs can be
retrieved for all services, or for a subset.

   ```
   docker-compose logs hbase
   ```

The logs can be followed so new lines are automatically shown.

   ```
   docker-compose logs -f hbase
   ```

## Getting an HBase shell

To access the HBase shell it's necessary to use a Docker container. This
can be run as a separate container.

   ```
   make hbase-shell
   ```

## Configuration

There are a number of environment variables that can be used to configure
the system. Some of them are for configuring Kafka2Hbase itself, and some
are for configuring the built-in ACM PCA client to perform mutual auth.

### Kafka2Hbase Configuration

#### Hbase

By default Kafka2Hbase will connect to Zookeeper at `zookeeper:2181` use the parent uri `hbase`
and create tables in the `k2hb` namespace. The data will be stored in `cf:data`
with at least `1` version and at most `10` versions and a TTL of 10 days.

* **K2HB_HBASE_ZOOKEEPER_PARENT**
    The hbase parent uri, defaults to `/hbase` but should be set to `/hbase-unsecure` for AWS HBase
* **K2HB_HBASE_ZOOKEEPER_QUORUM**
    Comma separated list of Zookeeper servers
* **K2HB_HBASE_ZOOKEEPER_PORT**
    The listening port of the Zookeeper servers
* **K2HB_HBASE_DATA_TABLE**
    The name of the table to store message bodies in
* **K2HB_HBASE_DATA_FAMILY**
    The name of the column family to store message bodies in
* **K2HB_HBASE_TOPIC_TABLE**
    The name of the table to store topic message counts in
* **K2HB_HBASE_TOPIC_FAMILY**
    The name of the column family to store topic message counts in
* **K2HB_HBASE_TOPIC_QUALIFIER**
    The name of the column qualifier to store topic message counts in

#### Kafka

By default Kafka2Hbase will connect to Kafka at `kafka:9092` in the `k2hb`
consumer group. It will poll the `test-topic` topic with a poll timeout of
`10` days, and refresh the topics list every 10 seconds (`10000` ms).

* **K2HB_KAFKA_BOOTSTRAP_SERVERS**
    Comma separated list of Kafka servers and ports
* **K2HB_KAFKA_CONSUMER_GROUP**
    The name of the consumer group to join
* **K2HB_KAFKA_TOPIC_REGEX**
    A regex that will fetch a list of topics to listen to, e.g. `db.*`. Defaults to `test-topic.*`
* **K2HB_KAFKA_META_REFRESH_MS** (Optional)
    The frequency that the consumer will ask the broker for metadata updates, which also checks for new topics.
    Defaults to `10000` ms (10 seconds).
    Typically, should be an order of magnitude less than `K2HB_KAFKA_POLL_TIMEOUT`, else new topics will not be discovered within each polling interval.
* **K2HB_KAFKA_POLL_TIMEOUT**
    The maximum time to wait for messages in ISO-8601 duration format (e.g. `PT10S`).
    Defaults to 1 Hour.
    Should be greater than `K2HB_KAFKA_META_REFRESH_MS`, else new topics will not be discovered within each polling interval.
* **K2HB_KAFKA_INSECURE**
    Disable SSL entirely (useful for dev / test) with `K2HB_KAFKA_INSECURE=true`
* **K2HB_KAFKA_CERT_MODE**
    If SSL is enabled, either create certs in ACM-PCA with value `CERTGEN` or retrieve
    them from ACM with value `RETRIEVE`

#### SSL Mutual Authentication (CERTGEN mode)

By default the SSL is enabled but has no defaults. These must either be
configured in full or disabled entirely via `K2HB_KAFKA_INSECURE=FALSE`
and `K2HB_KAFKA_CERT_MODE=CERTGEN`.

For an authoritative full list of arguments see the tool help; Arguments not listed here are
defaulted in the `entrypoint.sh` script.

* **CERTGEN_CA_ARN**
    The AWS CA ARN to use to generate the cert
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
* **CERTGEN_PRIVATE_KEY_ALIAS**
    Alias for the private key
* **CERTGEN_TRUSTSTORE_CERTS**
    Comma delimited list of S3 URIs pointing to certificates to be included in the trust store
* **CERTGEN_TRUSTSTORE_ALIASES**
    Comma delimited list of aliases for the certificate
* **CERTGEN_LOG_LEVEL**
    The log level of the certificate generator (`CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`)


#### SSL Mutual Authentication (RETRIEVE mode)

By default the SSL is enabled but has no defaults. These must either be
configured in full or disabled entirely via `K2HB_KAFKA_INSECURE=FALSE`
and `K2HB_KAFKA_CERT_MODE=RETRIEVE`.

For an authoritative full list of arguments see the tool help; Arguments not listed here are
defaulted in the `entrypoint.sh` script.

* **RETRIEVER_ACM_CERT_ARN**
    ARN in AWS ACM to use to fetch the required cert, cert chain, and key
* **RETRIEVER_ADD_DOWNLOADED_CHAIN**
    Whether or not to add the downloaded cert chain from the ARN to the trust store
    Allowed missing, `true`, `false`, `yes`, `no`, `1` or `0`
    If missing defaults to false
* **RETRIEVE_TRUSTSTORE_CERTS**
    Comma delimited list of S3 URIs pointing to certificates to be included in the trust store
* **RETRIEVE_TRUSTSTORE_ALIASES**
    Comma delimited list of aliases for the certificate
* **RETRIEVE_LOG_LEVEL**
    The log level of the certificate generator (`CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`)
