# TestKafka2Hbase

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