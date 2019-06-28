# TestKafka2Hbase

Providing a way of migrating data in Kafka topics into tables
in Hbase, preserving versions based on Kafka message timestamps.

## Build

Ensure a JVM is installed and run the gradle wrapper.

    ./gradlew build

## Distribute

If a standard zip file is required, just use the assembleDist command.

    ./gradlew assembleDist

Otherwise if a tarball is required use the distTar command.

    ./gradlew distTar
    
## Run full local stack

A full local stack can be run using the provided Dockerfile and
Docker Compose configuration. The Dockerfile uses a multi-stage build so no pre-compilation is required.

    docker-compose up --build -d
    docker-compose run --rm kafka2hbase bin/kafka2hbase

The environment can be completely removed.

    docker-compose down