FROM zenika/kotlin:1.3-jdk8-slim as build

WORKDIR /kafka2hbase

# Copy and generate the gradle wrapper
COPY gradlew .
COPY gradle/ ./gradle
RUN ./gradlew --no-daemon wrapper

# Copy the gradle config and install dependencies
COPY build.gradle.kts .
COPY settings.gradle .
COPY gradle.properties .
RUN ./gradlew --no-daemon build

# Copy the source
COPY src/ ./src

RUN ./gradlew --no-daemon distTar

FROM openjdk:8

ARG VERSION=1.0-SNAPSHOT
ARG DIST=kafka2hbase-$VERSION
ARG DIST_FILE=$DIST.tar

COPY --from=build /kafka2hbase/build/distributions/$DIST_FILE .

RUN tar -xf $DIST_FILE

WORKDIR $DIST

CMD ["./bin/kafka2hbase"]