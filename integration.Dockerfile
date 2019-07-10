FROM zenika/kotlin:1.3-jdk8-slim as build

WORKDIR /kafka2hbase

ENV GRADLE "/kafka2hbase/gradlew --no-daemon"

# Copy and generate the gradle wrapper
COPY gradlew .
COPY gradle/ ./gradle
RUN $GRADLE wrapper

# Copy the gradle config and install dependencies
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
RUN $GRADLE build && $GRADLE test

RUN apt-get update && apt-get install -y gosu && rm -rf /var/lib/apt/lists/*

COPY . .

ENTRYPOINT ["./entrypoint.sh"]