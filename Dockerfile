ARG http_proxy_host=""
ARG https_proxy_host=""
ARG http_proxy_port=""
ARG https_proxy_port=""

# Multi stage docker build - stage 1 builds jar file
FROM zenika/kotlin:1.3-jdk8-slim as build

WORKDIR /kafka2hbase

# Set environment variables to pass to proxy script
ENV http_proxy_host=${http_proxy_host}
ENV https_proxy_host=${https_proxy_host}
ENV http_proxy_port=${http_proxy_port}
ENV https_proxy_port=${https_proxy_port}

# Copy proxy set script and execute it
COPY set-proxy.sh .
RUN ./set-proxy.sh

ENV GRADLE "/kafka2hbase/gradlew --no-daemon"

# Copy and generate the gradle wrapper
COPY gradlew .
COPY gradle/ ./gradle
RUN $GRADLE wrapper

# Copy the gradle config and install dependencies
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
RUN $GRADLE build

# Copy the source
COPY src/ ./src

RUN $GRADLE distTar

# Second build stage starts here
FROM openjdk:8-slim

# Set environment variables to pass to proxy script
ENV http_proxy_host=${http_proxy_host}
ENV https_proxy_host=${https_proxy_host}
ENV http_proxy_port=${http_proxy_port}
ENV https_proxy_port=${https_proxy_port}

# Copy proxy set script and execute it (needed for apt-get here)
COPY set-proxy.sh .
RUN ./set-proxy.sh

ARG VERSION=1.0-SNAPSHOT
ARG DIST=kafka2hbase-$VERSION
ARG DIST_FILE=$DIST.tar

ENV acm_cert_helper_version 0.8.0
RUN echo "===> Installing Dependencies ..." \
    && apt-get -qq update \
    && apt-get install -y gosu uuid \
    && echo "===> Installing acm_pca_cert_generator ..." \
    && apt-get install -y gcc python3-pip \
    && pip3 install https://github.com/dwp/acm-pca-cert-generator/releases/download/${acm_cert_helper_version}/acm_cert_helper-${acm_cert_helper_version}.tar.gz \
    && echo "===> Cleaning up ..."  \
    && apt-get remove -y gcc \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /tmp/* /var/lib/apt/lists/* \
    && echo "==Dependencies done=="

COPY ./entrypoint.sh /

WORKDIR /kafka2hbase

COPY --from=build /kafka2hbase/build/distributions/$DIST_FILE .

RUN tar -xf $DIST_FILE --strip-components=1

ENTRYPOINT ["/entrypoint.sh"]
CMD ["./bin/kafka2hbase"]
