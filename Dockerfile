# Multi stage docker build - stage 1 builds jar file
FROM zenika/kotlin:1.3-jdk8-slim as build

ARG http_proxy_host=""
ARG http_proxy_port=""

WORKDIR /kafka2hbase

# Set gradle proxy
ENV GRADLE_OPTS="${GRADLE_OPTS} -Dhttp.proxyHost=$http_proxy_host -Dhttp.proxyPort=$http_proxy_port"
ENV GRADLE_OPTS="${GRADLE_OPTS} -Dhttps.proxyHost=$http_proxy_host -Dhttps.proxyPort=$http_proxy_port"

RUN echo "ENV gradle: ${GRADLE_OPTS}" \
    && echo "ARG host: ${http_proxy_host}" \
    && echo "ARG port: ${http_proxy_port}"

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
FROM openjdk:8-alpine

ARG http_proxy_full=""

# Set user to run the process as in the docker contianer
ENV USER_NAME=user
ENV GROUP_NAME=usergroup

# Set environment variables for apk
ENV http_proxy=${http_proxy_full}
ENV https_proxy=${http_proxy_full}
ENV HTTP_PROXY=${http_proxy_full}
ENV HTTPS_PROXY=${http_proxy_full}

ARG VERSION=1.0-SNAPSHOT
ARG DIST=kafka2hbase-$VERSION
ARG DIST_FILE=$DIST.tar

RUN echo "ENV http: ${http_proxy}" \
    && echo "ENV https: ${https_proxy}" \
    && echo "ENV HTTP: ${HTTP_PROXY}" \
    && echo "ENV HTTPS: ${HTTPS_PROXY}" \
    && echo "ARG full: ${http_proxy_full}"

ENV acm_cert_helper_version 0.8.0
# Note you get an error when trying to add gosu and uuid in the same line
RUN echo "===> Installing Dependencies ..." \
    && apk update \
    && apk add --no-cache --virtual gosu \
    && apk add --no-cache --virtual uuid \
    && echo "===> Installing acm_pca_cert_generator ..." \
    && apk add --no-cache g++ python3-dev libffi-dev openssl-dev gcc su-exec \
    && pip3 install --upgrade pip setuptools \
    && pip3 install https://github.com/dwp/acm-pca-cert-generator/releases/download/${acm_cert_helper_version}/acm_cert_helper-${acm_cert_helper_version}.tar.gz \
    && echo "==Dependencies done=="

COPY ./entrypoint.sh /

WORKDIR /kafka2hbase

COPY --from=build /kafka2hbase/build/distributions/$DIST_FILE .

RUN tar -xf $DIST_FILE --strip-components=1

ENTRYPOINT ["/entrypoint.sh"]
CMD ["./bin/kafka2hbase"]
