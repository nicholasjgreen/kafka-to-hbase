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

ENV GRADLE "/kafka2hbase/gradlew"


# Copy the gradle wrapper
COPY gradlew .
COPY gradle/ ./gradle

# Copy the gradle config
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .

# Copy the source
COPY src/ ./src

# Generate Wrapper, install dependencies and Create DistTar
RUN $GRADLE :unit build -x test \
    && $GRADLE distTar

# Second build stage starts here
FROM openjdk:14-alpine

ARG http_proxy_full=""

# Set user to run the process as in the docker contianer
ENV USER_NAME=k2hb
ENV GROUP_NAME=k2hb

# Create group and user to execute task
RUN addgroup ${GROUP_NAME}
RUN adduser --system --ingroup ${GROUP_NAME} ${USER_NAME}

# Set environment variables for apk
ENV http_proxy=${http_proxy_full}
ENV https_proxy=${http_proxy_full}
ENV HTTP_PROXY=${http_proxy_full}
ENV HTTPS_PROXY=${http_proxy_full}

ARG VERSION
ARG DIST=kafka2hbase-$VERSION
ARG DIST_FILE=$DIST.tar

RUN echo "ENV http: ${http_proxy}" \
    && echo "ENV https: ${https_proxy}" \
    && echo "ENV HTTP: ${HTTP_PROXY}" \
    && echo "ENV HTTPS: ${HTTPS_PROXY}" \
    && echo "ARG full: ${http_proxy_full}" \
    && echo DIST_FILE: \'$DIST_FILE\'.

ENV acm_cert_helper_version 0.8.0
RUN echo "===> Installing Dependencies ..." \
    && echo "===> Updating base packages ..." \
    && apk update \
    && apk upgrade \
    && echo "==Update done==" \
    && apk add --no-cache util-linux \
    && echo "===> Installing acm_pca_cert_generator ..." \
    && apk add --no-cache g++ python3-dev libffi-dev openssl-dev gcc \
    && pip3 install --upgrade pip setuptools \
    && pip3 install https://github.com/dwp/acm-pca-cert-generator/releases/download/${acm_cert_helper_version}/acm_cert_helper-${acm_cert_helper_version}.tar.gz \
    && echo "==Dependencies done=="

COPY ./entrypoint.sh /

WORKDIR /kafka2hbase

COPY --from=build /kafka2hbase/build/distributions/$DIST_FILE .

RUN tar -xf $DIST_FILE --strip-components=1

RUN chown ${USER_NAME}:${GROUP_NAME} . -R

USER $USER_NAME
ENV APPLICATION=kafka2hbase

ENTRYPOINT ["/entrypoint.sh"]
CMD ["./bin/kafka2hbase"]
