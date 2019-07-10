#!/bin/bash

set -e

# Create a user to run the process as instead of root

: ${SUID:=1000}
: ${SGID:=1000}

if ! getent passwd user > /dev/null
then
    groupadd --non-unique --gid "$SGID" user
    useradd --create-home --no-user-group --non-unique --uid "$SUID" --gid "$SGID" user
fi

# If a proxy is requested, set it up

if [ "${INTERNET_PROXY}" ]; then
  export http_proxy="http://${INTERNET_PROXY}:3128"
  export https_proxy="https://${INTERNET_PROXY}:3128"
  export no_proxy=169.254.169.254
  echo "Using proxy ${INTERNET_PROXY}"
fi

# Generate a cert for Kafka mutual auth

if [[ ${K2HB_KAFKA_INSECURE} != "true" ]]
then
    echo "Generating cert for host ${HOSTNAME}"

    SSL_DIR="$(mktemp -d)"

    export K2HB_PRIVATE_KEY_PASSWORD="$(uuid -v4)"

    export K2HB_KEYSTORE_PATH="${SSL_DIR}/k2hb.keystore"
    export K2HB_KEYSTORE_PASSWORD="$(uuid -v4)"

    export K2HB_TRUSTSTORE_PATH="${SSL_DIR}/k2hb.truststore"
    export K2HB_TRUSTSTORE_PASSWORD="$(uuid -v4)"

    acm-pca-cert-generator \
        --subject-cn "${HOSTNAME}" \
        --keystore-path "${K2HB_KEYSTORE_PATH}" \
        --keystore-password "${K2HB_KEYSTORE_PASSWORD}" \
        --private-key-password "${K2HB_PRIVATE_KEY_PASSWORD}" \
        --truststore-path "${K2HB_TRUSTSTORE_PATH}" \
        --truststore-password "${K2HB_TRUSTSTORE_PASSWORD}"

    echo "Cert generation result for ${HOSTNAME} is: $?"
else
    echo "Skipping cert generation for host ${HOSTNAME}"
fi

chown ${SUID}:${SGID} . -R
exec gosu user "${@}"
