#!/bin/bash

if [[ ! -z "$http_proxy_host" ]]; then
    if [ -d "/etc/apt" ]; then
        echo "Acquire::http::proxy \"$http_proxy_host:$http_proxy_port\";" >> /etc/apt/apt.conf
    fi
    export http_proxy="$http_proxy_host:$http_proxy_port"
    export HTTP_PROXY="$http_proxy_host:$http_proxy_port"
    export GRADLE_OPTS="$GRADLE_OPTS -Dhttp.proxyHost=$http_proxy_host -Dhttp.proxyPort=$http_proxy_port"
fi

if [[ ! -z "$https_proxy_host" ]]; then
    if [ -d "/etc/apt" ]; then
        echo "Acquire::https::proxy \"$https_proxy_host:$https_proxy_port\";" >> /etc/apt/apt.conf
    fi
    export https_proxy="$https_proxy_host:$https_proxy_port"
    export HTTPS_PROXY="$http_proxy_host:$http_proxy_port"
    export GRADLE_OPTS="$GRADLE_OPTS -Dhttps.proxyHost=$https_proxy_host -Dhttps.proxyPort=$https_proxy_port"
fi