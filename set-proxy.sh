#!/bin/bash

if [[ ! -z "$http_proxy" ]]; then
    echo "Acquire::http::Proxy $http_proxy;" > /etc/apt/apt.conf
fi