#!/bin/bash

if [[ ! -z "$http_proxy" ]]; then
    echo "Acquire::http::proxy \"$http_proxy\";" > /etc/apt/apt.conf
fi