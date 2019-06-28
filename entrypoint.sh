#!/bin/bash

set -e

: ${SUID:=1000}
: ${SGID:=1000}

if ! getent passwd user > /dev/null
then
    groupadd --non-unique --gid "$SGID" user
    useradd --create-home --no-user-group --non-unique --uid "$SUID" --gid "$SGID" user
fi

exec gosu user "${@}"