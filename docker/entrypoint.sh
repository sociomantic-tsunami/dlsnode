#!/bin/sh
set -e

if [ -z "$CREDENTIALS" ]; then
    echo "Must defined CREDENTIALS env var in form of client_name:key"
    exit 1
fi;

echo $CREDENTIALS > etc/credentials

dlsnode
