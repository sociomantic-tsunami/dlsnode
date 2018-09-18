#!/bin/sh
set -xeu

apt update

# Prepare folder structure and install dhtnode

mkdir -p /srv/dlsnode/etc
mkdir -p /srv/dlsnode/log
mkdir -p /srv/dlsnode/data
apt install -y /packages/dlsnode_*
