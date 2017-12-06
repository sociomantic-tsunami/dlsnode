#!/bin/sh

# Only on clean install
if [ "$1" = "configure" -a -z "$2" ]
then
    # Check if the user exists, and creates one if not
    getent passwd dlsnodereadonly > /dev/null || useradd -d /srv/dlsnode/ -g dlsnode -s /bin/false dlsnodereadonly

    chmod 644 "/etc/init/dlsreadonly.conf"
    mkdir -p "/srv/dlsnode/readonly/etc/"

    mkdir -p "/srv/dlsnode/readonly/log/"
    mkdir -p "/srv/dlsnode/readonly/etc/"
    chown dlsnodereadonly:dlsnode "/srv/dlsnode/readonly/log"
fi
