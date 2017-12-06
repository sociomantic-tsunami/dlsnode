#!/bin/sh

# Only on clean install
if [ "$1" = "configure" -a -z "$2" ]
then
    # Check if the user exists, and creates one if not
    getent passwd dlsnodereadonly > /dev/null || useradd -d /srv/dlsnode/ -s /bin/false dlsnodereadonly

    chmod 644 "/etc/init/dlsreadonly.conf"
fi

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
    systemctl enable dlsnode
fi
