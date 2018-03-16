#!/bin/sh

# Only on clean install
if [ "$1" = "configure" -a -z "$2" ]
then
    # Check if the user:group exists, and creates one if not
    getent group core > /dev/null || groupadd core
    getent passwd dlsnode > /dev/null || useradd -d /srv/dlsnode/ -g core -s /bin/false dlsnode

    mkdir -p "/srv/dlsnode/data"
    mkdir -p "/srv/dlsnode/log"

    # Don't use -R for data (and root) directory,
    # since it normally consists of millions of files,
    # taking a long time to complete.
    chown dlsnode:core "/srv/dlsnode/" \
        "/srv/dlsnode/data"

    chown -R dlsnode:core "/srv/dlsnode/etc" \
        "/srv/dlsnode/log"

    chmod 644 "/etc/init/dls.conf" \
        "/etc/cron.d/compress_dls_data" \
        "/etc/logrotate.d/dlsnode-logs"
fi

if [ -d /run/systemd/system ]; then
    systemctl enable dlsnode
    systemctl daemon-reload
fi
