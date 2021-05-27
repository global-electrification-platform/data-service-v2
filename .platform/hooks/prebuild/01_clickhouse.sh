#!/bin/bash

if [ ! -d /var/lib/clickhouse ]; then
    mkdir /var/lib/clickhouse;
fi

if [ ! -d /mnt/clickhouse ]; then
    mkdir /mnt/clickhouse;
fi

if [ ! -d /mnt/clickhouse/data ]; then
    mount /dev/sdj /mnt/clickhouse
    mount -o bind /mnt/clickhouse/data /var/lib/clickhouse
    chown -R clickhouse:clickhouse  /var/lib/clickhouse
    systemctl start clickhouse-server
fi
