#!/bin/bash

if [ ! -d /var/lib/clickhouse ]; then
    mkdir /var/lib/clickhouse;
fi

if [ ! -d /mnt/clickhouse ]; then
    mkdir /mnt/clickhouse;
fi

if [ ! -d /mnt/clickhouse/data ]; then
    sudo /usr/bin/clickhouse stop || true
    mount /dev/sdj /mnt/clickhouse
    mount -o bind /mnt/clickhouse/data /var/lib/clickhouse
    chown -R clickhouse:clickhouse  /var/lib/clickhouse
    sudo /usr/bin/clickhouse restart
fi
