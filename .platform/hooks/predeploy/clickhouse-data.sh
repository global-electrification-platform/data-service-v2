#!/bin/sh

# Note, this slaps in a new database backup each time we deploy, interrupting clickhouse for a bit
# this means that if we want to do seamless deploys, we're going to need to do rolling deploys or similar,
# because this is going to kill the connection to the existing server.
# alternately, we can write a flag into the backup and check to see if we're downloading a new one
# and only do the deploy here if the version is new.
# But we do have to at least try this each time, or we're going to get it into
# a state where a new one comes up and then we don't have our data.

echo "stopping clickhouse, violently"
sudo killall -KILL clickhouse-server
echo "cleaning directory"
rm -r /var/lib/clickhouse/*
echo "untarring files"
sudo -u clickhouse tar -C /var/lib/clickhouse/ -zxvf /tmp/cs-backup.tar.gz
echo "restarting server"
systemctl start clickhouse-server
