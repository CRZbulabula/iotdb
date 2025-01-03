#!/bin/bash

given_timestamp=$(date -d "2023-01-01T00:00:00.000+08:00" +%s%3N)

sleep 600
# sleep 900

# Initialize TTL
current_timestamp=$(date +%s%3N)
ttl=$(($current_timestamp - $given_timestamp))
# bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.46 -e "set ttl to root.toyotads.** $ttl"
bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.46 -e "set ttl to root.** $ttl"
echo "$(date): set ttl to $ttl"

cnt=0
while true; do
    sleep 60
    cnt=$(($cnt + 1))
    current_timestamp=$(date +%s%3N)
    ttl=$(($current_timestamp - $given_timestamp - $cnt * 86400000))
    # bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.46 -e "set ttl to root.toyotads.** $ttl"
    bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.46 -e "set ttl to root.** $ttl"
    echo "$(date): set ttl to $ttl"
done