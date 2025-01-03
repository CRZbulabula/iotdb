#!/bin/bash

bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.8 -e "migrate regions"
echo "$(date): start auto migration"