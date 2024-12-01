#!/bin/bash

USER="ubuntu"

RECOVERY_HOST="iotdb7"


# Stop recovery
ssh $USER@$RECOVERY_HOST "bash /home/ubuntu/stop_recovery.sh"
echo "$(date): stop remote recovery"