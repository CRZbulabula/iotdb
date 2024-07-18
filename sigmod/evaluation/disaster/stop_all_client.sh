#!/bin/bash

USER="ubuntu"

CLIENT1_HOST="iotdb2"
CLIENT2_HOST="iotdb3"
CLIENT3_HOST="iotdb20"
CLIENT4_HOST="iotdb21"


# 关闭 client
ssh $USER@$CLIENT1_HOST "bash /home/ubuntu/stop_client.sh"
echo "$(date): stop remote client1"
ssh $USER@$CLIENT2_HOST "bash /home/ubuntu/stop_client.sh"
echo "$(date): stop remote client2"
ssh $USER@$CLIENT3_HOST "bash /home/ubuntu/stop_client.sh"
echo "$(date): stop remote client3"
ssh $USER@$CLIENT4_HOST "bash /home/ubuntu/stop_client.sh"
echo "$(date): stop remote client4"