#!/bin/bash

USER="ubuntu"

BM1_HOST="iotdb2"
BM2_HOST="iotdb3"
BM3_HOST="iotdb20"
BM4_HOST="iotdb21"


# Stop benchmark
ssh $USER@$BM1_HOST "bash /home/ubuntu/stop_bm.sh"
echo "$(date): stop remote bm1"
ssh $USER@$BM2_HOST "bash /home/ubuntu/stop_bm.sh"
echo "$(date): stop remote bm2"
ssh $USER@$BM3_HOST "bash /home/ubuntu/stop_bm.sh"
echo "$(date): stop remote bm3"
ssh $USER@$BM4_HOST "bash /home/ubuntu/stop_bm.sh"
echo "$(date): stop remote bm4"