#!/bin/bash

USER="ubuntu"

CONFIGNODE="172.21.32.15"
DATANODE_1="172.21.32.4"
DATANODE_2="172.21.32.2"
DATANODE_3="172.21.32.3"
DATANODE_4="172.21.32.11"
DATANODE_5="172.21.32.14"
DATANODE_6="172.21.32.13"
DATANODE_7="172.21.32.9"
DATANODE_8="172.21.32.6"



ssh $USER@$DATANODE_1 "bash /home/ubuntu/delete.sh"
echo "clean up datanode1"
ssh $USER@$DATANODE_2 "bash /home/ubuntu/delete.sh"
echo "clean up datanode2"
ssh $USER@$DATANODE_3 "bash /home/ubuntu/delete.sh"
echo "clean up datanode3"
ssh $USER@$DATANODE_4 "bash /home/ubuntu/delete.sh"
echo "clean up datanode4"
ssh $USER@$DATANODE_5 "bash /home/ubuntu/delete.sh"
echo "clean up datanode5"
ssh $USER@$DATANODE_6 "bash /home/ubuntu/delete.sh"
echo "clean up datanode6"
ssh $USER@$DATANODE_7 "bash /home/ubuntu/delete.sh"
echo "clean up datanode7"
ssh $USER@$DATANODE_8 "bash /home/ubuntu/delete.sh"
echo "clean up datanode8"