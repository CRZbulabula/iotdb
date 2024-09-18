#!/bin/bash

USER="ubuntu"

CONFIGNODE="iotdb1"
DATANODE_1="iotdb4"
DATANODE_2="iotdb5"
DATANODE_3="iotdb6"
DATANODE_4="iotdb7"
DATANODE_5="iotdb8"
DATANODE_6="iotdb9"
DATANODE_7="iotdb10"
DATANODE_8="iotdb11"
DATANODE_9="iotdb12"
DATANODE_10="iotdb13"
DATANODE_11="iotdb14"
DATANODE_12="iotdb15"
DATANODE_13="iotdb16"
DATANODE_14="iotdb17"
DATANODE_15="iotdb18"
DATANODE_16="iotdb19"

SEND_BASH="/home/ubuntu/evaluation/start_datanode.sh"

scp $SEND_BASH $USER@$DATANODE_1:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_2:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_3:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_4:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_5:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_6:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_7:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_8:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_9:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_10:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_11:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_12:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_13:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_14:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_15:/home/ubuntu/
scp $SEND_BASH $USER@$DATANODE_16:/home/ubuntu/
