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

ssh $USER@$DATANODE_1 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn1"
ssh $USER@$DATANODE_2 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn2"
ssh $USER@$DATANODE_3 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn3"
ssh $USER@$DATANODE_4 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn4"
ssh $USER@$DATANODE_5 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn5"
ssh $USER@$DATANODE_6 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn6"
ssh $USER@$DATANODE_7 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn7"
ssh $USER@$DATANODE_8 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn8"
ssh $USER@$DATANODE_9 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn9"
ssh $USER@$DATANODE_10 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn10"
ssh $USER@$DATANODE_11 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn11"
ssh $USER@$DATANODE_12 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn12"
ssh $USER@$DATANODE_13 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn13"
ssh $USER@$DATANODE_14 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn14"
ssh $USER@$DATANODE_15 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn15"
ssh $USER@$DATANODE_16 "bash /home/ubuntu/stop_datanode.sh"
echo "$(date): stop remote dn16"
