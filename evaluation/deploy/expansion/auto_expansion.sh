#!/bin/bash

USER="ubuntu"

BM1_HOST="iotdb2"
BM2_HOST="iotdb3"
BM3_HOST="iotdb20"
BM4_HOST="iotdb21"

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

given_timestamp=$(date -d "2023-11-30T00:00:00.000+08:00" +%s%3N)

# LEADER_ALGS=("CFD" "RANDOM" "GREEDY" "CFD" "CFD" "CFD")
# REPLICA_ALGS=("PGR" "PGR" "PGR" "COPY_SET" "TIERED_REPLICATION" "GREEDY")

# LEADER_ALGS=("GREEDY" "GREEDY" "GREEDY" "GREEDY" "GREEDY" "GREEDY" "CFD" "CFD" "CFD")
# REPLICA_ALGS=("PGR" "PGR" "PGR" "PGR" "PGR" "PGR" "PGR" "PGR" "PGR")
LEADER_ALGS=("RANDOM" "RANDOM" "RANDOM")
REPLICA_ALGS=("PGR" "PGR" "PGR")

YAML_PATH="/home/ubuntu/evaluation/iotd/config/expansion.yaml"

for ((j = 0; j < ${#REPLICA_ALGS[@]}; j++)); do
    leader_alg=${LEADER_ALGS[$j]}
    replica_alg=${REPLICA_ALGS[$j]}
    echo "$(date): Begin testing leader_alg=$leader_alg, replica_alg=$replica_alg"

    bash /home/ubuntu/evaluation/expansion/stop_all_bm.sh
    echo "clean side effect"

    # Modify IoTDB deployment configurations
    sed -i "/leader_distribution_policy:/s/:.*/: $leader_alg/" $YAML_PATH
    sed -i "/region_group_allocate_policy:/s/:.*/: $replica_alg/" $YAML_PATH
    grep "leader_distribution_policy" $YAML_PATH
    grep "region_group_allocate_policy" $YAML_PATH 
    
    # Deploy IoTDB
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster stop expansion
    sleep 2
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster destroy expansion
    sleep 2
    bash /home/ubuntu/evaluation/stop_all_datanodes.sh
    bash /home/ubuntu/evaluation/auto_clean.sh
    echo "$(date): clean up"
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster deploy expansion
    sleep 2
    echo "$(date): deploy iotdb cluster"

    # Start 1C8D
    nohup bash /home/ubuntu/evaluation/start_confignode.sh > /dev/null 2>&1 &
    echo "$(date): start remote confignode"
    ssh $USER@$DATANODE_1 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode1"
    ssh $USER@$DATANODE_2 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode2"
    ssh $USER@$DATANODE_3 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode3"
    ssh $USER@$DATANODE_4 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode4"
    ssh $USER@$DATANODE_5 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode5"
    ssh $USER@$DATANODE_6 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode6"
    ssh $USER@$DATANODE_7 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode7"
    ssh $USER@$DATANODE_8 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode8"
    sleep 10
    echo "$(date): start 1C8D"

    # Start benchmark-1
    ssh $USER@$BM1_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm1"
    ssh $USER@$BM2_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm2"

    # Ensure the first time partitions are allocated
    sleep 700
    
    # expansion 8D
    ssh $USER@$DATANODE_9 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode9"
    ssh $USER@$DATANODE_10 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode10"
    ssh $USER@$DATANODE_11 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode11"
    ssh $USER@$DATANODE_12 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode12"
    ssh $USER@$DATANODE_13 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode13"
    ssh $USER@$DATANODE_14 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode14"
    ssh $USER@$DATANODE_15 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode15"
    ssh $USER@$DATANODE_16 "nohup bash /home/ubuntu/start_datanode.sh > /dev/null 2>&1 &"
    echo "$(date): start remote datanode16"
    sleep 10
    echo "$(date): start 8D"

    # Ensure writing stable
    sleep 100

    # Stop benchmark-1
    bash /home/ubuntu/evaluation/expansion/stop_all_bm.sh

    # Start benchmark-2
    ssh $USER@$BM1_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm1"
    ssh $USER@$BM2_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm2"
    ssh $USER@$BM3_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm3"
    ssh $USER@$BM4_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm4"

    # Ensure stable
    sleep 300

    # set ttl
    current_timestamp=$(date +%s%3N)
    ttl=$(($current_timestamp - $given_timestamp))
    bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.14 -e "set ttl to root.toyotads.** $ttl"
    echo "$(date): set ttl to $ttl"
    
    # Ensure TTL elimination
    sleep 900

    # Stop benchmark
    bash /home/ubuntu/evaluation/expansion/stop_all_bm.sh
    echo "stop all bm"

    sleep 2
    echo "$(date): End testing leader_alg=$leader_alg, replica_alg=$replica_alg"

done