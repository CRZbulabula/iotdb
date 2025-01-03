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

EXPERIMENT_COMBOS=(
    "CFD ROUND_ROBIN 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "CFD COPY_SET 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "CFD TIERED_REPLICATION 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "CFD GEMINI 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "CFD HYDRA 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "CFD PGP 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "RANDOM PGP 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "GREEDY PGP 0 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "LOGSTORE PGP 60 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
    "ESDB PGP 60 960 org.apache.iotdb.consensus.iot.FastIoTConsensus"
)

LEADER_ALGS=()
REPLICA_ALGS=()
DYNAMIC_ALGS=()
SLOT_ALGS=()
CONSENSUS_ALGS=()

for ((j = 0; j < 100; j++)); do
    for ((i = 0; i < ${#EXPERIMENT_COMBOS[@]}; i++)); do
        combo=${EXPERIMENT_COMBOS[$i]}
        leader_alg=$(echo $combo | awk '{print $1}')
        replica_alg=$(echo $combo | awk '{print $2}')
        dynamic_alg=$(echo $combo | awk '{print $3}')
        slot_alg=$(echo $combo | awk '{print $4}')
        consensus_alg=$(echo $combo | awk '{print $5}')
        LEADER_ALGS+=("$leader_alg")
        REPLICA_ALGS+=("$replica_alg")
        DYNAMIC_ALGS+=("$dynamic_alg")
        SLOT_ALGS+=("$slot_alg")
        CONSENSUS_ALGS+=("$consensus_alg")
    done
done

echo "LEADER_ALGS: ${LEADER_ALGS[@]}"
echo "REPLICA_ALGS: ${REPLICA_ALGS[@]}"
echo "DYNAMIC_ALGS: ${DYNAMIC_ALGS[@]}"
echo "SLOT_ALGS: ${SLOT_ALGS[@]}"
echo "CONSENSUS_ALGS: ${CONSENSUS_ALGS[@]}"

FILE_NAME="expansion"
YAML_PATH="/home/ubuntu/evaluation/iotd/config/${FILE_NAME}.yaml"
echo "YAML_PATH: $YAML_PATH"

for ((j = 0; j < ${#REPLICA_ALGS[@]}; j++)); do
    leader_alg=${LEADER_ALGS[$j]}
    replica_alg=${REPLICA_ALGS[$j]}
    dynamic_alg=${DYNAMIC_ALGS[$j]}
    slot_alg=${SLOT_ALGS[$j]}
    consensus_alg=${CONSENSUS_ALGS[$j]}
    echo "$(date): Begin testing leader_alg=$leader_alg, replica_alg=$replica_alg, dynamic_alg=$dynamic_alg, slot_alg=$slot_alg, consensus_alg=$consensus_alg"

    bash /home/ubuntu/evaluation/expansion/stop_ttl.sh
    bash /home/ubuntu/evaluation/expansion/stop_all_bm.sh
    bash /home/ubuntu/evaluation/disaster/stop_all_power.sh
    bash /home/ubuntu/evaluation/disaster/start_all_power.sh
    echo "clean side effect"

    # Modify IoTDB configurations
    sed -i "/leader_distribution_policy:/s/:.*/: $leader_alg/" $YAML_PATH
    sed -i "/region_group_allocate_policy:/s/:.*/: $replica_alg/" $YAML_PATH
    sed -i "/data_region_consensus_protocol_class:/s/:.*/: $consensus_alg/" $YAML_PATH
    sed -i "/series_slot_num:/s/:.*/: $slot_alg/" $YAML_PATH
    grep "leader_distribution_policy" $YAML_PATH
    grep "region_group_allocate_policy" $YAML_PATH
    grep "data_region_consensus_protocol_class" $YAML_PATH
    grep "series_slot_num" $YAML_PATH
    sed -i "/dynamic_leader_balancing_cycle:/s/:.*/: $dynamic_alg/" $YAML_PATH
    grep "dynamic_leader_balancing_cycle" $YAML_PATH
    sed -i "/time_partition_interval:/s/:.*/: 86400000/" $YAML_PATH
    grep "time_partition_interval" $YAML_PATH
    
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

    # Start BM-1
    ssh $USER@$BM1_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm1"
    ssh $USER@$BM2_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm2"

    # Set ttl
    nohup bash /home/ubuntu/evaluation/expansion/start_ttl.sh > /dev/null 2>&1 &
    echo "$(date): start ttl"

    # Simulate before expansion
    sleep 900
    
    # Expand 8D
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

    # Close BM1
    bash /home/ubuntu/evaluation/expansion/stop_all_bm.sh

    # Start BM-2 to double the write load
    ssh $USER@$BM1_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm1"
    ssh $USER@$BM2_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm2"
    ssh $USER@$BM3_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm3"
    ssh $USER@$BM4_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm4"
    
    # Ensure new Regions are created
    sleep 100

    # Trigger migration when necessary
    # if [ $replica_alg == "AEROSPIKE" ]; then
    #     nohup bash /home/ubuntu/evaluation/expansion/trigger_migration.sh > /dev/null 2>&1 &
    #     echo "$(date): start migration"
    # fi
    # nohup bash /home/ubuntu/evaluation/expansion/trigger_migration.sh > /dev/null 2>&1 &
    # echo "$(date): start migration"

    # Simulate after expansion
    sleep 1400
    # sleep 1200

    # Close BM
    bash /home/ubuntu/evaluation/expansion/stop_all_bm.sh
    echo "stop all bm"

    # Close ttl
    bash /home/ubuntu/evaluation/expansion/stop_ttl.sh
    echo "stop ttl"

    sleep 2
    echo "$(date): End testing leader_alg=$leader_alg, replica_alg=$replica_alg"

done