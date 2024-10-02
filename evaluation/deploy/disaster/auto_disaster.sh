#!/bin/bash

USER="ubuntu"

BM1_HOST="iotdb2"
BM2_HOST="iotdb3"
BM3_HOST="iotdb20"
BM4_HOST="iotdb21"
RECOVERY_HOST="iotdb7"

# EXPERIMENT_COMBOS=(
#     "GREEDY PGR"
#     "RANDOM PGR"
#     "CFD PGR"
#     "CFD COPY_SET"
#     "CFD TIERED_REPLICATION"
#     "CFD GREEDY"
# )

# EXPERIMENT_COUNTS=(0 0 0 0 0 0)

# for ((i = 0; i < ${#EXPERIMENT_COMBOS[@]}; i++)); do
#     combo=${EXPERIMENT_COMBOS[$i]}
#     count=${EXPERIMENT_COUNTS[$i]}
    
#     leader_alg=$(echo $combo | awk '{print $1}')
#     replica_alg=$(echo $combo | awk '{print $2}')
    
#     for ((j = 0; j < $count; j++)); do
#         LEADER_ALGS+=("$leader_alg")
#         REPLICA_ALGS+=("$replica_alg")
#     done
# done

EXPERIMENT_COMBOS=(
    "CFD GREEDY"
    "CFD PGR"
    "CFD COPY_SET"
    "CFD TIERED_REPLICATION"
)

LEADER_ALGS=()
REPLICA_ALGS=()

for ((j = 0; j < 10; j++)); do
    for ((i = 0; i < ${#EXPERIMENT_COMBOS[@]}; i++)); do
        combo=${EXPERIMENT_COMBOS[$i]}
        leader_alg=$(echo $combo | awk '{print $1}')
        replica_alg=$(echo $combo | awk '{print $2}')
        LEADER_ALGS+=("$leader_alg")
        REPLICA_ALGS+=("$replica_alg")
    done
done

echo "LEADER_ALGS: ${LEADER_ALGS[@]}"
echo "REPLICA_ALGS: ${REPLICA_ALGS[@]}"

FILE_NAME="placement_disaster"
YAML_PATH="/home/ubuntu/evaluation/iotd/config/${FILE_NAME}.yaml"
echo "YAML_PATH: $YAML_PATH"

for ((j = 0; j < ${#REPLICA_ALGS[@]}; j++)); do
    leader_alg=${LEADER_ALGS[$j]}
    replica_alg=${REPLICA_ALGS[$j]}
    echo "$(date): Begin testing leader_alg=$leader_alg, replica_alg=$replica_alg"

    bash /home/ubuntu/evaluation/disaster/stop_all_bm.sh
    bash /home/ubuntu/evaluation/disaster/stop_recovery.sh
    echo "clean side effect"

    # Modify IoTDB deployment configuration
    sed -i "/leader_distribution_policy:/s/:.*/: $leader_alg/" $YAML_PATH
    sed -i "/region_group_allocate_policy:/s/:.*/: $replica_alg/" $YAML_PATH
    grep "leader_distribution_policy" $YAML_PATH
    grep "region_group_allocate_policy" $YAML_PATH
    
    # Start IoTDB
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster stop disaster
    sleep 2
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster destroy disaster
    sleep 2
    bash /home/ubuntu/evaluation/stop_all_datanodes.sh
    bash /home/ubuntu/evaluation/auto_clean.sh
    echo "$(date): clean up"
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster deploy disaster
    sleep 2
    sudo /home/ubuntu/evaluation/iotd/sbin/iotd cluster start disaster
    sleep 2
    echo "$(date): start iotdb cluster"

    # Start benchmark-1
    ssh $USER@$BM1_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm1"
    ssh $USER@$BM2_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm2"
    ssh $USER@$BM3_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm3"
    ssh $USER@$BM4_HOST "nohup bash /home/ubuntu/start_bm.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote bm4"

    # Start disaster
    ssh $USER@$RECOVERY_HOST "nohup bash /home/ubuntu/start_recovery.sh > /dev/null 2>&1 &"
    echo "$(date): start remote recovery"

    # Ensure data writing stable
    sleep 800

    # Stop benchmark-1
    bash /home/ubuntu/evaluation/disaster/stop_all_bm.sh
    echo "stop BM-1"

    # Start benchmark-2
    ssh $USER@$BM1_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm1"
    ssh $USER@$BM2_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm2"
    ssh $USER@$BM3_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm3"
    ssh $USER@$BM4_HOST "nohup bash /home/ubuntu/start_bm.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote bm4"

    sleep 1200

    # Stop benchmark-2
    bash /home/ubuntu/evaluation/disaster/stop_all_bm.sh
    echo "stop BM-2"

    sleep 2
    echo "$(date): End testing leader_alg=$leader_alg, replica_alg=$replica_alg"
done