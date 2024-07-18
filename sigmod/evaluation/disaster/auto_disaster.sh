#!/bin/bash

USER="ubuntu"

CLIENT1_HOST="iotdb2"
CLIENT2_HOST="iotdb3"
CLIENT3_HOST="iotdb20"
CLIENT4_HOST="iotdb21"
RECOVERY_HOST="iotdb7"

LEADER_ALGS=("CFD" "RANDOM" "GREEDY" "CFD" "CFD" "CFD")
REPLICA_ALGS=("PGR" "PGR" "PGR" "COPY_SET" "TIERED_REPLICATION" "GREEDY")


YAML_PATH="/home/ubuntu/evaluation/iotd/config/disaster.yaml"

for ((j = 0; j < ${#REPLICA_ALGS[@]}; j++)); do
    leader_alg=${LEADER_ALGS[$j]}
    replica_alg=${REPLICA_ALGS[$j]}
    echo "$(date): Begin testing leader_alg=$leader_alg, replica_alg=$replica_alg"

    bash /home/ubuntu/evaluation/disaster/stop_all_client.sh
    bash /home/ubuntu/evaluation/disaster/stop_recovery.sh
    echo "clean side effect"

    # 修改 IoTDB 的配置
    sed -i "/leader_distribution_policy:/s/:.*/: $leader_alg/" $YAML_PATH
    sed -i "/region_group_allocate_policy:/s/:.*/: $replica_alg/" $YAML_PATH
    grep "leader_distribution_policy" $YAML_PATH
    grep "region_group_allocate_policy" $YAML_PATH
    
    # 启动 IoTDB
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

    # 启动 client-1
    ssh $USER@$CLIENT1_HOST "nohup bash /home/ubuntu/start_client.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote client1"
    ssh $USER@$CLIENT2_HOST "nohup bash /home/ubuntu/start_client.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote client2"

    # 启动 disaster
    ssh $USER@$RECOVERY_HOST "nohup bash /home/ubuntu/start_recovery.sh > /dev/null 2>&1 &"
    echo "$(date): start remote recovery"

    # 确保数据写入稳定
    sleep 800

    # 关闭 client
    bash /home/ubuntu/evaluation/disaster/stop_all_client.sh
    echo "stop client-1"

    # 启动 client-2
    ssh $USER@$CLIENT3_HOST "nohup bash /home/ubuntu/start_client.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote client1"
    ssh $USER@$CLIENT4_HOST "nohup bash /home/ubuntu/start_client.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote client2"

    sleep 1200

    # 关闭 client
    bash /home/ubuntu/evaluation/disaster/stop_all_client.sh
    echo "stop client-2"

    sleep 2
    echo "$(date): End testing leader_alg=$leader_alg, replica_alg=$replica_alg"
done