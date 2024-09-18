#!/bin/bash

USER="ubuntu"

CLIENT1_HOST="iotdb2"
CLIENT2_HOST="iotdb3"
CLIENT3_HOST="iotdb20"
CLIENT4_HOST="iotdb21"

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
DATANODE_13="iotdb16"·
DATANODE_14="iotdb17"
DATANODE_15="iotdb18"
DATANODE_16="iotdb19"

given_timestamp=$(date -d "2023-11-30T00:00:00.000+08:00" +%s%3N)

# LEADER_ALGS=("CFD" "RANDOM" "GREEDY" "CFD" "CFD" "CFD")
# REPLICA_ALGS=("PGR" "PGR" "PGR" "COPY_SET" "TIERED_REPLICATION" "GREEDY")

YAML_PATH="/home/ubuntu/evaluation/iotd/config/expansion.yaml"

for ((j = 0; j < ${#REPLICA_ALGS[@]}; j++)); do
    leader_alg=${LEADER_ALGS[$j]}
    replica_alg=${REPLICA_ALGS[$j]}
    echo "$(date): Begin testing leader_alg=$leader_alg, replica_alg=$replica_alg"

    bash /home/ubuntu/evaluation/expansion/stop_all_client.sh
    echo "clean side effect"

    # 修改 IoTDB 的配置
    sed -i "/leader_distribution_policy:/s/:.*/: $leader_alg/" $YAML_PATH
    sed -i "/region_group_allocate_policy:/s/:.*/: $replica_alg/" $YAML_PATH
    grep "leader_distribution_policy" $YAML_PATH
    grep "region_group_allocate_policy" $YAML_PATH
    
    # 部署 IoTDB
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

    # 启动 1C8D
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

    # 启动 client-1
    ssh $USER@$CLIENT1_HOST "nohup bash /home/ubuntu/start_client.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote client1"
    ssh $USER@$CLIENT2_HOST "nohup bash /home/ubuntu/start_client.sh 1 > /dev/null 2>&1 &"
    echo "$(date): start remote client2"

    # 确保元数据注册完成，第一批时间分区分配完成
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

    # 确保数据写入稳定
    sleep 100

    # 关闭 client1
    bash /home/ubuntu/evaluation/expansion/stop_all_client.sh

    # 启动 client-2
    ssh $USER@$CLIENT1_HOST "nohup bash /home/ubuntu/start_client.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote client1"
    ssh $USER@$CLIENT2_HOST "nohup bash /home/ubuntu/start_client.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote client2"
    ssh $USER@$CLIENT3_HOST "nohup bash /home/ubuntu/start_client.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote client3"
    ssh $USER@$CLIENT4_HOST "nohup bash /home/ubuntu/start_client.sh 2 > /dev/null 2>&1 &"
    echo "$(date): start remote CLIENT4"

    # 确保数据写入稳定
    sleep 300

    # set ttl
    current_timestamp=$(date +%s%3N)
    ttl=$(($current_timestamp - $given_timestamp))
    bash /home/ubuntu/data/iotdb-deploy/confignode/iotdb/sbin/start-cli.sh -h 172.21.32.14 -e "set ttl to root.toyotads.** $ttl"
    echo "$(date): set ttl to $ttl"
    
    # 确保数据删除且写入稳定
    sleep 900

    # 关闭 client
    bash /home/ubuntu/evaluation/expansion/stop_all_client.sh
    echo "stop all client"

    sleep 2
    echo "$(date): End testing leader_alg=$leader_alg, replica_alg=$replica_alg"

done