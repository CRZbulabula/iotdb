<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

The source code mentioned in our paper are listed as follows: 

+ Series partition operator: The url of our series partition operator is: https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/partition/executor/SeriesPartitionExecutor.java. Please find the default hash operators' implementations in the same folder.
+ Time partition operator: The url of our time partition operator is: https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/utils/TimePartitionUtils.java 
+ Partition allocation strategy: The url of our partition allocation strategy is: https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/partition/DataPartitionPolicyTable.java
+ Replica placement algorithms: The url of our partite graph placement (PGP) algorithm is: https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/region/PartiteGraphReplicationRegionGroupAllocator.java. Please find the alternative algorithms under the same folder.
+ Leader selection algorithms: The url of our cost flow selection (CFS) algorithm is: https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/router/leader/MinCostFlowLeaderBalancer.java. Please find the alternative algorithms under the same folder.

All the aforementioned source codes except those alternative algorithms could be found at the Apache IoTDB repository: https://github.com/apache/iotdb:

+ Series partition operator: The url of our series partition operator is: https://github.com/apache/iotdb/blob/master/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/partition/executor/SeriesPartitionExecutor.java
+ Time partition operator: The url of our time partition operator is: https://github.com/apache/iotdb/blob/master/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/utils/TimePartitionUtils.java
+ Partition allocation strategy: The url of our partition allocation strategy is: https://github.com/apache/iotdb/blob/master/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/partition/DataPartitionPolicyTable.java
+ Replica placement algorithms: The url of our partite graph placement (PGP) algorithm is: https://github.com/apache/iotdb/blob/master/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/region/PartiteGraphReplicationRegionGroupAllocator.java
+ Leader selection algorithms: The url of our cost flow selection (CFS) algorithm is: https://github.com/apache/iotdb/blob/master/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/router/leader/MinCostFlowLeaderBalancer.java

Under the evaluation folder, which locates at the same folder as README file, is our evaluation scripts:

+ client folder: scripts that start and stop the write client
+ datanode folder: scripts that start, stop and restart the nodes
+ deploy folder: configuration files and scripts that deploy the IoTDB cluster
+ simulate-workload folder: write client java classes

To reproduce the evaluation, please follow the README file in the Apache IoTDB repository (https://github.com/apache/iotdb) to build and deploy IoTDB.

