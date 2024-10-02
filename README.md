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

+ Series partition operator: This is the [executor](https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/partition/executor/SeriesPartitionExecutor.java) that organize different operators. Please find the default hash operators' implementations in the same folder.
+ Time partition operator: Our [operator](https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/utils/TimePartitionUtils.java). 
+ Partition allocation strategy: Our [strategy](https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/partition/DataPartitionPolicyTable.java).
+ Replica placement algorithms: Our algorithm is the [PGP](https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/region/PartiteGraphPlacementRegionGroupAllocator.java). Please find the alternative algorithms under the same folder.
+ Leader selection algorithms: Our algorithm is the [CFS](https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/router/leader/CostFlowSelectionLeaderBalancer.java). Please find the alternative algorithms under the same folder.

All the aforementioned source codes except those alternative algorithms could be found at the Apache IoTDB [repository](https://github.com/apache/iotdb). The following links are the root folder of the corresponding modules:

+ [Data Partitioning](https://github.com/apache/iotdb/tree/master/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/partition)
+ [Partition Allocation](https://github.com/apache/iotdb/tree/master/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/partition)
+ [Replica Placement](https://github.com/apache/iotdb/tree/master/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/region)
+ [Leader Selection](https://github.com/apache/iotdb/tree/master/iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/manager/load/balancer/router/leader)

Under the evaluation folder, which locates at the same folder as README file, is our evaluation scripts:

+ client folder: scripts that start and stop the write client
+ datanode folder: scripts that start, stop and restart the nodes
+ deploy folder: configuration files and scripts that deploy the IoTDB cluster
+ simulate-workload folder: write client java classes

To reproduce the evaluation, please:

* Prepare adequate servers and proper dataset.
* Distribute the client script `/evaluation/client` and your dataset to the client servers.
* Select a server as the controller, then:
  * Follow [this](https://zhuanlan.zhihu.com/p/601908903) to build and deploy both Prometheus and Grafana;
  * Collect necessary Prometheus metrics in [IoTDB](https://github.com/CRZbulabula/iotdb/blob/migration-free-elastic-storage/iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/service/metric/enums/Metric.java) and present them in your Grafana dashboard;
  * Run `bash install /evaluation/deploy/iotd/install-iotdbctl.sh` to install the IoTDB deployment tool.
* Follow [this](https://github.com/apache/iotdb) to build this repository, and:
  * Move `/iotdb/distribution/target/apache-iotdb-2.0.0-SNAPSHOT-all-bin.zip` to `/evaluation/deploy/iotd/iotdb.zip` in your controller server.
* Run `nohup bash /evaluation/expansion/auto_expansion.sh &` or `nohup bash /evaluation/disaster/auto_disaster.sh &` to start the corresponding evaluations.

