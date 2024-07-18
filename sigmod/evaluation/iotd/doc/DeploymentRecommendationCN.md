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

## IoTDB 部署模式选型

|      模式      | 性能  | 扩展性 | 高可用 | 一致性 |
|:------------:|:----|:----|:----|:----|
|    轻量单机模式    | 最高  | 无   | 无   | 高   |
|   可扩展单节点模式 （默认）  | 高   | 高   | 中   | 高   |
| 高性能分布式模式     | 高   | 高   | 高   | 中   |
|   强一致分布式模式   | 中   | 高   | 高   | 高   |


|                           配置                           | 轻量单机模式 | 可扩展单节点模式 | 高性能分布式模式 | 强一致分布式模式 |
|:------------------------------------------------------:|:-------|:---------|:---------|:---------|
|                     ConfigNode 个数                      | 1      | ≥1 （奇数）  | ≥1 （奇数）  | ≥1（奇数）   |
|                      DataNode 个数                       | 1      | ≥1       | ≥3       | ≥3       |
|            元数据副本 schema_replication_factor             | 1      | 1        | 3        | 3        |
|              数据副本 data_replication_factor              | 1      | 1        | 2        | 3        |
|   ConfigNode 协议 config_node_consensus_protocol_class   | Simple | Ratis    | Ratis    | Ratis    |
| SchemaRegion 协议 schema_region_consensus_protocol_class | Simple | Ratis    | Ratis    | Ratis    |
|   DataRegion 协议 data_region_consensus_protocol_class   | Simple | IoT      | IoT      | Ratis    |



