/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.List;
import java.util.Map;

/** Refer from "Gemini: Fast Failure Recovery in Distributed Training with In-Memory Checkpoints" */
public class GeminiRegionGroupAllocator implements IRegionGroupAllocator {

  private static int CURRENT_NODE_GROUP_ID = 0;
  private static int CURRENT_RING_GROUP_ID = 0;

  TDataNodeLocation[] dataNodeList;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    int dataNodeNum = 0;
    dataNodeList = new TDataNodeLocation[availableDataNodeMap.size()];
    for (TDataNodeConfiguration dataNodeConfiguration : availableDataNodeMap.values()) {
      dataNodeList[dataNodeNum++] = dataNodeConfiguration.getLocation();
    }
    int nodeGroupCnt = dataNodeNum / replicationFactor;
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    // Reset global ids
    CURRENT_RING_GROUP_ID = CURRENT_RING_GROUP_ID % replicationFactor;
    CURRENT_NODE_GROUP_ID = CURRENT_NODE_GROUP_ID % nodeGroupCnt;
    if (CURRENT_NODE_GROUP_ID < nodeGroupCnt - 1 || dataNodeNum % replicationFactor == 0) {
      // GEMINI's group placement strategy
      for (int i = 0; i < replicationFactor; i++) {
        result.addToDataNodeLocations(dataNodeList[CURRENT_NODE_GROUP_ID * replicationFactor + i]);
      }
    } else {
      // GEMINI's ring placement strategy
      int ringGroupSize = dataNodeNum % replicationFactor + replicationFactor;
      for (int i = 0; i < replicationFactor; i++) {
        int offset = (CURRENT_RING_GROUP_ID + i) % ringGroupSize;
        result.addToDataNodeLocations(dataNodeList[CURRENT_RING_GROUP_ID * ringGroupSize + offset]);
      }
      CURRENT_RING_GROUP_ID = (CURRENT_RING_GROUP_ID + replicationFactor) % ringGroupSize;
    }
    CURRENT_NODE_GROUP_ID = (CURRENT_NODE_GROUP_ID + 1) % nodeGroupCnt;
    return result;
  }
}
