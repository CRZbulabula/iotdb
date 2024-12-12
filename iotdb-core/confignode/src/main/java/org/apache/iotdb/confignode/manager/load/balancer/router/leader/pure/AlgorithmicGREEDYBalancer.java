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

package org.apache.iotdb.confignode.manager.load.balancer.router.leader.pure;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AlgorithmicGREEDYBalancer implements ILeaderBalancer {

  private int[] leaderCounter;
  private Map<TConsensusGroupId, Integer> result;

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    result = new TreeMap<>();
    leaderCounter = new int[availableDataNodeMap.size() + 1];
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      int minCount = Integer.MAX_VALUE, leaderId = -1;
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        // Select the DataNode with the minimal leader count as the new leader
        int dataNodeId = dataNodeLocation.getDataNodeId();
        int count = leaderCounter[dataNodeId];
        if (count < minCount) {
          minCount = count;
          leaderId = dataNodeId;
        }
      }
      leaderCounter[leaderId]++;
      result.put(replicaSet.getRegionId(), leaderId);
    }
    return result;
  }
}
