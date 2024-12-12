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
import java.util.Random;

/**
 * Refer from "Hydra: Resilient and Highly Available Remote Memory", git:
 * Hydra/resilience_manager/is_main.c#L1923-L1969
 */
public class HydraRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Random random = new Random();

  TDataNodeLocation[] dataNodeList;
  int[] selection;
  int[] randomSelection;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    int i = 0, j;
    int avail_node = availableDataNodeMap.size();
    dataNodeList = new TDataNodeLocation[avail_node];
    for (TDataNodeConfiguration dataNodeConfiguration : availableDataNodeMap.values()) {
      dataNodeList[i++] = dataNodeConfiguration.getLocation();
    }

    selection = new int[replicationFactor];
    randomSelection = new int[avail_node];
    for (j = 0; j < replicationFactor; j++) {
      selection[j] = -1; // Not selected yet
    }
    for (i = 0; i < avail_node; i++) {
      randomSelection[i] = -1;
    }
    if (avail_node <= replicationFactor) {
      // Select all available nodes in this case
      for (i = 0; i < avail_node; i++) {
        selection[i] = i;
      }
    } else {
      for (j = 0; j < replicationFactor; j++) {
        int randomNum = random.nextInt(avail_node);
        while (randomSelection[randomNum] != -1) {
          randomNum += 1;
          randomNum %= avail_node;
        }
        selection[j] = randomNum;
        randomSelection[randomNum] = 1;
      }
    }

    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (j = 0; j < replicationFactor; j++) {
      result.addToDataNodeLocations(dataNodeList[selection[j]]);
    }
    return result;
  }
}
