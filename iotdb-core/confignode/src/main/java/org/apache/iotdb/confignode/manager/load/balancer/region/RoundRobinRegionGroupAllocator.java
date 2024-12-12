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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Allocate Region Greedily */
public class RoundRobinRegionGroupAllocator implements IRegionGroupAllocator {

  private int[] regionCounter;
  private DataNodeEntry[] entryList;

  public RoundRobinRegionGroupAllocator() {
    // Empty constructor
  }

  public static class DataNodeEntry implements Comparable<DataNodeEntry> {

    public int dataNodeId;
    public int regionCount;

    public DataNodeEntry(int dataNodeId, int regionCount) {
      this.dataNodeId = dataNodeId;
      this.regionCount = regionCount;
    }

    @Override
    public int compareTo(DataNodeEntry other) {
      if (this.regionCount != other.regionCount) {
        return this.regionCount - other.regionCount;
      } else {
        return this.dataNodeId - other.dataNodeId;
      }
    }
  }

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    // Construct DataNode priority list
    int maxDataNodeId = availableDataNodeMap.keySet().stream().max(Integer::compareTo).orElse(0);
    regionCounter = new int[maxDataNodeId + 1];
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        regionCounter[dataNodeLocation.getDataNodeId()]++;
      }
    }
    int cnt = 0;
    entryList = new DataNodeEntry[availableDataNodeMap.size()];
    for (TDataNodeConfiguration dataNodeConfiguration : availableDataNodeMap.values()) {
      int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
      entryList[cnt++] = new DataNodeEntry(dataNodeId, regionCounter[dataNodeId]);
    }
    Arrays.sort(entryList);
    // Collect result
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < replicationFactor; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(entryList[i].dataNodeId).getLocation());
    }
    return result;
  }
}
