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

package org.apache.iotdb.confignode.manager.load.balancer.region.migrator;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.iotdb.confignode.manager.load.balancer.region.AerospikeRegionGroupAllocator.generateReplicationList;

public class AerospikeMigrator implements IRegionGroupMigrator {

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor) {
    Map<TConsensusGroupId, TRegionReplicaSet> result = new TreeMap<>();
    List<Integer> dataNodeList = new ArrayList<>(availableDataNodeMap.keySet());
    allocatedRegionGroups.forEach(
        regionGroup -> {
          int regionId = regionGroup.getRegionId().getId();
          List<Integer> targetDataNodeIds =
              generateReplicationList(regionId, dataNodeList).subList(0, replicationFactor);
          TRegionReplicaSet newRegionGroup = new TRegionReplicaSet();
          newRegionGroup.setRegionId(regionGroup.getRegionId());
          for (int dataNodeId : targetDataNodeIds) {
            newRegionGroup.addToDataNodeLocations(
                availableDataNodeMap.get(dataNodeId).getLocation());
          }
          result.put(regionGroup.getRegionId(), newRegionGroup);
        });
    return result;
  }
}
