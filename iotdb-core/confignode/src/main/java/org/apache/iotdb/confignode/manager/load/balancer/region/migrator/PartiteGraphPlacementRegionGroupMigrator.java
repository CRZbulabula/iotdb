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
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphPlacementRegionGroupMigrator implements IRegionGroupMigrator {

  private static final PartiteGraphPlacementRegionGroupAllocator PGP_ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();

  @Override
  public Map<TConsensusGroupId, TRegionReplicaSet> generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor) {
    int regionGroupNumber = allocatedRegionGroups.size();
    int retainedSize = regionGroupNumber / 2;
    List<RegionGroupEntry> regionGroupEntries =
        allocatedRegionGroups.stream()
            .map(
                regionReplicaSet ->
                    new RegionGroupEntry(
                        regionReplicaSet,
                        regionGroupStatisticsMap
                            .get(regionReplicaSet.getRegionId())
                            .getDiskUsage()))
            .sorted()
            .collect(Collectors.toList());
    LinkedList<TRegionReplicaSet> resultList = new LinkedList<>();
    for (int i = 0; i < retainedSize; i++) {
      resultList.addLast(regionGroupEntries.get(i).regionReplicaSet);
    }
    for (int i = 0; i < retainedSize; i++) {
      TRegionReplicaSet head = resultList.poll();
      resultList.addLast(
          PGP_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              availableDataNodeMap,
              new TreeMap<>(),
              resultList,
              resultList,
              replicationFactor,
              head.getRegionId()));
    }
    for (int i = retainedSize; i < regionGroupNumber; i++) {
      resultList.addLast(
          PGP_ALLOCATOR.generateOptimalRegionReplicasDistribution(
              availableDataNodeMap,
              new TreeMap<>(),
              resultList,
              resultList,
              replicationFactor,
              regionGroupEntries.get(i).regionReplicaSet.getRegionId()));
    }
    return resultList.stream()
        .collect(
            Collectors.toMap(TRegionReplicaSet::getRegionId, regionReplicaSet -> regionReplicaSet));
  }

  private static class RegionGroupEntry implements Comparable<RegionGroupEntry> {
    TRegionReplicaSet regionReplicaSet;
    long diskUsage;

    public RegionGroupEntry(TRegionReplicaSet regionReplicaSet, long diskUsage) {
      this.regionReplicaSet = regionReplicaSet;
      this.diskUsage = diskUsage;
    }

    @Override
    public int compareTo(RegionGroupEntry o) {
      // In descending order
      return Long.compare(o.diskUsage, this.diskUsage);
    }
  }
}
