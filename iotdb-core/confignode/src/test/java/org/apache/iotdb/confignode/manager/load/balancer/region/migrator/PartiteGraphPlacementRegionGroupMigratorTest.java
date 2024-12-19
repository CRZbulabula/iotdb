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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphPlacementRegionGroupMigratorTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PartiteGraphPlacementRegionGroupMigratorTest.class);

  private static final Random RANDOM = new Random();

  private static final Map<Integer, TDataNodeConfiguration> BEFORE_NODE_MAP = new TreeMap<>();
  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new TreeMap<>();
  private static final Map<Integer, Double> BEFORE_SPACE_MAP = new TreeMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();

  private static final PartiteGraphPlacementRegionGroupAllocator ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();
  private static final PartiteGraphPlacementRegionGroupMigrator MIGRATOR =
      new PartiteGraphPlacementRegionGroupMigrator();

  private static final int DATA_NODE_NUM = 16;
  private static final int REGION_PER_NODE = 8;
  private static final int REPLICATION_FACTOR = 2;

  @Test
  public void generateOptimalRegionReplicasDistribution() {
    Random random = new Random();
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= DATA_NODE_NUM / 2; i++) {
      TDataNodeConfiguration dataNodeConfiguration =
          new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i));
      double space = random.nextDouble();
      BEFORE_NODE_MAP.put(i, dataNodeConfiguration);
      AVAILABLE_DATA_NODE_MAP.put(i, dataNodeConfiguration);
      BEFORE_SPACE_MAP.put(i, space);
      FREE_SPACE_MAP.put(i, space);
    }
    for (int i = DATA_NODE_NUM / 2 + 1; i <= DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }
    List<TRegionReplicaSet> allocatedResult = new ArrayList<>();
    int regionGroupNum = DATA_NODE_NUM * REGION_PER_NODE / REPLICATION_FACTOR;
    for (int i = 0; i < regionGroupNum / 2; i++) {
      allocatedResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              BEFORE_NODE_MAP,
              BEFORE_SPACE_MAP,
              allocatedResult,
              allocatedResult,
              REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }
    for (int i = regionGroupNum / 2; i < regionGroupNum; i++) {
      allocatedResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocatedResult,
              allocatedResult,
              REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, i)));
    }

    Map<TConsensusGroupId, RegionGroupStatistics> fakeStatisticsMap = new TreeMap<>();
    allocatedResult.forEach(
        regionGroup -> {
          RegionGroupStatistics fakeStatistics =
              RegionGroupStatistics.generateDefaultRegionGroupStatistics();
          fakeStatistics.setDiskUsage(RANDOM.nextInt());
          fakeStatisticsMap.put(
              regionGroup.getRegionId(),
              RegionGroupStatistics.generateDefaultRegionGroupStatistics());
        });
    Map<TConsensusGroupId, TRegionReplicaSet> migrationPlan =
        MIGRATOR.generateOptimalRegionReplicasDistribution(
            AVAILABLE_DATA_NODE_MAP, fakeStatisticsMap, allocatedResult, REPLICATION_FACTOR);

    for (TRegionReplicaSet regionReplicaSet : allocatedResult) {
      LOGGER.info(
          "Id: {}, origin: {}, target: {}",
          regionReplicaSet.getRegionId().getId(),
          regionReplicaSet.getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()),
          migrationPlan.get(regionReplicaSet.getRegionId()).getDataNodeLocations().stream()
              .map(TDataNodeLocation::getDataNodeId)
              .collect(Collectors.toList()));
    }
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    for (TRegionReplicaSet regionReplicaSet : migrationPlan.values()) {
      regionReplicaSet
          .getDataNodeLocations()
          .forEach(location -> regionCounter.merge(location.getDataNodeId(), 1, Integer::sum));
    }
    LOGGER.info("Region counter: {}", regionCounter);
  }
}
