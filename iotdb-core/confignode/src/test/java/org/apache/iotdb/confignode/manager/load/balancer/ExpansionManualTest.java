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

package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.TieredReplicationAllocator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class ExpansionManualTest {

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new TreeMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();

  private static final int TARGET_DATA_NODE_NUM = 8;
  private static final int INIT_DATA_NODE_NUM = 4;
  private static final int EXPAND_DATA_NODE_NUM = 4;
  private static final int DATA_REPLICATION_FACTOR = 2;

  private static final IRegionGroupAllocator ALLOCATOR = new TieredReplicationAllocator();

  @Test
  public void expansionTest() {
    Random random = new Random();
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= INIT_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }

    int currentDataNode = 4;
    int currentRegionGroup = 0;
    List<TRegionReplicaSet> allocateResults = new ArrayList<>();
    while (currentDataNode <= TARGET_DATA_NODE_NUM) {
      int targetRegionGroup =
          (int)
              (ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode()
                  * currentDataNode
                  / DATA_REPLICATION_FACTOR);
      while (currentRegionGroup < targetRegionGroup) {
        allocateResults.add(
            ALLOCATOR.generateOptimalRegionReplicasDistribution(
                AVAILABLE_DATA_NODE_MAP,
                FREE_SPACE_MAP,
                allocateResults,
                new ArrayList<>(),
                DATA_REPLICATION_FACTOR,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, currentRegionGroup)));
        currentRegionGroup++;
      }
      for (int i = 1; i <= EXPAND_DATA_NODE_NUM; i++) {
        AVAILABLE_DATA_NODE_MAP.put(
            currentDataNode + i,
            new TDataNodeConfiguration()
                .setLocation(new TDataNodeLocation().setDataNodeId(currentDataNode + i)));
        FREE_SPACE_MAP.put(currentDataNode + i, random.nextDouble());
      }
      currentDataNode += EXPAND_DATA_NODE_NUM;
    }

    /* Count Region in each DataNode */
    // Map<DataNodeId, RegionGroup Count>
    Map<Integer, Integer> regionCounter = new TreeMap<>();
    allocateResults.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));
    for (int i = 1; i <= TARGET_DATA_NODE_NUM; i++) {
      System.out.println(
          "DataNode " + i + " has " + regionCounter.getOrDefault(i, 0) + " DataRegion(s)");
    }
  }
}
