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

package org.apache.iotdb.confignode.manager.load.balancer.router.leader;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ESDBLeaderBalancerTest {

  private static final ESDBLeaderBalancer BALANCER = new ESDBLeaderBalancer();

  private static final String DATABASE = "root.database";

  /**
   * In this case shows the balance ability for big cluster.
   *
   * <p>i.e. Simulate 64 RegionGroups and 16 DataNodes
   */
  @Test
  public void bigClusterTest() {
    final int regionGroupNum = 64;
    final int dataNodeNum = 16;
    final int replicationFactor = 2;
    Map<Integer, NodeStatistics> dataNodeStatisticsMap = new TreeMap<>();
    for (int i = 0; i < dataNodeNum; i++) {
      dataNodeStatisticsMap.put(i, new NodeStatistics(NodeStatus.Running));
    }

    int dataNodeId = 0;
    Random random = new Random();
    Map<String, List<TConsensusGroupId>> databaseRegionGroupMap = new TreeMap<>();
    databaseRegionGroupMap.put(DATABASE, new ArrayList<>());
    Map<TConsensusGroupId, Set<Integer>> regionReplicaSetMap = new TreeMap<>();
    Map<TConsensusGroupId, Integer> regionLeaderMap = new TreeMap<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap = new TreeMap<>();
    for (int i = 0; i < regionGroupNum; i++) {
      TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, i);
      int leaderId = (dataNodeId + random.nextInt(replicationFactor)) % dataNodeNum;

      TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
      regionReplicaSet.setRegionId(regionGroupId);
      Map<Integer, RegionStatistics> regionStatistics = new TreeMap<>();
      for (int j = 0; j < replicationFactor; j++) {
        regionReplicaSet.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(dataNodeId));
        regionStatistics.put(dataNodeId, new RegionStatistics(RegionStatus.Running));
        dataNodeId = (dataNodeId + 1) % dataNodeNum;
      }
      regionStatisticsMap.put(regionGroupId, regionStatistics);

      databaseRegionGroupMap.get(DATABASE).add(regionGroupId);
      regionReplicaSetMap.put(
        regionGroupId,
        regionReplicaSet.getDataNodeLocations().stream()
          .map(TDataNodeLocation::getDataNodeId)
          .collect(Collectors.toSet()));
      regionLeaderMap.put(regionGroupId, leaderId);
    }

    // Do balancing
    Map<TConsensusGroupId, Integer> leaderDistribution =
      BALANCER.generateOptimalLeaderDistribution(
        databaseRegionGroupMap,
        regionReplicaSetMap,
        regionLeaderMap,
        dataNodeStatisticsMap,
        regionStatisticsMap);
    // All RegionGroup got a leader
    Assert.assertEquals(regionGroupNum, leaderDistribution.size());

    Map<Integer, Integer> leaderCounter = new ConcurrentHashMap<>();
    leaderDistribution.values().forEach(leaderId -> leaderCounter.merge(leaderId, 1, Integer::sum));
    int minLeaderNum = leaderCounter.values().stream().min(Integer::compareTo).orElse(0);
    int maxLeaderNum = leaderCounter.values().stream().max(Integer::compareTo).orElse(0);
    System.out.println(
      "Min Leader Num: "
        + minLeaderNum
        + ", Max Leader Num: "
        + maxLeaderNum);
  }
}
