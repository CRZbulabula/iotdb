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
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.iotdb.confignode.manager.load.balancer.region.AerospikeRegionGroupAllocator.generateReplicationList;

/** Refer from "Techniques and Efficiencies from Building a Real-Time DBMS" */
public class AerospikeLeaderBalancer extends AbstractLeaderBalancer {

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, Set<Integer>> regionLocationMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap) {
    Map<TConsensusGroupId, Integer> result = new TreeMap<>();
    regionLocationMap.forEach(
        (regionId, dataNodeList) -> {
          // Select the first available region as the leader.
          int replicationFactor = dataNodeList.size();
          List<Integer> replicationList =
              generateReplicationList(regionId.getId(), new ArrayList<>(dataNodeList));
          for (int i = 0; i < replicationFactor; i++) {
            if (isDataNodeAvailable(replicationList.get(i))) {
              result.put(regionId, replicationList.get(i));
              break;
            }
          }
          // If no available data node, select the first one.
          result.put(regionId, replicationList.get(0));
        });
    return result;
  }
}
