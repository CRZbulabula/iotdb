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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.manager.load.balancer.region.AerospikeRegionGroupAllocator.generateReplicationList;

public class AlgorithmicAerospikeLeaderBalancer implements ILeaderBalancer {

  private int[][] dataNodeIds;
  private Map<TConsensusGroupId, Integer> result;

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    int cnt = 0;
    result = new TreeMap<>();
    dataNodeIds =
        new int[allocatedRegionGroups.size()]
            [allocatedRegionGroups.get(0).getDataNodeLocationsSize()];
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      for (int i = 0; i < replicaSet.getDataNodeLocationsSize(); i++) {
        dataNodeIds[cnt][i] = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
      }
      List<Integer> replicationList =
          generateReplicationList(
              replicaSet.getRegionId().getId(),
              Arrays.stream(dataNodeIds[cnt]).boxed().collect(Collectors.toList()));
      result.put(replicaSet.getRegionId(), replicationList.get(0));
      cnt += 1;
    }
    return null;
  }
}
