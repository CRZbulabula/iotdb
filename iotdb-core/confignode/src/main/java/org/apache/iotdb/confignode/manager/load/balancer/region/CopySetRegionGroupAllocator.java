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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/** Refer from "Copysets: Reducing the Frequency of Data Loss in Cloud Storage" */
public class CopySetRegionGroupAllocator implements IRegionGroupAllocator {

  private final Random RANDOM = new Random();
  private final Map<TConsensusGroupType, Integer> dataNodeNumMap = new TreeMap<>();
  private final Map<TConsensusGroupType, Map<Integer, List<List<Integer>>>> COPY_SETS =
      new TreeMap<>();

  private void init(
      int dataNodeNum,
      int replicationFactor,
      int loadFactor,
      TConsensusGroupType consensusGroupType) {
    this.dataNodeNumMap.put(consensusGroupType, dataNodeNum);
    Map<Integer, List<List<Integer>>> copy_sets =
        COPY_SETS.computeIfAbsent(consensusGroupType, k -> new TreeMap<>());
    // sum of COPY_SETS value .size()
    int p = copy_sets.values().stream().mapToInt(List::size).sum();
    BitSet bitSet = new BitSet(dataNodeNum + 1);
    copy_sets.values().forEach(cps -> cps.forEach(cp -> cp.forEach(bitSet::set)));
    while (p < loadFactor || bitSet.cardinality() < dataNodeNum) {
      int[] permutation = new int[dataNodeNum];
      for (int i = 1; i <= dataNodeNum; i++) {
        permutation[i - 1] = i;
      }
      for (int i = 1; i < dataNodeNum; i++) {
        int pos = RANDOM.nextInt(i);
        int tmp = permutation[i];
        permutation[i] = permutation[pos];
        permutation[pos] = tmp;
      }
      p += 1;
      for (int i = 0; i + replicationFactor <= dataNodeNum; i += replicationFactor) {
        List<Integer> copySet = new ArrayList<>();
        for (int j = 0; j < replicationFactor; j++) {
          int e = permutation[i + j];
          copySet.add(e);
          bitSet.set(e);
        }
        for (int c : copySet) {
          copy_sets.computeIfAbsent(c, k -> new ArrayList<>()).add(copySet);
        }
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
    if (this.dataNodeNumMap.getOrDefault(consensusGroupId.getType(), -1)
        != availableDataNodeMap.size()) {
      int regionPerDataNode;
      if (consensusGroupId.getType().equals(TConsensusGroupType.SchemaRegion)) {
        regionPerDataNode =
            (int) ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode();
      } else {
        regionPerDataNode =
            (int) ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode();
      }
      init(
          availableDataNodeMap.size(),
          replicationFactor,
          regionPerDataNode,
          consensusGroupId.getType());
    }

    TRegionReplicaSet result = new TRegionReplicaSet();
    int[] regionCounter = new int[availableDataNodeMap.size() + 1];
    Arrays.fill(regionCounter, 0);
    for (TRegionReplicaSet regionGroup : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionGroup.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        regionCounter[dataNodeLocation.getDataNodeId()]++;
      }
    }
    int firstRegion = -1, minCount = Integer.MAX_VALUE;
    int dataNodeNumber = dataNodeNumMap.get(consensusGroupId.getType());
    for (int i = 1; i <= dataNodeNumber; i++) {
      if (regionCounter[i] < minCount) {
        minCount = regionCounter[i];
        firstRegion = i;
      } else if (regionCounter[i] == minCount && RANDOM.nextBoolean()) {
        firstRegion = i;
      }
    }
    List<Integer> copySet =
        COPY_SETS
            .get(consensusGroupId.getType())
            .get(firstRegion)
            .get(RANDOM.nextInt(COPY_SETS.get(consensusGroupId.getType()).get(firstRegion).size()));
    for (int dataNodeId : copySet) {
      result.addToDataNodeLocations(availableDataNodeMap.get(dataNodeId).getLocation());
    }
    return result.setRegionId(consensusGroupId);
  }
}
