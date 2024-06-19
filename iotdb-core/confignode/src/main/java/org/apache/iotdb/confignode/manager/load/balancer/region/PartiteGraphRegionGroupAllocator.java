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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PartiteGraphRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Random RANDOM = new Random();
  private static final GreedyRegionGroupAllocator GREEDY_ALLOCATOR = new GreedyRegionGroupAllocator();

  private int partiteCount;
  private int replicationFactor;
  private int regionPerDataNode;

  private int dataNodeNum;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;
  private Map<Integer, Integer> fakeToRealIdMap;
  private Map<Integer, Integer> realToFakeIdMap;

  private int primaryDataNodeNum;
  private int minScatterWidth;
  // First Key: the sum of overlapped 2-Region combination Regions with
  // other allocated RegionGroups is minimal
  private int optimalCombinationSum;
  // Second Key: the sum of DataRegions in selected DataNodes is minimal
  private int optimalRegionSum;
  private int[] optimalPrimaryDataNodes;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {

    this.regionPerDataNode = (int) (consensusGroupId.getType().equals(TConsensusGroupType.DataRegion) ?
          ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode() :
          ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode());
    prepare(replicationFactor, availableDataNodeMap, allocatedRegionGroups);

    for (int i = 0; i < partiteCount; i++) {
      primaryPartiteSearch(i, 0, primaryDataNodeNum, 0, 0, optimalPrimaryDataNodes);
    }
    if (optimalCombinationSum == Integer.MAX_VALUE) {
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(availableDataNodeMap, freeDiskSpaceMap, allocatedRegionGroups, databaseAllocatedRegionGroups, replicationFactor, consensusGroupId);
    }

    List<Integer> backupDataNodes = new ArrayList<>();
    int selectedPartite = optimalPrimaryDataNodes[0] % partiteCount;
    for (int i = 0; i < partiteCount; i++) {
      if (i == selectedPartite) {
        continue;
      }
      backupPartiteSearch(i, backupDataNodes);
    }
    Collections.shuffle(backupDataNodes);
    if (backupDataNodes.size() < replicationFactor - primaryDataNodeNum) {
      return GREEDY_ALLOCATOR.generateOptimalRegionReplicasDistribution(availableDataNodeMap, freeDiskSpaceMap, allocatedRegionGroups, databaseAllocatedRegionGroups, replicationFactor, consensusGroupId);
    }

    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < primaryDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(fakeToRealIdMap.get(optimalPrimaryDataNodes[i])).getLocation());
    }
    for (int i = 0; i < replicationFactor - primaryDataNodeNum; i++) {
      result.addToDataNodeLocations(availableDataNodeMap.get(fakeToRealIdMap.get(backupDataNodes.get(i))).getLocation());
    }
    return result;
  }

  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {

    this.partiteCount = replicationFactor - 1;
    this.replicationFactor = replicationFactor;

    this.fakeToRealIdMap = new TreeMap<>();
    this.realToFakeIdMap = new TreeMap<>();
    this.dataNodeNum = availableDataNodeMap.size();
    List<Integer> dataNodeIdList = availableDataNodeMap.values().stream().map(c -> c.getLocation().getDataNodeId()).collect(Collectors.toList());
    for (int i = 0; i < dataNodeNum; i++) {
      fakeToRealIdMap.put(i, dataNodeIdList.get(i));
      realToFakeIdMap.put(dataNodeIdList.get(i), i);
    }

    // Compute regionCounter, databaseRegionCounter and combinationCounter
    this.regionCounter = new int[dataNodeNum];
    Arrays.fill(regionCounter, 0);
    this.combinationCounter = new int[dataNodeNum][dataNodeNum];
    for (int i = 0; i < dataNodeNum; i++) {
      Arrays.fill(combinationCounter[i], 0);
    }
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (int i = 0; i < dataNodeLocations.size(); i++) {
        int fakeIId = realToFakeIdMap.get(dataNodeLocations.get(i).getDataNodeId());
        regionCounter[fakeIId]++;
        for (int j = i + 1; j < dataNodeLocations.size(); j++) {
          int fakeJId = realToFakeIdMap.get(dataNodeLocations.get(j).getDataNodeId());
          combinationCounter[fakeIId][fakeJId] = 1;
          combinationCounter[fakeJId][fakeIId] = 1;
        }
      }
    }

    // Reset the optimal result
    this.primaryDataNodeNum =
        Math.max(replicationFactor % 2 == 1 ? replicationFactor / 2 + 1 : replicationFactor / 2, 2);
    this.minScatterWidth = replicationFactor / 2;
    this.optimalCombinationSum = Integer.MAX_VALUE;
    this.optimalRegionSum = Integer.MAX_VALUE;
    this.optimalPrimaryDataNodes = new int[primaryDataNodeNum];
  }

  private void primaryPartiteSearch(
      int firstIndex,
      int currentReplica,
      int replicaNum,
      int combinationSum,
      int regionSum,
      int[] currentReplicaSet) {

    if (currentReplica == replicaNum) {
      if (combinationSum < optimalCombinationSum
          || (combinationSum == optimalCombinationSum && regionSum < optimalRegionSum)) {
        // Reset the optimal result when a better one is found
        optimalCombinationSum = combinationSum;
        optimalRegionSum = regionSum;
        optimalPrimaryDataNodes = Arrays.copyOf(currentReplicaSet, replicationFactor);
      } else if (combinationSum == optimalCombinationSum
          && regionSum == optimalRegionSum
          && RANDOM.nextBoolean()) {
        optimalPrimaryDataNodes = Arrays.copyOf(currentReplicaSet, replicationFactor);
      }
      return;
    }

    for (int i = firstIndex; i < dataNodeNum; i += partiteCount) {
      if (regionCounter[i] >= regionPerDataNode) {
        // Pruning: skip DataNodes already satisfied
        continue;
      }
      int nxtCombinationSum = combinationSum;
      for (int j = 0; j < currentReplica; j++) {
        nxtCombinationSum += combinationCounter[i][currentReplicaSet[j]];
      }
      if (combinationSum > optimalCombinationSum) {
        // Pruning: no needs for further searching when the first key
        // is bigger than the historical optimal result
        return;
      }
      int nxtRegionSum = regionSum + regionCounter[i];
      if (combinationSum == optimalCombinationSum && regionSum > optimalRegionSum) {
        // Pruning: no needs for further searching when the second key
        // is bigger than the historical optimal result
        return;
      }
      currentReplicaSet[currentReplica] = i;
      primaryPartiteSearch(
          i + partiteCount,
          currentReplica + 1,
          replicaNum,
          nxtCombinationSum,
          nxtRegionSum,
          currentReplicaSet);
    }
  }

  private void backupPartiteSearch(int partiteIndex, List<Integer> backupDataNodes) {
    int bestRegionSum = Integer.MAX_VALUE;
    int selectedDataNode = -1;
    for (int j = partiteIndex; j < dataNodeNum; j += partiteCount) {
      if (regionCounter[j] >= regionPerDataNode) {
        continue;
      }
      int scatterWidth = primaryDataNodeNum;
      for (int k = 0; k < primaryDataNodeNum; k++) {
        scatterWidth -= combinationCounter[j][optimalPrimaryDataNodes[k]];
      }
      if (scatterWidth < minScatterWidth) {
        continue;
      }
      if (regionCounter[j] < bestRegionSum) {
        bestRegionSum = regionCounter[j];
        selectedDataNode = j;
      } else if (regionCounter[j] == bestRegionSum && RANDOM.nextBoolean()) {
        selectedDataNode = j;
      }
    }
    if (selectedDataNode != -1) {
      backupDataNodes.add(selectedDataNode);
    }
  }
}
