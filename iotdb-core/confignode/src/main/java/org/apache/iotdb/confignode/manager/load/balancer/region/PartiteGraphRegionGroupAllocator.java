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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class PartiteGraphRegionGroupAllocator implements IRegionGroupAllocator {

  private static final Random RANDOM = new Random();

  private int replicationFactor;
  // The number of allocated Regions in each DataNode
  private int[] regionCounter;
  // The number of 2-Region combinations in current cluster
  private int[][] combinationCounter;

  int maxDataNodeId;
  int primaryDataNodeNum;
  int minScatterWidth;
  // First Key: the sum of overlapped 2-Region combination Regions with
  // other allocated RegionGroups is minimal
  int optimalCombinationSum;
  // Second Key: the sum of DataRegions in selected DataNodes is minimal
  int optimalRegionSum;
  int[] optimalPrimaryDataNodes;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {

    prepare(replicationFactor, availableDataNodeMap, allocatedRegionGroups);

    int partiteCount = replicationFactor - 1;
    for (int i = 0; i < partiteCount; i++) {
      partiteDfs(i, 0, primaryDataNodeNum, 0, 0, optimalPrimaryDataNodes);
    }

    List<Integer> backupDataNodes = new ArrayList<>();
    int selectedPartite = optimalPrimaryDataNodes[0] % partiteCount;
    for (int i = 0; i < partiteCount; i++) {
      if (i == selectedPartite) {
        continue;
      }
      partiteSearch(i, backupDataNodes);
    }
    Collections.shuffle(backupDataNodes);

    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < primaryDataNodeNum; i++) {
      result.addToDataNodeLocations(
          availableDataNodeMap.get(optimalPrimaryDataNodes[i]).getLocation());
    }
    for (int i = 0; i < replicationFactor - primaryDataNodeNum; i++) {
      result.addToDataNodeLocations(availableDataNodeMap.get(backupDataNodes.get(i)).getLocation());
    }

    return null;
  }

  /**
   * Prepare some statistics before dfs.
   *
   * @param replicationFactor replication factor in the cluster
   * @param availableDataNodeMap currently available DataNodes, ensure size() >= replicationFactor
   * @param allocatedRegionGroups already allocated RegionGroups in the cluster
   */
  private void prepare(
      int replicationFactor,
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {

    this.replicationFactor = replicationFactor;
    // Store the maximum DataNodeId
    this.maxDataNodeId =
        Math.max(
            availableDataNodeMap.keySet().stream().max(Integer::compareTo).orElse(0),
            allocatedRegionGroups.stream()
                .flatMap(regionGroup -> regionGroup.getDataNodeLocations().stream())
                .mapToInt(TDataNodeLocation::getDataNodeId)
                .max()
                .orElse(0));

    // Compute regionCounter, databaseRegionCounter and combinationCounter
    this.regionCounter = new int[maxDataNodeId + 1];
    Arrays.fill(regionCounter, 0);
    this.combinationCounter = new int[maxDataNodeId + 1][maxDataNodeId + 1];
    for (int i = 0; i <= maxDataNodeId; i++) {
      Arrays.fill(combinationCounter[i], 0);
    }
    for (TRegionReplicaSet regionReplicaSet : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionReplicaSet.getDataNodeLocations();
      for (int i = 0; i < dataNodeLocations.size(); i++) {
        regionCounter[dataNodeLocations.get(i).getDataNodeId()]++;
        for (int j = i + 1; j < dataNodeLocations.size(); j++) {
          combinationCounter[dataNodeLocations.get(i).getDataNodeId()][
              dataNodeLocations.get(j).getDataNodeId()]++;
          combinationCounter[dataNodeLocations.get(j).getDataNodeId()][
              dataNodeLocations.get(i).getDataNodeId()]++;
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

  private void partiteDfs(
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

    for (int i = firstIndex; i <= maxDataNodeId; i += replicationFactor) {
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
      partiteDfs(
          i + replicationFactor,
          currentReplica + 1,
          replicaNum,
          nxtCombinationSum,
          nxtRegionSum,
          currentReplicaSet);
    }
  }

  private void partiteSearch(int partiteIndex, List<Integer> backupDataNodes) {
    int bestRegionSum = Integer.MAX_VALUE;
    int selectedDataNode = partiteIndex;
    for (int j = partiteIndex; j <= maxDataNodeId; j += replicationFactor) {
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
    backupDataNodes.add(selectedDataNode);
  }
}
