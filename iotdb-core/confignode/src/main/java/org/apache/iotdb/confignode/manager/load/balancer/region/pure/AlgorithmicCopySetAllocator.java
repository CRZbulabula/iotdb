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

package org.apache.iotdb.confignode.manager.load.balancer.region.pure;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/** Refer from "Copysets: Reducing the Frequency of Data Loss in Cloud Storage" */
public class AlgorithmicCopySetAllocator implements IRegionGroupAllocator {

  //  private static final int MAX_COPY_SET_NUM = 2020;
  private static final SplittableRandom RANDOM = new SplittableRandom();

  private int dataNodeNum;
  private int[] REGION_COUNTER;
  //  private int[] COPY_SETS_COUNTER;
  //  private CopySet[][] COPY_SETS;

  private static final int MAX_NODE_NUM = 1010;
  private static final int MAX_COPY_SET_NUM = 20;
  private int[] COPY_SETS_COUNTER;
  //  private final int[][][] COPY_SETS = new
  // int[MAX_NODE_NUM][MAX_COPY_SET_NUM][MAX_REPLICATION_FACTOR];
  private int[][] COPY_SETS;

  public void init(int dataNodeNum, int replicationFactor, int loadFactor) {
    if (dataNodeNum < replicationFactor) {
      return;
    }
    COPY_SETS_COUNTER = new int[dataNodeNum + 1];
    COPY_SETS = new int[dataNodeNum + 1][MAX_COPY_SET_NUM * replicationFactor];
    int p = 0;
    BitSet bitSet = new BitSet(dataNodeNum + 1);
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
        int[] copySet = new int[replicationFactor];
        for (int j = 0; j < replicationFactor; j++) {
          int e = permutation[i + j];
          copySet[j] = e;
          bitSet.set(e);
        }
        for (int c : copySet) {
          System.arraycopy(
              copySet,
              0,
              COPY_SETS[c],
              COPY_SETS_COUNTER[c] * replicationFactor,
              replicationFactor);
          COPY_SETS_COUNTER[c]++;
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
    int dataNodeNumber = availableDataNodeMap.size();
    if (dataNodeNum != dataNodeNumber) {
      dataNodeNum = dataNodeNumber;
      int regionPerDataNode;
      if (consensusGroupId.getType().equals(TConsensusGroupType.SchemaRegion)) {
        regionPerDataNode =
            (int) ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode();
      } else {
        regionPerDataNode =
            (int) ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerDataNode();
      }
      init(dataNodeNumber, replicationFactor, regionPerDataNode);
    }

    REGION_COUNTER = new int[dataNodeNumber + 1];
    for (TRegionReplicaSet regionGroup : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionGroup.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        REGION_COUNTER[dataNodeLocation.getDataNodeId()]++;
      }
    }
    TRegionReplicaSet result = new TRegionReplicaSet();
    int firstRegion = -1, minCount = Integer.MAX_VALUE;
    for (int i = 1; i <= dataNodeNumber; i++) {
      if (REGION_COUNTER[i] < minCount) {
        minCount = REGION_COUNTER[i];
        firstRegion = i;
      } else if (REGION_COUNTER[i] == minCount && RANDOM.nextBoolean()) {
        firstRegion = i;
      }
    }
    int copySetIndex = RANDOM.nextInt(COPY_SETS_COUNTER[firstRegion]);
    int[] copySet = new int[replicationFactor];
    System.arraycopy(
        COPY_SETS[firstRegion], copySetIndex * replicationFactor, copySet, 0, replicationFactor);
    for (int dataNodeId : copySet) {
      result.addToDataNodeLocations(availableDataNodeMap.get(dataNodeId).getLocation());
    }
    return result.setRegionId(consensusGroupId);
  }
}
