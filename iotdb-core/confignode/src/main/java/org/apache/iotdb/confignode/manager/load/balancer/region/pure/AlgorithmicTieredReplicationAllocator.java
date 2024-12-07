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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/** Refer from "Tiered Replication: A Cost-effective Alternative to Full Cluster Geo-replication" */
public class AlgorithmicTieredReplicationAllocator implements IRegionGroupAllocator {

  private final SplittableRandom RANDOM = new SplittableRandom();
  private int dataNodeNum;
  private int[][] COPY_SETS;
  private int[] COPY_SETS_COUNTER;
  private BitSet[] scatterWidth;

  private static class DataNodeEntry {

    private final int dataNodeId;
    private final int scatterWidth;

    public DataNodeEntry(int dataNodeId, int scatterWidth) {
      this.dataNodeId = dataNodeId;
      this.scatterWidth = scatterWidth;
    }

    public int getDataNodeId() {
      return dataNodeId;
    }

    public int compare(DataNodeEntry other) {
      return Integer.compare(scatterWidth, other.scatterWidth);
    }
  }

  private void init(int dataNodeNum, int replicationFactor, int loadFactor) {
    if (dataNodeNum < replicationFactor) {
      return;
    }
    int targetScatterWidth = Math.min(loadFactor * (replicationFactor - 1), dataNodeNum - 1);
    scatterWidth = new BitSet[dataNodeNum + 1];
    for (int i = 1; i <= dataNodeNum; i++) {
      scatterWidth[i] = new BitSet(dataNodeNum + 1);
    }
    COPY_SETS_COUNTER = new int[dataNodeNum + 1];
    COPY_SETS = new int[dataNodeNum + 1][targetScatterWidth + 10];
    while (existScatterWidthUnsatisfied(dataNodeNum, targetScatterWidth)) {
      for (int firstRegion = 1; firstRegion <= dataNodeNum; firstRegion++) {
        if (scatterWidth[firstRegion].cardinality() < targetScatterWidth) {
          int tail = 1;
          int[] copySet = new int[replicationFactor];
          copySet[0] = firstRegion;
          List<DataNodeEntry> otherDataNodes = new ArrayList<>();
          for (int i = 1; i <= dataNodeNum; i++) {
            if (i != firstRegion) {
              otherDataNodes.add(new DataNodeEntry(i, scatterWidth[i].cardinality()));
            }
          }
          otherDataNodes.sort(DataNodeEntry::compare);
          for (DataNodeEntry entry : otherDataNodes) {
            boolean accepted = true;
            int secondRegion = entry.getDataNodeId();
            for (int index = 0; index < tail; index++) {
              if (scatterWidth[copySet[index]].get(secondRegion)) {
                accepted = false;
                break;
              }
            }
            if (accepted) {
              copySet[tail++] = secondRegion;
            }
            if (tail == replicationFactor) {
              break;
            }
          }

          while (tail < replicationFactor) {
            boolean notOK;
            int secondRegion;
            do {
              notOK = false;
              secondRegion = RANDOM.nextInt(dataNodeNum) + 1;
              for (int index = 0; index < tail; index++) {
                if (copySet[index] == secondRegion) {
                  notOK = true;
                  break;
                }
              }
            } while (notOK);
            copySet[tail++] = secondRegion;
          }

          for (int i = 0; i < replicationFactor; i++) {
            for (int j = i + 1; j < replicationFactor; j++) {
              scatterWidth[copySet[i]].set(copySet[j]);
              scatterWidth[copySet[j]].set(copySet[i]);
            }
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
          break;
        }
      }
    }
  }

  private boolean existScatterWidthUnsatisfied(int dataNodeNum, int targetScatterWidth) {
    for (int i = 1; i <= dataNodeNum; i++) {
      if (scatterWidth[i].cardinality() < targetScatterWidth) {
        return true;
      }
    }
    return false;
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

    TRegionReplicaSet result = new TRegionReplicaSet();
    int[] regionCounter = new int[dataNodeNum + 1];
    for (TRegionReplicaSet regionGroup : allocatedRegionGroups) {
      List<TDataNodeLocation> dataNodeLocations = regionGroup.getDataNodeLocations();
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        regionCounter[dataNodeLocation.getDataNodeId()]++;
      }
    }
    int firstRegion = -1, minCount = Integer.MAX_VALUE;
    for (int i = 1; i <= dataNodeNum; i++) {
      if (regionCounter[i] < minCount) {
        minCount = regionCounter[i];
        firstRegion = i;
      } else if (regionCounter[i] == minCount && RANDOM.nextBoolean()) {
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
