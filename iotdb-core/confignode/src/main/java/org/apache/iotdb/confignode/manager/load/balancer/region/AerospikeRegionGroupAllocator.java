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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Refer from "Techniques and Efficiencies from Building a Real-Time DBMS" */
public class AerospikeRegionGroupAllocator implements IRegionGroupAllocator {

  // FNV-1a parameters
  private static final long OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long PRIME = 0x100000001b3L;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    List<Integer> replicationList =
        generateReplicationList(
            consensusGroupId.getId(), new ArrayList<>(availableDataNodeMap.keySet()));
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < replicationFactor; i++) {
      result.addToDataNodeLocations(availableDataNodeMap.get(replicationList.get(i)).getLocation());
    }
    return result;
  }

  public static List<Integer> generateReplicationList(int regionId, List<Integer> dataNodeIdList) {
    List<Pair<Long, Integer>> nodeHashList = new ArrayList<>();
    for (int dataNodeId : dataNodeIdList) {
      long hash = nodeHashCompute(dataNodeId, regionId);
      nodeHashList.add(new Pair<>(hash, dataNodeId));
    }
    nodeHashList.sort(Comparator.comparingLong(o -> o.left));
    return nodeHashList.stream().map(Pair::getRight).collect(Collectors.toList());
  }

  private static long nodeHashCompute(int nodeId, int regionId) {
    long nodeIdHash = fnv1aHash(nodeId);
    long regionIdHash = fnv1aHash(regionId);
    return jenkinsHash(nodeIdHash, regionIdHash);
  }

  private static long fnv1aHash(int id) {
    long hash = OFFSET_BASIS;
    for (int i = 0; i < 4; i++) {
      byte byteValue = (byte) ((id >> (i * 8)) & 0xFF);
      hash ^= byteValue;
      hash *= PRIME;
    }
    return hash;
  }

  private static long jenkinsHash(long nodeIdHash, long regionIdHash) {
    long hash = 0;
    for (int i = 0; i < 8; i++) {
      byte byteValue = (byte) ((nodeIdHash >> (i * 8)) & 0xFF);
      hash += byteValue;
      hash += hash << 10;
      hash ^= hash >>> 6;
    }
    for (int i = 0; i < 8; i++) {
      byte byteValue = (byte) ((regionIdHash >> (i * 8)) & 0xFF);
      hash += byteValue;
      hash += hash << 10;
      hash ^= hash >>> 6;
    }
    hash += hash << 3;
    hash ^= hash >>> 11;
    hash += hash << 15;
    return hash;
  }
}
