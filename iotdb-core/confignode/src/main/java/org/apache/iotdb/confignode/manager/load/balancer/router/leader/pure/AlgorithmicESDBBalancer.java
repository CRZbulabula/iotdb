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

import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class AlgorithmicESDBBalancer implements ILeaderBalancer {

  private static final Random RANDOM = new Random();

  // Constants for 128 bit variant
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;
  private static final int R1 = 31;
  private static final int R2 = 27;
  private static final int M = 5;
  private static final int N1 = 0x52dce729;

  public static final int DEFAULT_SEED = 104729;

  private HashRing[] ring;
  private Map<TConsensusGroupId, Integer> result;

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    int cnt = 0;
    long currentTime = System.nanoTime();
    ring = new HashRing[allocatedRegionGroups.size()];
    result = new TreeMap<>();
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      TConsensusGroupId regionId = replicaSet.getRegionId();
      Pair<String, Integer>[] replicas = new Pair[replicaSet.getDataNodeLocationsSize()];
      for (int i = 0; i < replicaSet.getDataNodeLocationsSize(); i++) {
        int dataNodeId = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
        replicas[i] = new Pair<>(regionId.toString() + dataNodeId, dataNodeId);
      }
      ring[cnt] = new HashRing();
      ring[cnt].init(replicas);
      // Dynamic power of 2 offset
      int offset = (int) Math.pow(2, Math.floor(RANDOM.nextDouble() * 32.0));
      int newLeader = ring[cnt].get(new Pair<>(regionId.toString(), offset), currentTime);
      result.put(regionId, newLeader);
    }
    return result;
  }

  private static class HashRing {
    // the hash ring of replicas, Map<HashValue, DataNodeId>
    private final Pair<Long, Integer>[] circle;

    public HashRing() {
      this.circle = new Pair[2];
      this.circle[0] = new Pair<>(0L, 0);
      this.circle[1] = new Pair<>(Long.MAX_VALUE, 0);
    }

    public void init(Pair<String, Integer>[] replicas) {
      //      circle = new Pair[replicas.length];
      for (int i = 0; i < replicas.length; i++) {
        circle[i].left = hash64(replicas[i].getLeft());
        circle[i].right = replicas[i].getRight();
      }
    }

    /**
     * Dynamically decide leader location.
     *
     * @param key Pair<TConsensusGroupId+DataNodeId, DataNodeId, loadOffset>
     * @param timestamp Region current timestamp
     * @return DataNodeId where the leader should locate
     */
    public Integer get(Pair<String, Integer> key, long timestamp) {
      long hash = hash64(key.getLeft());
      // Dynamic double hash
      hash = (hash % circle.length + hash64(timestamp) % key.getRight()) % circle.length;
      int id = -1;
      long maxDis = Long.MAX_VALUE;
      for (int i = 0; i < circle.length; i++) {
        long dis = Math.abs(circle[i].getLeft() - hash);
        if (dis < maxDis) {
          maxDis = dis;
          id = i;
        }
      }
      return circle[id].getRight();
    }
  }

  /**
   * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
   *
   * @param data - input byte array
   * @return - hashcode
   */
  public static long hash64(String data) {
    return hash64(data.getBytes());
  }

  public static long hash64(long data) {
    long hash = DEFAULT_SEED;
    long k = Long.reverseBytes(data);
    int length = Long.BYTES;
    // mix functions
    k *= C1;
    k = Long.rotateLeft(k, R1);
    k *= C2;
    hash ^= k;
    hash = Long.rotateLeft(hash, R2) * M + N1;
    // finalization
    hash ^= length;
    hash = fmix64(hash);
    return hash;
  }

  public static long hash64(byte[] data) {
    return hash64(data, 0, data.length, DEFAULT_SEED);
  }

  /**
   * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
   *
   * @param data - input byte array
   * @param length - length of array
   * @param seed - seed. (default is 0)
   * @return - hashcode
   */
  public static long hash64(byte[] data, int offset, int length, int seed) {
    long hash = seed;
    final int nblocks = length >> 3;

    // body
    for (int i = 0; i < nblocks; i++) {
      final int i8 = i << 3;
      long k =
          ((long) data[offset + i8] & 0xff)
              | (((long) data[offset + i8 + 1] & 0xff) << 8)
              | (((long) data[offset + i8 + 2] & 0xff) << 16)
              | (((long) data[offset + i8 + 3] & 0xff) << 24)
              | (((long) data[offset + i8 + 4] & 0xff) << 32)
              | (((long) data[offset + i8 + 5] & 0xff) << 40)
              | (((long) data[offset + i8 + 6] & 0xff) << 48)
              | (((long) data[offset + i8 + 7] & 0xff) << 56);

      // mix functions
      k *= C1;
      k = Long.rotateLeft(k, R1);
      k *= C2;
      hash ^= k;
      hash = Long.rotateLeft(hash, R2) * M + N1;
    }

    // tail
    long k1 = 0;
    int tailStart = nblocks << 3;
    switch (length - tailStart) {
      case 7:
        k1 ^= ((long) data[offset + tailStart + 6] & 0xff) << 48;
      case 6:
        k1 ^= ((long) data[offset + tailStart + 5] & 0xff) << 40;
      case 5:
        k1 ^= ((long) data[offset + tailStart + 4] & 0xff) << 32;
      case 4:
        k1 ^= ((long) data[offset + tailStart + 3] & 0xff) << 24;
      case 3:
        k1 ^= ((long) data[offset + tailStart + 2] & 0xff) << 16;
      case 2:
        k1 ^= ((long) data[offset + tailStart + 1] & 0xff) << 8;
      case 1:
        k1 ^= ((long) data[offset + tailStart] & 0xff);
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        hash ^= k1;
    }

    // finalization
    hash ^= length;
    hash = fmix64(hash);

    return hash;
  }

  private static long fmix64(long h) {
    h ^= (h >>> 33);
    h *= 0xff51afd7ed558ccdL;
    h ^= (h >>> 33);
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= (h >>> 33);
    return h;
  }
}
