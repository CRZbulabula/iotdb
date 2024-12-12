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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.pure.AlgorithmicAerospikeLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.pure.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;

import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class SelectorCPUMemoryManualTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectorCPUMemoryManualTest.class);
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int TEST_LOOP = 10;
  private static final int MIN_DATA_NODE_NUM = 1;
  private static final int MAX_DATA_NODE_NUM = 1000;
  private static final int MIN_DATA_REGION_PER_DATA_NODE = 10;
  private static final int MAX_DATA_REGION_PER_DATA_NODE = 10;
  private static final int DATA_REPLICATION_FACTOR = 2;
  private static final String DATABASE = "root.db";
  private static final IRegionGroupAllocator ALLOCATOR =
      new PartiteGraphPlacementRegionGroupAllocator();

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new TreeMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();
  private static final Map<Integer, NodeStatistics> DATA_NODE_STATISTICS_MAP = new TreeMap<>();

  public static class DataEntry {
    public final Integer N;
    public final Double maxMemoryInMB;
    public final Double avgCPUTimeInMS;

    private DataEntry(int N, double maxMemoryInMB, double avgCPUTimeInMS) {
      this.N = N;
      this.maxMemoryInMB = maxMemoryInMB;
      this.avgCPUTimeInMS = avgCPUTimeInMS;
    }
  }

  @Test
  public void selectTest() throws IOException {
    List<DataEntry> testResult = new ArrayList<>();
    THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
    // Warm up
    for (int dataNodeNum = 1; dataNodeNum <= 100; dataNodeNum++) {
      for (int dataRegionPerDataNode = MIN_DATA_REGION_PER_DATA_NODE;
          dataRegionPerDataNode <= MAX_DATA_REGION_PER_DATA_NODE;
          dataRegionPerDataNode++) {
        CONF.setDataRegionPerDataNode(dataRegionPerDataNode);
        singleTest(dataNodeNum, dataRegionPerDataNode);
      }
    }
    // Real test
    for (int dataNodeNum = MIN_DATA_NODE_NUM; dataNodeNum <= MAX_DATA_NODE_NUM; dataNodeNum++) {
      for (int dataRegionPerDataNode = MIN_DATA_REGION_PER_DATA_NODE;
          dataRegionPerDataNode <= MAX_DATA_REGION_PER_DATA_NODE;
          dataRegionPerDataNode++) {
        CONF.setDataRegionPerDataNode(dataRegionPerDataNode);
        testResult.add(singleTest(dataNodeNum, dataRegionPerDataNode));
      }
    }

    FileWriter cpuW =
        new FileWriter("/Users/yongzaodan/Desktop/simulation/resource/selection/AEROSPIKE-cpu.log");
    FileWriter memW =
        new FileWriter("/Users/yongzaodan/Desktop/simulation/resource/selection/AEROSPIKE-mem.log");
    for (DataEntry entry : testResult) {
      cpuW.write(entry.avgCPUTimeInMS + "\n");
      cpuW.flush();
      memW.write(entry.maxMemoryInMB + "\n");
      memW.flush();
    }
    cpuW.close();
    memW.close();
  }

  private DataEntry singleTest(int N, int W) {
    if (N < DATA_REPLICATION_FACTOR) {
      return new DataEntry(N, 0.0, 0.0);
    }
    // Construct N DataNodes
    Random random = new Random();
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= N; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
      DATA_NODE_STATISTICS_MAP.put(i, new NodeStatistics(NodeStatus.Running));
    }

    // Place Regions
    final int dataRegionGroupNum = W * N / DATA_REPLICATION_FACTOR;
    List<TRegionReplicaSet> allocateResult = new ArrayList<>();
    for (int index = 0; index < dataRegionGroupNum; index++) {
      TRegionReplicaSet newRegion =
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocateResult,
              allocateResult,
              DATA_REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index));
      allocateResult.add(newRegion);
    }

    // Select leader
    double maxMemoryInMB = 0.0;
    double currentMemoryInMB = (double) RamUsageEstimator.sizeOf(ALLOCATOR) / 1024.0 / 1024.0;
    long threadID = Thread.currentThread().getId();
    long startTime = THREAD_MX_BEAN.getThreadCpuTime(threadID), totalTime = 0;
    for (int loop = 1; loop <= TEST_LOOP; loop++) {
      startTime = THREAD_MX_BEAN.getThreadCpuTime(threadID);
      ILeaderBalancer BALANCER = new AlgorithmicAerospikeLeaderBalancer();
      BALANCER.generateOptimalLeaderDistribution(AVAILABLE_DATA_NODE_MAP, allocateResult);
      totalTime += THREAD_MX_BEAN.getThreadCpuTime(threadID) - startTime;
      currentMemoryInMB = (double) RamUsageEstimator.sizeOf(BALANCER) / 1024.0 / 1024.0;
      maxMemoryInMB = Math.max(maxMemoryInMB, currentMemoryInMB);
    }

    double cpuTimePerRegionInMS = (double) totalTime / (double) TEST_LOOP / 1000000.0;
    LOGGER.info("Test N={}, memory={}MB, cpuTime={}MS", N, maxMemoryInMB, cpuTimePerRegionInMS);
    return new DataEntry(N, maxMemoryInMB, cpuTimePerRegionInMS);
  }
}
