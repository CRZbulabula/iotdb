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

package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphRegionGroupAllocator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RegionGroupAllocatorSimulation {

    private final static Logger LOGGER = LoggerFactory.getLogger(RegionGroupAllocatorSimulation.class);

    private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
    private static final int TEST_LOOP = 1;
//    private static final double EXAM_LOOP = 100000;
    private static final int MIN_DATA_NODE_NUM = 15;
    private static final int MAX_DATA_NODE_NUM = 15;
    private static final int MIN_DATA_REGION_PER_DATA_NODE = 1;
//    private static final int MAX_DATA_REGION_PER_DATA_NODE = 10;
    private static final int DATA_REPLICATION_FACTOR = 3;

    private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
            new TreeMap<>();
    private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();

    public static class DataEntry {
        public final Integer countRange;
        public final Integer minScatterWidth;
//        public final List<Double> disabledPercent;

//        private DataEntry(
//                int countRange, int minScatterWidth, List<Double> disabledPercent) {
//            this.countRange = countRange;
//            this.minScatterWidth = minScatterWidth;
//            this.disabledPercent = disabledPercent;
//        }
        private DataEntry(int countRange, int minScatterWidth) {
            this.countRange = countRange;
            this.minScatterWidth = minScatterWidth;
        }
    }

    @Test
    public void allocateTest() throws IOException {
        List<DataEntry> testResult = new ArrayList<>();
        for (int dataNodeNum = MIN_DATA_NODE_NUM; dataNodeNum <= MAX_DATA_NODE_NUM; dataNodeNum++) {
            for (int dataRegionPerDataNode = MIN_DATA_REGION_PER_DATA_NODE;
                 dataRegionPerDataNode <= dataNodeNum;
                 dataRegionPerDataNode++) {
                CONF.setDataRegionPerDataNode(dataRegionPerDataNode);
                testResult.add(singleTest(dataNodeNum, dataRegionPerDataNode));
            }
//            LOGGER.info("{}, finish", dataNodeNum);
        }

//        final String path = "~/Desktop/simulation/";
//        String allocatorPath = "PGR";
//             FileWriter countW = new FileWriter(path + "pgr-simulate/r=2/" + allocatorPath + "-w");
//             FileWriter scatterW = new FileWriter(path + "pgr-simulate/r=2/" + allocatorPath + "-s");
//
//        for (DataEntry entry : testResult) {
//                        countW.write(entry.countRange.toString());
//                        countW.write("\n");
//                        countW.flush();
//                  scatterW.write(entry.minScatterWidth.toString());
//                  scatterW.write("\n");
//                  scatterW.flush();
//        }
//
//                countW.close();
//            scatterW.close();
    }

    private DataEntry singleTest(int N, int W) {
        if (N < DATA_REPLICATION_FACTOR) {
            return new DataEntry(0, 0);
        }
        // Construct N DataNodes
        Random random = new Random();
        AVAILABLE_DATA_NODE_MAP.clear();
        FREE_SPACE_MAP.clear();
        for (int i = 1; i <= N; i++) {
            AVAILABLE_DATA_NODE_MAP.put(
                    i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
            FREE_SPACE_MAP.put(i, random.nextDouble());
        }

        boolean passScatter = true;
        final int dataRegionGroupNum =
                W * N / DATA_REPLICATION_FACTOR;
        List<Integer> regionCountList = new ArrayList<>();
        List<Integer> scatterWidthList = new ArrayList<>();
        for (int loop = 1; loop <= TEST_LOOP; loop++) {
            IRegionGroupAllocator ALLOCATOR = new PartiteGraphRegionGroupAllocator();
            List<TRegionReplicaSet> allocateResult = new ArrayList<>();
            for (int index = 0; index < dataRegionGroupNum; index++) {
                allocateResult.add(
                        ALLOCATOR.generateOptimalRegionReplicasDistribution(
                                AVAILABLE_DATA_NODE_MAP,
                                FREE_SPACE_MAP,
                                allocateResult,
                                allocateResult,
                                DATA_REPLICATION_FACTOR,
                                new TConsensusGroupId(TConsensusGroupType.DataRegion, index)));
            }

            /* Count Region in each DataNode */
            // Map<DataNodeId, RegionGroup Count>
            Map<Integer, Integer> regionCounter = new TreeMap<>();
            allocateResult.forEach(
                    regionReplicaSet ->
                            regionReplicaSet
                                    .getDataNodeLocations()
                                    .forEach(
                                            dataNodeLocation ->
                                                    regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));

            /* Calculate scatter width for each DataNode */
            // Map<DataNodeId, ScatterWidth>
            Map<Integer, BitSet> scatterWidthMap = new TreeMap<>();
            for (TRegionReplicaSet replicaSet : allocateResult) {
                for (int i = 0; i < DATA_REPLICATION_FACTOR; i++) {
                    for (int j = i + 1; j < DATA_REPLICATION_FACTOR; j++) {
                        int dataNodeId1 = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
                        int dataNodeId2 = replicaSet.getDataNodeLocations().get(j).getDataNodeId();
                        scatterWidthMap.computeIfAbsent(dataNodeId1, empty -> new BitSet()).set(dataNodeId2);
                        scatterWidthMap.computeIfAbsent(dataNodeId2, empty -> new BitSet()).set(dataNodeId1);
                    }
                }
            }

            int scatterWidthSum = 0;
            int minScatterWidth = Integer.MAX_VALUE;
            int maxScatterWidth = Integer.MIN_VALUE;
            for (int i = 1; i <= N; i++) {
                int scatterWidth =
                        scatterWidthMap.containsKey(i) ? scatterWidthMap.get(i).cardinality() : 0;
                if (scatterWidth < Math.min(regionCounter.getOrDefault(i, 0) * (DATA_REPLICATION_FACTOR / 2), N - 1)) {
                    passScatter = false;
                }
                scatterWidthSum += scatterWidth;
                minScatterWidth = Math.min(minScatterWidth, scatterWidth);
                maxScatterWidth = Math.max(maxScatterWidth, scatterWidth);
                regionCountList.add(regionCounter.getOrDefault(i, 0));
                scatterWidthList.add(scatterWidth);
            }
        }

        int regionRange = regionCountList.stream().mapToInt(Integer::intValue).max().orElse(0)
            - regionCountList.stream().mapToInt(Integer::intValue).min().orElse(0);
        int minScatter = scatterWidthList.stream().mapToInt(Integer::intValue).min().orElse(0);
        LOGGER.info("Test N={}, W={}, regionRange={} {}, minScatter={} {},", N, W, regionRange, regionRange <= 1, minScatter, passScatter);
//        LOGGER.info("Test DataNodeNum={}, RegionPerDataNode={}, minRegion={}, maxRegion={}, minScatter={}", N, W,
//          regionCountList.stream().mapToInt(Integer::intValue).min().orElse(0),
//          regionCountList.stream().mapToInt(Integer::intValue).max().orElse(0),
//          minScatter);
        return new DataEntry(regionRange, minScatter);
    }
}
