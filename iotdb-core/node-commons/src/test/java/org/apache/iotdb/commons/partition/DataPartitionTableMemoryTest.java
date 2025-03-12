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

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataPartitionTableMemoryTest {

  private static final Random RANDOM = new Random();
  private static final long TIME_PARTITION_INTERVAL = 24 * 60 * 60 * 1000L;
  private static final int SERIES_SLOT_NUM = 1000;
  private static final int MAX_TIME_SLOT_NUM = 3650;

  @Test
  public void testDataPartitionTableMemory() throws IOException {
    FileWriter memW =
        new FileWriter(
            "/Users/yongzaodan/Desktop/thesis_replica_placement/dimention/table-mem.log");
    AlgorithmicDataPartitionTable table = new AlgorithmicDataPartitionTable();
    for (int i = 1; i <= MAX_TIME_SLOT_NUM; i++) {
      for (int j = 0; j < SERIES_SLOT_NUM; j++) {
        table.insertDataPartition(
            new TSeriesPartitionSlot(j),
            new TTimePartitionSlot(i * TIME_PARTITION_INTERVAL),
            new TConsensusGroupId(TConsensusGroupType.DataRegion, RANDOM.nextInt()));
      }
      if (i % 50 == 0) {
        double memInMB = (double) RamUsageEstimator.sizeOf(table) / 1024.0 / 1024.0;
        System.out.println("timeSlotNum: " + i + ", memInMB: " + memInMB);
        memW.write(memInMB + "\n");
      }
    }
    memW.close();
  }
}
