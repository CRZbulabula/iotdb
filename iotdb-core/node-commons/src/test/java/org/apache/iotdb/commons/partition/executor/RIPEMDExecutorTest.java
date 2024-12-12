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

package org.apache.iotdb.commons.partition.executor;

import org.apache.iotdb.commons.partition.executor.hash.RIPEMD160Executor;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.logging.Logger;

public class RIPEMDExecutorTest {

  private static final Logger LOGGER = Logger.getLogger(RIPEMDExecutorTest.class.getName());

  private static final int SERIES_SLOT_NUM = 4096;
  private static final RIPEMD160Executor RIPEMD160_EXECUTOR =
      new RIPEMD160Executor(SERIES_SLOT_NUM);
  private static final String PATH_PREFIX = "root.db.g1.d";

  @Test
  public void RIPEMDExecutorEffectivenessTest() {
    int[] deviceBucket = new int[SERIES_SLOT_NUM];
    int[] deviceIdBucket = new int[SERIES_SLOT_NUM];
    for (int suffix = 0; suffix < 1000_000; suffix++) {
      String device = PATH_PREFIX + suffix;
      IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(PATH_PREFIX + suffix);
      int deviceSlot = RIPEMD160_EXECUTOR.getSeriesPartitionSlot(device).getSlotId();
      int deviceIdSlot = RIPEMD160_EXECUTOR.getSeriesPartitionSlot(deviceID).getSlotId();
      Assert.assertEquals(deviceSlot, deviceIdSlot);
      deviceBucket[deviceSlot]++;
      deviceIdBucket[deviceIdSlot]++;
    }
    // Calculate std
    double deviceVariance = calculateVariance(deviceBucket);
    double deviceIdVariance = calculateVariance(deviceIdBucket);
    LOGGER.info("Device std: " + Math.sqrt(deviceVariance));
    LOGGER.info("DeviceID std: " + Math.sqrt(deviceIdVariance));
  }

  private double calculateVariance(int[] bucket) {
    double sum = 0;
    for (int i : bucket) {
      sum += i;
    }
    double average = sum / bucket.length;
    double variance = 0;
    for (int i : bucket) {
      variance += (i - average) * (i - average);
    }
    return variance / bucket.length;
  }
}
