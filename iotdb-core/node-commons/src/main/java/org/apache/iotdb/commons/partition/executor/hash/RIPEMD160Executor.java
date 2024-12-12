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

package org.apache.iotdb.commons.partition.executor.hash;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.math.BigInteger;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.commons.partition.executor.hash.Ripemd160.getHash;

/** Refer from "Techniques and Efficiencies from Building a Real-Time DBMS" */
public class RIPEMD160Executor extends SeriesPartitionExecutor {

  public RIPEMD160Executor(int seriesPartitionSlotNum) {
    // To utilize this executor, configuring the seriesPartitionSlotNum to 4096.
    super(seriesPartitionSlotNum);
  }

  @Override
  public TSeriesPartitionSlot getSeriesPartitionSlot(String device) {
    byte[] hashBytes = getHash(device.getBytes());
    BigInteger hashValue = new BigInteger(1, hashBytes);
    return new TSeriesPartitionSlot(
        hashValue.mod(BigInteger.valueOf(seriesPartitionSlotNum)).intValue());
  }

  @Override
  public TSeriesPartitionSlot getSeriesPartitionSlot(IDeviceID deviceID) {
    int segmentNum = deviceID.segmentNum();
    StringBuilder builder = new StringBuilder();
    for (int segmentID = 0; segmentID < segmentNum; segmentID++) {
      Object segment = deviceID.segment(segmentID);
      if (segment instanceof String) {
        builder.append(segment);
      } else {
        builder.append(NULL_SEGMENT_HASH_NUM);
      }
      if (segmentID < segmentNum - 1) {
        builder.append(PATH_SEPARATOR);
      }
    }
    byte[] hashBytes = getHash(builder.toString().getBytes());
    BigInteger hashValue = new BigInteger(1, hashBytes);
    return new TSeriesPartitionSlot(
        hashValue.mod(BigInteger.valueOf(seriesPartitionSlotNum)).intValue());
  }
}
