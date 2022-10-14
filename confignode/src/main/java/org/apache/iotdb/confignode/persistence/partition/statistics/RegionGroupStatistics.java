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
package org.apache.iotdb.confignode.persistence.partition.statistics;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class RegionGroupStatistics {

  // The DataNode where the leader resides
  private int leaderDataNodeId;

  private RegionGroupStatus regionGroupStatus;

  private final Map<Integer, RegionStatistics> regionStatisticsMap;

  public RegionGroupStatistics() {
    this.regionStatisticsMap = new HashMap<>();
  }

  public RegionGroupStatistics(int leaderDataNodeId, RegionGroupStatus regionGroupStatus, Map<Integer, RegionStatistics> regionStatisticsMap) {
    this.leaderDataNodeId = leaderDataNodeId;
    this.regionGroupStatus = regionGroupStatus;
    this.regionStatisticsMap = regionStatisticsMap;
  }

  public static RegionGroupStatistics generateDefaultRegionGroupStatistics() {

  }

  public int getLeaderDataNodeId() {
    return leaderDataNodeId;
  }

  public RegionGroupStatus getRegionGroupStatus() {
    return regionGroupStatus;
  }

  /**
   * Get the specified Region's status
   *
   * @param dataNodeId Where the Region resides
   * @return Region's latest status if received heartbeat recently, Unknown otherwise
   */
  public RegionStatus getRegionStatus(int dataNodeId) {
    return regionStatisticsMap.containsKey(dataNodeId)
            ? regionStatisticsMap.get(dataNodeId).getRegionStatus()
            : RegionStatus.Unknown;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(leaderDataNodeId, stream);
    ReadWriteIOUtils.write(regionGroupStatus.getStatus(), stream);

    ReadWriteIOUtils.write(regionStatisticsMap.size(), stream);
    for (Map.Entry<Integer, RegionStatistics> regionStatisticsEntry : regionStatisticsMap.entrySet()) {
      ReadWriteIOUtils.write(regionStatisticsEntry.getKey(), stream);
      regionStatisticsEntry.getValue().serialize(stream);
    }
  }

  public void deserialize(ByteBuffer buffer) {
    this.leaderDataNodeId = buffer.getInt();
    this.regionGroupStatus = RegionGroupStatus.parse(ReadWriteIOUtils.readString(buffer));

    int regionNum = buffer.getInt();
    for (int i = 0; i < regionNum; i++) {
      int belongedDataNodeId = buffer.getInt();
      RegionStatistics regionStatistics = new RegionStatistics();
      regionStatistics.deserialize(buffer);
      regionStatisticsMap.put(belongedDataNodeId, regionStatistics);
    }
  }
}
