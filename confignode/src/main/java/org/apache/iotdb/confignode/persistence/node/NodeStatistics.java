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
package org.apache.iotdb.confignode.persistence.node;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class NodeStatistics {

  // For guiding queries, the higher the score the higher the load
  private long loadScore;

  // The current status of the Node
  private NodeStatus status;
  // The reason why lead to the current NodeStatus (for showing cluster)
  // Notice: Default is null
  private String statusReason;

  public NodeStatistics() {
    // Empty constructor
  }

  public NodeStatistics(long loadScore, NodeStatus status, String statusReason) {
    this.loadScore = loadScore;
    this.status = status;
    this.statusReason = statusReason;
  }

  public long getLoadScore() {
    return loadScore;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public String getStatusReason() {
    return statusReason;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(loadScore, stream);
    ReadWriteIOUtils.write(status.getStatus(), stream);
    if (statusReason != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(statusReason, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  public void deserialize(ByteBuffer buffer) {
    loadScore = buffer.getLong();
    status = NodeStatus.parse(ReadWriteIOUtils.readString(buffer));
    if (ReadWriteIOUtils.readBool(buffer)) {
      statusReason = ReadWriteIOUtils.readString(buffer);
    } else {
      statusReason = null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeStatistics that = (NodeStatistics) o;
    return loadScore == that.loadScore
        && status == that.status
        && Objects.equals(statusReason, that.statusReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(loadScore, status, statusReason);
  }
}
