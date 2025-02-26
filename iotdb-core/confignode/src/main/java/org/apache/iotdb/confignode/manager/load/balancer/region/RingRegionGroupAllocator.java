package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.List;
import java.util.Map;

public class RingRegionGroupAllocator implements IRegionGroupAllocator {

  private static int CURRENT_RING_GROUP_ID = 0;
  TDataNodeLocation[] dataNodeList;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    int dataNodeNum = 0;
    dataNodeList = new TDataNodeLocation[availableDataNodeMap.size()];
    for (TDataNodeConfiguration dataNodeConfiguration : availableDataNodeMap.values()) {
      dataNodeList[dataNodeNum++] = dataNodeConfiguration.getLocation();
    }
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    for (int i = 0; i < replicationFactor; i++) {
      int dataNodeIndex = (CURRENT_RING_GROUP_ID + i) % dataNodeList.length;
      result.addToDataNodeLocations(dataNodeList[dataNodeIndex]);
    }
    CURRENT_RING_GROUP_ID = (CURRENT_RING_GROUP_ID + 1) % dataNodeList.length;
    return result;
  }
}
