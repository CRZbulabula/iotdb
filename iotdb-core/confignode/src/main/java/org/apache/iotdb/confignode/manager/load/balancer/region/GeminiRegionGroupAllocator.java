package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GeminiRegionGroupAllocator implements IRegionGroupAllocator {

  private static int CURRENT_NODE_GROUP_ID = 0;
  private static int CURRENT_RING_GROUP_ID = 0;

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {
    List<TDataNodeConfiguration> dataNodeList =
        availableDataNodeMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    int nodeGroupCnt = dataNodeList.size() / replicationFactor;
    TRegionReplicaSet result = new TRegionReplicaSet();
    result.setRegionId(consensusGroupId);
    if (CURRENT_NODE_GROUP_ID < nodeGroupCnt - 1 || dataNodeList.size() % replicationFactor == 0) {
      // GEMINI's group placement strategy
      for (int i = 0; i < replicationFactor; i++) {
        result.addToDataNodeLocations(
            dataNodeList.get(CURRENT_NODE_GROUP_ID * replicationFactor + i).getLocation());
      }
    } else {
      // GEMINI's ring placement strategy
      int ringGroupSize = dataNodeList.size() % replicationFactor + replicationFactor;
      for (int i = 0; i < replicationFactor; i++) {
        int offset = (CURRENT_RING_GROUP_ID + i) % ringGroupSize;
        result.addToDataNodeLocations(
            dataNodeList.get(CURRENT_RING_GROUP_ID * ringGroupSize + offset).getLocation());
      }
      CURRENT_RING_GROUP_ID = (CURRENT_RING_GROUP_ID + replicationFactor) % ringGroupSize;
    }
    CURRENT_NODE_GROUP_ID = (CURRENT_NODE_GROUP_ID + 1) % nodeGroupCnt;
    return result;
  }
}
