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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.region.AerospikeRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.CopySetRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.GeminiRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyCopySetRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.HydraRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.PartiteGraphPlacementRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.RoundRobinRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.TieredReplicationAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.migrator.IRegionGroupMigrator;
import org.apache.iotdb.confignode.manager.load.balancer.region.migrator.PartiteGraphPlacementRegionGroupMigrator;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link RegionBalancer} provides interfaces to generate optimal Region allocation and
 * migration plans
 */
public class RegionBalancer {

  private final IManager configManager;
  private final IRegionGroupAllocator regionGroupAllocator;
  private final IRegionGroupMigrator regionGroupMigrator;

  public RegionBalancer(IManager configManager) {
    this.configManager = configManager;
    this.regionGroupMigrator = new PartiteGraphPlacementRegionGroupMigrator();

    switch (ConfigNodeDescriptor.getInstance().getConf().getRegionGroupAllocatePolicy()) {
      case ROUND_ROBIN:
        this.regionGroupAllocator = new RoundRobinRegionGroupAllocator();
        break;
      case COPY_SET:
        this.regionGroupAllocator = new CopySetRegionGroupAllocator();
        break;
      case TIERED_REPLICATION:
        this.regionGroupAllocator = new TieredReplicationAllocator();
        break;
      case GEMINI:
        this.regionGroupAllocator = new GeminiRegionGroupAllocator();
        break;
      case HYDRA:
        this.regionGroupAllocator = new HydraRegionGroupAllocator();
        break;
      case AEROSPIKE:
        this.regionGroupAllocator = new AerospikeRegionGroupAllocator();
        break;
      case PGP:
        this.regionGroupAllocator = new PartiteGraphPlacementRegionGroupAllocator();
        break;
      case GCR:
      default:
        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
    }
  }

  /**
   * Generate a RegionGroups' allocation plan({@link CreateRegionGroupsPlan})
   *
   * @param allotmentMap Map<DatabaseName, RegionGroup allotment>
   * @param consensusGroupType {@link TConsensusGroupType} of the new RegionGroups
   * @return CreateRegionGroupsPlan
   * @throws NotEnoughDataNodeException When the number of DataNodes is not enough for allocation
   * @throws DatabaseNotExistsException When some StorageGroups don't exist
   */
  public CreateRegionGroupsPlan genRegionGroupsAllocationPlan(
      Map<String, Integer> allotmentMap, TConsensusGroupType consensusGroupType)
      throws NotEnoughDataNodeException, DatabaseNotExistsException {

    // Some new RegionGroups will have to occupy unknown DataNodes
    // if the number of online DataNodes is insufficient
    List<TDataNodeConfiguration> availableDataNodes =
        getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Unknown);

    // Make sure the number of available DataNodes is enough for allocating new RegionGroups
    for (String database : allotmentMap.keySet()) {
      int replicationFactor =
          getClusterSchemaManager().getReplicationFactor(database, consensusGroupType);
      if (availableDataNodes.size() < replicationFactor) {
        throw new NotEnoughDataNodeException(availableDataNodes, replicationFactor);
      }
    }

    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    // Only considering the specified ConsensusGroupType when doing allocation
    List<TRegionReplicaSet> allocatedRegionGroups =
        getPartitionManager().getAllReplicaSets(consensusGroupType);

    for (Map.Entry<String, Integer> entry : allotmentMap.entrySet()) {
      String database = entry.getKey();
      int allotment = entry.getValue();
      int replicationFactor =
          getClusterSchemaManager().getReplicationFactor(database, consensusGroupType);
      // Only considering the specified Database when doing allocation
      List<TRegionReplicaSet> databaseAllocatedRegionGroups =
          getPartitionManager().getAllReplicaSets(database, consensusGroupType);

      for (int i = 0; i < allotment; i++) {
        // Prepare input data
        Map<Integer, TDataNodeConfiguration> availableDataNodeMap =
            new HashMap<>(availableDataNodes.size());
        Map<Integer, Double> freeDiskSpaceMap = new HashMap<>(availableDataNodes.size());
        availableDataNodes.forEach(
            dataNodeConfiguration -> {
              int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
              availableDataNodeMap.put(dataNodeId, dataNodeConfiguration);
              freeDiskSpaceMap.put(dataNodeId, getLoadManager().getFreeDiskSpace(dataNodeId));
            });

        // Generate allocation plan
        TRegionReplicaSet newRegionGroup =
            regionGroupAllocator.generateOptimalRegionReplicasDistribution(
                availableDataNodeMap,
                freeDiskSpaceMap,
                allocatedRegionGroups,
                databaseAllocatedRegionGroups,
                replicationFactor,
                new TConsensusGroupId(
                    consensusGroupType, getPartitionManager().generateNextRegionGroupId()));
        createRegionGroupsPlan.addRegionGroup(database, newRegionGroup);

        // Mark the new RegionGroup as allocated
        allocatedRegionGroups.add(newRegionGroup);
        databaseAllocatedRegionGroups.add(newRegionGroup);
      }
    }

    return createRegionGroupsPlan;
  }

  /**
   * Generate an optimal RegionReplicas' distribution for cluster RegionGroups.
   *
   * @param availableDataNodeMap DataNodes that can be used for allocation
   * @param regionGroupStatisticsMap Statistics of RegionGroups
   * @param allocatedRegionGroups Allocated RegionGroups
   * @param replicationFactor Replication factor of TRegionReplicaSet
   * @return The optimal TRegionReplicaSet derived by the specified algorithm
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<TConsensusGroupId, RegionGroupStatistics> regionGroupStatisticsMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      int replicationFactor) {
    return regionGroupMigrator.generateOptimalRegionReplicasDistribution(
        availableDataNodeMap, regionGroupStatisticsMap, allocatedRegionGroups, replicationFactor);
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ClusterSchemaManager getClusterSchemaManager() {
    return configManager.getClusterSchemaManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }

  public enum RegionGroupAllocatePolicy {
    ROUND_ROBIN,
    GCR,
    COPY_SET,
    TIERED_REPLICATION,
    GEMINI,
    HYDRA,
    AEROSPIKE,
    PGP
  }
}
