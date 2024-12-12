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

package org.apache.iotdb.confignode.manager.load.balancer.router.leader.pure;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class AlgorithmicCFDBalancer implements ILeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;

  /** Graph nodes */
  // Super source node
  private static final int S_NODE = 0;

  // Super terminal node
  private static final int T_NODE = 1;
  // Maximum index of graph nodes
  private int maxNode = T_NODE + 1;
  // Map<RegionGroupId, rNode>
  private final Map<TConsensusGroupId, Integer> rNodeMap;
  // Map<DataNodeId, dNode>
  private final Map<Integer, Integer> dNodeMap;
  // Map<dNode, DataNodeId>>
  private final Map<Integer, Integer> dNodeReflect;

  /** Graph edges */
  // Maximum index of graph edges
  private int maxEdge = 0;

  private MinCostFlowEdge[] minCostFlowEdges;
  private int[] nodeHeadEdge;
  private int[] nodeCurrentEdge;

  private boolean[] isNodeVisited;
  private int[] nodeMinimumCost;

  private Map<TConsensusGroupId, Integer> result;

  private int maximumFlow = 0;
  private int minimumCost = 0;

  public AlgorithmicCFDBalancer() {
    this.rNodeMap = new TreeMap<>();
    this.dNodeMap = new TreeMap<>();
    this.dNodeReflect = new TreeMap<>();
  }

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    constructMCFGraph(availableDataNodeMap, allocatedRegionGroups);
    dinicAlgorithm();
    return collectLeaderDistribution(allocatedRegionGroups);
  }

  private void constructMCFGraph(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {
    int replicationFactor = allocatedRegionGroups.get(0).getDataNodeLocationsSize();
    this.maximumFlow = 0;
    this.minimumCost = 0;
    this.minCostFlowEdges =
        new MinCostFlowEdge
            [availableDataNodeMap.size() * 20
                + allocatedRegionGroups.size() * (replicationFactor + 1) * 2];
    /* Indicate nodes in mcf */
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      rNodeMap.put(replicaSet.getRegionId(), maxNode++);
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        int dataNodeId = dataNodeLocation.getDataNodeId();
        if (!dNodeMap.containsKey(dataNodeId)) {
          dNodeMap.put(dataNodeId, maxNode);
          dNodeReflect.put(maxNode++, dataNodeId);
        }
      }
    }

    /* Prepare arrays */
    isNodeVisited = new boolean[maxNode];
    nodeMinimumCost = new int[maxNode];
    nodeCurrentEdge = new int[maxNode];
    nodeHeadEdge = new int[maxNode];
    Arrays.fill(nodeHeadEdge, -1);

    /* Construct edges: sNode -> rNodes */
    for (int rNode : rNodeMap.values()) {
      // Capacity: 1, Cost: 0, each RegionGroup should elect exactly 1 leader
      addAdjacentEdges(S_NODE, rNode, 1, 0);
    }

    /* Construct edges: rNodes -> dNodes */
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      int rNode = rNodeMap.get(replicaSet.getRegionId());
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        int dataNodeId = dataNodeLocation.getDataNodeId();
        int dNode = dNodeMap.get(dataNodeId);
        // Capacity: 1, Cost: 0, each RegionGroup should elect exactly 1 leader
        addAdjacentEdges(rNode, dNode, 1, 0);
      }
    }

    /* Construct edges: dNodes -> tNode */
    // Count the possible maximum number of leader in each DataNode
    int[] leaderCounter = new int[availableDataNodeMap.size() + 1];
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        int dataNodeId = dataNodeLocation.getDataNodeId();
        int dNode = dNodeMap.get(dataNodeId);
        leaderCounter[dataNodeId]++;
        // Cost: x^2 for the x-th edge at the current dNode.
        // Thus, the leader distribution will be as balance as possible within the
        // cluster
        // Based on the Jensen's-Inequality.
        addAdjacentEdges(dNode, T_NODE, 1, leaderCounter[dataNodeId] * leaderCounter[dataNodeId]);
      }
    }
  }

  private void addAdjacentEdges(int fromNode, int destNode, int capacity, int cost) {
    addEdge(fromNode, destNode, capacity, cost);
    addEdge(destNode, fromNode, 0, -cost);
  }

  private void addEdge(int fromNode, int destNode, int capacity, int cost) {
    MinCostFlowEdge edge = new MinCostFlowEdge(destNode, capacity, cost, nodeHeadEdge[fromNode]);
    minCostFlowEdges[maxEdge] = edge;
    nodeHeadEdge[fromNode] = maxEdge++;
  }

  /**
   * Check whether there is an augmented path in the MCF graph by Bellman-Ford algorithm.
   *
   * <p>Notice: Never use Dijkstra algorithm to replace this since there might exist negative
   * circles.
   *
   * @return True if there exist augmented paths, false otherwise.
   */
  private boolean bellmanFordCheck() {
    Arrays.fill(isNodeVisited, false);
    Arrays.fill(nodeMinimumCost, INFINITY);

    Queue<Integer> queue = new LinkedList<>();
    nodeMinimumCost[S_NODE] = 0;
    isNodeVisited[S_NODE] = true;
    queue.offer(S_NODE);
    while (!queue.isEmpty()) {
      int currentNode = queue.poll();
      isNodeVisited[currentNode] = false;
      for (int currentEdge = nodeHeadEdge[currentNode];
          currentEdge >= 0;
          currentEdge = minCostFlowEdges[currentEdge].nextEdge) {
        MinCostFlowEdge edge = minCostFlowEdges[currentEdge];
        if (edge.capacity > 0
            && nodeMinimumCost[currentNode] + edge.cost < nodeMinimumCost[edge.destNode]) {
          nodeMinimumCost[edge.destNode] = nodeMinimumCost[currentNode] + edge.cost;
          if (!isNodeVisited[edge.destNode]) {
            isNodeVisited[edge.destNode] = true;
            queue.offer(edge.destNode);
          }
        }
      }
    }

    return nodeMinimumCost[T_NODE] < INFINITY;
  }

  /** Do augmentation by dfs algorithm */
  private int dfsAugmentation(int currentNode, int inputFlow) {
    if (currentNode == T_NODE || inputFlow == 0) {
      return inputFlow;
    }

    int currentEdge;
    int outputFlow = 0;
    isNodeVisited[currentNode] = true;
    for (currentEdge = nodeCurrentEdge[currentNode];
        currentEdge >= 0;
        currentEdge = minCostFlowEdges[currentEdge].nextEdge) {
      MinCostFlowEdge edge = minCostFlowEdges[currentEdge];
      if (nodeMinimumCost[currentNode] + edge.cost == nodeMinimumCost[edge.destNode]
          && edge.capacity > 0
          && !isNodeVisited[edge.destNode]) {

        int subOutputFlow = dfsAugmentation(edge.destNode, Math.min(inputFlow, edge.capacity));

        minimumCost += subOutputFlow * edge.cost;

        edge.capacity -= subOutputFlow;
        minCostFlowEdges[currentEdge ^ 1].capacity += subOutputFlow;

        inputFlow -= subOutputFlow;
        outputFlow += subOutputFlow;

        if (inputFlow == 0) {
          break;
        }
      }
    }
    nodeCurrentEdge[currentNode] = currentEdge;

    if (outputFlow > 0) {
      isNodeVisited[currentNode] = false;
    }
    return outputFlow;
  }

  private void dinicAlgorithm() {
    while (bellmanFordCheck()) {
      int currentFlow;
      System.arraycopy(nodeHeadEdge, 0, nodeCurrentEdge, 0, maxNode);
      while ((currentFlow = dfsAugmentation(S_NODE, INFINITY)) > 0) {
        maximumFlow += currentFlow;
      }
    }
  }

  /**
   * @return Map<RegionGroupId, DataNodeId where the new leader locate>
   */
  private Map<TConsensusGroupId, Integer> collectLeaderDistribution(
      List<TRegionReplicaSet> allocatedRegionGroups) {
    result = new TreeMap<>();
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      TConsensusGroupId regionGroupId = replicaSet.getRegionId();
      for (int currentEdge = nodeHeadEdge[rNodeMap.get(regionGroupId)];
          currentEdge >= 0;
          currentEdge = minCostFlowEdges[currentEdge].nextEdge) {
        MinCostFlowEdge edge = minCostFlowEdges[currentEdge];
        if (edge.destNode != S_NODE && edge.capacity == 0) {
          result.put(regionGroupId, dNodeReflect.get(edge.destNode));
        }
      }
    }
    return result;
  }

  public static class MinCostFlowEdge {

    private final int destNode;
    private int capacity;
    private final int cost;
    private final int nextEdge;

    private MinCostFlowEdge(int destNode, int capacity, int cost, int nextEdge) {
      this.destNode = destNode;
      this.capacity = capacity;
      this.cost = cost;
      this.nextEdge = nextEdge;
    }
  }
}
