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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class AlgorithmicLogStoreBalancer implements ILeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;
  // Refer from the original paper
  private static final double MAX_CAPACITY_RATE = 0.85;

  /** Graph nodes */
  // Super source node
  private static final int S_NODE = 0;

  // Super terminal node
  private static final int T_NODE = 1;
  // Maximum index of graph nodes
  private int maxVertex = T_NODE + 1;
  // Map<RegionGroupId, rNode>
  private final Map<TConsensusGroupId, Integer> rNodeMap;
  // Map<DataNodeId, dNode>
  private final Map<Integer, Integer> dNodeMap;
  // Map<dNode, DataNodeId>>
  private final Map<Integer, Integer> dNodeReflect;

  /** Graph edges */
  // Maximum index of graph edges
  private int maxEdge = 0;

  private final List<MaxFlowEdge> maxFlowEdges;
  private int[] vertexHeadEdge;
  private int[] vertexCurrentEdge;
  private int[] vertexLevel;

  private boolean[] isVertexVisited;

  private int maximumFlow = 0;
  private int maxCapacity = 0;

  public AlgorithmicLogStoreBalancer() {
    this.rNodeMap = new TreeMap<>();
    this.dNodeMap = new TreeMap<>();
    this.dNodeReflect = new TreeMap<>();
    this.maxFlowEdges = new ArrayList<>();
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
    this.maximumFlow = 0;

    /* Indicate nodes in mcf */
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      rNodeMap.put(replicaSet.getRegionId(), maxVertex++);
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        int dataNodeId = dataNodeLocation.getDataNodeId();
        if (!dNodeMap.containsKey(dataNodeId)) {
          dNodeMap.put(dataNodeId, maxVertex);
          dNodeReflect.put(maxVertex++, dataNodeId);
        }
      }
    }

    /* Prepare arrays */
    isVertexVisited = new boolean[maxVertex];
    vertexCurrentEdge = new int[maxVertex];
    vertexHeadEdge = new int[maxVertex];
    vertexLevel = new int[maxVertex];
    Arrays.fill(vertexHeadEdge, -1);

    /* Construct edges: sNode -> rNodes */
    for (int rNode : rNodeMap.values()) {
      // Capacity: 1, each RegionGroup should elect exactly 1 leader
      addAdjacentEdges(S_NODE, rNode, 1);
    }

    /* Construct edges: rNodes -> dNodes */
    int[] potentialLeaderCounter = new int[availableDataNodeMap.size() + 1];
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      int rNode = rNodeMap.get(replicaSet.getRegionId());
      for (TDataNodeLocation dataNodeLocation : replicaSet.getDataNodeLocations()) {
        int dataNodeId = dataNodeLocation.getDataNodeId();
        int dNode = dNodeMap.get(dataNodeId);
        // Capacity: 1, each RegionGroup should elect exactly 1 leader
        addAdjacentEdges(rNode, dNode, 1);
        potentialLeaderCounter[dataNodeId]++;
      }
    }

    /* Construct edges: dNodes -> tNode */
    // Count the possible maximum number of leader in each DataNode
    for (TDataNodeConfiguration dataNodeConfiguration : availableDataNodeMap.values()) {
      int dataNodeId = dataNodeConfiguration.getLocation().getDataNodeId();
      int dNode = dNodeMap.get(dataNodeId);
      // Capacity: x, each DataNode should elect at most x leaders
      addAdjacentEdges(
          dNode, T_NODE, (int) Math.round(MAX_CAPACITY_RATE * potentialLeaderCounter[dataNodeId]));
    }
  }

  private void addAdjacentEdges(int fromVertex, int destVertex, int capacity) {
    addEdge(fromVertex, destVertex, capacity);
    addEdge(destVertex, fromVertex, 0);
  }

  private void addEdge(int fromVertex, int destVertex, int capacity) {
    MaxFlowEdge edge = new MaxFlowEdge(destVertex, capacity, vertexHeadEdge[fromVertex]);
    maxFlowEdges.add(edge);
    vertexHeadEdge[fromVertex] = maxEdge++;
  }

  private boolean bfs() {
    Arrays.fill(isVertexVisited, false);
    Arrays.fill(vertexLevel, 0);
    Queue<Integer> queue = new LinkedList<>();
    vertexLevel[S_NODE] = 1;
    isVertexVisited[S_NODE] = true;
    queue.add(S_NODE);
    while (!queue.isEmpty()) {
      int vertex = queue.poll();
      for (int edgeIndex = vertexHeadEdge[vertex];
          edgeIndex != -1;
          edgeIndex = maxFlowEdges.get(edgeIndex).nextEdge) {
        MaxFlowEdge edge = maxFlowEdges.get(edgeIndex);
        if (!isVertexVisited[edge.destVertex] && edge.capacity > 0) {
          vertexLevel[edge.destVertex] = vertexLevel[vertex] + 1;
          isVertexVisited[edge.destVertex] = true;
          queue.add(edge.destVertex);
        }
      }
    }
    return isVertexVisited[T_NODE];
  }

  private int dfs(int vertex, int inputFlow) {
    if (vertex == T_NODE) {
      return inputFlow;
    }
    int edgeIndex;
    int totalFlow = 0;
    for (edgeIndex = vertexCurrentEdge[vertex];
        edgeIndex != -1;
        edgeIndex = maxFlowEdges.get(edgeIndex).nextEdge) {
      MaxFlowEdge edge = maxFlowEdges.get(edgeIndex);
      if (edge.capacity > 0 && vertexLevel[edge.destVertex] == vertexLevel[vertex] + 1) {
        int currentFlow = dfs(edge.destVertex, Math.min(inputFlow, edge.capacity));
        if (currentFlow > 0) {
          edge.capacity -= currentFlow;
          maxFlowEdges.get(edgeIndex ^ 1).capacity += currentFlow;
          totalFlow += currentFlow;
          inputFlow -= currentFlow;
          if (inputFlow == 0) {
            break;
          }
        }
      }
    }
    vertexCurrentEdge[vertex] = edgeIndex;
    return totalFlow;
  }

  private void dinicAlgorithm() {
    while (bfs()) {
      System.arraycopy(vertexHeadEdge, 0, vertexCurrentEdge, 0, maxVertex);
      maximumFlow += dfs(S_NODE, INFINITY);
    }
  }

  /**
   * @return Map<RegionGroupId, DataNodeId where the new leader locate>
   */
  private Map<TConsensusGroupId, Integer> collectLeaderDistribution(
      List<TRegionReplicaSet> allocatedRegionGroups) {
    Map<TConsensusGroupId, Integer> result = new TreeMap<>();
    for (TRegionReplicaSet replicaSet : allocatedRegionGroups) {
      TConsensusGroupId regionGroupId = replicaSet.getRegionId();
      for (int currentEdge = vertexHeadEdge[rNodeMap.get(regionGroupId)];
          currentEdge >= 0;
          currentEdge = maxFlowEdges.get(currentEdge).nextEdge) {
        MaxFlowEdge edge = maxFlowEdges.get(currentEdge);
        if (edge.destVertex != S_NODE && edge.capacity == 0) {
          result.put(regionGroupId, dNodeReflect.get(edge.destVertex));
        }
      }
    }
    return result;
  }

  private static class MaxFlowEdge {

    private final int destVertex;
    private int capacity;
    private final int nextEdge;

    private MaxFlowEdge(int destVertex, int capacity, int nextEdge) {
      this.destVertex = destVertex;
      this.capacity = capacity;
      this.nextEdge = nextEdge;
    }
  }
}
