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

package org.apache.iotdb.confignode.manager.load.balancer.router.leader;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Leader distribution balancer in LogStore, which employs the maximum flow algorithm */
public class LogStoreLeaderBalancer extends AbstractLeaderBalancer {

  private static final int INFINITY = Integer.MAX_VALUE;

  // Refer from the original paper
  private static final double MAX_CAPACITY_RATE = 0.85;

  /** Graph vertices */
  // Super source vertex
  private static final int S_VERTEX = 0;

  // Super terminal vertex
  private static final int T_VERTEX = 1;
  // Maximum index of graph vertices
  private int maxVertex = T_VERTEX + 1;
  // Map<RegionGroupId, rVertex>
  private final Map<TConsensusGroupId, Integer> rVertexMap;
  // Map<Database, Map<DataNodeId, sDVertex>>
  private final Map<String, Map<Integer, Integer>> sDVertexMap;
  // Map<Database, Map<sDVertex, DataNodeId>>
  private final Map<String, Map<Integer, Integer>> sDVertexReflect;
  // Map<DataNodeId, tDVertex>
  private final Map<Integer, Integer> tDVertexMap;

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

  public LogStoreLeaderBalancer() {
    super();
    this.rVertexMap = new TreeMap<>();
    this.sDVertexMap = new TreeMap<>();
    this.sDVertexReflect = new TreeMap<>();
    this.tDVertexMap = new TreeMap<>();
    this.maxFlowEdges = new ArrayList<>();
  }

  @Override
  public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistribution(
      Map<String, List<TConsensusGroupId>> databaseRegionGroupMap,
      Map<TConsensusGroupId, Set<Integer>> regionLocationMap,
      Map<TConsensusGroupId, Integer> regionLeaderMap,
      Map<Integer, NodeStatistics> dataNodeStatisticsMap,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regionStatisticsMap) {
    initialize(
        databaseRegionGroupMap,
        regionLocationMap,
        regionLeaderMap,
        dataNodeStatisticsMap,
        regionStatisticsMap);
    Map<TConsensusGroupId, Integer> result;
    constructFlowNetwork();
    dinicAlgorithm();
    result = collectLeaderDistribution();
    clear();
    return result;
  }

  @Override
  protected void clear() {
    super.clear();
    this.rVertexMap.clear();
    this.sDVertexMap.clear();
    this.sDVertexReflect.clear();
    this.tDVertexMap.clear();
    this.maxFlowEdges.clear();
    this.vertexHeadEdge = null;
    this.vertexCurrentEdge = null;
    this.vertexLevel = null;
    this.isVertexVisited = null;
    this.maxVertex = T_VERTEX + 1;
    this.maxEdge = 0;
  }

  private void constructFlowNetwork() {
    this.maximumFlow = 0;
    this.maxCapacity = 0;

    /* Indicate vertices */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      sDVertexMap.put(database, new TreeMap<>());
      sDVertexReflect.put(database, new TreeMap<>());
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        if (regionGroupIntersection.contains(regionGroupId)) {
          // Map region to region vertices
          rVertexMap.put(regionGroupId, maxVertex++);
          regionLocationMap
              .get(regionGroupId)
              .forEach(
                  dataNodeId -> {
                    if (isDataNodeAvailable(dataNodeId)) {
                      // Map DataNode to DataNode vertices
                      if (!sDVertexMap.get(database).containsKey(dataNodeId)) {
                        sDVertexMap.get(database).put(dataNodeId, maxVertex);
                        sDVertexReflect.get(database).put(maxVertex, dataNodeId);
                        maxVertex += 1;
                      }
                      if (!tDVertexMap.containsKey(dataNodeId)) {
                        tDVertexMap.put(dataNodeId, maxVertex);
                        maxVertex += 1;
                      }
                    }
                  });
        }
      }
    }

    /* Prepare arrays */
    isVertexVisited = new boolean[maxVertex];
    vertexCurrentEdge = new int[maxVertex];
    vertexHeadEdge = new int[maxVertex];
    vertexLevel = new int[maxVertex];
    Arrays.fill(vertexHeadEdge, -1);

    /* Construct edges: sVertex -> rVertices */
    for (int rVertex : rVertexMap.values()) {
      // Capacity: 1, select exactly 1 leader for each RegionGroup
      addAdjacentEdges(S_VERTEX, rVertex, 1);
    }

    /* Construct edges: rVertices -> sDVertices */
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        if (regionGroupIntersection.contains(regionGroupId)) {
          int rVertex = rVertexMap.get(regionGroupId);
          regionLocationMap
              .get(regionGroupId)
              .forEach(
                  dataNodeId -> {
                    if (isDataNodeAvailable(dataNodeId)
                        && isRegionAvailable(regionGroupId, dataNodeId)) {
                      int sDVertex = sDVertexMap.get(database).get(dataNodeId);
                      // Capacity: 1
                      addAdjacentEdges(rVertex, sDVertex, 1);
                    }
                  });
        }
      }
    }

    /* Construct edges: sDVertices -> tDVertices */
    // Map<DataNodeId, leader number>
    Map<Integer, Integer> leaderCounter = new TreeMap<>();
    for (Map.Entry<String, List<TConsensusGroupId>> databaseEntry :
        databaseRegionGroupMap.entrySet()) {
      String database = databaseEntry.getKey();
      for (TConsensusGroupId regionGroupId : databaseEntry.getValue()) {
        if (regionGroupIntersection.contains(regionGroupId)) {
          regionLocationMap
              .get(regionGroupId)
              .forEach(
                  dataNodeId -> {
                    if (isDataNodeAvailable(dataNodeId)) {
                      int sDVertex = sDVertexMap.get(database).get(dataNodeId);
                      int tDVertex = tDVertexMap.get(dataNodeId);
                      leaderCounter.merge(dataNodeId, 1, Integer::sum);
                      // Capacity: 1
                      addAdjacentEdges(sDVertex, tDVertex, 1);
                    }
                  });
        }
      }
    }

    /* Construct edges: tDVertices -> tVertex */
    leaderCounter.forEach(
        (dataNodeId, leaderCount) -> {
          int tDVertex = tDVertexMap.get(dataNodeId);
          // Capacity: leaderCount * capacityRate
          maxCapacity = Math.max(maxCapacity, (int) Math.round(MAX_CAPACITY_RATE * leaderCount));
          addAdjacentEdges(tDVertex, T_VERTEX, (int) Math.round(MAX_CAPACITY_RATE * leaderCount));
        });
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
    vertexLevel[S_VERTEX] = 1;
    isVertexVisited[S_VERTEX] = true;
    queue.add(S_VERTEX);
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
    return isVertexVisited[T_VERTEX];
  }

  private int dfs(int vertex, int inputFlow) {
    if (vertex == T_VERTEX) {
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
      maximumFlow += dfs(S_VERTEX, INFINITY);
    }
  }

  /**
   * @return Map<RegionGroupId, DataNodeId where the new leader locate>
   */
  private Map<TConsensusGroupId, Integer> collectLeaderDistribution() {
    Map<TConsensusGroupId, Integer> result = new ConcurrentHashMap<>();
    databaseRegionGroupMap.forEach(
        (database, regionGroupIds) ->
            regionGroupIds.forEach(
                regionGroupId -> {
                  int originalLeader = regionLeaderMap.getOrDefault(regionGroupId, -1);
                  if (!regionGroupIntersection.contains(regionGroupId)) {
                    result.put(regionGroupId, originalLeader);
                    return;
                  }
                  boolean matchLeader = false;
                  for (int currentEdge = vertexHeadEdge[rVertexMap.get(regionGroupId)];
                      currentEdge >= 0;
                      currentEdge = maxFlowEdges.get(currentEdge).nextEdge) {
                    MaxFlowEdge edge = maxFlowEdges.get(currentEdge);
                    if (edge.destVertex != S_VERTEX && edge.capacity == 0) {
                      matchLeader = true;
                      result.put(regionGroupId, sDVertexReflect.get(database).get(edge.destVertex));
                    }
                  }
                  if (!matchLeader) {
                    result.put(regionGroupId, originalLeader);
                  }
                }));
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

  @TestOnly
  public int getMaximumFlow() {
    return maximumFlow;
  }

  @TestOnly
  public int getMaxCapacity() {
    return maxCapacity;
  }
}
