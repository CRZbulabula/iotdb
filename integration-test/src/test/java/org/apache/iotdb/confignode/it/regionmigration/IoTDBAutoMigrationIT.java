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

package org.apache.iotdb.confignode.it.regionmigration;

import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.manager.load.balancer.region.AerospikeRegionGroupAllocator.generateReplicationList;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBAutoMigrationIT {

  private static final Logger LOGGER = IoTDBTestLogger.logger;
  private static final Random RANDOM = new Random();
  private static final String SHOW_DATA_REGIONS = "show data regions";
  private static final String INSERTION_FORMAT =
      "INSERT INTO root.db.d%d(timestamp, speed, temperature) values(%d, %d, %d)";
  private static final int REPLICATION_FACTOR = 2;
  private static final int SERIES_NUM = 20;
  private static final int BATCH_SIZE = 10;
  private static final int STEP = 36000;
  private static final long DAY_ONE = 954432000000L;
  private static final long DAY_TWO = 1711814400000L;
  private static final double DATA_REGION_PER_DATA_NODE = 4;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSeriesPartitionExecutorClass(
            "org.apache.iotdb.commons.partition.executor.hash.RIPEMD160Executor")
        .setRegionGroupAllocatePolicy("AEROSPIKE")
        .setEnableAutoLeaderBalanceForIoTConsensus(true)
        .setLeaderDistributionPolicy("AEROSPIKE")
        .setTimestampPrecision("ms")
        .setTimePartitionInterval(86400000)
        .setSeriesSlotNum(SERIES_NUM / 4)
        .setDataRegionPerDataNode(DATA_REGION_PER_DATA_NODE)
        .setDataReplicationFactor(REPLICATION_FACTOR)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS_V2)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    // Start a 1C2D cluster
    EnvFactory.getEnv().initClusterEnvironment(1, 2);
  }

  @After
  public void tearDown() throws InterruptedException {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testAutoMigration() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Insert day one data to create DataRegions
      batchedInsertData(statement, DAY_ONE);
      LOGGER.info("Insert day one data complete.");
      // Register 2 new DataNodes
      EnvFactory.getEnv().registerNewDataNode(true);
      EnvFactory.getEnv().registerNewDataNode(true);
      LOGGER.info("Register new DataNodes complete.");
      // Insert data two data to create more DataRegions
      batchedInsertData(statement, DAY_TWO);
      LOGGER.info("Insert day two data complete.");
      // Trigger auto migration
      statement.execute("migrate regions");

      // Collect region destinations
      List<Integer> dataNodeIds = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery("show datanodes")) {
        while (resultSet.next()) {
          dataNodeIds.add(resultSet.getInt(ColumnHeaderConstant.NODE_ID));
        }
      }
      Set<Integer> dataRegionIds = new HashSet<>();
      try (final ResultSet resultSet = statement.executeQuery(SHOW_DATA_REGIONS)) {
        while (resultSet.next()) {
          dataRegionIds.add(resultSet.getInt(ColumnHeaderConstant.REGION_ID));
        }
      }
      Map<Integer, Set<Integer>> destinationMap = new HashMap<>();
      for (int regionId : dataRegionIds) {
        destinationMap.put(
            regionId,
            new HashSet<>(
                generateReplicationList(regionId, dataNodeIds).subList(0, REPLICATION_FACTOR)));
      }

      // Wait until migration success
      for (int iter = 0; iter < 120; iter++) {
        boolean isMigrationComplete = true;
        try (final ResultSet resultSet = statement.executeQuery(SHOW_DATA_REGIONS)) {
          Map<Integer, Set<Integer>> regionDataNodeMap = new HashMap<>();
          while (resultSet.next()) {
            int regionId = resultSet.getInt(ColumnHeaderConstant.REGION_ID);
            int dataNodeId = resultSet.getInt(ColumnHeaderConstant.DATA_NODE_ID);
            String status = resultSet.getString(ColumnHeaderConstant.STATUS);
            if (!destinationMap.get(regionId).contains(dataNodeId)) {
              // Migration does not complete since the old region has not been deleted.
              isMigrationComplete = false;
              break;
            }
            if (!RegionStatus.Running.getStatus().equals(status)) {
              // Migration does not complete since the new region has not synchronized all data.
              isMigrationComplete = false;
              break;
            }
            regionDataNodeMap.computeIfAbsent(regionId, k -> new HashSet<>()).add(dataNodeId);
          }
          // Migration is completed iff. the destination map equals to the region data node map.
          if (!destinationMap.equals(regionDataNodeMap)) {
            isMigrationComplete = false;
          }
        }
        if (isMigrationComplete) {
          return;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }
    Assert.fail("Migration failed");
  }

  private void batchedInsertData(Statement statement, long startTimestamp) throws SQLException {
    for (int i = 0; i < SERIES_NUM; i++) {
      long timestamp = startTimestamp;
      for (long row = 0; row < BATCH_SIZE; row++) {
        String insertion =
            String.format(INSERTION_FORMAT, i, timestamp, RANDOM.nextInt(), RANDOM.nextInt());
        statement.execute(insertion);
        timestamp += STEP;
      }
    }
  }
}
