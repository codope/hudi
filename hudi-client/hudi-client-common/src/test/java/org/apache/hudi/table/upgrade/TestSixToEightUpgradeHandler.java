/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE;
import static org.apache.hudi.common.table.HoodieTableConfig.BOOTSTRAP_INDEX_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.INITIAL_VERSION;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestSixToEightUpgradeHandler {

  @Mock
  private HoodieTable table;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieEngineContext context;
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTableConfig tableConfig;
  @Mock
  private SupportsUpgradeDowngrade upgradeDowngradeHelper;

  private SixToEightUpgradeHandler upgradeHandler;

  @BeforeEach
  void setUp() {
    upgradeHandler = new SixToEightUpgradeHandler();
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
  }

  @Test
  void testPropertyUpgrade() {
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    // Simulate config values for key generator and partition path
    when(config.getString(anyString())).thenReturn("partition_field");
    when(tableConfig.getKeyGeneratorClassName()).thenReturn("CustomKeyGenerator");

    // Upgrade properties
    upgradeHandler.upgradePartitionFields(config, tableConfig, tablePropsToAdd);
    assertEquals("partition_field", tablePropsToAdd.get(PARTITION_FIELDS));

    upgradeHandler.setInitialVersion(config, tableConfig, tablePropsToAdd);
    assertEquals("SIX", tablePropsToAdd.get(INITIAL_VERSION));

    // Mock record merge mode configuration for merging behavior
    when(config.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    upgradeHandler.setRecordMergeMode(config, tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(RECORD_MERGE_MODE));
    assertNotNull(tablePropsToAdd.get(RECORD_MERGE_MODE));

    // Simulate bootstrap index type upgrade
    when(tableConfig.getBooleanOrDefault(BOOTSTRAP_INDEX_ENABLE)).thenReturn(true);
    when(tableConfig.contains(BOOTSTRAP_INDEX_CLASS_NAME)).thenReturn(true);
    when(tableConfig.getString(BOOTSTRAP_INDEX_CLASS_NAME)).thenReturn(HFileBootstrapIndex.class.getName());
    when(config.getString(BOOTSTRAP_INDEX_TYPE)).thenReturn("GLOBAL_BLOOM");
    upgradeHandler.upgradeBootstrapIndexType(config, tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_CLASS_NAME));
    assertTrue(tablePropsToAdd.containsKey(BOOTSTRAP_INDEX_TYPE));

    // Simulate key generator type upgrade
    upgradeHandler.upgradeKeyGeneratorType(config, tableConfig, tablePropsToAdd);
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_CLASS_NAME));
    assertTrue(tablePropsToAdd.containsKey(KEY_GENERATOR_TYPE));
  }

  @Test
  void testTimelineUpgrade() {
    List<HoodieInstant> instants = Arrays.asList(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, CLUSTERING_ACTION, "20211012123000"),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, CLUSTERING_ACTION, "20211012123001")
    );
    when(metaClient.getActiveTimeline().getInstants()).thenReturn(instants);

    try (MockedStatic<UpgradeDowngradeUtils> upgradeDowngradeUtilsMock = mockStatic(UpgradeDowngradeUtils.class)) {
      upgradeHandler.upgrade(config, context, "20211012123000", upgradeDowngradeHelper);

      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.runCompaction(any(), any(), any(), any()), times(1));
      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.syncCompactionRequestedFileToAuxiliaryFolder(any()), times(1));
      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.upgradeToLSMTimeline(any(), any(), any()), times(1));
      upgradeDowngradeUtilsMock.verify(() -> UpgradeDowngradeUtils.upgradeActiveTimelineInstant(any(), any(), any(), any(), any(), any()), times(instants.size()));
    }
  }
}
