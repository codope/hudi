/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.upgrade;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.TimelineArchivers;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.hudi.common.table.timeline.HoodieInstant.UNDERSCORE;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V1;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V2;

public class UpgradeDowngradeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeDowngradeUtils.class);

  // Map of actions that were renamed in table version 8, to their original names in table version 6
  static final Map<String, String> EIGHT_TO_SIX_TIMELINE_ACTION_MAP = CollectionUtils.createImmutableMap(
      Pair.of(CLUSTERING_ACTION, REPLACE_COMMIT_ACTION)
  );
  static final Map<String, String> SIX_TO_EIGHT_TIMELINE_ACTION_MAP = CollectionUtils.reverseMap(EIGHT_TO_SIX_TIMELINE_ACTION_MAP);

  /**
   * Utility method to run compaction for MOR table as part of downgrade step.
   */
  public static void runCompaction(HoodieTable table, HoodieEngineContext context, HoodieWriteConfig config,
                                   SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    try {
      if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
        // set required configs for scheduling compaction.
        HoodieInstantTimeGenerator.setCommitTimeZone(table.getMetaClient().getTableConfig().getTimelineTimezone());
        HoodieWriteConfig compactionConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), CompactionTriggerStrategy.NUM_COMMITS.name());
        compactionConfig.setValue(HoodieCompactionConfig.COMPACTION_STRATEGY.key(), UnBoundedCompactionStrategy.class.getName());
        compactionConfig.setValue(HoodieMetadataConfig.ENABLE.key(), String.valueOf(config.isMetadataTableEnabled()));
        try (BaseHoodieWriteClient writeClient = upgradeDowngradeHelper.getWriteClient(compactionConfig, context)) {
          Option<String> compactionInstantOpt = writeClient.scheduleCompaction(Option.empty());
          if (compactionInstantOpt.isPresent()) {
            writeClient.compact(compactionInstantOpt.get());
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * See HUDI-6040.
   */
  public static void syncCompactionRequestedFileToAuxiliaryFolder(HoodieTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    TimelineFactory timelineFactory = metaClient.getTimelineLayout().getTimelineFactory();
    InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
    HoodieTimeline compactionTimeline = timelineFactory.createActiveTimeline(metaClient, false).filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    compactionTimeline.getInstantsAsStream().forEach(instant -> {
      String fileName = instantFileNameGenerator.getFileName(instant);
      try {
        if (!metaClient.getStorage().exists(new StoragePath(metaClient.getMetaAuxiliaryPath(), fileName))) {
          FileIOUtils.copy(metaClient.getStorage(),
              new StoragePath(metaClient.getTimelinePath(), fileName),
              new StoragePath(metaClient.getMetaAuxiliaryPath(), fileName));
        }
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  static void updateMetadataTableVersion(HoodieEngineContext context, HoodieTableVersion toVersion, HoodieTableMetaClient dataMetaClient) throws HoodieIOException {
    try {
      StoragePath metadataBasePath = new StoragePath(dataMetaClient.getBasePath(), HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
      if (dataMetaClient.getStorage().exists(metadataBasePath)) {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
            .setConf(context.getStorageConf().newInstance())
            .setBasePath(metadataBasePath)
            .build();
        metaClient.getTableConfig().setTableVersion(toVersion);
        HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error while updating metadata table version", e);
    }
  }

  static void rollbackFailedWritesAndCompact(HoodieTable table, HoodieEngineContext context, HoodieWriteConfig config,
                                             SupportsUpgradeDowngrade upgradeDowngradeHelper, boolean shouldCompact) {
    try {
      // set required configs for rollback
      HoodieInstantTimeGenerator.setCommitTimeZone(table.getMetaClient().getTableConfig().getTimelineTimezone());
      HoodieWriteConfig rollbackWriteConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
      // set eager cleaning
      rollbackWriteConfig.setValue(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.EAGER.name());
      // TODO: Check if we should hardcode disable rollback using markers.
      //       With changes in https://github.com/apache/hudi/pull/12206, we may not need to do that.
      rollbackWriteConfig.setValue(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key(), String.valueOf(config.shouldRollbackUsingMarkers()));
      // set compaction configs
      if (shouldCompact) {
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), CompactionTriggerStrategy.NUM_COMMITS.name());
        rollbackWriteConfig.setValue(HoodieCompactionConfig.COMPACTION_STRATEGY.key(), UnBoundedCompactionStrategy.class.getName());
      } else {
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "false");
      }
      // TODO: Check if we need to disable metadata table.
      rollbackWriteConfig.setValue(HoodieMetadataConfig.ENABLE.key(), String.valueOf(config.isMetadataTableEnabled()));

      // TODO: This util method is used in both upgrade and downgrade.
      //       Check if we need to instantiate write client with correct table version or is that handled internally.
      try (BaseHoodieWriteClient writeClient = upgradeDowngradeHelper.getWriteClient(rollbackWriteConfig, context)) {
        writeClient.rollbackFailedWrites();
        HoodieTableMetaClient.reload(table.getMetaClient());
        if (shouldCompact) {
          Option<String> compactionInstantOpt = writeClient.scheduleCompaction(Option.empty());
          if (compactionInstantOpt.isPresent()) {
            writeClient.compact(compactionInstantOpt.get());
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  static void upgradeToLSMTimeline(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    table.getMetaClient().getTableConfig().getTimelineLayoutVersion().ifPresent(
        timelineLayoutVersion -> ValidationUtils.checkState(TimelineLayoutVersion.LAYOUT_VERSION_1.equals(timelineLayoutVersion),
            "Upgrade to LSM timeline is only supported for layout version 1. Given version: " + timelineLayoutVersion));
    HoodieArchivedTimeline archivedTimeline = table.getMetaClient().getArchivedTimeline();
    if (archivedTimeline.getInstants().isEmpty()) {
      return;
    }

    Consumer<Exception> exceptionHandler = e -> {
      if (config.isFailOnTimelineArchivingEnabled()) {
        throw new HoodieException(e);
      }
    };

    try {
      // Migrate the LSM timeline back to the old archived timeline format
      HoodieTimelineArchiver archiver = TimelineArchivers.getInstance(TimelineLayoutVersion.LAYOUT_VERSION_2, config, table);
      archiver.archiveInstants(engineContext, archivedTimeline.getInstants(), true);
    } catch (Exception e) {
      exceptionHandler.accept(e);
    }
  }

  static void downgradeFromLSMTimeline(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    // if timeline layout version is present in the Option then check if it is LAYOUT_VERSION_2
    table.getMetaClient().getTableConfig().getTimelineLayoutVersion().ifPresent(
        timelineLayoutVersion -> ValidationUtils.checkState(TimelineLayoutVersion.LAYOUT_VERSION_2.equals(timelineLayoutVersion),
            "Downgrade from LSM timeline is only supported for layout version 2. Given version: " + timelineLayoutVersion));
    HoodieArchivedTimeline lsmArchivedTimeline = table.getMetaClient().getArchivedTimeline();
    if (lsmArchivedTimeline.getInstants().isEmpty()) {
      return;
    }

    Consumer<Exception> exceptionHandler = e -> {
      if (config.isFailOnTimelineArchivingEnabled()) {
        throw new HoodieException("Failed to downgrade LSM timeline to old archived format", e);
      }
    };

    try {
      // Migrate the LSM timeline back to the old archived timeline format
      HoodieTimelineArchiver archiver = TimelineArchivers.getInstance(table.getMetaClient().getTimelineLayoutVersion(), config, table);
      archiver.archiveInstants(engineContext, lsmArchivedTimeline.getInstants(), true);
    } catch (Exception e) {
      exceptionHandler.accept(e);
    }
  }

  static boolean downgradeActiveTimelineInstant(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName,
                                                CommitMetadataSerDeV2 commitMetadataSerDeV2, CommitMetadataSerDeV1 commitMetadataSerDeV1,
                                                ActiveTimelineV1 activeTimelineV1) {
    String replacedFileName = originalFileName;
    boolean isCompleted = instant.isCompleted();
    // Rename the metadata file name from the ${instant_time}_${completion_time}.action[.state] format in version 1.x
    // to the ${instant_time}.action[.state] format in version 0.x.
    if (isCompleted) {
      replacedFileName = replacedFileName.replaceAll(UNDERSCORE + "\\d+", "");
    }
    // Rename the action if necessary (e.g., CLUSTERING_ACTION to REPLACE_COMMIT_ACTION).
    // NOTE: New action names were only applied for pending instants. Completed instants do not have any change in action names.
    if (EIGHT_TO_SIX_TIMELINE_ACTION_MAP.containsKey(instant.getAction()) && !isCompleted) {
      replacedFileName = replacedFileName.replace(instant.getAction(), EIGHT_TO_SIX_TIMELINE_ACTION_MAP.get(instant.getAction()));
    }
    try {
      return renameTimelineV2InstantFileToV1Format(instant, metaClient, originalFileName, replacedFileName, commitMetadataSerDeV2, commitMetadataSerDeV1, activeTimelineV1);
    } catch (IOException e) {
      LOG.error("Can not to complete the downgrade from version eight to version seven. The reason for failure is {}", e.getMessage());
      throw new HoodieException(e);
    }
  }

  private static boolean renameTimelineV2InstantFileToV1Format(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName, String replacedFileName,
                                                               CommitMetadataSerDeV2 commitMetadataSerDeV2, CommitMetadataSerDeV1 commitMetadataSerDeV1, ActiveTimelineV1 activeTimelineV1)
      throws IOException {
    StoragePath fromPath = new StoragePath(TIMELINE_LAYOUT_V2.getTimelinePath(metaClient.getBasePath()), originalFileName);
    long modificationTime = instant.isCompleted() ? convertCompletionTimeToEpoch(instant) : -1;
    StoragePath toPath = new StoragePath(TIMELINE_LAYOUT_V1.getTimelinePath(metaClient.getBasePath()), replacedFileName);
    boolean success = true;
    if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION) || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
      HoodieCommitMetadata commitMetadata =
          commitMetadataSerDeV2.deserialize(instant, metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
      Option<byte[]> data = commitMetadataSerDeV1.serialize(commitMetadata);
      String toPathStr = toPath.toUri().toString();
      activeTimelineV1.createFileInMetaPath(toPathStr, data, true);
      /**
       * When we downgrade the table from 1.0 to 0.x, it is important to set the modification
       * timestamp of the 0.x completed instant to match the completion time of the
       * corresponding 1.x instant. Otherwise,  log files in previous file slices could
       * be wrongly attributed to latest file slice for 1.0 readers.
       * (see HoodieFileGroup.getBaseInstantTime)
       */
      if (modificationTime > 0) {
        metaClient.getStorage().setModificationTime(toPath, modificationTime);
      }
      metaClient.getStorage().deleteFile(fromPath);
    } else {
      success = metaClient.getStorage().rename(fromPath, toPath);
    }
    if (!success) {
      throw new HoodieIOException("an error that occurred while renaming " + fromPath + " to: " + toPath);
    }
    return true;
  }

  static boolean upgradeActiveTimelineInstant(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName,
                                              CommitMetadataSerDeV2 commitMetadataSerDeV2, CommitMetadataSerDeV1 commitMetadataSerDeV1,
                                              ActiveTimelineV2 activeTimelineV2) {
    String replacedFileName = originalFileName;
    boolean isCompleted = instant.isCompleted();
    // Rename the metadata file name from the ${instant_time}.action[.state] format in version 0.x
    // to the ${instant_time}_${completion_time}.action[.state] format in version 1.x.
    if (isCompleted) {
      String completionTime = instant.getCompletionTime(); // this is the file modification time
      String startTime = instant.requestedTime();
      replacedFileName = replacedFileName.replace(startTime, startTime + UNDERSCORE + completionTime);
    }
    // Rename the action if necessary (e.g., REPLACE_COMMIT_ACTION to CLUSTERING_ACTION).
    // NOTE: New action names were only applied for pending instants. Completed instants do not have any change in action names.
    if (SIX_TO_EIGHT_TIMELINE_ACTION_MAP.containsKey(instant.getAction()) && !isCompleted) {
      replacedFileName = replacedFileName.replace(instant.getAction(), SIX_TO_EIGHT_TIMELINE_ACTION_MAP.get(instant.getAction()));
    }
    try {
      return renameTimelineV1InstantFileToV2Format(instant, metaClient, originalFileName, replacedFileName, commitMetadataSerDeV1, commitMetadataSerDeV2, activeTimelineV2);
    } catch (IOException e) {
      LOG.warn("Can not to complete the upgrade from version seven to version eight. The reason for failure is {}", e.getMessage());
    }
    return false;
  }

  private static boolean renameTimelineV1InstantFileToV2Format(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName, String replacedFileName,
                                                               CommitMetadataSerDeV1 commitMetadataSerDeV1, CommitMetadataSerDeV2 commitMetadataSerDeV2, ActiveTimelineV2 activeTimelineV2)
      throws IOException {
    StoragePath fromPath = new StoragePath(TIMELINE_LAYOUT_V1.getTimelinePath(metaClient.getBasePath()), originalFileName);
    StoragePath toPath = new StoragePath(TIMELINE_LAYOUT_V2.getTimelinePath(metaClient.getBasePath()), replacedFileName);
    boolean success = true;
    if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION) || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
      HoodieCommitMetadata commitMetadata =
          commitMetadataSerDeV1.deserialize(instant, metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
      Option<byte[]> data = commitMetadataSerDeV2.serialize(commitMetadata);
      String toPathStr = toPath.toUri().toString();
      activeTimelineV2.createFileInMetaPath(toPathStr, data, true);
      metaClient.getStorage().deleteFile(fromPath);
    } else {
      success = metaClient.getStorage().rename(fromPath, toPath);
    }
    if (!success) {
      throw new HoodieIOException("an error that occurred while renaming " + fromPath + " to: " + toPath);
    }
    return true;
  }

  private static void deleteAnyLeftOverMarkers(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig config, String instantTime) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);
    if (writeMarkers.deleteMarkerDir(engineContext, config.getMarkersDeleteParallelism())) {
      LOG.info("Cleaned up left over marker directory for instant: {}", instantTime);
    }
  }

  /**
   * Extract Epoch time from completion time string
   *
   * @param instant : HoodieInstant
   * @return
   */
  public static long convertCompletionTimeToEpoch(HoodieInstant instant) {
    try {
      String completionTime = instant.getCompletionTime();
      // In Java 8, no direct API to convert to epoch in millis.
      // Strip off millis
      String completionTimeInSecs = completionTime.substring(0, completionTime.length() - 3);
      DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
      ZoneId zoneId = ZoneId.systemDefault();
      LocalDateTime ldtInSecs = LocalDateTime.parse(completionTimeInSecs, inputFormatter);
      long millis = Long.parseLong(completionTime.substring(completionTime.length() - 3));
      return ldtInSecs.atZone(zoneId).toEpochSecond() * 1000 + millis;
    } catch (Exception e) {
      LOG.warn("Failed to parse completion time string for instant " + instant, e);
      return -1;
    }
  }
}
