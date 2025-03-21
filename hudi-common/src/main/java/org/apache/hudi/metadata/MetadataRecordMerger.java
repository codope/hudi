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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;

/**
 * Implementation of {@link HoodieRecordMerger} for metadata table records.
 * This class handles merging different types of metadata records based on their partition type.
 */
public abstract class MetadataRecordMerger implements HoodieRecordMerger {

  public MetadataRecordMerger(HoodieEngineContext engineContext) {
  }

  /**
   * Merge two metadata records.
   * This method dispatches to the appropriate merge method based on the partition type of the record.
   *
   * @param older the older record
   * @param newer the newer record
   * @return the merged record
   */
  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema,
                                                  HoodieRecord newer, Schema newSchema,
                                                  TypedProperties props) {
    Option<HoodieRecord> mergedRecord = getMergedRecord(older, newer);
    if (mergedRecord.isPresent()) {
      return Option.of(Pair.of(mergedRecord.get(), newSchema));
    }
    return Option.of(Pair.of(newer, newSchema));
  }

  private Option<HoodieRecord> getMergedRecord(HoodieRecord older, HoodieRecord newer) {
    try {
      // Extract partition type from the record partition path
      String partitionPath = older.getPartitionPath();
      MetadataPartitionType partitionType = MetadataPartitionType.fromPartitionPath(partitionPath);

      switch (partitionType) {
        case FILES:
        case ALL_PARTITIONS:
          return mergeFilesPartitionRecords(older, newer);
        case COLUMN_STATS:
          return mergeColumnStatsRecords(older, newer);
        case BLOOM_FILTERS:
          return mergeBloomFilterRecords(older, newer);
        case PARTITION_STATS:
          return mergePartitionStatsRecords(older, newer);
        case RECORD_INDEX:
          return mergeRecordIndexRecords(older, newer);
        case EXPRESSION_INDEX:
          return mergeExpressionIndexRecords(older, newer);
        case SECONDARY_INDEX:
          return mergeSecondaryIndexRecords(older, newer);
        default:
          throw new IllegalArgumentException("Unknown metadata partition type: " + partitionType);
      }
    } catch (Exception e) {
      throw new HoodieException("Error merging metadata records", e);
    }
  }

  /**
   * Merge records for the FILES partition.
   * This handles file additions and deletions.
   */
  protected abstract Option<HoodieRecord> mergeFilesPartitionRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Merge records for the COLUMN_STATS partition.
   * This combines statistical information about columns.
   */
  protected abstract Option<HoodieRecord> mergeColumnStatsRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Merge records for the BLOOM_FILTERS partition.
   * Typically, newer bloom filters replace older ones.
   */
  protected abstract Option<HoodieRecord> mergeBloomFilterRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Merge records for the PARTITION_STATS partition.
   * This combines statistical information about partitions.
   */
  protected abstract Option<HoodieRecord> mergePartitionStatsRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Merge records for the RECORD_INDEX partition.
   * This handles mapping from record keys to file locations.
   */
  protected abstract Option<HoodieRecord> mergeRecordIndexRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Merge records for the EXPRESSION_INDEX partition.
   * This handles indexed expression values.
   */
  protected abstract Option<HoodieRecord> mergeExpressionIndexRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Merge records for the SECONDARY_INDEX partition.
   * This handles secondary key to primary key mappings.
   */
  protected abstract Option<HoodieRecord> mergeSecondaryIndexRecords(HoodieRecord older, HoodieRecord newer);
}