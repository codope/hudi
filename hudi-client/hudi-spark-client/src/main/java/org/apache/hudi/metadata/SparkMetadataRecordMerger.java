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

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieMetadataFileInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieSparkIOContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.combineFileSystemMetadata;

/**
 * Spark implementation of {@link MetadataRecordMerger} that works with Spark's InternalRow format.
 */
public abstract class SparkMetadataRecordMerger extends MetadataRecordMerger {

  public SparkMetadataRecordMerger(HoodieEngineContext engineContext) {
    super(engineContext);
  }

  @Override
  protected Option<HoodieRecord> mergeFilesPartitionRecords(HoodieRecord older, HoodieRecord newer) {
    // Extract metadata payloads from records
    HoodieMetadataPayload olderPayload = extractPayload(older);
    HoodieMetadataPayload newerPayload = extractPayload(newer);

    // Use the existing combineFileSystemMetadata logic from HoodieTableMetadataUtil
    Map<String, HoodieMetadataFileInfo> mergedFileInfo = combineFileSystemMetadata(olderPayload, newerPayload);

    // Create new payload with merged data
    HoodieMetadataPayload mergedPayload = new HoodieMetadataPayload(
        newerPayload.key, newerPayload.type, mergedFileInfo);

    // Create new record with merged payload
    return Option.of(new HoodieAvroRecord<>(new HoodieKey(newerPayload.key, newer.getPartitionPath()), mergedPayload));
  }

  @Override
  protected Option<HoodieRecord> mergeColumnStatsRecords(HoodieRecord older, HoodieRecord newer) {
    // Extract metadata payloads from records
    HoodieMetadataPayload olderPayload = extractPayload(older);
    HoodieMetadataPayload newerPayload = extractPayload(newer);

    // Ensure column stats are present in both records
    if (!olderPayload.getColumnStatMetadata().isPresent() || !newerPayload.getColumnStatMetadata().isPresent()) {
      // If one doesn't have column stats, return the newer record
      return Option.of(newer);
    }

    // Merge column stats using existing utility method
    HoodieMetadataColumnStats mergedStats = HoodieTableMetadataUtil.mergeColumnStatsRecords(olderPayload.getColumnStatMetadata().get(), newerPayload.getColumnStatMetadata().get());

    // Create new payload with merged stats
    HoodieMetadataPayload mergedPayload = new HoodieMetadataPayload(
        newerPayload.key, mergedStats, newerPayload.type);

    // Create new record with merged payload
    return Option.of(new HoodieAvroRecord<>(new HoodieKey(newerPayload.key, newer.getPartitionPath()), mergedPayload));
  }

  @Override
  protected Option<HoodieRecord> mergeBloomFilterRecords(HoodieRecord older, HoodieRecord newer) {
    // For bloom filters, newer bloom filter always takes precedence (no actual merging)
    HoodieMetadataPayload newerPayload = extractPayload(newer);

    // If the newer record is marked for deletion, return empty to delete the record
    if (newerPayload.isDeleted() || (newerPayload.getBloomFilterMetadata().isPresent() && newerPayload.getBloomFilterMetadata().get().getIsDeleted())) {
      return Option.empty();
    }

    // Otherwise just return the newer record
    return Option.of(newer);
  }

  @Override
  protected Option<HoodieRecord> mergePartitionStatsRecords(HoodieRecord older, HoodieRecord newer) {
    // Partition stats are merged similarly to column stats
    return mergeColumnStatsRecords(older, newer);
  }

  @Override
  protected Option<HoodieRecord> mergeRecordIndexRecords(HoodieRecord older, HoodieRecord newer) {
    // For record index, newer record always takes precedence (no actual merging)
    // This is because record index maps a record key to its latest location
    return Option.of(newer);
  }

  @Override
  protected Option<HoodieRecord> mergeExpressionIndexRecords(HoodieRecord older, HoodieRecord newer) {
    // For expression index, newer record always takes precedence
    return Option.of(newer);
  }

  @Override
  protected Option<HoodieRecord> mergeSecondaryIndexRecords(HoodieRecord older, HoodieRecord newer) {
    // Extract payload from newer record to check if it's a delete
    HoodieMetadataPayload newerPayload = extractPayload(newer);

    // If the newer record is marked for deletion, return empty to delete the record
    if (newerPayload.isDeleted() || (newerPayload.secondaryIndexMetadata != null && newerPayload.secondaryIndexMetadata.getIsDeleted())) {
      return Option.empty();
    }

    // Otherwise just return the newer record
    return Option.of(newer);
  }

  /**
   * Helper method to convert from HoodieRecord to HoodieMetadataPayload.
   */
  private HoodieMetadataPayload extractPayload(HoodieRecord record) {
    if (record.getData() instanceof HoodieMetadataPayload) {
      return (HoodieMetadataPayload) record.getData();
    } else {
      // Convert from engine-specific format if needed
      // This would handle InternalRow for Spark
      throw new UnsupportedOperationException("Converting from engine-specific format not yet implemented");
    }
  }

  /**
   * Helper method to convert an InternalRow to HoodieMetadataPayload.
   * This would be implemented in a real implementation.
   */
  private HoodieMetadataPayload convertRowToMetadataPayload(InternalRow row, HoodieSparkIOContext context) {
    // Implementation would convert Spark's InternalRow to HoodieMetadataPayload
    // For now, this is a placeholder
    throw new UnsupportedOperationException("Converting from InternalRow not yet implemented");
  }
}