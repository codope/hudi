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

import org.apache.hudi.common.engine.DeleteMarkerEvaluator;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieIOContext;
import org.apache.hudi.common.engine.OrderingComparator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupIO;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;

/**
 * Specialized implementation of {@link HoodieFileGroupIO} for the metadata table.
 * This handles metadata-specific merge semantics for different partition types.
 */
public abstract class HoodieMetadataFileGroupIO extends HoodieFileGroupIO<HoodieRecord> {

  /**
   * The merger used to merge metadata records. This will be a concrete implementation
   * like SparkMetadataRecordMerger.
   */
  private final MetadataRecordMerger metadataRecordMerger;

  /**
   * The type of partition this file group IO is handling.
   */
  private final MetadataPartitionType partitionType;

  public HoodieMetadataFileGroupIO(HoodieTableMetaClient metaClient,
                                   HoodieIOContext ioContext,
                                   DeleteMarkerEvaluator deleteMarkerEvaluator,
                                   OrderingComparator orderingComparator,
                                   MetadataRecordMerger recordMerger,
                                   MetadataPartitionType partitionType) {
    super(metaClient, ioContext, deleteMarkerEvaluator, orderingComparator, recordMerger);
    this.metadataRecordMerger = recordMerger;
    this.partitionType = partitionType;
  }

  /**
   * Merge two metadata records according to the partition-specific merge logic.
   * This delegates to the MetadataRecordMerger.
   *
   * @param older the older record
   * @param newer the newer record
   * @return the merged record or empty if the record should be deleted
   */
  @Override
  public Option<HoodieRecord> mergeRecords(HoodieRecord older, HoodieRecord newer) {
    // TODO: set new schema
    return metadataRecordMerger.merge(older, null, newer, null, null)
        .map(Pair::getLeft);
  }

  /**
   * Process a list of records for writing, applying partition-specific transformations.
   * This will be implemented by engine-specific subclasses.
   *
   * @param records the list of records to process
   * @return the processed records ready for writing
   */
  @Override
  public abstract List<?> processRecordsForWrite(List<HoodieRecord> records);

  /**
   * Factory method to create a HoodieMetadataFileGroupIO instance appropriate for the given
   * metadata partition type and engine context.
   *
   * @param metaClient            the metadata table meta client
   * @param engineType            the engine type
   * @param deleteMarkerEvaluator evaluator for delete markers
   * @param orderingComparator    comparator for record ordering
   * @param partitionType         the metadata partition type
   * @return a HoodieMetadataFileGroupIO instance
   */
  public static HoodieMetadataFileGroupIO create(HoodieTableMetaClient metaClient,
                                                 HoodieEngineContext context,
                                                 EngineType engineType,
                                                 DeleteMarkerEvaluator deleteMarkerEvaluator,
                                                 OrderingComparator orderingComparator,
                                                 MetadataPartitionType partitionType) {
    // Delegate to engine-specific factory implementations
    // This will be better implemented using ServiceLoader in a full implementation
    switch (engineType) {
      case SPARK:
        try {
          // Use reflection to avoid direct dependency on Spark classes in common module
          Class<?> sparkIOClass = Class.forName("org.apache.hudi.metadata.SparkHoodieMetadataFileGroupIO");
          return (HoodieMetadataFileGroupIO) sparkIOClass.getMethod("create",
                  HoodieTableMetaClient.class, HoodieEngineContext.class,
                  DeleteMarkerEvaluator.class, OrderingComparator.class,
                  MetadataPartitionType.class)
              .invoke(null, metaClient, context, deleteMarkerEvaluator, orderingComparator, partitionType);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create SparkHoodieMetadataFileGroupIO", e);
        }
      case FLINK:
        // Future implementation for Flink
        throw new UnsupportedOperationException("Flink implementation of HoodieMetadataFileGroupIO not yet available");
      case JAVA:
        // Future implementation for Java
        throw new UnsupportedOperationException("Java implementation of HoodieMetadataFileGroupIO not yet available");
      default:
        throw new UnsupportedOperationException("Unsupported engine type: " + engineType);
    }
  }
}