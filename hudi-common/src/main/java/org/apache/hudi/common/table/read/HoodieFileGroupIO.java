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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.DeleteMarkerEvaluator;
import org.apache.hudi.common.engine.HoodieIOContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.OrderingComparator;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A file group I/O handler that provides both read and write capabilities for a single file group.
 * <p>
 * This is an enhanced version of HoodieFileGroupReader that adds write capabilities and works
 * with engine-native record formats directly, eliminating the need for Avro conversion.
 */
public abstract class HoodieFileGroupIO<I> implements Closeable {

  // The underlying reader context used for reading records (engine specific)
  private final HoodieIOContext ioContext;

  // Table metadata client for table operations
  private final HoodieTableMetaClient metaClient;

  // The record merger to use for merging records
  private final HoodieRecordMerger recordMerger;

  // The ordering comparator for determining record order
  private final OrderingComparator orderingComparator;

  // The delete marker evaluator for detecting deleted records
  private final DeleteMarkerEvaluator deleteMarkerEvaluator;

  // Stats for both reading and writing
  private final HoodieReadStats readStats;

  public HoodieFileGroupIO(HoodieTableMetaClient metaClient,
                           HoodieIOContext ioContext,
                           DeleteMarkerEvaluator deleteMarkerEvaluator,
                           OrderingComparator orderingComparator,
                           HoodieRecordMerger recordMerger) {
    this.metaClient = metaClient;
    this.ioContext = ioContext;
    this.deleteMarkerEvaluator = deleteMarkerEvaluator;
    this.orderingComparator = orderingComparator;
    this.recordMerger = recordMerger;
    this.readStats = new HoodieReadStats();
  }

  /**
   * Get the underlying reader context.
   */
  public HoodieReaderContext getReaderContext() {
    return ioContext.getReaderContext();
  }

  /**
   * Get the underlying IO context.
   */
  public HoodieIOContext getIOContext() {
    return ioContext;
  }

  /**
   * Get the table metadata client.
   */
  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  /**
   * Get the read stats.
   */
  public HoodieReadStats getReadStats() {
    return readStats;
  }

  /**
   * Get the record merger.
   */
  public HoodieRecordMerger getRecordMerger() {
    return recordMerger;
  }

  /**
   * Get the ordering comparator.
   */
  public OrderingComparator<I> getOrderingComparator() {
    return orderingComparator;
  }

  /**
   * Get the delete marker evaluator.
   */
  public DeleteMarkerEvaluator<I> getDeleteMarkerEvaluator() {
    return deleteMarkerEvaluator;
  }

  /**
   * Creates a file group reader for the given file slice.
   * This adapts the file group IO to use the HoodieFileGroupReader.
   *
   * @param fileSlice       The file slice to read from
   * @param requestedSchema The schema to use for reading
   * @param props           The properties to use for reading
   * @return a file group reader
   */
  public abstract HoodieFileGroupReader createFileGroupReader(
      FileSlice fileSlice,
      Schema requestedSchema,
      TypedProperties props);

  /**
   * Creates a file group reader for the given file slice.
   * This is a shorthand for {@link #createFileGroupReader(FileSlice, Schema, TypedProperties)}.
   *
   * @param fileSlice The file slice to read from
   * @return a file group reader
   */
  public HoodieFileGroupReader createFileGroupReader(FileSlice fileSlice) {
    return createFileGroupReader(fileSlice, null, new TypedProperties());
  }

  /**
   * Get the record key from a record.
   *
   * @param record The record to get the key from
   * @return the record key
   */
  public abstract String getRecordKey(Object record);

  /**
   * Merge two records.
   *
   * @param older The older record
   * @param newer The newer record
   * @return The merged record or empty if the record should be deleted
   */
  public abstract Option<HoodieRecord> mergeRecords(HoodieRecord older, HoodieRecord newer);

  /**
   * Process a list of records for writing.
   * Convert records to the engine's native format (like InternalRow in Spark)
   * and perform any necessary transformations before writing.
   *
   * @param records The records to process
   * @return The processed records ready for writing
   */
  public abstract List<?> processRecordsForWrite(List<HoodieRecord> records);

  @Override
  public void close() throws IOException {
    // Nothing to close by default
  }
}
