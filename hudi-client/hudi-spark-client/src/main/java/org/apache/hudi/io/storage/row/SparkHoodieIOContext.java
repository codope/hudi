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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.DeleteMarkerEvaluator;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieIOContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.OrderingComparator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Spark-specific implementation of HoodieIOContext that works with InternalRow.
 */
public class SparkHoodieIOContext implements HoodieIOContext<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieIOContext.class);

  private final HoodieEngineContext engineContext;
  private final HoodieStorage storage;
  private final TypedProperties props;
  private final KeyGenerator keyGenerator;
  private final String orderingField;
  private final SparkHoodieReaderContext readerContext;

  public SparkHoodieIOContext(
      HoodieEngineContext engineContext,
      HoodieStorage storage,
      HoodieWriteConfig writeConfig,
      KeyGenerator keyGenerator) {
    this.engineContext = engineContext;
    this.storage = storage;
    this.props = writeConfig.getProps();
    this.keyGenerator = keyGenerator;
    this.orderingField = props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), null);
    this.readerContext = new SparkHoodieReaderContext(engineContext, props, keyGenerator);
  }

  /**
   * Constructor for use when only JavaSparkContext is available.
   */
  public SparkHoodieIOContext(org.apache.spark.api.java.JavaSparkContext jsc) {
    this.engineContext = new org.apache.hudi.client.common.HoodieSparkEngineContext(jsc);
    this.storage = null;
    this.props = new TypedProperties();
    this.keyGenerator = null;
    this.orderingField = null;
    this.readerContext = new SparkHoodieReaderContext(engineContext, props, keyGenerator);
  }

  @Override
  public HoodieEngineContext getEngineContext() {
    return engineContext;
  }

  @Override
  public HoodieStorage getStorage() {
    return storage;
  }

  @Override
  public Option<String> getConfigValue(String configName) {
    return Option.ofNullable(props.getString(configName, null));
  }

  @Override
  public HoodieReaderContext<?> getReaderContext() {
    return readerContext;
  }

  @Override
  public long writeRecords(List<HoodieRecord> records, Schema schema) throws IOException {
    // Implementation would convert and write records
    // For now this is a placeholder
    return records.size();
  }

  @Override
  public Object convertToEngineSpecificRecord(HoodieRecord record) {
    // Implementation would convert HoodieRecord to InternalRow
    // For now this is a placeholder
    return null;
  }

  /**
   * Extract record key from InternalRow using KeyGenerator.
   */
  public String getRecordKey(InternalRow row) {
    // This would use keyGenerator to extract the key from the InternalRow
    // For now this is a placeholder
    return "record-key";
  }

  /**
   * Extract partition path from InternalRow using KeyGenerator.
   */
  public String getPartitionPath(InternalRow row) {
    // This would use keyGenerator to extract the partition path from the InternalRow
    // For now this is a placeholder
    return "partition-path";
  }

  /**
   * Extract ordering value from InternalRow.
   */
  public Comparable getOrderingValue(InternalRow row) {
    // This would extract the ordering value from the ordering field
    // For now this is a placeholder
    return 0L;
  }

  /**
   * Create ordering comparator based on ordering field.
   */
  public OrderingComparator<InternalRow> getOrderingComparator() {
    return (row1, row2) -> {
      Comparable val1 = getOrderingValue((InternalRow) row1);
      Comparable val2 = getOrderingValue((InternalRow) row2);

      if (val1 == null && val2 == null) {
        return 0;
      } else if (val1 == null) {
        return -1;
      } else if (val2 == null) {
        return 1;
      }

      return val1.compareTo(val2);
    };
  }

  /**
   * Create delete marker evaluator.
   */
  public DeleteMarkerEvaluator<InternalRow> getDeleteMarkerEvaluator() {
    return row -> {
      // Implementation would check if the row represents a delete marker
      // For now this is a placeholder
      return false;
    };
  }

  /**
   * Close any resources.
   */
  @Override
  public void close() throws IOException {
    // Close any resources
  }
}