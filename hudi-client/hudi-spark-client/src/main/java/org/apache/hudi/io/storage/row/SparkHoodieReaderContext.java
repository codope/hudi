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

import org.apache.hudi.BaseSparkInternalRowReaderContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Spark implementation of {@link org.apache.hudi.common.engine.HoodieReaderContext} that works with {@link InternalRow}.
 */
public class SparkHoodieReaderContext extends BaseSparkInternalRowReaderContext {
  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieReaderContext.class);

  private final HoodieEngineContext engineContext;
  private final TypedProperties props;
  private final KeyGenerator keyGenerator;

  public SparkHoodieReaderContext(
      HoodieEngineContext engineContext,
      TypedProperties props,
      KeyGenerator keyGenerator) {
    this.engineContext = engineContext;
    this.props = props;
    this.keyGenerator = keyGenerator;
  }

  public String getRecordKey(InternalRow record) {
    return "record-key";  // TODO: implement using keyGenerator
  }

  public String getPartitionPath(InternalRow record) {
    return "partition-path";  // TODO: implement using keyGenerator
  }

  @Override
  public ClosableIterator<InternalRow> getFileRecordIterator(StoragePath filePath, long start, long length, Schema dataSchema, Schema requiredSchema, HoodieStorage storage) throws IOException {
    return null;
  }

  @Override
  public InternalRow convertAvroRecord(IndexedRecord avroRecord) {
    return null;
  }

  @Override
  public GenericRecord convertToAvroRecord(InternalRow record, Schema schema) {
    return null;
  }

  @Override
  public ClosableIterator<InternalRow> mergeBootstrapReaders(ClosableIterator<InternalRow> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<InternalRow> dataFileIterator,
                                                             Schema dataRequiredSchema) {
    return null;
  }

  @Override
  public InternalRow processRecordWithNewVersion(InternalRow currentRecord, Object newRecord) {
    return null;
  }
}