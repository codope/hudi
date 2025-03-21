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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.util.List;

/**
 * Spark implementation of {@link HoodieIOContext} for Spark's InternalRow format.
 */
public class HoodieSparkIOContext implements HoodieIOContext<InternalRow> {

  @Override
  public HoodieEngineContext getEngineContext() {
    return null;
  }

  @Override
  public HoodieStorage getStorage() {
    return null;
  }

  @Override
  public Option<String> getConfigValue(String configName) {
    return null;
  }

  @Override
  public HoodieReaderContext<?> getReaderContext() {
    return null;
  }

  @Override
  public long writeRecords(List<HoodieRecord> records, Schema schema) throws IOException {
    // Implementation of writing InternalRows would go here
    // This would delegate to a Spark-specific writer that handles InternalRow
    throw new UnsupportedOperationException("Writing InternalRows not yet implemented");
  }

  @Override
  public InternalRow convertToEngineSpecificRecord(HoodieRecord record) {
    // Implementation of converting HoodieRecord to InternalRow would go here
    throw new UnsupportedOperationException("Converting to InternalRow not yet implemented");
  }

  @Override
  public void close() throws IOException {
    // Clean up any resources
  }
}