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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface that defines I/O operations to be used by different engines (Spark, Flink, etc.).
 * Mainly consists of contextual information and operation interfaces needed to perform
 * storage I/O operations.
 */
public interface HoodieIOContext<T> extends Closeable {

  /**
   * @return Engine-specific context.
   */
  HoodieEngineContext getEngineContext();

  /**
   * @return Hoodie storage.
   */
  HoodieStorage getStorage();

  /**
   * @return The storage configuration.
   */
  Option<String> getConfigValue(String configName);

  /**
   * Create a HoodieReaderContext for this engine context.
   *
   * @return HoodieReaderContext for this engine
   */
  HoodieReaderContext<?> getReaderContext();

  /**
   * Write a list of records to storage using the engine-specific format.
   *
   * @param records The records to write
   * @param schema  The schema to use for writing
   * @return Number of records written
   * @throws IOException if write fails
   */
  long writeRecords(List<HoodieRecord> records, Schema schema) throws IOException;

  /**
   * Convert a HoodieRecord to the engine-specific format.
   *
   * @param record The HoodieRecord to convert
   * @return The engine-specific record
   */
  Object convertToEngineSpecificRecord(HoodieRecord record);

  @Override
  default void close() throws IOException {
  }
}