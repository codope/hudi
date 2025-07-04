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

package org.apache.hudi.io.storage;

import org.apache.hudi.storage.StoragePath;

import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * HoodieFileBinaryCopier is a high-performance utility designed for efficient merging of data files at the binary level.
 * Unlike conventional hoodie writers, it bypasses costly data processing operations through a block-based approach, like:
 * 1. Merge multi-parquet files at rowgroup level
 * 2. Merge multi-orc files at stripe level
 */
public interface HoodieFileBinaryCopier {

  long binaryCopy(List<StoragePath> inputFilePaths, List<StoragePath> outputFilePath, MessageType writeSchema, Properties props) throws IOException;

  void close() throws IOException;
}
