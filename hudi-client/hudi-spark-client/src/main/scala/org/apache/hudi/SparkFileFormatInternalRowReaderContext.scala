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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{HoodieInternalRowUtils, SparkSession}

/**
 * Implementation of [[HoodieReaderContext]] to read [[InternalRow]]s with [[ParquetFileFormat]] on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param sparkSession      [[SparkSession]] instance.
 * @param parquetFileFormat [[ParquetFileFormat]] instance for parquet file format in Spark.
 * @param hadoopConf        Hadoop configuration.
 */
class SparkFileFormatInternalRowReaderContext(sparkSession: SparkSession,
                                              parquetFileFormat: ParquetFileFormat,
                                              hadoopConf: Configuration) extends BaseSparkInternalRowReaderContext {
  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter

  override def getFileRecordIterator(filePath: Path,
                                     start: Long,
                                     length: Long,
                                     dataSchema: Schema,
                                     requiredSchema: Schema,
                                     conf: Configuration): ClosableIterator[InternalRow] = {
    val fileInfo = sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
      InternalRow.empty, filePath, start, length)

    val dataStructSchema = HoodieInternalRowUtils.getCachedSchema(dataSchema)
    val requiredStructSchema = HoodieInternalRowUtils.getCachedSchema(requiredSchema)
    new CloseableInternalRowIterator(parquetFileFormat.buildReaderWithPartitionValues(
      sparkSession, dataStructSchema, StructType(Seq.empty), requiredStructSchema, Seq.empty,
      Map(), conf
    ).apply(fileInfo))
  }
}
