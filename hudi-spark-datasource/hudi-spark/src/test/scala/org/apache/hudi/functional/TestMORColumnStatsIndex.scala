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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.{HoodieStorageConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.util.ColumnStatsTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestMORColumnStatsIndex extends HoodieClientTestBase {

  val tableType = HoodieTableType.MERGE_ON_READ
  var spark: SparkSession = _

  val sourceTableSchema =
    new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9, 3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c8", ByteType)

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    initFileSystem()

    setTableName("hoodie_test")
    initMetaClient()

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  @ParameterizedTest
  @MethodSource(Array("testParams"))
  def testMetadataColumnStatsIndex(forceFullLogScan: String, shouldReadInMemory: String): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      // NOTE: Currently only this setting is used like following by different MT partitions:
      //          - Files: using it
      //          - Column Stats: NOT using it (defaults to doing "point-lookups")
      HoodieMetadataConfig.ENABLE_FULL_SCAN_LOG_FILES.key -> forceFullLogScan,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    doWriteAndValidateColumnStats(tableType, shouldReadInMemory.toBoolean, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    doWriteAndValidateColumnStats(tableType, shouldReadInMemory.toBoolean, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    val expectedColStatsSourcePath = "index/colstats/mor-updated2-column-stats-index-table.json"

    doWriteAndValidateColumnStats(tableType, shouldReadInMemory.toBoolean, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }

  private def doWriteAndValidateColumnStats(tableType: HoodieTableType,
                                            shouldReadInMemory: Boolean,
                                            metadataOpts: Map[String, String],
                                            hudiOpts: Map[String, String],
                                            dataSourcePath: String,
                                            expectedColStatsSourcePath: String,
                                            operation: String,
                                            saveMode: SaveMode): Unit = {
    val sourceJSONTablePath = getClass.getClassLoader.getResource(dataSourcePath).toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(hudiOpts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    // Currently, routine manually validating the column stats (by actually reading every column of every file)
    // only supports parquet files. Therefore we skip such validation when delta-log files are present, and only
    // validate in following cases: (1) COW: all operations; (2) MOR: insert only.
    val shouldValidateColumnStatsManually = tableType == HoodieTableType.COPY_ON_WRITE ||
      operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

    ColumnStatsTestUtils.validateColumnStatsIndex(shouldReadInMemory, metadataOpts, expectedColStatsSourcePath,
      shouldValidateColumnStatsManually, spark, fs, sourceTableSchema, metaClient, basePath)
  }
}

object TestMORColumnStatsIndex {

  def testParams(): java.util.stream.Stream[Arguments] = {
    // forceFullLogScan, shouldReadInMemory
    java.util.stream.Stream.of(
      arguments("false", "false"),
      arguments("false", "true"),
      arguments("true", "false"),
      arguments("true", "true")
    )
  }
}
