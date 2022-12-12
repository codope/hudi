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

package org.apache.hudi.util

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hudi.ColumnStatsIndexSupport
import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals

import scala.collection.JavaConverters._

object ColumnStatsTestUtils {

  def validateColumnStatsIndex(shouldReadInMemory: Boolean,
                               metadataOpts: Map[String, String],
                               expectedColStatsSourcePath: String,
                               validateColumnStatsManually: Boolean,
                               spark: SparkSession,
                               fs: FileSystem,
                               sourceTableSchema: StructType,
                               metaClient: HoodieTableMetaClient,
                               basePath: String): Unit = {
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

    val expectedColStatsSchema = composeIndexSchema(sourceTableSchema.fieldNames, sourceTableSchema)
    val validationSortColumns = Seq("c1_maxValue", "c1_minValue", "c2_maxValue", "c2_minValue")

    columnStatsIndex.loadTransposed(sourceTableSchema.fieldNames, shouldReadInMemory) { transposedColStatsDF =>
      // Match against expected column stats table
      val expectedColStatsIndexTableDf =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource(expectedColStatsSourcePath).toString)

      assertEquals(expectedColStatsIndexTableDf.schema, transposedColStatsDF.schema)
      // NOTE: We have to drop the `fileName` column as it contains semi-random components
      //       that we can't control in this test. Nevertheless, since we manually verify composition of the
      //       ColStats Index by reading Parquet footers from individual Parquet files, this is not an issue
      assertEquals(asJson(sort(expectedColStatsIndexTableDf, validationSortColumns)),
        asJson(sort(transposedColStatsDF.drop("fileName"), validationSortColumns)))

      if (validateColumnStatsManually) {
        // TODO(HUDI-4557): support validation of column stats of avro log files
        // Collect Column Stats manually (reading individual Parquet files)
        val manualColStatsTableDF =
        buildColumnStatsTableManually(basePath, sourceTableSchema.fieldNames, sourceTableSchema.fieldNames, expectedColStatsSchema, spark, fs, sourceTableSchema)

        assertEquals(asJson(sort(manualColStatsTableDF, validationSortColumns)),
          asJson(sort(transposedColStatsDF, validationSortColumns)))
      }
    }
  }

  def buildColumnStatsTableManually(tablePath: String,
                                    includedCols: Seq[String],
                                    indexedCols: Seq[String],
                                    indexSchema: StructType,
                                    spark: SparkSession,
                                    fs: FileSystem,
                                    sourceTableSchema: StructType): DataFrame = {
    val files = {
      val it = fs.listFiles(new Path(tablePath), true)
      var seq = Seq[LocatedFileStatus]()
      while (it.hasNext) {
        seq = seq :+ it.next()
      }
      seq.filter(fs => fs.getPath.getName.endsWith(".parquet"))
    }

    spark.createDataFrame(
      files.flatMap(file => {
        val df = spark.read.schema(sourceTableSchema).parquet(file.getPath.toString)
        val exprs: Seq[String] =
          s"'${typedLit(file.getPath.getName)}' AS file" +:
            s"sum(1) AS valueCount" +:
            df.columns
              .filter(col => includedCols.contains(col))
              .flatMap(col => {
                val minColName = s"${col}_minValue"
                val maxColName = s"${col}_maxValue"
                if (indexedCols.contains(col)) {
                  Seq(
                    s"min($col) AS $minColName",
                    s"max($col) AS $maxColName",
                    s"sum(cast(isnull($col) AS long)) AS ${col}_nullCount"
                  )
                } else {
                  Seq(
                    s"null AS $minColName",
                    s"null AS $maxColName",
                    s"null AS ${col}_nullCount"
                  )
                }
              })

        df.selectExpr(exprs: _*)
          .collect()
      }).asJava,
      indexSchema
    )
  }

  def asJson(df: DataFrame) =
    df.toJSON
      .select("value")
      .collect()
      .toSeq
      .map(_.getString(0))
      .mkString("\n")

  def sort(df: DataFrame): DataFrame = {
    sort(df, Seq("c1_maxValue", "c1_minValue"))
  }

  private def sort(df: DataFrame, sortColumns: Seq[String]): DataFrame = {
    val sortedCols = df.columns.sorted
    // Sort dataset by specified columns (to minimize non-determinism in case multiple files have the same
    // value of the first column)
    df.select(sortedCols.head, sortedCols.tail: _*)
      .sort(sortColumns.head, sortColumns.tail: _*)
  }
}
