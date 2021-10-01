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
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.{Tag, Test}

import java.util
import java.util.List
import scala.collection.JavaConverters.seqAsJavaListConverter;

@Tag("functional")
class TestCOWDataSourceConsistentInserts extends SparkClientFunctionalTestHarness {

  val opts = Map(
    DataSourceWriteOptions.TABLE_NAME.key() -> "language",
    DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    DataSourceWriteOptions.OPERATION.key() -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
    DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "lang",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "score",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "",
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> classOf[NonpartitionedKeyGenerator].getCanonicalName,
    DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key() -> "true",
    DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getCanonicalName,
    HoodieWriteConfig.TBL_NAME.key() -> "language",
    HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key() -> "4"
  )

  @Test
  def insertsWithDuplicatesShouldBeConsistent(): Unit = {
    val schema = StructType(
      StructField("lang", StringType, true) ::
        StructField("score", IntegerType, true) ::
        StructField("level", IntegerType, true) :: Nil)
    val data1 = Seq(
      Row("python", 1, 0),
      Row("scala", 2, 1)
    ).toList.asJava
    val dfFromData1 = spark.createDataFrame(data1, schema)
    dfFromData1.printSchema()
    dfFromData1.write.format("hudi").options(opts).mode(Overwrite).save(basePath)
    val tableData1 = spark.read.format("hudi").load(basePath)
    tableData1.show()

    val data2 = Seq(
      Row("python", 2, null),
      Row("scala", 2, 1)
    ).toList.asJava
    val dfFromData2 = spark.createDataFrame(data2, schema)
    dfFromData2.printSchema()
    dfFromData2.write.format("hudi").options(opts).mode(Append).save(basePath)
    val tableData2 = spark.read.format("hudi").load(basePath)
    tableData2.show()
    //assertEquals(3, tableData2.where(col("_hoodie_record_key") === "scala").count())
    //assertEquals(2, tableData2.where(col("_hoodie_record_key") === "java").count())
  }
}
