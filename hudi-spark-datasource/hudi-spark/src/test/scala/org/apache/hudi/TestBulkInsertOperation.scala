/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.JavaConverters

class TestBulkInsertOperation extends TestingHoodieOperationSparkEvents {

  override def performWriteOperation(data: DataFrame, options: Map[String, String]): Unit = {
    data.write
      .format("hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(tempPath.toString)
  }

  test("test bulk insert stages") {
    val hudiOptions = Map(
      HoodieWriteConfig.TBL_NAME.key -> "bulk_insert_table",
      TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name,
      OPERATION.key -> BULK_INSERT_OPERATION_OPT_VAL,
      RECORDKEY_FIELD.key -> "_row_key",
      PARTITIONPATH_FIELD.key -> "partition",
      KEYGENERATOR_CLASS_NAME.key -> classOf[SimpleKeyGenerator].getName,
    )

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)
    val recordsSeq = convertRowListToSeq(inserts)
    val dataToInsert: DataFrame = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    val expectedNumberOfStagesWrite = 20

    runWriteTest(dataToInsert, hudiOptions, expectedNumberOfStagesWrite)

    // Perform a read operation and assert no new stages as it's simple load
    val expectedNumberOfStagesRead = 0
    runReadTest(tempPath.toString, expectedNumberOfStagesRead)
  }

  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq
}
