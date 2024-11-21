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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSevenToEightUpgrade extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionFieldsWithUpgrade(tableType: HoodieTableType): Unit = {
    val partitionFields = "partition:simple"
    // Downgrade handling for metadata not yet ready.
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> KeyGeneratorType.CUSTOM.getClassName,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionFields,
      "hoodie.metadata.enable" -> "false")

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = Overwrite,
      validate = false)
    metaClient = getLatestMetaClient(true)

    // assert table version is eight and the partition fields in table config has partition type
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

    // downgrade table props to version seven
    // assert table version is seven and the partition fields in table config does not have partition type
    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.SEVEN, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.SEVEN, metaClient.getTableConfig.getTableVersion)
    assertEquals("partition", HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

    // auto upgrade the table
    // assert table version is eight and the partition fields in table config has partition type
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = Append,
      validate = false)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
  }

  @Test
  def testDowngradeTable(): Unit = {
    val tableName = "trips_table"
    val basePath = "file:///tmp/trips_table"

    // spark-shell
    /*val columns = Seq("ts","uuid","rider","driver","fare","city")
    val data =
      Seq((1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
        (1695091554788L,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
        (1695046462179L,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
        (1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"    ),
        (1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai"));

    var inserts = spark.createDataFrame(data).toDF(columns:_*)
    inserts.write.format("hudi").
      option("hoodie.datasource.write.partitionpath.field", "city").
      option("hoodie.table.name", tableName).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option("hoodie.metadata.enable", "false").
      mode(Overwrite).
      save(basePath)*/

    // Query Data
    // spark-shell
    val tripsDF = spark.read.format("hudi").load(basePath)
    tripsDF.createOrReplaceTempView("trips_table")

    spark.sql("SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0").show()
    spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM  trips_table").show()

    // Update Data
    // Lets read data from target Hudi table, modify fare column for rider-D and update it.
    val _spark = spark
    import _spark.implicits._
    val updatesDf = spark.read.format("hudi").load(basePath).filter($"rider" === "rider-D").withColumn("fare", col("fare") * 10)

    updatesDf.write.format("hudi").
      option("hoodie.datasource.write.operation", "upsert").
      option("hoodie.datasource.write.partitionpath.field", "city").
      option("hoodie.table.name", tableName).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option("hoodie.metadata.enable", "false").
      mode(Append).
      save(basePath)

    // check data
    spark.sql("SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0").show()
  }
}
