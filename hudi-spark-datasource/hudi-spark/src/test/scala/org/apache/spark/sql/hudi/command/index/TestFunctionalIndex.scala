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

package org.apache.spark.sql.hudi.command.index

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.Option
import org.apache.hudi.hive.HiveSyncConfigHolder._
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
import org.apache.hudi.hive.testutils.HiveTestUtil
import org.apache.hudi.sync.common.HoodieSyncConfig.{META_SYNC_BASE_PATH, META_SYNC_DATABASE_NAME, META_SYNC_PARTITION_EXTRACTOR_CLASS, META_SYNC_TABLE_NAME}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, ShowIndexesCommand}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

class TestFunctionalIndex extends HoodieSparkSqlTestBase {

  test("Test Create Functional Index Syntax") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val databaseName = "default"
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          val sqlParser: ParserInterface = spark.sessionState.sqlParser
          val analyzer: Analyzer = spark.sessionState.analyzer

          var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
          var resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

          logicalPlan = sqlParser.parsePlan(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
          resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
          assertResult("idx_datestr")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
          assertResult("column_stats")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
          assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
          assertResult(Map("func" -> "from_unixtime", "format" -> "yyyy-MM-dd"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)

          logicalPlan = sqlParser.parsePlan(s"create index idx_name on $tableName using bloom_filters(name) options(func='lower')")
          resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
          assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
          assertResult("bloom_filters")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
          assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
          assertResult(Map("func" -> "lower"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)
        }
      }
    }
  }

  test("Test Create Functional Index") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val databaseName = "default"
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          val sqlParser: ParserInterface = spark.sessionState.sqlParser
          val analyzer: Analyzer = spark.sessionState.analyzer

          var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
          var resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

          val createIndexSql = s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')"
          logicalPlan = sqlParser.parsePlan(createIndexSql)
          resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
          assertResult("idx_datestr")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
          assertResult("column_stats")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
          assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)

          spark.sql(createIndexSql)
          val metaClient = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(spark.sessionState.newHadoopConf())
            .build()
          assertTrue(metaClient.getFunctionalIndexMetadata.isPresent)
          val functionalIndexMetadata = metaClient.getFunctionalIndexMetadata.get()
          assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
          assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)
        }
      }
    }
  }

  test("Test Functional Index With Hive Sync Non Partitioned Table") {
    if (HoodieSparkUtils.gteqSpark3_2) {
      withTempDir { tmp =>
        Seq("mor").foreach { tableType =>
          val databaseName = "default"
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          val createIndexSql = s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')"
          spark.sql(createIndexSql)
          val metaClient = HoodieTableMetaClient.builder()
            .setBasePath(basePath)
            .setConf(spark.sessionState.newHadoopConf())
            .build()
          assertTrue(metaClient.getFunctionalIndexMetadata.isPresent)
          val functionalIndexMetadata = metaClient.getFunctionalIndexMetadata.get()
          assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
          assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)

          // sync to hive without partition metadata
          val hiveSyncProps = new TypedProperties()
          hiveSyncProps.setProperty(HIVE_URL.key, "jdbc:hive2://localhost:10000")
          hiveSyncProps.setProperty(HIVE_USER.key, "")
          hiveSyncProps.setProperty(HIVE_PASS.key, "")
          hiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key, databaseName)
          hiveSyncProps.setProperty(META_SYNC_TABLE_NAME.key, tableName)
          hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key, basePath)
          hiveSyncProps.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key, "false")
          // hiveSyncProps.setProperty(META_SYNC_PARTITION_FIELDS.key, "datestr")
          hiveSyncProps.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key, classOf[SlashEncodedDayPartitionValueExtractor].getName)
          hiveSyncProps.setProperty(HIVE_BATCH_SYNC_PARTITION_NUM.key, "3")
          HiveTestUtil.setUp(Option.of(hiveSyncProps))

          // check query result
          val queryResult = spark.sql(s"select * from $tableName where datestr='1970-01-02'")
          assertResult(1)(queryResult.count())
          HiveTestUtil.clear()
          HiveTestUtil.shutdown()
        }
      }
    }
  }

  private def assertTableIdentifier(catalogTable: CatalogTable,
                                    expectedDatabaseName: String,
                                    expectedTableName: String): Unit = {
    assertResult(Some(expectedDatabaseName))(catalogTable.identifier.database)
    assertResult(expectedTableName)(catalogTable.identifier.table)
  }
}
