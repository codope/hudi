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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

case class BeginTransactionCommand(database: CatalogDatabase,
                                   tables: mutable.Map[String, CatalogTable]) extends TransactionBaseCommand {
  def run(sparkSession: SparkSession,
          tables: mutable.Map[String, CatalogTable],
          statements: Seq[String]): Seq[Row] = {
    val database = sparkSession.sessionState.catalog.getCurrentDatabase
    val transactionId = HoodieActiveTimeline.createNewInstantTime()
    statements.foreach { statement =>
      val tableIdentifier = sparkSession.sessionState.sqlParser.parseTableIdentifier(statement)
      val table = sparkSession.sessionState.catalog.getTableMetadata(tableIdentifier)
      tables.put(tableIdentifier.table, table)
      // sparkSession.sessionState.catalog.addTableToTransaction(tableIdentifier.table)
    }
    Seq.empty[Row]
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val database = sparkSession.sessionState.catalog.getCurrentDatabase
    val transactionId = HoodieActiveTimeline.createNewInstantTime()
    run(sparkSession, tables, Seq.empty[String])
    Seq.empty[Row]
  }
}

abstract class TransactionBaseCommand extends HoodieLeafRunnableCommand with Logging {

  /**
   * Create hoodie table meta client according to given table identifier and
   * spark session
   *
   * @param tableId      The table identifier
   * @param sparkSession The spark session
   * @return The hoodie table meta client
   */
  def createHoodieTableMetaClient(tableId: TableIdentifier,
                                  sparkSession: SparkSession): HoodieTableMetaClient = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableId)
    val basePath = getTableLocation(catalogTable, sparkSession)
    HoodieTableMetaClient.builder()
      .setConf(sparkSession.sqlContext.sparkContext.hadoopConfiguration)
      .setBasePath(basePath)
      .build()
  }
}
