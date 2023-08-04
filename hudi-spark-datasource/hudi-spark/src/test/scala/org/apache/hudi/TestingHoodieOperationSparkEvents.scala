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

import org.apache.commons.io.FileUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

abstract class TestingHoodieOperationSparkEvents extends FunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _
  var tempPath: java.nio.file.Path = _

  override def beforeEach(): Unit = {
    initSparkContext()
    tempPath = java.nio.file.Files.createTempDirectory("hoodie_test_path")
  }

  override def afterEach(): Unit = {
    cleanupSparkContexts()
    FileUtils.deleteDirectory(tempPath.toFile)
  }

  def initSparkContext(): Unit = {
    val sparkConf = getSparkConfForTest(getClass.getSimpleName)
    spark = SparkSession.builder()
      .withExtensions(new HoodieSparkSessionExtension)
      .config(sparkConf)
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  def cleanupSparkContexts(): Unit = {
    if (sqlContext != null) {
      sqlContext.clearCache()
      sqlContext = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
    if (spark != null) {
      spark.close()
    }
  }

  class HoodieStageListener extends SparkListener {
    var stageCounter: Int = 0
    var triggerCount: Int = 0

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      stageCounter += 1
    }
  }

  class HoodieEventTriggerListener(eventToTrack: String) extends SparkListener {
    var triggerCount: Int = 0

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      if (stageCompleted.stageInfo.details.contains(eventToTrack)) {
        triggerCount += 1
      }
    }
  }

  def performWriteOperation(data: DataFrame, options: Map[String, String]): Unit

  def runWriteTest(data: DataFrame, options: Map[String, String], expectedNumberOfStages: Int): Unit = {
    val sparkContext = spark.sparkContext
    val stageListener = new HoodieStageListener
    sparkContext.addSparkListener(stageListener)

    performWriteOperation(data, options)

    assert(stageListener.stageCounter == expectedNumberOfStages)
  }

  def runReadTest(path: String, expectedNumberOfStages: Int): Unit = {
    val sparkContext = spark.sparkContext
    val stageListener = new HoodieStageListener
    sparkContext.addSparkListener(stageListener)

    spark.read.format("hudi").load(path)

    assert(stageListener.stageCounter == expectedNumberOfStages)
  }

  def validateNoRepeatedDagTrigger(data: DataFrame, options: Map[String, String], expectedNumberOfStages: Int, eventToTrack: String): Unit = {
    val sparkContext = spark.sparkContext
    val eventTriggerListener = new HoodieEventTriggerListener(eventToTrack)
    sparkContext.addSparkListener(eventTriggerListener)

    performWriteOperation(data, options)

    assert(eventTriggerListener.triggerCount == expectedNumberOfStages)
  }
}
