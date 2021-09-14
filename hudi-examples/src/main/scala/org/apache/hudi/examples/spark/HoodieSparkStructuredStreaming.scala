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

package org.apache.hudi.examples.spark

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.FAIL_ON_TIMELINE_ARCHIVING_ENABLE
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDateTime

object HoodieSparkStructuredStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SparkHudi")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", 1)
      .config("spark.sql.shuffle.partitions", 1)
      //      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") // set log levels appropriately

    // Add listeners, complete each batch, print information about the batch, such as start offset, grab the number of records, and process time to the console
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    // Define kafka flow
    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "impressions")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 5000)
      .option("failOnDataLoss", true)

    // Loading stream data, because it is only for testing purposes, reading kafka messages directly without any other processing,
    // is that spark structured streams automatically generate kafka metadata for each set of messages,
    // such as the subject of the message, partition, offset, and so on.
    val df = dataStreamReader.load()
      .selectExpr(
        "topic as kafka_topic",
        "CAST(partition AS STRING) kafka_partition",
        "cast(timestamp as String) kafka_timestamp",
        "CAST(offset AS STRING) kafka_offset",
        "CAST(key AS STRING) kafka_key",
        "CAST(value AS STRING) kafka_value",
        "current_timestamp() current_time")
      .selectExpr(
        "kafka_topic",
        "concat(kafka_partition,'-',kafka_offset) kafka_partition_offset",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "kafka_value",
        "substr(regexp_replace(current_time, ':', ''),1,15) partition_date") // minute-level partitions

    // Create and start query
    val query = df
      .writeStream
      .queryName("demo")
      .foreachBatch { (batchDF: DataFrame, _: Long) => {
        batchDF.persist()

        println(LocalDateTime.now() + " start writing cow table")
        batchDF.write.format("org.apache.hudi")
          .option(TABLE_TYPE.key, "COPY_ON_WRITE")
          .option(PRECOMBINE_FIELD.key, "kafka_timestamp")
          // Use kafka partition and offset as combined primary key
          .option(RECORDKEY_FIELD.key, "kafka_partition_offset")
          // Partition with current date
          .option(PARTITIONPATH_FIELD.key, "partition_date")
          .option(TABLE_NAME.key, "copy_on_write_table")
          .option(HIVE_SYNC_ENABLED.key, false)
          .option(HIVE_STYLE_PARTITIONING.key, true)
          .option(FAIL_ON_TIMELINE_ARCHIVING_ENABLE.key, false)
          .option(STREAMING_IGNORE_FAILED_BATCH.key, false)
          .option(STREAMING_RETRY_CNT.key, 0)
          .option("hoodie.table.name", "copy_on_write_table")
          .mode(SaveMode.Append)
          .save("/tmp/hudi_streaming_kafka/COPY_ON_WRITE")

        println(LocalDateTime.now() + " start writing mor table")
        batchDF.write.format("org.apache.hudi")
          .option(TABLE_TYPE.key, "MERGE_ON_READ")
          .option(TABLE_TYPE.key, "COPY_ON_WRITE")
          .option(PRECOMBINE_FIELD.key, "kafka_timestamp")
          .option(RECORDKEY_FIELD.key, "kafka_partition_offset")
          .option(PARTITIONPATH_FIELD.key, "partition_date")
          .option(TABLE_NAME.key, "merge_on_read_table")
          .option("hoodie.table.name", "merge_on_read_table")
          .option(HIVE_SYNC_ENABLED.key, false)
          .option(HIVE_STYLE_PARTITIONING.key, true)
          .option(FAIL_ON_TIMELINE_ARCHIVING_ENABLE.key, false)
          .option(STREAMING_IGNORE_FAILED_BATCH.key, false)
          .option(STREAMING_RETRY_CNT.key, 0)
          .mode(SaveMode.Append)
          .save("/tmp/hudi_streaming_kafka/MERGE_ON_READ")

        println(LocalDateTime.now() + " finish")
        batchDF.unpersist()
      }
      }
      .option("checkpointLocation", "/tmp/hudi_streaming_kafka/checkpoint/")
      .start()

    query.awaitTermination()
  }
}
