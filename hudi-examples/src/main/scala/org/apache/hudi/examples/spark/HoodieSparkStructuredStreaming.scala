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
      .option("subscribe", "testTopic")
    //      .option("startingOffsets", "latest")
    //      .option("maxOffsetsPerTrigger", 100000)
    //      .option("failOnDataLoss", false)

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
        "substr(current_time,1,10) partition_date")

    // Create and start query
    val query = df
      .writeStream
      .queryName("demo")
      .foreachBatch { (batchDF: DataFrame, _: Long) => {
        batchDF.persist()

        println(LocalDateTime.now() + "start writing cow table")
        batchDF.write.format("org.apache.hudi")
          .option(TABLE_TYPE_OPT_KEY.key, "COPY_ON_WRITE")
          .option(PRECOMBINE_FIELD_OPT_KEY.key, "kafka_timestamp")
          // Use kafka partition and offset as combined primary key
          .option(RECORDKEY_FIELD_OPT_KEY.key, "kafka_partition_offset")
          // Partition with current date
          .option(PARTITIONPATH_FIELD_OPT_KEY.key, "partition_date")
          .option(TABLE_NAME_OPT_KEY.key, "copy_on_write_table")
          .option(HIVE_SYNC_ENABLED_OPT_KEY.key, false)
          .option(HIVE_STYLE_PARTITIONING_OPT_KEY.key, true)
          .option("hoodie.table.name", "copy_on_write_table")
          .mode(SaveMode.Append)
          .save("/tmp/sparkHudi/COPY_ON_WRITE")

        println(LocalDateTime.now() + "start writing mor table")
        batchDF.write.format("org.apache.hudi")
          .option(TABLE_TYPE_OPT_KEY.key, "MERGE_ON_READ")
          .option(TABLE_TYPE_OPT_KEY.key, "COPY_ON_WRITE")
          .option(PRECOMBINE_FIELD_OPT_KEY.key, "kafka_timestamp")
          .option(RECORDKEY_FIELD_OPT_KEY.key, "kafka_partition_offset")
          .option(PARTITIONPATH_FIELD_OPT_KEY.key, "partition_date")
          .option(TABLE_NAME_OPT_KEY.key, "merge_on_read_table")
          .option("hoodie.table.name", "merge_on_read_table")
          .option(HIVE_SYNC_ENABLED_OPT_KEY.key, false)
          .option(HIVE_STYLE_PARTITIONING_OPT_KEY.key, true)
          .mode(SaveMode.Append)
          .save("/tmp/sparkHudi/MERGE_ON_READ")

        println(LocalDateTime.now() + "finish")
        batchDF.unpersist()
      }
      }
      .option("checkpointLocation", "/tmp/sparkHudi/checkpoint/")
      .start()

    query.awaitTermination()
  }
}
