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

package org.apache.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetOutputFormat, ParquetRecordReader}
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.URI

class HoodieParquetFileFormat extends ParquetFileFormat {

  def merge(dataSchema: StructType, sparkSqlSchema: StructType,
            requiredSchema: StructType, partitionSchema: StructType): StructType = {
    sparkSqlSchema
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[HoodieParquetReadSupport].getName)
    hadoopConf.set(
      "org.apache.spark.sql.parquet.row.requested_schema",
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      sqlConf.parquetVectorizedReaderEnabled
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val converter = new ParquetToSparkSchemaConverter(
      assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString,
      assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val fileSplit =
        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
      val filePath = fileSplit.getPath

      val split =
        new org.apache.parquet.hadoop.ParquetInputSplit(
          filePath,
          fileSplit.getStart,
          fileSplit.getStart + fileSplit.getLength,
          fileSplit.getLength,
          fileSplit.getLocations,
          null)

      val sharedConf = broadcastedHadoopConf.value.value

      lazy val footerFileMetaData =
        ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val sparkSqlSchema = converter.convert(parquetSchema)
        // reconcile schema in footer with requiredSchema
        val mergedSchema = merge(dataSchema, sparkSqlSchema, requiredSchema, partitionSchema)
        val parquetFilters = new HoodieParquetFilters(pushDownDate, pushDownTimestamp, pushDownDecimal,
          pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter(parquetSchema, _))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

      // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
      // *only* if the file was created by something other than "parquet-mr", so check the actual
      // writer here for this file.  We have to do this per-file, as each file in the table may
      // have different writers.
      // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
      def isCreatedByParquetMr: Boolean =
        footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

      val convertTz =
        if (timestampConversion && !isCreatedByParquetMr) {
          Some(DateTimeUtils.getTimeZone(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
        } else {
          None
        }

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      // Try to push down filters when filter push-down is enabled.
      // Notice: This push-down is RowGroups level, not individual records.
      if (pushed.isDefined) {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      }
      val taskContext = Option(TaskContext.get())
      if (enableVectorizedReader) {
        val vectorizedReader = new VectorizedParquetRecordReader(
          convertTz.orNull, enableOffHeapColumnVector && taskContext.isDefined, capacity)
        val iter = new RecordReaderIterator(vectorizedReader)
        // SPARK-23457 Register a task completion lister before `initialization`.
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        vectorizedReader.initialize(split, hadoopAttemptContext)
        vectorizedReader.initBatch(partitionSchema, file.partitionValues)
        if (returningBatch) {
          vectorizedReader.enableReturningBatches()
        }

        // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        // ParquetRecordReader returns UnsafeRow
        val reader = if (pushed.isDefined && enableRecordFilter) {
          val parquetFilter = FilterCompat.get(pushed.get, null)
          new ParquetRecordReader[UnsafeRow](new HoodieParquetReadSupport(convertTz), parquetFilter)
        } else {
          new ParquetRecordReader[UnsafeRow](new HoodieParquetReadSupport(convertTz))
        }
        val iter = new RecordReaderIterator(reader)
        // SPARK-23457 Register a task completion lister before `initialization`.
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        reader.initialize(split, hadoopAttemptContext)

        //val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
        val joinedRow = new JoinedRow()
        //val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

        // This is a horrible erasure hack...  if we type the iterator above, then it actually check
        // the type in next() and we get a class cast exception.  If we make that function return
        // Object, then we can defer the cast until later!
        if (partitionSchema.length == 0) {
          // There is no partition columns
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          /*iter.asInstanceOf[Iterator[InternalRow]]
            .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))*/
          iter.asInstanceOf[Iterator[InternalRow]]
        }
      }
    }
  }
}

object ParquetWriteSupport {
  val SPARK_ROW_SCHEMA: String = "org.apache.spark.sql.parquet.row.attributes"

  def setSchema(schema: StructType, configuration: Configuration): Unit = {
    schema.map(_.name).foreach(ParquetSchemaConverter.checkFieldName)
    configuration.set(SPARK_ROW_SCHEMA, schema.json)
    configuration.setIfUnset(
      ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)
  }
}

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = try {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = try {
    value = new Configuration(false)
    value.readFields(in)
  }
}

object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)

  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new Exception(message)
    }
  }
}

/*
object StructType {
  def fromString(raw: String): StructType = {
    Try(DataType.fromJson(raw)).getOrElse(LegacyTypeStringParser.parse(raw)) match {
      case t: StructType => t
      //case _ => throw new RuntimeException(s"Failed parsing ${StructType.simpleString}: $raw")
    }
  }
}*/

object HoodieParquetFileFormat extends Logging {
}
