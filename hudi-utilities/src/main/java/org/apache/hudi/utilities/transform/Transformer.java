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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Transform source to target dataset before writing.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface Transformer {

  /**
   * Transform source RDD to target RDD.
   *
   * @param jsc JavaSparkContext
   * @param sparkSession Spark Session
   * @param rowDataset Source DataSet
   * @param properties Config properties
   * @return Transformed Dataset
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties);

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    Dataset<Row> emptyDataset = sparkSession.createDataFrame(sparkSession.emptyDataFrame().rdd(), incomingStruct);
    Dataset<Row> transformedDataset = this.apply(jsc, sparkSession, emptyDataset, properties);
    return transformedDataset.schema();
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default boolean validateImplementation(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    try {
      Dataset<Row> transformedDataset = this.apply(jsc, sparkSession, rowDataset, properties);
      // Perform additional checks here to validate the transformedDataset
      // You can also validate against a predefined schema or values
      // Return true if validation passes, for example count validation
      return transformedDataset.count() == rowDataset.count();
    } catch (Exception e) {
      // Return false if validation fails
      return false;
    }
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default Dataset<Row> applyWithRetry(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties, int maxRetries) {
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return this.apply(jsc, sparkSession, rowDataset, properties);
      } catch (Exception e) {
        // You may want to log the exception here
        if (attempt == maxRetries - 1) { // if last attempt
          throw e; // rethrow last exception
        }
        // Implement back-off strategy (like Thread.sleep()) or continue based on the exception
      }
    }
    // Should never reach here
    return null;
  }
}
