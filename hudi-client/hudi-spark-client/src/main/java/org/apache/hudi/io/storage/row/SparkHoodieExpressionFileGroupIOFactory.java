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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieIOContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.FileGroupIOFactory;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.avro.HoodieAvroDeserializer;
import org.apache.spark.sql.catalyst.InternalRow$;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType;

/**
 * A factory for creating SparkHoodieExpressionFileGroupIO instances.
 * This is used for MERGE INTO operations to evaluate expressions.
 */
public class SparkHoodieExpressionFileGroupIOFactory implements FileGroupIOFactory<InternalRow$> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieExpressionFileGroupIOFactory.class);

  private static final String EXPRESSIONS_UPDATE_KEY = "hoodie.filegroup.io.expressions.update";
  private static final String EXPRESSIONS_INSERT_KEY = "hoodie.filegroup.io.expressions.insert";
  private static final String EXPRESSIONS_DELETE_KEY = "hoodie.filegroup.io.expressions.delete";

  private final TypedProperties props;
  private final Schema writerSchema;
  private final StructType writerStructType;
  private final boolean isMorTable;

  // Cached expressions
  private final Map<Expression, List<Alias>> updateConditionsAndAssignments;
  private final Map<Expression, List<Alias>> insertConditionsAndAssignments;
  private final Expression deleteCondition;

  /**
   * Constructor with properties.
   *
   * @param props            Properties containing the serialized expressions
   * @param writerSchema     Writer schema
   * @param writerStructType Spark StructType for the writer schema
   * @param isMorTable       Whether this is a Merge-On-Read table
   */
  public SparkHoodieExpressionFileGroupIOFactory(
      TypedProperties props,
      Schema writerSchema,
      StructType writerStructType,
      boolean isMorTable) {
    this.props = props;
    this.writerSchema = writerSchema;
    this.writerStructType = writerStructType;
    this.isMorTable = isMorTable;

    // Deserialize the expressions
    this.updateConditionsAndAssignments = deserializeConditionsAndAssignments(props.getString(EXPRESSIONS_UPDATE_KEY, null));
    this.insertConditionsAndAssignments = deserializeConditionsAndAssignments(props.getString(EXPRESSIONS_INSERT_KEY, null));
    this.deleteCondition = deserializeDeleteCondition(props.getString(EXPRESSIONS_DELETE_KEY, null));

    LOG.info("Initialized SparkHoodieExpressionFileGroupIOFactory with "
        + updateConditionsAndAssignments.size() + " update conditions, "
        + insertConditionsAndAssignments.size() + " insert conditions, "
        + (deleteCondition != null ? "1" : "0") + " delete condition");
  }

  /**
   * Constructor with serialized expressions.
   *
   * @param ioContext         The Spark IO context
   * @param updateExpressions Base64 encoded update expressions
   * @param insertExpressions Base64 encoded insert expressions
   * @param deleteExpression  Base64 encoded delete expression
   */
  public SparkHoodieExpressionFileGroupIOFactory(
      SparkHoodieIOContext ioContext,
      String updateExpressions,
      String insertExpressions,
      String deleteExpression) {
    this.props = new TypedProperties();
    if (updateExpressions != null && !updateExpressions.isEmpty()) {
      props.setProperty(EXPRESSIONS_UPDATE_KEY, updateExpressions);
    }
    if (insertExpressions != null && !insertExpressions.isEmpty()) {
      props.setProperty(EXPRESSIONS_INSERT_KEY, insertExpressions);
    }
    if (deleteExpression != null && !deleteExpression.isEmpty()) {
      props.setProperty(EXPRESSIONS_DELETE_KEY, deleteExpression);
    }

    // Set default values for now
    this.writerSchema = null;
    this.writerStructType = null;
    this.isMorTable = false;

    // Deserialize the expressions
    this.updateConditionsAndAssignments = deserializeConditionsAndAssignments(updateExpressions);
    this.insertConditionsAndAssignments = deserializeConditionsAndAssignments(insertExpressions);
    this.deleteCondition = deserializeDeleteCondition(deleteExpression);

    LOG.info("Initialized SparkHoodieExpressionFileGroupIOFactory with "
        + updateConditionsAndAssignments.size() + " update conditions, "
        + insertConditionsAndAssignments.size() + " insert conditions, "
        + (deleteCondition != null ? "1" : "0") + " delete condition");
  }

  /**
   * Deserialize conditions and assignments from Base64 encoded string.
   */
  @SuppressWarnings("unchecked")
  private Map<Expression, List<Alias>> deserializeConditionsAndAssignments(String encoded) {
    if (encoded == null) {
      return new HashMap<>();
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(encoded);
      HoodieAvroDeserializer deserializer = SparkAdapterSupport$.MODULE$.sparkAdapter().createAvroDeserializer(writerSchema, convertAvroSchemaToStructType(writerSchema));
      Map<Expression, List<Expression>> decoded = (Map<Expression, List<Expression>>) (deserializer.deserialize(bytes)).get();

      // Convert the decoded map to the format needed by SparkHoodieExpressionFileGroupIO
      return decoded.entrySet().stream()
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              entry -> entry.getValue().stream()
                  .map(expr -> (Alias) expr)
                  .collect(Collectors.toList())
          ));
    } catch (Exception e) {
      LOG.error("Failed to deserialize conditions and assignments", e);
      return new HashMap<>();
    }
  }

  /**
   * Deserialize delete condition from Base64 encoded string.
   */
  private Expression deserializeDeleteCondition(String encoded) {
    if (encoded == null) {
      return null;
    }

    try {
      byte[] bytes = Base64.getDecoder().decode(encoded);
      HoodieAvroDeserializer deserializer = SparkAdapterSupport$.MODULE$.sparkAdapter().createAvroDeserializer(writerSchema, convertAvroSchemaToStructType(writerSchema));
      return (Expression) (deserializer.deserialize(bytes)).get();
    } catch (Exception e) {
      LOG.error("Failed to deserialize delete condition", e);
      return null;
    }
  }

  @Override
  public SparkHoodieExpressionFileGroupIO createFileGroupIO(
      HoodieIOContext ioContext,
      HoodieTableMetaClient metaClient,
      String partitionPath) {
    ValidationUtils.checkArgument(ioContext instanceof SparkHoodieIOContext,
        "IOContext must be an instance of SparkHoodieIOContext");

    // Create a Projection for the delete condition if it exists
    Expression deleteConditionProjection = null;
    if (deleteCondition != null) {
      deleteConditionProjection = deleteCondition;
    }

    // Create the SparkHoodieExpressionFileGroupIO
    return new SparkHoodieExpressionFileGroupIO(
        (SparkHoodieIOContext) ioContext,
        convertToExpressionMap(updateConditionsAndAssignments),
        convertToExpressionMap(insertConditionsAndAssignments),
        deleteConditionProjection,
        writerSchema,
        writerStructType,
        isMorTable);
  }

  /**
   * Converts a map of Expression to List<Alias> to Map<Expression, List<Expression>>.
   */
  private Map<Expression, List<Expression>> convertToExpressionMap(Map<Expression, List<Alias>> map) {
    if (map == null) {
      return new HashMap<>();
    }
    return map.entrySet().stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().stream()
                .map(alias -> (Expression) alias)
                .collect(Collectors.toList())
        ));
  }
}