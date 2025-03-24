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

import org.apache.hudi.common.engine.OrderingComparator;
import org.apache.hudi.common.table.read.HoodieFileGroupIO;
import org.apache.hudi.spark.util.SparkExpressionEvaluator;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SafeProjection;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spark-specific implementation of HoodieFileGroupIO that supports evaluating Spark SQL expressions
 * on records for MERGE INTO statements. This replaces ExpressionPayload with a more efficient implementation
 * that avoids Avro conversions.
 */
public class SparkHoodieExpressionFileGroupIO extends HoodieFileGroupIO<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkHoodieExpressionFileGroupIO.class);

  // Sentinel record to indicate that a record should be skipped
  private static final InternalRow SKIP_RECORD = null;

  // Compiled expressions for condition evaluation and transformations
  private final List<ExpressionOperation> updateOperations;
  private final List<ExpressionOperation> insertOperations;
  private final SafeProjection deleteConditionEvaluator;
  private final SparkHoodieIOContext ioContext;

  // Flag to determine if this is MOR (Merge-On-Read) table
  private final boolean isMorTable;

  // Schema for writing records
  private final Schema writerSchema;
  private final StructType writerStructType;

  /**
   * Constructor.
   *
   * @param ioContext                      Spark IO context
   * @param updateConditionsAndAssignments Map of update conditions to assignments
   * @param insertConditionsAndAssignments Map of insert conditions to assignments
   * @param deleteCondition                Delete condition expression
   * @param writerSchema                   Schema for writing records
   * @param writerStructType               Spark StructType for writing records
   * @param isMorTable                     Whether this is a Merge-On-Read table
   */
  public SparkHoodieExpressionFileGroupIO(
      SparkHoodieIOContext ioContext,
      Map<Expression, List<Expression>> updateConditionsAndAssignments,
      Map<Expression, List<Expression>> insertConditionsAndAssignments,
      Expression deleteCondition,
      Schema writerSchema,
      StructType writerStructType,
      boolean isMorTable) {
    super(null, // HoodieTableMetaClient - we don't need this for our use case yet
        ioContext,
        ioContext.getDeleteMarkerEvaluator(),
        ioContext.getOrderingComparator(),
        null); // HoodieRecordMerger - we don't need this for our use case yet
    this.writerSchema = writerSchema;
    this.ioContext = ioContext;
    this.writerStructType = writerStructType;
    this.isMorTable = isMorTable;

    // Compile expressions once for reuse
    this.updateOperations = updateConditionsAndAssignments.entrySet().stream()
        .map(entry -> new ExpressionOperation(
            SparkExpressionEvaluator.createProjection(entry.getKey()),
            SparkExpressionEvaluator.createProjection(entry.getValue())
        ))
        .collect(Collectors.toList());

    this.insertOperations = insertConditionsAndAssignments.entrySet().stream()
        .map(entry -> new ExpressionOperation(
            SparkExpressionEvaluator.createProjection(entry.getKey()),
            SparkExpressionEvaluator.createProjection(entry.getValue())
        ))
        .collect(Collectors.toList());

    this.deleteConditionEvaluator = deleteCondition != null ?
        SparkExpressionEvaluator.createProjection(deleteCondition) : null;

    LOG.info("Initialized SparkHoodieExpressionFileGroupIO with "
        + updateOperations.size() + " update operations, "
        + insertOperations.size() + " insert operations, "
        + (deleteConditionEvaluator != null ? "1" : "0") + " delete condition");
  }

  public void write(InternalRow record) throws IOException {
    // Metadata field indicating if this is an update operation
    boolean isUpdateOperation = isUpdateOperation(record);

    // For MOR tables, we need to check if this is an update or insert operation
    if (isMorTable && isUpdateOperation) {
      InternalRow transformedRecord = evaluateUpdateConditions(record);
      if (transformedRecord != SKIP_RECORD) {
        // Write the transformed record - actual writing will be handled by the write handle
        writeRecord(transformedRecord);
      }
    } else {
      // For inserts or COW tables
      InternalRow transformedRecord = evaluateInsertConditions(record);
      if (transformedRecord != SKIP_RECORD) {
        writeRecord(transformedRecord);
      }
    }
  }

  /**
   * Determines if the record represents an update operation.
   * This information is typically stored in a metadata field.
   */
  private boolean isUpdateOperation(InternalRow record) {
    // Implementation depends on how update flag is stored in the record
    // For merge-into operations, this could be stored in a metadata field
    // This is a placeholder implementation
    return record.getBoolean(record.numFields() - 1);
  }

  /**
   * Evaluates update conditions and applies transformations for update paths.
   */
  private InternalRow evaluateUpdateConditions(InternalRow record) {
    // First check if the record matches any update condition
    for (ExpressionOperation operation : updateOperations) {
      boolean conditionResult = evaluateCondition(operation.getCondition(), record);

      if (conditionResult) {
        // TODO: Apply the assignment expressions to transform the record
        // return operation.getAssignment().apply(record);
        return null;
      }
    }

    // If no update condition matched, check if it matches the delete condition
    if (deleteConditionEvaluator != null) {
      boolean shouldDelete = evaluateCondition(deleteConditionEvaluator, record);
      if (shouldDelete) {
        // For deletes, we'll create a special delete marker or sentinel
        return createDeleteMarker(record);
      }
    }

    // No matching condition - skip this record
    return SKIP_RECORD;
  }

  /**
   * Evaluates insert conditions and applies transformations for insert paths.
   */
  private InternalRow evaluateInsertConditions(InternalRow record) {
    // Check if the record matches any insert condition
    for (ExpressionOperation operation : insertOperations) {
      boolean conditionResult = evaluateCondition(operation.getCondition(), record);

      if (conditionResult) {
        // TODO: Apply the assignment expressions to transform the record
        // return operation.getAssignment().apply(record);
      }
    }

    // No matching condition - skip this record
    return SKIP_RECORD;
  }

  /**
   * Evaluates a condition expression against a record.
   */
  private boolean evaluateCondition(SafeProjection condition, InternalRow record) {
    // TODO: Fix projection and assignment to work with InternalRow. Check ExpressionPayload for reference
    // InternalRow result = condition.apply(record);
    InternalRow result = null;
    // The condition projection should return a single boolean value
    return !result.isNullAt(0) && result.getBoolean(0);
  }

  /**
   * Creates a delete marker for the given record.
   */
  private InternalRow createDeleteMarker(InternalRow record) {
    // In Spark, we can represent deletes in different ways
    // Option 1: Return a special sentinel value (HoodieRecord.SENTINEL)
    // Option 2: Return a record with null values except for key fields
    // For now, we'll use InternalRow.empty().
    // TODO: We should use HoodieRecord.SENTINEL. Check ExpressionPayload for reference.
    return InternalRow.empty();
  }

  /**
   * Writes a record to the target location.
   */
  private void writeRecord(InternalRow record) throws IOException {
    // In a real implementation, this would write to a file or buffer
    // Here, we'll just add to a collection or pass to the write handle
    // The actual write is handled by the Hudi write path
  }

  @Override
  public org.apache.hudi.common.table.read.HoodieFileGroupReader createFileGroupReader(
      org.apache.hudi.common.model.FileSlice fileSlice,
      org.apache.avro.Schema requestedSchema,
      org.apache.hudi.common.config.TypedProperties props) {
    // This is a placeholder implementation for now
    throw new UnsupportedOperationException("createFileGroupReader not implemented");
  }

  @Override
  public String getRecordKey(Object record) {
    // Extract record key using SparkIOContext
    return ((SparkHoodieIOContext) getIOContext()).getRecordKey((InternalRow) record);
  }

  @Override
  public org.apache.hudi.common.util.Option<org.apache.hudi.common.model.HoodieRecord> mergeRecords(
      org.apache.hudi.common.model.HoodieRecord older,
      org.apache.hudi.common.model.HoodieRecord newer) {
    // This is a placeholder implementation for now
    return org.apache.hudi.common.util.Option.of(newer);
  }

  @Override
  public List<?> processRecordsForWrite(List<org.apache.hudi.common.model.HoodieRecord> records) {
    // This is a placeholder implementation for now
    return records;
  }

  /**
   * Determines if a record is a delete marker.
   */
  private boolean isDeleteMarker(InternalRow record) {
    // Implementation depends on how delete markers are represented
    // For example, a record might be considered a delete marker if it has null values
    // except for key fields
    return false; // Placeholder implementation
  }

  @Override
  public OrderingComparator<InternalRow> getOrderingComparator() {
    // Return standard ordering comparator from the IO context
    return ((SparkHoodieIOContext) ioContext).getOrderingComparator();
  }

  /**
   * Class representing a condition and its corresponding assignment expressions.
   */
  private static class ExpressionOperation {
    private final SafeProjection condition;
    private final SafeProjection assignment;

    public ExpressionOperation(SafeProjection condition, SafeProjection assignment) {
      this.condition = condition;
      this.assignment = assignment;
    }

    public SafeProjection getCondition() {
      return condition;
    }

    public SafeProjection getAssignment() {
      return assignment;
    }
  }
}