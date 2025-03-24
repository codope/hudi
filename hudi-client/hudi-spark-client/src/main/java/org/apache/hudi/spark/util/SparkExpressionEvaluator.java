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

package org.apache.hudi.spark.util;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Projection;
import org.apache.spark.sql.catalyst.expressions.SafeProjection;

import java.util.Collections;
import java.util.List;

import scala.collection.JavaConverters;

/**
 * Utility class for evaluating Spark SQL expressions.
 */
public class SparkExpressionEvaluator {

  /**
   * Create a Projection from a single expression.
   * 
   * @param expression The expression to compile
   * @return A Projection that can evaluate the expression
   */
  public static SafeProjection createProjection(Expression expression) {
    return createProjection(Collections.singletonList(expression));
  }
  
  /**
   * Create a Projection from a list of expressions.
   * 
   * @param expressions The expressions to compile
   * @return A Projection that can evaluate the expressions
   */
  public static SafeProjection createProjection(List<Expression> expressions) {
    // TODO: fix me
    return SafeProjection.create(JavaConverters.collectionAsScalaIterable(expressions).toSeq());
  }
}