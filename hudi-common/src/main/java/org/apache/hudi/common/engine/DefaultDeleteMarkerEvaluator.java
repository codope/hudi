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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.util.Option;

/**
 * Default implementation of DeleteMarkerEvaluator that uses a configurable field name
 * to determine if a record is a delete marker.
 */
public class DefaultDeleteMarkerEvaluator implements DeleteMarkerEvaluator {

  private final String deleteFieldName;
  private final Option<HoodieReaderContext> readerContext;
  
  /**
   * Constructor with required parameters.
   * 
   * @param deleteFieldName The field name that indicates if a record is a delete marker
   * @param readerContext The reader context to extract values from engine-specific records
   */
  public DefaultDeleteMarkerEvaluator(String deleteFieldName, Option<HoodieReaderContext> readerContext) {
    this.deleteFieldName = deleteFieldName;
    this.readerContext = readerContext;
  }
  
  @Override
  public boolean isDelete(Object record) {
    if (readerContext.isPresent()) {
      Object deleteValue = readerContext.get().getValue(record, null, deleteFieldName);
      return deleteValue != null && Boolean.TRUE.equals(deleteValue);
    }
    
    // Fallback implementation for when no reader context is available
    if (record instanceof java.util.Map) {
      Object deleteValue = ((java.util.Map) record).get(deleteFieldName);
      return deleteValue != null && Boolean.TRUE.equals(deleteValue);
    }
    
    return false;
  }
}