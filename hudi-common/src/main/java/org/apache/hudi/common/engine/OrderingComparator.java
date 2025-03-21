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

import java.io.Serializable;

/**
 * Interface for comparing ordering values between records.
 * Used to replace payload-based ordering with a more direct approach.
 */
public interface OrderingComparator extends Serializable {
  
  /**
   * Compare two ordering values.
   * 
   * @param orderingValue1 First ordering value
   * @param orderingValue2 Second ordering value
   * @return negative if value1 < value2, 0 if equal, positive if value1 > value2
   */
  int compare(Object orderingValue1, Object orderingValue2);
}