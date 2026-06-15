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

package org.apache.hudi.exception;

/**
 * Thrown (in strict mode) when the file system view detects a file group that has been replaced by a
 * clustering/replace commit but still has a live file slice written <em>after</em> the replace
 * instant. Such a slice is silently excluded from query results today, i.e. a concurrent write was
 * lost. See RFC-108 / HUDI-1045 (support updates during clustering), Phase 0a guard.
 */
public class HoodieReplacedFileGroupLostWriteException extends HoodieException {

  public HoodieReplacedFileGroupLostWriteException(String msg) {
    super(msg);
  }
}
