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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface that supports updating metadata for a given table, as actions complete.
 */
public interface HoodieTableMetadataWriter extends Serializable, AutoCloseable {

  /**
   * Execute the index action for the given partitions.
   *
   * @param engineContext
   * @param indexPartitionInfos - partitions to index
   */
  void index(HoodieEngineContext engineContext, List<HoodieIndexPartitionInfo> indexPartitionInfos);

  /**
   * Initialize file groups while scheduling index action.
   *
   * @param dataMetaClient    - meta client for the data table
   * @param metadataPartition - metadata partition for which file groups needs to be initialized
   * @param instantTime       - instant time of the index action
   * @param fileGroupCount    - number of file groups to be initialized
   * @throws IOException
   */
  void initializeFileGroups(HoodieTableMetaClient dataMetaClient, MetadataPartitionType metadataPartition, String instantTime, int fileGroupCount) throws IOException;

  void dropIndex(List<MetadataPartitionType> indexesToDrop) throws IOException;

  /**
   * Update the metadata table due to a COMMIT operation.
   * @param commitMetadata commit metadata of the operation of interest.
   * @param instantTime instant time of the commit.
   * @param isTableServiceAction true if caller is a table service. false otherwise. Only regular write operations can trigger metadata table services and this argument
   *                       will assist in this.
   */
  void update(HoodieCommitMetadata commitMetadata, String instantTime, boolean isTableServiceAction);

  /**
   * Update the metadata table due to a CLEAN operation.
   * @param cleanMetadata clean metadata of the operation of interest.
   * @param instantTime instant time of the commit.
   */
  void update(HoodieCleanMetadata cleanMetadata, String instantTime);

  /**
   * Update the metadata table due to a RESTORE operation.
   * @param restoreMetadata restore metadata of the operation of interest.
   * @param instantTime instant time of the commit.
   */
  void update(HoodieRestoreMetadata restoreMetadata, String instantTime);

  /**
   * Update the metadata table due to a ROLLBACK operation.
   * @param rollbackMetadata rollback metadata of the operation of interest.
   * @param instantTime instant time of the commit.
   */
  void update(HoodieRollbackMetadata rollbackMetadata, String instantTime);
}
