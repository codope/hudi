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

package org.apache.hudi.source.stats;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.FlinkClientUtil;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.source.stats.ColumnStatsIndices.METADATA_DATA_TYPE;

/**
 * Utilities for abstracting away heavy-lifting of interactions with Metadata Table's Partition Stats Index.
 */
public class PartitionStatsIndices {

  private static List<RowData> readPartitionStatsIndexByColumns(
      String basePath,
      String[] targetColumns,
      HoodieMetadataConfig metadataConfig) {

    // Read Metadata Table's Partition Stats Index into Flink's RowData list by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Deserializing fetched records into [[RowData]]s
    HoodieTableMetadata metadataTable = HoodieTableMetadata.create(
        HoodieFlinkEngineContext.DEFAULT, new HoodieHadoopStorage(basePath, FlinkClientUtil.getHadoopConf()),
        metadataConfig, basePath);

    // TODO encoding should be done internally w/in HoodieBackedTableMetadata
    List<String> encodedTargetColumnNames = Arrays.stream(targetColumns)
        .map(colName -> new ColumnIndexID(colName).asBase64EncodedString()).collect(Collectors.toList());

    HoodieData<HoodieRecord<HoodieMetadataPayload>> records =
        metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames, HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, false);

    org.apache.hudi.util.AvroToRowDataConverters.AvroToRowDataConverter converter =
        AvroToRowDataConverters.createRowConverter((RowType) METADATA_DATA_TYPE.getLogicalType());
    return records.collectAsList().stream().parallel().map(record -> {
          // schema and props are ignored for generating metadata record from the payload
          // instead, the underlying file system, or bloom filter, or columns stats metadata (part of payload) are directly used
          GenericRecord genericRecord;
          try {
            genericRecord = (GenericRecord) record.getData().getInsertValue(null, null).orElse(null);
          } catch (IOException e) {
            throw new HoodieException("Exception while getting insert value from metadata payload");
          }
          return (RowData) converter.convert(genericRecord);
        }
    ).collect(Collectors.toList());
  }

  private static
}
