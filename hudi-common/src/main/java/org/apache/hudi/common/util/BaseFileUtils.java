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

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utils for Hudi base file.
 */
public abstract class BaseFileUtils {

  public static BaseFileUtils getInstance(String path) {
    if (path.endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
      return new ParquetUtils();
    } else if (path.endsWith(HoodieFileFormat.ORC.getFileExtension())) {
      return new OrcUtils();
    }
    throw new UnsupportedOperationException("The format for file " + path + " is not supported yet.");
  }

  public static BaseFileUtils getInstance(HoodieFileFormat fileFormat) {
    if (HoodieFileFormat.PARQUET.equals(fileFormat)) {
      return new ParquetUtils();
    } else if (HoodieFileFormat.ORC.equals(fileFormat)) {
      return new OrcUtils();
    }
    throw new UnsupportedOperationException(fileFormat.name() + " format not supported yet.");
  }

  /**
   * Aggregate column range statistics across files in a partition.
   *
   * @param fileRanges List of column range statistics for each file in a partition
   */
  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<T> getColumnRangeInPartition(@Nonnull List<HoodieColumnRangeMetadata<T>> fileRanges) {
    if (fileRanges.size() == 1) {
      // Only one parquet file, we can just return that range.
      return fileRanges.get(0);
    }
    // There are multiple files. Compute min(file_mins) and max(file_maxs)
    return fileRanges.stream()
        .sequential()
        .reduce(BaseFileUtils::mergeRanges).get();
  }

  private static  <T extends Comparable<T>> HoodieColumnRangeMetadata<T> mergeRanges(HoodieColumnRangeMetadata<T> one,
                                                                                     HoodieColumnRangeMetadata<T> another) {
    final T minValue;
    final T maxValue;
    if (one.getMinValue() != null && another.getMinValue() != null) {
      minValue = one.getMinValue().toString().compareTo(another.getMinValue().toString()) < 0 ? one.getMinValue() : another.getMinValue();
    } else if (one.getMinValue() == null) {
      minValue = another.getMinValue();
    } else {
      minValue = one.getMinValue();
    }

    if (one.getMaxValue() != null && another.getMaxValue() != null) {
      maxValue = one.getMaxValue().toString().compareTo(another.getMaxValue().toString()) < 0 ? another.getMaxValue() : one.getMaxValue();
    } else if (one.getMaxValue() == null) {
      maxValue = another.getMaxValue();
    } else {
      maxValue = one.getMaxValue();
    }

    return HoodieColumnRangeMetadata.create(
        null, one.getColumnName(), minValue, maxValue,
        one.getNullCount() + another.getNullCount(),
        one.getValueCount() + another.getValueCount(),
        one.getTotalSize() + another.getTotalSize(),
        one.getTotalUncompressedSize() + another.getTotalUncompressedSize());
  }

  /**
   * Read the rowKey list from the given data file.
   *
   * @param filePath      The data file path
   * @param configuration configuration to build fs object
   * @return Set Set of row keys
   */
  public Set<String> readRowKeys(Configuration configuration, Path filePath) {
    return filterRowKeys(configuration, filePath, new HashSet<>())
        .stream().map(Pair::getKey).collect(Collectors.toSet());
  }

  /**
   * Read the bloom filter from the metadata of the given data file.
   * @param configuration Configuration
   * @param filePath The data file path
   * @return a BloomFilter object
   */
  public BloomFilter readBloomFilterFromMetadata(Configuration configuration, Path filePath) {
    Map<String, String> footerVals =
        readFooter(configuration, false, filePath,
            HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY,
            HoodieAvroWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY,
            HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE);
    String footerVal = footerVals.get(HoodieAvroWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    if (null == footerVal) {
      // We use old style key "com.uber.hoodie.bloomfilter"
      footerVal = footerVals.get(HoodieAvroWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    }
    BloomFilter toReturn = null;
    if (footerVal != null) {
      if (footerVals.containsKey(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE)) {
        toReturn = BloomFilterFactory.fromString(footerVal,
            footerVals.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE));
      } else {
        toReturn = BloomFilterFactory.fromString(footerVal, BloomFilterTypeCode.SIMPLE.name());
      }
    }
    return toReturn;
  }

  /**
   * Read the min and max record key from the metadata of the given data file.
   * @param configuration Configuration
   * @param filePath The data file path
   * @return A array of two string where the first is min record key and the second is max record key
   */
  public String[] readMinMaxRecordKeys(Configuration configuration, Path filePath) {
    Map<String, String> minMaxKeys = readFooter(configuration, true, filePath,
        HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER, HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER);
    if (minMaxKeys.size() != 2) {
      throw new HoodieException(
          String.format("Could not read min/max record key out of footer correctly from %s. read) : %s",
              filePath, minMaxKeys));
    }
    return new String[] {minMaxKeys.get(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER),
        minMaxKeys.get(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER)};
  }

  /**
   * Read the data file
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   * @param configuration Configuration
   * @param filePath The data file path
   * @return A list of GenericRecord
   */
  public abstract List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath);

  /**
   * Read the data file using the given schema
   * NOTE: This literally reads the entire file contents, thus should be used with caution.
   * @param configuration Configuration
   * @param filePath The data file path
   * @return A list of GenericRecord
   */
  public abstract List<GenericRecord> readAvroRecords(Configuration configuration, Path filePath, Schema schema);

  /**
   * Read the footer data of the given data file.
   * @param configuration Configuration
   * @param required require the footer data to be in data file
   * @param filePath The data file path
   * @param footerNames The footer names to read
   * @return A map where the key is the footer name and the value is the footer value
   */
  public abstract Map<String, String> readFooter(Configuration configuration, boolean required, Path filePath,
                                                 String... footerNames);

  /**
   * Returns the number of records in the data file.
   * @param configuration Configuration
   * @param filePath The data file path
   */
  public abstract long getRowCount(Configuration configuration, Path filePath);

  /**
   * Read the rowKey list matching the given filter, from the given data file.
   * If the filter is empty, then this will return all the row keys and corresponding positions.
   *
   * @param filePath      The data file path
   * @param configuration configuration to build fs object
   * @param filter        record keys filter
   * @return Set Set of pairs of row key and position matching candidateRecordKeys
   */
  public abstract Set<Pair<String, Long>> filterRowKeys(Configuration configuration, Path filePath, Set<String> filter);

  /**
   * Fetch {@link HoodieKey}s with positions from the given data file.
   *
   * @param configuration configuration to build fs object
   * @param filePath      The data file path
   * @return {@link List} of pairs of {@link HoodieKey} and position fetched from the data file
   */
  public abstract List<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(Configuration configuration, Path filePath);

  /**
   * Provides a closable iterator for reading the given data file.
   * @param configuration configuration to build fs object
   * @param filePath      The data file path
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the file
   */
  public abstract ClosableIterator<HoodieKey> getHoodieKeyIterator(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt);

  /**
   * Provides a closable iterator for reading the given data file.
   * @param configuration configuration to build fs object
   * @param filePath      The data file path
   * @return {@link ClosableIterator} of {@link HoodieKey}s for reading the file
   */
  public abstract ClosableIterator<HoodieKey> getHoodieKeyIterator(Configuration configuration, Path filePath);

  /**
   * Fetch {@link HoodieKey}s with positions from the given data file.
   *
   * @param configuration   configuration to build fs object
   * @param filePath        The data file path
   * @param keyGeneratorOpt instance of KeyGenerator.
   * @return {@link List} of pairs of {@link HoodieKey} and position fetched from the data file
   */
  public abstract List<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(Configuration configuration, Path filePath, Option<BaseKeyGenerator> keyGeneratorOpt);

  /**
   * Read the Avro schema of the data file.
   * @param configuration Configuration
   * @param filePath The data file path
   * @return The Avro schema of the data file
   */
  public abstract Schema readAvroSchema(Configuration configuration, Path filePath);

  /**
   * @return The subclass's {@link HoodieFileFormat}.
   */
  public abstract HoodieFileFormat getFormat();

  /**
   * Writes properties to the meta file.
   *
   * @param fs       {@link FileSystem} instance.
   * @param filePath file path to write to.
   * @param props    properties to write.
   * @throws IOException upon write error.
   */
  public abstract void writeMetaFile(FileSystem fs,
                                     Path filePath,
                                     Properties props) throws IOException;
}
