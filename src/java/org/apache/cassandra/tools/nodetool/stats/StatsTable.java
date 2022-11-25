/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool.stats;

import java.util.ArrayList;
import java.util.List;

public class StatsTable {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(StatsTable.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(StatsTable.class);

    public transient String fullName;

    public transient String keyspaceName;

    public transient String tableName;

    public transient boolean isIndex;

    public transient boolean isLeveledSstable = false;

    public transient Object sstableCount;

    public transient Object oldSSTableCount;

    public transient String spaceUsedLive;

    public transient String spaceUsedTotal;

    public transient String spaceUsedBySnapshotsTotal;

    public transient boolean offHeapUsed = false;

    public transient String offHeapMemoryUsedTotal;

    public transient Object sstableCompressionRatio;

    public transient Object numberOfPartitionsEstimate;

    public transient Object memtableCellCount;

    public transient String memtableDataSize;

    public transient boolean memtableOffHeapUsed = false;

    public transient String memtableOffHeapMemoryUsed;

    public transient Object memtableSwitchCount;

    public transient long localReadCount;

    public transient double localReadLatencyMs;

    public transient long localWriteCount;

    public transient double localWriteLatencyMs;

    public transient Object pendingFlushes;

    public transient Object bloomFilterFalsePositives;

    public transient Object bloomFilterFalseRatio;

    public transient String bloomFilterSpaceUsed;

    public transient boolean bloomFilterOffHeapUsed = false;

    public transient String bloomFilterOffHeapMemoryUsed;

    public transient boolean indexSummaryOffHeapUsed = false;

    public transient String indexSummaryOffHeapMemoryUsed;

    public transient boolean compressionMetadataOffHeapUsed = false;

    public transient String compressionMetadataOffHeapMemoryUsed;

    public transient long compactedPartitionMinimumBytes;

    public transient long compactedPartitionMaximumBytes;

    public transient long compactedPartitionMeanBytes;

    public transient double percentRepaired;

    public transient long bytesRepaired;

    public transient long bytesUnrepaired;

    public transient long bytesPendingRepair;

    public transient double averageLiveCellsPerSliceLastFiveMinutes;

    public transient long maximumLiveCellsPerSliceLastFiveMinutes;

    public transient double averageTombstonesPerSliceLastFiveMinutes;

    public transient long maximumTombstonesPerSliceLastFiveMinutes;

    public transient String droppedMutations;

    public transient List<String> sstablesInEachLevel = new ArrayList<>();

    // null: option not active
    public transient Boolean isInCorrectLocation = null;

    public transient double droppableTombstoneRatio;
}
