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
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public final class CompactionInfo {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(CompactionInfo.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(CompactionInfo.class);

    public static final transient String ID = "id";

    public static final transient String KEYSPACE = "keyspace";

    public static final transient String COLUMNFAMILY = "columnfamily";

    public static final transient String COMPLETED = "completed";

    public static final transient String TOTAL = "total";

    public static final transient String TASK_TYPE = "taskType";

    public static final transient String UNIT = "unit";

    public static final transient String COMPACTION_ID = "compactionId";

    public static final transient String SSTABLES = "sstables";

    private final transient TableMetadata metadata;

    private final transient OperationType tasktype;

    private final transient long completed;

    private final transient long total;

    private final transient Unit unit;

    private final transient UUID compactionId;

    private final transient ImmutableSet<SSTableReader> sstables;

    public CompactionInfo(TableMetadata metadata, OperationType tasktype, long bytesComplete, long totalBytes, UUID compactionId, Collection<SSTableReader> sstables) {
        this(metadata, tasktype, bytesComplete, totalBytes, Unit.BYTES, compactionId, sstables);
    }

    private CompactionInfo(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, UUID compactionId, Collection<SSTableReader> sstables) {
        this.tasktype = tasktype;
        this.completed = completed;
        this.total = total;
        this.metadata = metadata;
        this.unit = unit;
        this.compactionId = compactionId;
        this.sstables = ImmutableSet.copyOf(sstables);
    }

    /**
     * Special compaction info where we always need to cancel the compaction - for example ViewBuilderTask and AutoSavingCache where we don't know
     * the sstables at construction
     */
    public static CompactionInfo withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, Unit unit, UUID compactionId) {
        return new CompactionInfo(metadata, tasktype, completed, total, unit, compactionId, ImmutableSet.of());
    }

    /**
     * @return A copy of this CompactionInfo with updated progress.
     */
    public CompactionInfo forProgress(long complete, long total) {
        return new CompactionInfo(metadata, tasktype, complete, total, unit, compactionId, sstables);
    }

    public Optional<String> getKeyspace() {
        return Optional.ofNullable(metadata != null ? metadata.keyspace : null);
    }

    public Optional<String> getTable() {
        return Optional.ofNullable(metadata != null ? metadata.name : null);
    }

    public TableMetadata getTableMetadata() {
        return metadata;
    }

    public long getCompleted() {
        return completed;
    }

    public long getTotal() {
        return total;
    }

    public OperationType getTaskType() {
        return tasktype;
    }

    public UUID getTaskId() {
        return compactionId;
    }

    public Unit getUnit() {
        return unit;
    }

    public Set<SSTableReader> getSSTables() {
        return sstables;
    }

    @Override
    public String toString() {
        if (metadata != null) {
            return String.format("%s(%s, %s / %s %s)@%s(%s, %s)", tasktype, compactionId, completed, total, unit, metadata.id, metadata.keyspace, metadata.name);
        } else {
            return String.format("%s(%s, %s / %s %s)", tasktype, compactionId, completed, total, unit);
        }
    }

    public Map<String, String> asMap() {
        Map<String, String> ret = new HashMap<String, String>();
        ret.put(ID, metadata != null ? metadata.id.toString() : "");
        ret.put(KEYSPACE, getKeyspace().orElse(null));
        ret.put(COLUMNFAMILY, getTable().orElse(null));
        ret.put(COMPLETED, Long.toString(completed));
        ret.put(TOTAL, Long.toString(total));
        ret.put(TASK_TYPE, tasktype.toString());
        ret.put(UNIT, unit.toString());
        ret.put(COMPACTION_ID, compactionId == null ? "" : compactionId.toString());
        ret.put(SSTABLES, Joiner.on(',').join(sstables));
        return ret;
    }

    boolean shouldStop(Predicate<SSTableReader> sstablePredicate) {
        if (sstables.isEmpty()) {
            return true;
        }
        return sstables.stream().anyMatch(sstablePredicate);
    }

    public static abstract class Holder {

        private volatile transient boolean stopRequested = false;

        public abstract CompactionInfo getCompactionInfo();

        public void stop() {
            stopRequested = true;
        }

        /**
         * if this compaction involves several/all tables we can safely check globalCompactionsPaused
         * in isStopRequested() below
         */
        public abstract boolean isGlobal();

        public boolean isStopRequested() {
            return stopRequested || (isGlobal() && CompactionManager.instance.isGlobalCompactionPaused());
        }
    }

    public enum Unit {

        BYTES("bytes"), RANGES("token range parts"), KEYS("keys");

        private final String name;

        Unit(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }

        public static boolean isFileSize(String unit) {
            return BYTES.toString().equals(unit);
        }
    }
}
