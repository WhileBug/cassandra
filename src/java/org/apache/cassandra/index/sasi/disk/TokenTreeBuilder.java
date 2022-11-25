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
package org.apache.cassandra.index.sasi.disk;

import java.io.IOException;
import java.util.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;
import com.carrotsearch.hppc.LongSet;

public interface TokenTreeBuilder extends Iterable<Pair<Long, LongSet>> {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(TokenTreeBuilder.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(TokenTreeBuilder.class);

    transient int BLOCK_BYTES = 4096;

    transient int BLOCK_HEADER_BYTES = 64;

    transient int BLOCK_ENTRY_BYTES = 2 * Long.BYTES;

    transient int OVERFLOW_TRAILER_BYTES = 64;

    transient int OVERFLOW_ENTRY_BYTES = Long.BYTES;

    transient int OVERFLOW_TRAILER_CAPACITY = OVERFLOW_TRAILER_BYTES / OVERFLOW_ENTRY_BYTES;

    transient int TOKENS_PER_BLOCK = (BLOCK_BYTES - BLOCK_HEADER_BYTES - OVERFLOW_TRAILER_BYTES) / BLOCK_ENTRY_BYTES;

    // 48 bits for (signed) offset
    transient long MAX_OFFSET = (1L << 47) - 1;

    transient byte LAST_LEAF_SHIFT = 1;

    transient byte SHARED_HEADER_BYTES = 19;

    transient byte ENTRY_TYPE_MASK = 0x03;

    transient short AB_MAGIC = 0x5A51;

    // note: ordinal positions are used here, do not change order
    enum EntryType {

        SIMPLE, FACTORED, PACKED, OVERFLOW;

        public static EntryType of(int ordinal) {
            if (ordinal == SIMPLE.ordinal())
                return SIMPLE;
            if (ordinal == FACTORED.ordinal())
                return FACTORED;
            if (ordinal == PACKED.ordinal())
                return PACKED;
            if (ordinal == OVERFLOW.ordinal())
                return OVERFLOW;
            throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
        }
    }

    void add(Long token, long keyPosition);

    void add(SortedMap<Long, LongSet> data);

    void add(Iterator<Pair<Long, LongSet>> data);

    void add(TokenTreeBuilder ttb);

    boolean isEmpty();

    long getTokenCount();

    TokenTreeBuilder finish();

    int serializedSize();

    void write(DataOutputPlus out) throws IOException;
}
