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

import java.nio.ByteBuffer;
import org.apache.cassandra.index.sasi.Term;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class OnDiskBlock<T extends Term> {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(OnDiskBlock.class);

    public enum BlockType {

        POINTER, DATA
    }

    // this contains offsets of the terms and term data
    protected final transient MappedBuffer blockIndex;

    protected final transient int blockIndexSize;

    protected final transient boolean hasCombinedIndex;

    protected final transient TokenTree combinedIndex;

    public OnDiskBlock(Descriptor descriptor, MappedBuffer block, BlockType blockType) {
        blockIndex = block;
        if (blockType == BlockType.POINTER) {
            hasCombinedIndex = false;
            combinedIndex = null;
            // num terms * sizeof(short)
            blockIndexSize = block.getInt() << 1;
            return;
        }
        long blockOffset = block.position();
        int combinedIndexOffset = block.getInt(blockOffset + OnDiskIndexBuilder.BLOCK_SIZE);
        hasCombinedIndex = (combinedIndexOffset >= 0);
        long blockIndexOffset = blockOffset + OnDiskIndexBuilder.BLOCK_SIZE + 4 + combinedIndexOffset;
        combinedIndex = hasCombinedIndex ? new TokenTree(descriptor, blockIndex.duplicate().position(blockIndexOffset)) : null;
        blockIndexSize = block.getInt() * 2;
    }

    public SearchResult<T> search(AbstractType<?> comparator, ByteBuffer query) {
        int cmp = -1, start = 0, end = termCount() - 1, middle = 0;
        T element = null;
        while (start <= end) {
            middle = start + ((end - start) >> 1);
            element = getTerm(middle);
            cmp = element.compareTo(comparator, query);
            if (cmp == 0)
                return new SearchResult<>(element, cmp, middle);
            else if (cmp < 0)
                start = middle + 1;
            else
                end = middle - 1;
        }
        return new SearchResult<>(element, cmp, middle);
    }

    @SuppressWarnings("resource")
    protected T getTerm(int index) {
        MappedBuffer dup = blockIndex.duplicate();
        long startsAt = getTermPosition(index);
        if (// last element
        termCount() - 1 == index)
            dup.position(startsAt);
        else
            dup.position(startsAt).limit(getTermPosition(index + 1));
        return cast(dup);
    }

    protected long getTermPosition(int idx) {
        return getTermPosition(blockIndex, idx, blockIndexSize);
    }

    protected int termCount() {
        return blockIndexSize >> 1;
    }

    protected abstract T cast(MappedBuffer data);

    static long getTermPosition(MappedBuffer data, int idx, int indexSize) {
        idx <<= 1;
        assert idx < indexSize;
        return data.position() + indexSize + data.getShort(data.position() + idx);
    }

    public TokenTree getBlockIndex() {
        return combinedIndex;
    }

    public int minOffset(OnDiskIndex.IteratorOrder order) {
        return order == OnDiskIndex.IteratorOrder.DESC ? 0 : termCount() - 1;
    }

    public int maxOffset(OnDiskIndex.IteratorOrder order) {
        return minOffset(order) == 0 ? termCount() - 1 : 0;
    }

    public static class SearchResult<T> {

        public final transient T result;

        public final transient int index, cmp;

        public SearchResult(T result, int cmp, int index) {
            this.result = result;
            this.index = index;
            this.cmp = cmp;
        }
    }
}
