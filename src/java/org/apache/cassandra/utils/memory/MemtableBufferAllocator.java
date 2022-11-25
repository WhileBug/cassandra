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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class MemtableBufferAllocator extends MemtableAllocator {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(MemtableBufferAllocator.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(MemtableBufferAllocator.class);

    protected MemtableBufferAllocator(SubAllocator onHeap, SubAllocator offHeap) {
        super(onHeap, offHeap);
    }

    public abstract ByteBuffer allocate(int size, OpOrder.Group opGroup);

    protected Cloner allocator(OpOrder.Group opGroup) {
        return new ByteBufferCloner() {

            @Override
            public ByteBuffer allocate(int size) {
                return MemtableBufferAllocator.this.allocate(size, opGroup);
            }
        };
    }
}
