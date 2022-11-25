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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.dht.Token;

public class CachedHashDecoratedKey extends BufferDecoratedKey {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(CachedHashDecoratedKey.class);

    transient long hash0;

    transient long hash1;

    volatile transient boolean hashCached;

    public CachedHashDecoratedKey(Token token, ByteBuffer key) {
        super(token, key);
        hashCached = false;
    }

    @Override
    public void filterHash(long[] dest) {
        if (hashCached) {
            dest[0] = hash0;
            dest[1] = hash1;
        } else {
            super.filterHash(dest);
            hash0 = dest[0];
            hash1 = dest[1];
            hashCached = true;
        }
    }
}
