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

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraWriteContext implements WriteContext {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(CassandraWriteContext.class);

    private final transient OpOrder.Group opGroup;

    private final transient CommitLogPosition position;

    public CassandraWriteContext(OpOrder.Group opGroup, CommitLogPosition position) {
        Preconditions.checkArgument(opGroup != null);
        this.opGroup = opGroup;
        this.position = position;
    }

    public static CassandraWriteContext fromContext(WriteContext context) {
        Preconditions.checkArgument(context instanceof CassandraWriteContext);
        return (CassandraWriteContext) context;
    }

    public OpOrder.Group getGroup() {
        return opGroup;
    }

    public CommitLogPosition getPosition() {
        return position;
    }

    @Override
    public void close() {
        opGroup.close();
    }
}
