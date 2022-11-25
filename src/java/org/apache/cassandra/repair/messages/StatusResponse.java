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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.UUID;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.utils.UUIDSerializer;

public class StatusResponse extends RepairMessage {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(StatusResponse.class);

    public final transient UUID sessionID;

    public final transient ConsistentSession.State state;

    public StatusResponse(UUID sessionID, ConsistentSession.State state) {
        super(null);
        assert sessionID != null;
        assert state != null;
        this.sessionID = sessionID;
        this.state = state;
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StatusResponse that = (StatusResponse) o;
        if (!sessionID.equals(that.sessionID))
            return false;
        return state == that.state;
    }

    public int hashCode() {
        int result = sessionID.hashCode();
        result = 31 * result + state.hashCode();
        return result;
    }

    public String toString() {
        return "StatusResponse{" + "sessionID=" + sessionID + ", state=" + state + '}';
    }

    public static final transient IVersionedSerializer<StatusResponse> serializer = new IVersionedSerializer<StatusResponse>() {

        public void serialize(StatusResponse msg, DataOutputPlus out, int version) throws IOException {
            UUIDSerializer.serializer.serialize(msg.sessionID, out, version);
            out.writeInt(msg.state.ordinal());
        }

        public StatusResponse deserialize(DataInputPlus in, int version) throws IOException {
            return new StatusResponse(UUIDSerializer.serializer.deserialize(in, version), ConsistentSession.State.valueOf(in.readInt()));
        }

        public long serializedSize(StatusResponse msg, int version) {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID, version) + TypeSizes.sizeof(msg.state.ordinal());
        }
    };
}
