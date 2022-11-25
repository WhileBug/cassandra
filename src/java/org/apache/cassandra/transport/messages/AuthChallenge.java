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
package org.apache.cassandra.transport.messages;

import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.ProtocolVersion;
import java.nio.ByteBuffer;

/**
 * SASL challenge sent from client to server
 */
public class AuthChallenge extends Message.Response {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(AuthChallenge.class);

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(AuthChallenge.class);

    public static final transient Message.Codec<AuthChallenge> codec = new Message.Codec<AuthChallenge>() {

        public AuthChallenge decode(ByteBuf body, ProtocolVersion version) {
            ByteBuffer b = CBUtil.readValue(body);
            byte[] token = new byte[b.remaining()];
            b.get(token);
            return new AuthChallenge(token);
        }

        public void encode(AuthChallenge challenge, ByteBuf dest, ProtocolVersion version) {
            CBUtil.writeValue(challenge.token, dest);
        }

        public int encodedSize(AuthChallenge challenge, ProtocolVersion version) {
            return CBUtil.sizeOfValue(challenge.token);
        }
    };

    private transient byte[] token;

    public AuthChallenge(byte[] token) {
        super(Message.Type.AUTH_CHALLENGE);
        this.token = token;
    }

    public byte[] getToken() {
        return token;
    }
}
