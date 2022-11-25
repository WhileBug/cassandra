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
package org.apache.cassandra.schema;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;

public final class SchemaVersionVerbHandler implements IVerbHandler<NoPayload> {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(SchemaVersionVerbHandler.class);

    public static final transient SchemaVersionVerbHandler instance = new SchemaVersionVerbHandler();

    private final transient Logger logger = LoggerFactory.getLogger(SchemaVersionVerbHandler.class);

    public void doVerb(Message<NoPayload> message) {
        logger.trace("Received schema version request from {}", message.from());
        Message<UUID> response = message.responseWith(Schema.instance.getVersion());
        MessagingService.instance().send(response, message.from());
    }
}
