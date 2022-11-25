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
package org.apache.cassandra.auth;

import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import static java.lang.String.format;

public final class AuthKeyspace {

    public static transient org.slf4j.Logger logger_IC = org.slf4j.LoggerFactory.getLogger(AuthKeyspace.class);

    private AuthKeyspace() {
    }

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 0: original definition in 3.0
     * gen 1: compression chunk length reduced to 16KiB, memtable_flush_period_in_ms now unset on all tables in 4.0
     */
    public static final transient long GENERATION = 1;

    public static final transient String ROLES = "roles";

    public static final transient String ROLE_MEMBERS = "role_members";

    public static final transient String ROLE_PERMISSIONS = "role_permissions";

    public static final transient String RESOURCE_ROLE_INDEX = "resource_role_permissons_index";

    public static final transient String NETWORK_PERMISSIONS = "network_permissions";

    public static final transient long SUPERUSER_SETUP_DELAY = Long.getLong("cassandra.superuser_setup_delay_ms", 10000);

    private static final transient TableMetadata Roles = parse(ROLES, "role definitions", "CREATE TABLE %s (" + "role text," + "is_superuser boolean," + "can_login boolean," + "salted_hash text," + "member_of set<text>," + "PRIMARY KEY(role))");

    private static final transient TableMetadata RoleMembers = parse(ROLE_MEMBERS, "role memberships lookup table", "CREATE TABLE %s (" + "role text," + "member text," + "PRIMARY KEY(role, member))");

    private static final transient TableMetadata RolePermissions = parse(ROLE_PERMISSIONS, "permissions granted to db roles", "CREATE TABLE %s (" + "role text," + "resource text," + "permissions set<text>," + "PRIMARY KEY(role, resource))");

    private static final transient TableMetadata ResourceRoleIndex = parse(RESOURCE_ROLE_INDEX, "index of db roles with permissions granted on a resource", "CREATE TABLE %s (" + "resource text," + "role text," + "PRIMARY KEY(resource, role))");

    private static final transient TableMetadata NetworkPermissions = parse(NETWORK_PERMISSIONS, "user network permissions", "CREATE TABLE %s (" + "role text, " + "dcs frozen<set<text>>, " + "PRIMARY KEY(role))");

    private static TableMetadata parse(String name, String description, String cql) {
        return CreateTableStatement.parse(format(cql, name), SchemaConstants.AUTH_KEYSPACE_NAME).id(TableId.forSystemTable(SchemaConstants.AUTH_KEYSPACE_NAME, name)).comment(description).gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90)).build();
    }

    public static KeyspaceMetadata metadata() {
        return KeyspaceMetadata.create(SchemaConstants.AUTH_KEYSPACE_NAME, KeyspaceParams.simple(1), Tables.of(Roles, RoleMembers, RolePermissions, ResourceRoleIndex, NetworkPermissions));
    }
}
