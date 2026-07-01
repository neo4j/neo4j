/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.util.StoreIdDecodeUtils;

public record DatabaseDetails(
        // server level values - will differ for every member
        Optional<ServerId> serverId,
        TopologyGraphDbmsModel.DatabaseAccess databaseAccess,
        Optional<SocketAddress> boltAddress,
        Optional<String> role,
        boolean writer,
        String actualStatus,
        String statusMessage,
        OptionalLong lastCommittedTxId,
        OptionalLong txCommitLag,
        OptionalLong shardCommitLag,
        // database level values - will be the same for all members
        NamedDatabaseId namedDatabaseId,
        String requestedStatus,
        String type,
        Map<String, Object> options,
        Optional<StoreId> storeId,
        Optional<ExternalStoreId> externalStoreId,
        Integer requestedPrimariesCount,
        Integer actualPrimariesCount,
        Integer requestedSecondariesCount,
        Integer actualSecondariesCount,
        Optional<ZonedDateTime> creationTime,
        Optional<ZonedDateTime> lastStartTime,
        Optional<ZonedDateTime> lastStopTime,
        Optional<CypherVersion> cypherVersion) {

    public static final String ROLE_PRIMARY = "primary";
    public static final String ROLE_SECONDARY = "secondary";

    public static final String STATUS_UNKNOWN = "unknown";
    public static final String STATUS_MIXED = "mixed";
    public static final String STATUS_MESSAGE_UNKNOWN = "Server is unavailable";
    public static final String STATUS_MESSAGE_ORPHANED = "Database not currently allocated to any servers";

    public static final String TYPE_SYSTEM = "system";
    public static final String TYPE_STANDARD = "standard";
    public static final String TYPE_COMPOSITE = "composite";
    public static final String TYPE_GRAPH_SHARD = "graph shard";
    public static final String TYPE_PROPERTY_SHARD = "property shard";
    public static final String TYPE_MIRROR = "replica";
    public static final String TYPE_GRAPH_ENGINE = "virtual graph";

    public String databaseType() {
        return type;
    }

    public Optional<String> readableExternalStoreId() {
        return externalStoreId.flatMap(id -> Optional.of(StoreIdDecodeUtils.decodeId(id)));
    }

    public Optional<String> readableStoreId() {
        return storeId.map(StoreId::getStoreVersionUserString);
    }
}
