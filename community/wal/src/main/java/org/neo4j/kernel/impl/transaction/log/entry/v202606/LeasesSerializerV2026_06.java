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
package org.neo4j.kernel.impl.transaction.log.entry.v202606;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.kernel.api.leases.PropertyShardLeases;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.transaction.log.distributed.UUIDLogSerializer;
import org.neo4j.storageengine.api.Leases;

public class LeasesSerializerV2026_06 {
    private LeasesSerializerV2026_06() {}

    public static Leases parse(ReadableChannel channel) throws IOException {
        int leaseCount = channel.getInt();
        HashMap<DatabaseId, PropertyShardLeases.PropertyShardLease> leaseMap = new HashMap<>();
        for (int i = 0; i < leaseCount; ++i) {
            UUID databaseUUID = UUIDLogSerializer.parseNullable(channel);
            DatabaseId databaseId = (databaseUUID != null) ? DatabaseIdFactory.from(databaseUUID) : null;
            UUID serverIdUUID = UUIDLogSerializer.parseNullable(channel);
            ServerId serverId = (serverIdUUID != null) ? new ServerId(serverIdUUID) : null;
            long id = channel.getLong();
            leaseMap.put(databaseId, new PropertyShardLeases.PropertyShardLease(databaseId, serverId, id));
        }
        return new PropertyShardLeases(leaseMap);
    }

    public static int byteSize(Leases leases) {
        int size = Integer.BYTES;
        for (Leases.Lease lease : leases) {
            size += UUIDLogSerializer.nullableByteSize(
                    (lease.databaseId() != null) ? lease.databaseId().uuid() : null);
            size += UUIDLogSerializer.nullableByteSize(
                    (lease.serverId() != null) ? lease.serverId().uuid() : null);
            size += Long.BYTES;
        }
        return size;
    }

    public static void write(WritableChannel channel, Leases leases) throws IOException {
        channel.putInt(leases.size());
        for (Leases.Lease lease : leases) {
            UUIDLogSerializer.writeNullable(
                    channel, (lease.databaseId() != null) ? lease.databaseId().uuid() : null);
            UUIDLogSerializer.writeNullable(
                    channel, (lease.serverId() != null) ? lease.serverId().uuid() : null);
            channel.putLong(lease.id());
        }
    }
}
