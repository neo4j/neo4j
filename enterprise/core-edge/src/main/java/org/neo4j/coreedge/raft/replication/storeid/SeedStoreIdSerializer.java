/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.replication.storeid;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.replication.storeid.SeedStoreId;
import org.neo4j.coreedge.raft.replication.storeid.StoreIdDecoder;
import org.neo4j.coreedge.raft.replication.storeid.StoreIdEncoder;
import org.neo4j.kernel.impl.store.StoreId;

/**
 * Format:
 * ┌──────────────────────────────────────────┐
 * │contentLength                     4 bytes │
 * │contentType                       1 bytes │
 * │content       ┌──────────────────────────┐│
 * │              │creationTime       8 bytes││
 * │              │randomId           8 bytes││
 * │              │storeVersion       8 bytes││
 * │              │upgradeTime        8 bytes││
 * │              │upgradeId          8 bytes││
 * │              └──────────────────────────┘│
 * └──────────────────────────────────────────┘
 */
public class SeedStoreIdSerializer
{
    public static void serialize( SeedStoreId memberSet, ByteBuf byteBuf )
    {
        new StoreIdEncoder().encode( memberSet.storeId(), byteBuf );
    }

    public static SeedStoreId deserialize( ByteBuf buffer )
    {
        StoreId storeId = new StoreIdDecoder().decode( buffer );
        return new SeedStoreId( storeId );
    }
}
