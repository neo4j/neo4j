/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.store.StoreId;

public class Protocol323 extends Protocol310
{
    public Protocol323( int chunkSize, byte applicationProtocolVersion, byte internalProtocolVersion )
    {
        super( chunkSize, applicationProtocolVersion, internalProtocolVersion );
    }

    @Override
    protected StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer )
    {
        byteBuffer.clear();
        // creation time, random id, store version
        byteBuffer.limit( Long.BYTES + Long.BYTES + Long.BYTES );
        source.readBytes( byteBuffer );
        byteBuffer.flip();
        // read order matters - see Server.writeStoreId
        long creationTime = byteBuffer.getLong();
        long randomId = byteBuffer.getLong();
        long storeVersion = byteBuffer.getLong();
        return new StoreId( creationTime, randomId, storeVersion );
    }
}
