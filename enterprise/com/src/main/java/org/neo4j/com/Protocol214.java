/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.kernel.impl.store.StoreId;

public class Protocol214 extends Protocol
{
    public Protocol214( int chunkSize, byte applicationProtocolVersion, byte internalProtocolVersion )
    {
        super( chunkSize, applicationProtocolVersion, internalProtocolVersion );
    }

    @Override
    protected StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer )
    {
        byteBuffer.clear();
        byteBuffer.limit( 8 + 8 + 8 + 8 + 8 ); // creation time, random id, store version, upgrade time, upgrade id
        source.readBytes( byteBuffer );
        byteBuffer.flip();
        // read order matters - see Server.writeStoreId() for version 2.1.4
        long creationTime = byteBuffer.getLong();
        long randomId = byteBuffer.getLong();
        long storeVersion = byteBuffer.getLong();
        long upgradeTime = byteBuffer.getLong();
        long upgradeId = byteBuffer.getLong();
        return new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId );
    }
}
