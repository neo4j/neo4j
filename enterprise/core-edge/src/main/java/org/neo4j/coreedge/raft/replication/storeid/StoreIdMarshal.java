/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public final class StoreIdMarshal
{
    private StoreIdMarshal() { }

    public static void marshal( StoreId storeId, ByteBuf byteBuf )
    {
        if ( storeId == null)
        {
            byteBuf.writeByte( 0 );
            return;
        }

        byteBuf.writeByte( 1 );
        byteBuf.writeLong( storeId.getCreationTime() );
        byteBuf.writeLong( storeId.getRandomId() );
        byteBuf.writeLong( storeId.getStoreVersion() );
        byteBuf.writeLong( storeId.getUpgradeTime() );
        byteBuf.writeLong( storeId.getUpgradeId() );
    }

    public static StoreId unmarshal( ByteBuf byteBuf )
    {
        if ( byteBuf.readByte() == 0 )
        {
            return null;
        }

        long creationTime = byteBuf.readLong();
        long randomId = byteBuf.readLong();
        long storeVersion = byteBuf.readLong();
        long upgradeTime = byteBuf.readLong();
        long upgradeId = byteBuf.readLong();
        return new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId );
    }

    public static void marshal( StoreId storeId, WritableChannel channel ) throws IOException
    {
        if ( storeId == null)
        {
            channel.put( (byte) 0 );
            return;
        }

        channel.putLong( storeId.getCreationTime() );
        channel.putLong( storeId.getRandomId() );
        channel.putLong( storeId.getStoreVersion() );
        channel.putLong( storeId.getUpgradeTime() );
        channel.putLong( storeId.getUpgradeId() );
    }

    public static StoreId unmarshal( ReadableChannel channel ) throws IOException
    {
        try
        {
            if ( channel.get() == 0 )
            {
                return null;
            }

            long creationTime = channel.getLong();
            long randomId = channel.getLong();
            long storeVersion = channel.getLong();
            long upgradeTime = channel.getLong();
            long upgradeId = channel.getLong();
            return new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId );
        }
        catch ( ReadPastEndException notEnoughBytes )
        {
            return null;
        }
    }

}
