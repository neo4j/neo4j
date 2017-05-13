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
package org.neo4j.causalclustering.messaging.marshalling.storeid;

import io.netty.handler.codec.DecoderException;

import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public final class StoreIdMarshal extends SafeChannelMarshal<StoreId>
{
    public static final StoreIdMarshal INSTANCE = new StoreIdMarshal();

    private StoreIdMarshal() {}

    public void marshal( StoreId storeId, WritableChannel channel ) throws IOException
    {
        if ( storeId == null )
        {
            channel.put( (byte) 0 );
            return;
        }

        channel.put( (byte) 1 );
        channel.putLong( storeId.getCreationTime() );
        channel.putLong( storeId.getRandomId() );
        channel.putLong( storeId.getUpgradeTime() );
        channel.putLong( storeId.getUpgradeId() );
    }

    protected StoreId unmarshal0( ReadableChannel channel ) throws IOException
    {
        byte exists = channel.get();
        if ( exists == 0 )
        {
            return null;
        }
        else if ( exists != 1 )
        {
            throw new DecoderException( "Unexpected value: " + exists );
        }

        long creationTime = channel.getLong();
        long randomId = channel.getLong();
        long upgradeTime = channel.getLong();
        long upgradeId = channel.getLong();
        return new StoreId( creationTime, randomId, upgradeTime, upgradeId );
    }
}
