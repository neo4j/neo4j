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
package org.neo4j.coreedge.server;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.replication.StringMarshal;
import org.neo4j.coreedge.raft.state.ByteBufferMarshal;
import org.neo4j.server.AdvertisedSocketAddress;

public class AdvertisedSocketAddressByteBufferMarshal implements ByteBufferMarshal<AdvertisedSocketAddress>
{
    public void marshal( AdvertisedSocketAddress address, ByteBuf buffer )
    {
        StringMarshal.marshal( buffer, address.toString() );
    }

    public void marshal( AdvertisedSocketAddress address, ByteBuffer buffer )
    {
        StringMarshal.marshal( buffer, address.toString() );
    }

    public AdvertisedSocketAddress unmarshal( ByteBuf buffer )
    {
        try
        {
            String host = StringMarshal.unmarshal( buffer );
            return new AdvertisedSocketAddress( host );
        }
        catch( IndexOutOfBoundsException notEnoughBytes )
        {
            return null;
        }
    }

    public AdvertisedSocketAddress unmarshal( ByteBuffer buffer )
    {
        try
        {
            String host = StringMarshal.unmarshal( buffer );
            return new AdvertisedSocketAddress( host );
        }
        catch( BufferUnderflowException notEnoughBytes )
        {
            return null;
        }
    }
}
