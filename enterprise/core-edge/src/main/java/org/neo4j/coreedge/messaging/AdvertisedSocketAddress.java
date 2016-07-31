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
package org.neo4j.coreedge.messaging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;

import org.neo4j.coreedge.messaging.marsalling.StringMarshal;
import org.neo4j.coreedge.core.state.storage.SafeChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class AdvertisedSocketAddress
{
    private final String address;

    public AdvertisedSocketAddress( String address )
    {
        if (address == null)
        {
            throw new IllegalArgumentException( "address cannot be null" );
        }
        this.address = address;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        AdvertisedSocketAddress that = (AdvertisedSocketAddress) o;
        return Objects.equals( address, that.address );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( address );
    }

    public String toString()
    {
        return address;
    }

    public InetSocketAddress socketAddress()
    {
        String[] split = address.split( ":" );
        return new InetSocketAddress( split[0], Integer.valueOf( split[1] ) );
    }

    public static class AdvertisedSocketAddressChannelMarshal extends SafeChannelMarshal<AdvertisedSocketAddress>
    {
        @Override
        public void marshal( AdvertisedSocketAddress address, WritableChannel channel ) throws IOException
        {
            StringMarshal.marshal( channel, address.address );
        }

        @Override
        public AdvertisedSocketAddress unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            String host = StringMarshal.unmarshal( channel );
            return new AdvertisedSocketAddress( host );
        }
    }
}
