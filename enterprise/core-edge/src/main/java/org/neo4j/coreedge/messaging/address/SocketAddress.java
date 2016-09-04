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
package org.neo4j.coreedge.messaging.address;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.lang.String.format;

public abstract class SocketAddress
{
    final String address;
    private static final Pattern pattern = Pattern.compile( "(.+):(\\d+)" );

    public SocketAddress( String address )
    {
        this.address = validate( address );
    }

    private String validate( String address )
    {
        if ( address == null )
        {
            throw new IllegalArgumentException( getClass().getSimpleName() + " cannot be null" );
        }

        address = address.trim();

        if ( address.contains( " " ) )
        {
            throw new IllegalArgumentException( format( "Cannot initialize "+ getClass().getSimpleName() +" for %s. " +
                    "Whitespace" +
                    " characters cause unresolvable ambiguity.", address ) );
        }

        if ( !pattern.matcher( address ).matches() )
        {
            throw new IllegalArgumentException( format( getClass().getSimpleName() + " can only be created with " +
                    "hostname:port. %s is not acceptable", address ) );
        }

        return address;
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
        SocketAddress that = (AdvertisedSocketAddress) o;
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
}

