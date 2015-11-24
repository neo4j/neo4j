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
package org.neo4j.coreedge.server;

import java.net.InetSocketAddress;
import java.util.Objects;

public class ListenSocketAddress
{
    private final InetSocketAddress address;

    public ListenSocketAddress( InetSocketAddress address )
    {
        this.address = address;
    }

    @Override public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ListenSocketAddress that = (ListenSocketAddress) o;
        return Objects.equals( address, that.address );
    }

    @Override public int hashCode()
    {
        return Objects.hash( address );
    }

    public InetSocketAddress socketAddress()
    {
        return address;
    }

    @Override
    public String toString()
    {
        return address.getHostName() + ":" + address.getPort();
    }
}
