/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.helpers;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Socket address derived from configuration.
 * There is no network awareness at all, just stores the raw configuration exactly as it comes.
 */
public class SocketAddress
{
    private final String hostname;
    private final int port;

    public SocketAddress( String hostname, int port )
    {
        this.hostname = hostname;
        this.port = port;
    }

    /**
     * Hostname or IP address; we don't care.
     */
    public String getHostname()
    {
        return hostname;
    }

    public int getPort()
    {
        return port;
    }

    public InetSocketAddress socketAddress()
    {
        return new InetSocketAddress( hostname, port );
    }

    @Override
    public String toString()
    {
        return hostname + ":" + port;
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
        SocketAddress that = (SocketAddress) o;
        return port == that.port && Objects.equals( hostname, that.hostname );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( hostname, port );
    }
}
