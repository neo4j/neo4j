/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.configuration.helpers;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.annotations.api.PublicApi;

import static java.util.Arrays.asList;

/**
 * Socket address derived from configuration.
 * There is no network awareness at all, just stores the raw configuration exactly as it comes.
 */
@PublicApi
public class SocketAddress
{
    private static final Set<String> WILDCARDS = new HashSet<>( asList( "0.0.0.0", "::" ) );
    private final String hostname;
    private final int port;

    public SocketAddress( String hostname )
    {
        this( hostname, -1 );
    }

    public SocketAddress( int port )
    {
        this( null, port );
    }

    public SocketAddress( String hostname, int port )
    {
        if ( hostname != null && ( hostname.contains( "[" ) || hostname.contains( "]" ) ) )
        {
            throw new IllegalArgumentException( "hostname cannot contain '[' or ']'" );
        }

        this.hostname = hostname != null ? hostname : "";
        this.port = port;
    }

    /**
     * @return hostname or IP address; we don't care.
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

    public boolean isWildcard()
    {
        return WILDCARDS.contains( hostname );
    }

    public boolean isIPv6()
    {
        return isIPv6( hostname );
    }

    @Override
    public String toString()
    {
        return format( hostname, port );
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

    public static String format( java.net.SocketAddress address )
    {
        if ( address instanceof InetSocketAddress )
        {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
            return format( inetSocketAddress.getHostString(), inetSocketAddress.getPort() );
        }
        return address.toString();
    }

    public static String format( String hostname, int port )
    {
        String portStr = port >= 0 ? String.format( ":%s", port ) : "";
        String hostnameStr = "";
        if ( hostname != null )
        {
            hostnameStr = isIPv6( hostname ) ? String.format( "[%s]", hostname ) : hostname;
        }
        return hostnameStr + portStr;
    }

    private static boolean isIPv6( String hostname )
    {
        return hostname.contains( ":" );
    }
}
