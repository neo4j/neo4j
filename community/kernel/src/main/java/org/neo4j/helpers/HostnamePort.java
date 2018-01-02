/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.net.URI;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Represents a hostname and port, optionally with a port range.
 * Examples: myhost, myhost:1234, myhost:1234-1240, :1234, :1234-1240
 */
public class HostnamePort
{
    private final String host;
    private final int[] ports;

    public HostnamePort( String hostnamePort ) throws IllegalArgumentException
    {
        Objects.requireNonNull( hostnamePort );

        String[] parts = splitHostAndPort( hostnamePort );
        if ( parts.length == 1 )
        {
            host = Strings.defaultIfBlank( parts[0], null );
            ports = new int[]{0, 0};
        }
        else if ( parts.length == 2 )
        {
            host = Strings.defaultIfBlank( parts[0], null );

            String[] portStrings = parts[1].split( "-" );
            ports = new int[2];

            if ( portStrings.length == 1 )
            {
                ports[0] = ports[1] = Integer.parseInt( portStrings[0] );
            }
            else if ( portStrings.length == 2 )
            {
                ports[0] = Integer.parseInt( portStrings[0] );
                ports[1] = Integer.parseInt( portStrings[1] );
            }
            else
            {
                throw new IllegalArgumentException( format( "Cannot have more than two port ranges: %s",
                        hostnamePort ) );
            }
        }
        else
        {
            throw new IllegalArgumentException( hostnamePort );
        }
    }

    public HostnamePort( String host, int port )
    {
        this( host, port, port );
    }

    public HostnamePort( String host, int portFrom, int portTo )
    {
        this.host = host;
        ports = new int[]{portFrom, portTo};
    }

    /**
     * The host part, or {@code null} if not given.
     * 
     * @return the host part, or {@code null} if not given
     */
    public String getHost()
    {
        return host;
    }

    public static String getHostAddress( String host, String defaultHost )
    {
        if ( host == null )
        {
            return defaultHost;
        }
        else
        {
            return host;
        }
    }
    
    public String getHost( String defaultHost )
    {
    	return getHostAddress( host, defaultHost );
    }

    /**
     * The port range as two ints. If only one port given, then both ints have the same value.
     * If no port range is given, then the array has {0,0} as value.
     * 
     * @return the port range as two ints, which may have the same value; if no port range has been given both ints are {@code 0}
     */
    public int[] getPorts()
    {
        return ports;
    }

    /**
     * The first port, or 0 if no port was given.
     * 
     * @return the first port or {@code 0} if no port was given
     */
    public int getPort()
    {
        return ports[0];
    }

    public boolean isRange()
    {
        return ports[0] != ports[1];
    }

    @Override
    public String toString()
    {
        return toString( null /*no default host*/ );
    }

    public String toString( String defaultHost )
    {
        StringBuilder builder = new StringBuilder();
        String host = getHost( defaultHost );
        if ( host != null )
        {
            builder.append( host );
        }

        if ( getPort() != 0 )
        {
            builder.append( ":" );
            builder.append( getPort() );
            if ( isRange() )
            {
                builder.append( "-" ).append( getPorts()[1] );
            }
        }

        return builder.toString();
    }

    public boolean matches( URI toMatch )
    {
        boolean result = false;
        for ( int port = ports[0]; port <= ports[1]; port++ )
        {
            if ( port == toMatch.getPort() )
            {
                result = true;
                break;
            }
        }

        if ( host == null && toMatch.getHost() == null )
        {
            return result;
        }
        else if ( host == null )
        {
            return false;
        }

        // URI may contain IP, so make sure we check it too by converting ours, if necessary
        String toMatchHost = toMatch.getHost();
        
        // this tries to match hostnames as they are at first, then tries to extract and match ip addresses of both
        return result && ( host.equalsIgnoreCase( toMatchHost ) || getHost(null).equalsIgnoreCase( getHostAddress( toMatchHost, toMatchHost ) ) );
    }

    private static String[] splitHostAndPort( String hostnamePort )
    {
        hostnamePort = hostnamePort.trim();

        int indexOfSchemaSeparator = hostnamePort.indexOf( "://" );
        if ( indexOfSchemaSeparator != -1 )
        {
            hostnamePort = hostnamePort.substring( indexOfSchemaSeparator + 3 );
        }

        boolean isIPv6HostPort = hostnamePort.startsWith( "[" ) && hostnamePort.contains( "]" );
        if ( isIPv6HostPort )
        {
            int splitIndex = hostnamePort.indexOf( "]" ) + 1;

            String host = hostnamePort.substring( 0, splitIndex );
            String port = hostnamePort.substring( splitIndex );
            if ( !Strings.isBlank( port ) )
            {
                port = port.substring( 1 ); // remove ':'
                return new String[]{host, port};
            }
            return new String[]{host};
        }
        return hostnamePort.split( ":" );
    }
}
