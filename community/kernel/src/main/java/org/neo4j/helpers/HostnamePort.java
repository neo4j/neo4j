/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

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
        String[] parts = hostnamePort.split( ":" );
        if ( parts.length == 1 )
        {
            host = zeroLengthMeansNull( parts[0] );
            ports = new int[]{0, 0};
        }
        else if ( parts.length == 2 )
        {
            host = zeroLengthMeansNull( parts[0] );

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

    private String zeroLengthMeansNull( String string )
    {
        if ( string == null || string.length() == 0 )
            return null;
        return string;
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
     * The host part, or null if not given.
     *
     * @return
     */
    public String getHost()
    {
        return host;
    }

    public String getHost( String defaultHost )
    {
        if (host == null)
            return defaultHost;

        try
        {
            InetAddress ip = InetAddress.getByName( host );
            if (ip == null)
                return defaultHost;

            return ip.getHostAddress();
        }
        catch ( UnknownHostException e )
        {
            return host;
        }
    }

    /**
     * The port range as two ints. If only one port given, then both ints have the same value.
     * If no port range is given, then the array has {0,0} as value.
     *
     * @return
     */
    public int[] getPorts()
    {
        return ports;
    }

    /**
     * The first port, or 0 if no port was given.
     *
     * @return
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

        // URI always contains IP, so make sure we convert ours too

        return result && getHost(null).equalsIgnoreCase( toMatch.getHost() );
    }
}
