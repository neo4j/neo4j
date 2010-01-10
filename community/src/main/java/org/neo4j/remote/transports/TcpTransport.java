/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote.transports;

import java.net.URI;

import org.neo4j.remote.ConnectionTarget;
import org.neo4j.remote.Transport;

/**
 * A {@link Transport} that creates {@link ConnectionTarget}s for the raw
 * tcp-protocol. The format of a tcp URI is <code>tcp://host[:port]</code>,
 * where host can be either a host name or an IP address.
 * @author Tobias Ivarsson
 */
/*public*/final class TcpTransport extends Transport
{
    private static final int DEFAULT_PORT = 0;

    /**
     * Create a new {@link Transport} for the raw tcp:// protocol.
     */
    public TcpTransport()
    {
        super( "tcp", "tcps" );
    }

    @Override
    protected ConnectionTarget create( URI resourceUri )
    {
        if ( !handlesUri( resourceUri ) )
        {
            throw new IllegalArgumentException( "Unsupported resource URI." );
        }
        boolean useSSL = resourceUri.getScheme().equals( "tcps" );
        String host = resourceUri.getHost();
        int port = resourceUri.getPort();
        if ( port <= 0 )
        {
            port = DEFAULT_PORT;
        }
        return new CustomProtocolTarget( host, port, useSSL );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        if ( "tcp".equals( resourceUri.getScheme() )
            || "tcps".equals( resourceUri.getScheme() ) )
        {
            return resourceUri.getHost() != null
                && resourceUri.getRawPath() == null;
        }
        else
        {
            return false;
        }
    }
}
