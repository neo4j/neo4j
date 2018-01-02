/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.com;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;

import java.net.InetSocketAddress;

import org.neo4j.helpers.HostnamePort;

public class PortRangeSocketBinder
{
    private ServerBootstrap bootstrap;
    private static final String ALL_INTERFACES_ADDRESS = "0.0.0.0";

    public PortRangeSocketBinder( ServerBootstrap bootstrap )
    {
        this.bootstrap = bootstrap;
    }

    public Connection bindToFirstAvailablePortInRange( HostnamePort serverAddress ) throws ChannelException
    {
        int[] ports = serverAddress.getPorts();
        String host = serverAddress.getHost();

        Channel channel;
        InetSocketAddress socketAddress;
        ChannelException lastException = null;

        PortIterator portIterator = new PortIterator( ports );
        while ( portIterator.hasNext() )
        {
            Integer port = portIterator.next();
            if ( host == null || host.equals( ALL_INTERFACES_ADDRESS ) )
            {
                socketAddress = new InetSocketAddress( port );
            }
            else
            {
                socketAddress = new InetSocketAddress( host, port );
            }
            try
            {
                channel = bootstrap.bind( socketAddress );
                return new Connection( socketAddress, channel );
            }
            catch ( ChannelException e )
            {
                if ( lastException != null )
                {
                    e.addSuppressed( lastException );
                }
                lastException = e;
            }
        }

        throw lastException;
    }
}
