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
package org.neo4j.shell.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;

public class HostBoundSocketFactory implements RMIClientSocketFactory, RMIServerSocketFactory
{
    private final InetAddress inetAddress;

    public HostBoundSocketFactory( String host ) throws UnknownHostException
    {
        this.inetAddress = InetAddress.getByName( host );
    }

    @Override
    public Socket createSocket( String host, int port ) throws IOException
    {
        return new Socket( host, port );
    }

    @Override
    public ServerSocket createServerSocket( int port ) throws IOException
    {
        return new ServerSocket( port, 50, inetAddress );
    }
}
