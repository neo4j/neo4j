/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility for discovering free ports within a given range, also for remembering already discovered ports
 * so that next allocation will be faster. This utility is designed to be shared among many tests,
 * preferably all tests in an entire component.
 *
 * One test should call {@link #newSession()} and allocate its ports there and {@link Session#close() close}
 * when test is completed. Sessions share discovered ports for faster port allocation.
 */
public class FreePorts
{
    private static final int NOT_FOUND = -1;

    private final Set<Integer> discoveredPorts = new HashSet<>();
    private final Set<Integer> deniedPorts = new HashSet<>();
    private final Set<Integer> currentlyOccupiedPorts = new HashSet<>();

    public Session newSession()
    {
        return new Session();
    }

    private synchronized int findFreePort( int minPort, int maxPort ) throws IOException
    {
        int port = findFreeAlreadyDiscoveredPort( minPort, maxPort );
        if ( port == NOT_FOUND )
        {
            port = discoverPort( minPort, maxPort );
        }

        currentlyOccupiedPorts.add( port );
        return port;
    }

    private synchronized void releasePort( int port )
    {
        if ( !currentlyOccupiedPorts.remove( port ) )
        {
            throw new IllegalStateException( "Port " + port + " not occupied" );
        }
    }

    private int discoverPort( int minPort, int maxPort ) throws IOException
    {
        int port;
        for ( port = minPort; port <= maxPort; port++ )
        {
            if ( discoveredPorts.contains( port ) || deniedPorts.contains( port ) )
            {
                // This port has already been discovered or denied
                continue;
            }
            try
            {
                // try binding it at wildcard
                ServerSocket ss = new ServerSocket();
                ss.setReuseAddress( false );
                ss.bind( new InetSocketAddress( port ) );
                ss.close();
            }
            catch ( IOException e )
            {
                deniedPorts.add( port );
                continue;
            }
            try
            {
                // try binding it at loopback
                ServerSocket ss = new ServerSocket();
                ss.setReuseAddress( false );
                ss.bind( new InetSocketAddress( InetAddress.getLoopbackAddress(), port ) );
                ss.close();
            }
            catch ( IOException e )
            {
                deniedPorts.add( port );
                continue;
            }
            try
            {
                // try connecting to it at loopback
                Socket socket = new Socket( InetAddress.getLoopbackAddress(), port );
                socket.close();
                deniedPorts.add( port );
                continue;
            }
            catch ( IOException e )
            {
                // Port very likely free!
                discoveredPorts.add( port );
                return port;
            }
        }
        throw new IOException( "No open port could be found" );
    }

    private int findFreeAlreadyDiscoveredPort( int minPort, int maxPort )
    {
        for ( Integer candidate : discoveredPorts )
        {
            if ( candidate >= minPort && candidate <= maxPort && !currentlyOccupiedPorts.contains( candidate ) )
            {
                return candidate;
            }
        }
        return NOT_FOUND;
    }

    public class Session implements AutoCloseable
    {
        private final Set<Integer> takenPorts = new HashSet<>();

        public synchronized int findFreePort( int minPort, int maxPort ) throws IOException
        {
            int port = FreePorts.this.findFreePort( minPort, maxPort );
            takenPorts.add( port );
            return port;
        }

        public synchronized void releasePort( int port )
        {
            if ( !takenPorts.remove( port ) )
            {
                throw new IllegalStateException( port + " not taken" );
            }

            FreePorts.this.releasePort( port );
        }

        @Override
        public void close()
        {
            takenPorts.forEach( FreePorts.this::releasePort );
            takenPorts.clear();
        }
    }
}
