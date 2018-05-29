/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
