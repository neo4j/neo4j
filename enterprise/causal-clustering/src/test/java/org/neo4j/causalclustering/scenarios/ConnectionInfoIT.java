/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.neo4j.causalclustering.net.Server;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.causalclustering.ClusterRule;

public class ConnectionInfoIT
{
    private Socket testSocket;

    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );

    @After
    public void teardown() throws IOException
    {
        if ( testSocket != null )
        {
            unbind( testSocket );
        }
    }

    @Test
    public void testAddressAlreadyBoundMessage() throws Throwable
    {
        // given
        testSocket = bindPort( "localhost", PortAuthority.allocatePort() );

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        AssertableLogProvider userLogProvider = new AssertableLogProvider();
        ListenSocketAddress listenSocketAddress = new ListenSocketAddress( "localhost", testSocket.getLocalPort() );

        Server catchupServer = new Server( channel -> { }, logProvider, userLogProvider, listenSocketAddress, "server-name" );

        //then
        try
        {
            catchupServer.start();
        }
        catch ( Throwable throwable )
        {
            //expected.
        }
        logProvider.assertContainsMessageContaining( "server-name: address is already bound: " );
        userLogProvider.assertContainsMessageContaining( "server-name: address is already bound: " );
    }

    @SuppressWarnings( "SameParameterValue" )
    private Socket bindPort( String address, int port ) throws IOException
    {
        Socket socket = new Socket();
        socket.bind( new InetSocketAddress( address, port ) );
        return socket;
    }

    private void unbind( Socket socket ) throws IOException
    {
        socket.close();
    }
}
