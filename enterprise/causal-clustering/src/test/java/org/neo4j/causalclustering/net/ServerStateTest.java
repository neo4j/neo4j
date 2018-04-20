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
package org.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.helper.EnableableLifeCycleStateChangeTest;
import org.neo4j.causalclustering.helper.EnableableLifeCylcleEnableableStateChangeTest;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * More generalized state tests of EnableableLifeCycle can be found {@link EnableableLifeCycleStateChangeTest} and
 * {@link EnableableLifeCylcleEnableableStateChangeTest}
 */
public class ServerStateTest
{
    private Server server;
    private final EventLoopGroup clientGroup = new NioEventLoopGroup();

    @Before
    public void setUp() throws InterruptedException
    {
        server = createServer();
        server.init();
        assertFalse( canConnect( server.address(), clientGroup ) );
    }

    @After
    public void tearDown()
    {
        server.stop();
        server.shutdown();
    }

    @Test
    public void shouldStartServerNormally() throws InterruptedException
    {
        server.start();
        assertTrue( canConnect( server.address(), clientGroup ) );
    }

    @Test
    public void canDisableAndEnableServer() throws InterruptedException
    {
        server.start();
        assertTrue( canConnect( server.address(), clientGroup ) );

        server.disable();
        assertFalse( canConnect( server.address(), clientGroup ) );

        server.enable();
        assertTrue( canConnect( server.address(), clientGroup ) );
    }

    @Test
    public void serverCannotBeEnabledIfLifeCycleHasNotStarted() throws InterruptedException
    {
        server.enable();
        assertFalse( canConnect( server.address(), clientGroup ) );

        server.start();
        assertTrue( canConnect( server.address(), clientGroup ) );
    }

    @Test
    public void serverCannotStartIfDisabled() throws InterruptedException
    {
        server.disable();

        server.start();
        assertFalse( canConnect( server.address(), clientGroup ) );

        server.enable();
        assertTrue( canConnect( server.address(), clientGroup ) );
    }

    private static Server createServer()
    {
        return new Server( channel ->
                           {
                           }, FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ),
                           FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ),
                           new ListenSocketAddress( "localhost", PortAuthority.allocatePort() ), "serverName" );
    }

    private static boolean canConnect( ListenSocketAddress socketAddress, EventLoopGroup eventExecutors ) throws InterruptedException
    {
        return new Bootstrap().group( eventExecutors ).channel( NioSocketChannel.class ).handler( new ChannelInitializer<NioSocketChannel>()
        {
            @Override
            protected void initChannel( NioSocketChannel ch )
            {

            }
        } ).connect( socketAddress.getHostname(), socketAddress.getPort() ).await().isSuccess();
    }
}
