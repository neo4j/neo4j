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
package org.neo4j.causalclustering.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.causalclustering.helper.SuspendableLifeCycleLifeStateChangeTest;
import org.neo4j.causalclustering.helper.SuspendableLifeCycleSuspendedStateChangeTest;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * More generalized state tests of SuspendableLifeCycle can be found {@link SuspendableLifeCycleLifeStateChangeTest} and
 * {@link SuspendableLifeCycleSuspendedStateChangeTest}
 */
public class ServerStateTest
{
    private static Bootstrap bootstrap;
    private static EventLoopGroup clientGroup;
    private Server server;
    private Channel channel;

    @BeforeClass
    public static void initialSetup()
    {
        clientGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap()
                .group( clientGroup )
                .channel( NioSocketChannel.class )
                .handler( new ChannelInitializer<NioSocketChannel>()
                {
                    @Override
                    protected void initChannel( NioSocketChannel ch )
                    {

                    }
                } );
    }

    @Before
    public void setUp() throws Throwable
    {
        server = createServer();
        server.init();
        assertFalse( canConnect() );
    }

    @After
    public void tearDown() throws Throwable
    {
        if ( server != null )
        {
            server.stop();
            server.shutdown();
        }
        if ( channel != null )
        {
            channel.close();
        }
    }

    @AfterClass
    public static void finalTearDown()
    {
        clientGroup.shutdownGracefully();
    }

    @Test
    public void shouldStartServerNormally() throws Throwable
    {
        server.start();
        assertTrue( canConnect() );
    }

    @Test
    public void canDisableAndEnableServer() throws Throwable
    {
        server.start();
        assertTrue( canConnect() );

        server.disable();
        assertFalse( canConnect() );

        server.enable();
        assertTrue( canConnect() );
    }

    @Test
    public void serverCannotBeEnabledIfLifeCycleHasNotStarted() throws Throwable
    {
        server.enable();
        assertFalse( canConnect() );

        server.start();
        assertTrue( canConnect() );
    }

    @Test
    public void serverCannotStartIfDisabled() throws Throwable
    {
        server.disable();

        server.start();
        assertFalse( canConnect() );

        server.enable();
        assertTrue( canConnect() );
    }

    private static Server createServer()
    {
        return new Server( channel -> {}, FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ),
                           FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ),
                           new ListenSocketAddress( "localhost", PortAuthority.allocatePort() ), "serverName" );
    }

    private boolean canConnect() throws InterruptedException
    {
        ListenSocketAddress socketAddress = server.address();
        ChannelFuture channelFuture = bootstrap.connect( socketAddress.getHostname(), socketAddress.getPort() );
        channel = channelFuture.channel();
        return channelFuture.await().isSuccess();
    }
}
