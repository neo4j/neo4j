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
package org.neo4j.causalclustering.common.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.SocketChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;
import java.util.function.Function;

import org.neo4j.causalclustering.common.EventLoopContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientConnectorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowIllegalStateIfNotBootstrappedBeforeConnecting() throws Exception
    {
        ClientConnector<?> clientConnector = new ClientConnector<>( bootstrapFunction( null ) );
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "Need to be bootstrapped first" );

        clientConnector.connect( new InetSocketAddress( 0 ) );
    }

    @Test
    public void shouldThrowIllegalStateIfNotBootstrappedBeforeStarting() throws Exception
    {
        ClientConnector<?> clientConnector = new ClientConnector<>( bootstrapFunction( null ) );
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage( "Need to be bootstrapped first" );

        clientConnector.start();
    }

    @Test
    public void shouldConnectUsingProvidedBootstrapper() throws Exception
    {
        // given
        Bootstrap bootstrap = mockedBootstrapper();
        ClientConnector<?> clientConnector = new ClientConnector<>( bootstrapFunction( bootstrap ) );

        // when
        clientConnector.bootstrap( null );
        Channel connect = clientConnector.connect( new InetSocketAddress( 0 ) );

        // then
        verify( bootstrap, times( 1 ) ).connect( any() );
    }

    @Test
    public void shouldConnectUsingAugmentedBootstrapper() throws Exception
    {
        // given
        Bootstrap bootstrap = mockedBootstrapper();
        Bootstrap clonedBootstrap = mockedBootstrapper();
        when( bootstrap.clone() ).thenReturn( clonedBootstrap );
        ClientConnector<?> clientConnector = new ClientConnector<>( bootstrapFunction( bootstrap ) );

        // when
        clientConnector.bootstrap( null );
        Channel connect = clientConnector.connect( new InetSocketAddress( 0 ), bootstrap1 -> bootstrap1 );

        // then
        verify( clonedBootstrap, times( 1 ) ).connect( any() );
        verify( bootstrap, never() ).connect( any() );
    }

    @Test
    public void shouldCloseAllChannels() throws Exception
    {
        // given
        Bootstrap bootstrap = mockedBootstrapper();
        ClientConnector<?> clientConnector = new ClientConnector<>( bootstrapFunction( bootstrap ) );

        clientConnector.bootstrap( null );

        Channel connect = clientConnector.connect( new InetSocketAddress( 0 ) );

        //when
        clientConnector.closeChannels();

        // then
        verify( connect, times( 1 ) ).close();
    }

    private Bootstrap mockedBootstrapper()
    {
        Bootstrap bootstrap = mock( Bootstrap.class );
        ChannelFuture channelFuture = getMockedChannelFuture();
        when( bootstrap.connect( any() ) ).thenReturn( channelFuture );
        return bootstrap;
    }

    private ChannelFuture getMockedChannelFuture()
    {
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        Channel channel = mock( Channel.class );
        when( channel.close() ).thenReturn( channelFuture );
        when( channelFuture.awaitUninterruptibly() ).thenReturn( channelFuture );
        when( channelFuture.channel() ).thenReturn( channel );
        return channelFuture;
    }

    private Function<EventLoopContext<SocketChannel>,Bootstrap> bootstrapFunction( Bootstrap bootstrap )
    {
        return eventLoopContext -> bootstrap;
    }
}
