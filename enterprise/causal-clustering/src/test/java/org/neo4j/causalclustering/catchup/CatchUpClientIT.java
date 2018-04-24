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
package org.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.ClosedChannelException;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CatchUpClientIT
{

    private LifeSupport lifeSupport;

    @Before
    public void initLifeCycles()
    {
        lifeSupport = new LifeSupport();
    }

    @After
    public void shutdownLifeSupport()
    {
        lifeSupport.stop();
        lifeSupport.shutdown();
    }

    @Test
    public void shouldCloseHandlerIfChannelIsClosedInClient() throws LifecycleException
    {
        // given
        String hostname = "localhost";
        int port = PortAuthority.allocatePort();
        ListenSocketAddress listenSocketAddress = new ListenSocketAddress( hostname, port );
        AtomicBoolean wasClosedByClient = new AtomicBoolean( false );

        Server emptyServer = catchupServer( listenSocketAddress );
        CatchUpClient closingClient = closingChannelCatchupClient( wasClosedByClient );

        lifeSupport.add( emptyServer );
        lifeSupport.add( closingClient );

        // when
        lifeSupport.init();
        lifeSupport.start();

        // then
        assertClosedChannelException( hostname, port, closingClient );
        assertTrue( wasClosedByClient.get() );
    }

    @Test
    public void shouldCloseHandlerIfChannelIsClosedOnServer()
    {
        // given
        String hostname = "localhost";
        int port = PortAuthority.allocatePort();
        ListenSocketAddress listenSocketAddress = new ListenSocketAddress( hostname, port );
        AtomicBoolean wasClosedByServer = new AtomicBoolean( false );

        Server closingChannelServer = closingChannelCatchupServer( listenSocketAddress, wasClosedByServer );
        CatchUpClient emptyClient = emptyClient();

        lifeSupport.add( closingChannelServer );
        lifeSupport.add( emptyClient );

        // when
        lifeSupport.init();
        lifeSupport.start();

        // then
        assertClosedChannelException( hostname, port, emptyClient );
        assertTrue( wasClosedByServer.get() );
    }

    private CatchUpClient emptyClient()
    {
        return catchupClient( new MessageToByteEncoder<GetStoreIdRequest>()
        {
            @Override
            protected void encode( ChannelHandlerContext channelHandlerContext, GetStoreIdRequest getStoreIdRequest, ByteBuf byteBuf )
            {
                byteBuf.writeByte( (byte) 1 );
            }
        } );
    }

    private void assertClosedChannelException( String hostname, int port, CatchUpClient closingClient )
    {
        try
        {
            closingClient.makeBlockingRequest( new AdvertisedSocketAddress( hostname, port ), new GetStoreIdRequest(), neverCompletingAdaptor() );
            fail();
        }
        catch ( CatchUpClientException e )
        {
            Throwable cause = e.getCause();
            assertEquals( cause.getClass(), ExecutionException.class );
            Throwable actualCause = cause.getCause();
            assertEquals( actualCause.getClass(), ClosedChannelException.class );
        }
    }

    private CatchUpResponseAdaptor<Object> neverCompletingAdaptor()
    {
        return new CatchUpResponseAdaptor<>();
    }

    private CatchUpClient closingChannelCatchupClient( AtomicBoolean wasClosedByClient )
    {
        return catchupClient( new MessageToByteEncoder()
        {
            @Override
            protected void encode( ChannelHandlerContext ctx, Object msg, ByteBuf out )
            {
                wasClosedByClient.set( true );
                ctx.channel().close();
            }
        } );
    }

    private Server closingChannelCatchupServer( ListenSocketAddress listenSocketAddress, AtomicBoolean wasClosedByServer )
    {
        return catchupServer( listenSocketAddress, new ByteToMessageDecoder()
        {
            @Override
            protected void decode( ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list )
            {
                wasClosedByServer.set( true );
                ctx.channel().close();
            }
        } );
    }

    private CatchUpClient catchupClient( ChannelHandler... channelHandlers )
    {
        return new CatchUpClient( NullLogProvider.getInstance(), Clock.systemUTC(), 10000, catchUpResponseHandler -> new ChannelInitializer<SocketChannel>()
        {
            @Override
            protected void initChannel( SocketChannel ch )
            {
                ch.pipeline().addLast( channelHandlers );
            }
        } );
    }

    private Server catchupServer( ListenSocketAddress listenSocketAddress, ChannelHandler... channelHandlers )
    {
        return new Server( channel -> channel.pipeline().addLast( channelHandlers ), listenSocketAddress, "empty-test-server" );
    }
}
