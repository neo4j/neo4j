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
