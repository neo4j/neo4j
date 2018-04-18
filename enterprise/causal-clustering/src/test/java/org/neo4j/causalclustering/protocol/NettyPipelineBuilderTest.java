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
package org.neo4j.causalclustering.protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class NettyPipelineBuilderTest
{
    private AssertableLogProvider logProvider = new AssertableLogProvider();
    private Log log = logProvider.getLog( getClass() );
    private EmbeddedChannel channel = new EmbeddedChannel();
    private ChannelHandlerAdapter EMPTY_HANDLER = new ChannelHandlerAdapter()
    {
    };

    @Test
    public void shouldLogExceptionInbound()
    {
        // given
        RuntimeException ex = new RuntimeException();
        NettyPipelineBuilder.server( channel.pipeline(), log ).add( "read_handler", new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeOneInbound( new Object() );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( startsWith( "Exception in inbound" ), equalTo( ex ) ) );
        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldLogUnhandledMessageInbound()
    {
        // given
        Object msg = new Object();
        NettyPipelineBuilder.server( channel.pipeline(), log ).install();

        // when
        channel.writeOneInbound( msg );

        // then
        logProvider.assertExactly( inLog( getClass() )
                .error( equalTo( "Unhandled inbound message: %s for channel: %s" ), equalTo( msg ), any( Channel.class ) ) );
        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldLogUnhandledMessageOutbound()
    {
        // given
        Object msg = new Object();
        NettyPipelineBuilder.server( channel.pipeline(), log ).install();

        // when
        channel.writeAndFlush( msg );

        // then
        logProvider.assertExactly( inLog( getClass() )
                .error( equalTo( "Unhandled outbound message: %s for channel: %s" ), equalTo( msg ), any( Channel.class )  ) );
        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldLogExceptionOutbound()
    {
        RuntimeException ex = new RuntimeException();
        NettyPipelineBuilder.server( channel.pipeline(), log ).add( "write_handler", new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeAndFlush( new Object() );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( startsWith( "Exception in outbound" ), equalTo( ex ) ) );
        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldLogExceptionOutboundWithVoidPromise()
    {
        RuntimeException ex = new RuntimeException();
        NettyPipelineBuilder.server( channel.pipeline(), log ).add( "write_handler", new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeAndFlush( new Object(), channel.voidPromise() );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( startsWith( "Exception in outbound" ), equalTo( ex ) ) );
        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldNotLogAnythingForHandledInbound()
    {
        // given
        Object msg = new Object();
        ChannelInboundHandlerAdapter handler = new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                // handled
            }
        };
        NettyPipelineBuilder.server( channel.pipeline(), log ).add( "read_handler", handler ).install();

        // when
        channel.writeOneInbound( msg );

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldNotLogAnythingForHandledOutbound()
    {
        // given
        Object msg = new Object();
        ChannelOutboundHandlerAdapter encoder = new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                ctx.write( ctx.alloc().buffer() );
            }
        };
        NettyPipelineBuilder.server( channel.pipeline(), log ).add( "write_handler", encoder ).install();

        // when
        channel.writeAndFlush( msg );

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldReInstallWithPreviousGate()
    {
        // given
        Object gatedMessage = new Object();

        ServerNettyPipelineBuilder builderA = NettyPipelineBuilder.server( channel.pipeline(), log );
        builderA.addGate( p -> p == gatedMessage );
        builderA.install();

        assertEquals( 3, getHandlers( channel.pipeline() ).size() ); // head/tail error handlers also counted
        assertThat( channel.pipeline().names(),
                hasItems( NettyPipelineBuilder.ERROR_HANDLER_HEAD, NettyPipelineBuilder.MESSAGE_GATE_NAME,
                        NettyPipelineBuilder.ERROR_HANDLER_TAIL ) );

        // when
        ServerNettyPipelineBuilder builderB = NettyPipelineBuilder.server( channel.pipeline(), log );
        builderB.add( "my_handler", EMPTY_HANDLER );
        builderB.install();

        // then
        assertEquals( 4, getHandlers( channel.pipeline() ).size() ); // head/tail error handlers also counted
        assertThat( channel.pipeline().names(),
                hasItems( NettyPipelineBuilder.ERROR_HANDLER_HEAD, "my_handler", NettyPipelineBuilder.MESSAGE_GATE_NAME,
                        NettyPipelineBuilder.ERROR_HANDLER_TAIL ) );
    }

    @Test
    public void shouldInvokeCloseHandlerOnClose() throws InterruptedException
    {
        Semaphore semaphore = new Semaphore( 0 );
        NettyPipelineBuilder.server( channel.pipeline(), log ).onClose( semaphore::release ).install();

        // when
        channel.close();

        // then
        assertTrue( semaphore.tryAcquire( 1, TimeUnit.MINUTES ) );
        assertFalse( channel.isOpen() );
    }

    @Test
    public void shouldInvokeCloseHandlerOnPeerDisconnect() throws InterruptedException
    {
        Semaphore semaphore = new Semaphore( 0 );
        NettyPipelineBuilder.server( channel.pipeline(), log ).onClose( semaphore::release ).install();

        // when
        channel.disconnect();

        // then
        assertTrue( semaphore.tryAcquire( 1, TimeUnit.MINUTES ) );
        assertFalse( channel.isOpen() );
    }

    private List<ChannelHandler> getHandlers( ChannelPipeline pipeline )
    {
        return pipeline.names().stream().map( pipeline::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
    }
}
