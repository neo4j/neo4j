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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class NettyPipelineBuilderTest
{
    private AssertableLogProvider logProvider = new AssertableLogProvider();
    private Log log = logProvider.getLog( getClass() );
    private EmbeddedChannel channel = new EmbeddedChannel();

    @Test
    public void shouldLogExceptionInbound() throws Exception
    {
        // given
        RuntimeException ex = new RuntimeException();
        NettyPipelineBuilder.with( channel.pipeline(), log ).add( new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeOneInbound( new Object() );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( equalTo( "Exception in inbound" ), equalTo( ex ) ) );
    }

    @Test
    public void shouldLogUnhandledMessageInbound() throws Exception
    {
        // given
        Object msg = new Object();
        NettyPipelineBuilder.with( channel.pipeline(), log ).install();

        // when
        channel.writeOneInbound( msg );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( equalTo( "Unhandled inbound message: " + msg ) ) );
    }

    @Test
    public void shouldLogUnhandledMessageOutbound() throws Exception
    {
        // given
        Object msg = new Object();
        NettyPipelineBuilder.with( channel.pipeline(), log ).install();

        // when
        channel.writeAndFlush( msg );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( equalTo( "Unhandled outbound message: " + msg ) ) );
    }

    @Test
    public void shouldLogExceptionOutbound() throws Exception
    {
        RuntimeException ex = new RuntimeException();
        NettyPipelineBuilder.with( channel.pipeline(), log ).add( new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeAndFlush( new Object() );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( equalTo( "Exception in outbound" ), equalTo( ex ) ) );
    }

    @Test
    public void shouldLogExceptionOutboundWithVoidPromise() throws Exception
    {
        RuntimeException ex = new RuntimeException();
        NettyPipelineBuilder.with( channel.pipeline(), log ).add( new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
            {
                throw ex;
            }
        } ).install();

        // when
        channel.writeAndFlush( new Object(), channel.voidPromise() );

        // then
        logProvider.assertExactly( inLog( getClass() ).error( equalTo( "Exception in outbound" ), equalTo( ex ) ) );
    }

    @Test
    public void shouldNotLogAnythingForHandledInbound() throws Exception
    {
        // given
        Object msg = new Object();
        ChannelInboundHandlerAdapter handler = new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
            {
                // handled
            }
        };
        NettyPipelineBuilder.with( channel.pipeline(), log ).add( handler ).install();

        // when
        channel.writeOneInbound( msg );

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldNotLogAnythingForHandledOutbound() throws Exception
    {
        // given
        Object msg = new Object();
        ChannelOutboundHandlerAdapter encoder = new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
            {
                ctx.write( ctx.alloc().buffer() );
            }
        };
        NettyPipelineBuilder.with( channel.pipeline(), log ).add( encoder ).install();

        // when
        channel.writeAndFlush( msg );

        // then
        logProvider.assertNoLoggingOccurred();
    }
}
