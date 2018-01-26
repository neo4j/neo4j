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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.messaging.HandshakeGate;
import org.neo4j.logging.Log;

import static java.util.Arrays.asList;

/**
 * Builder and installer of pipelines.
 *
 * Makes sures to install sane last-resort error handling and
 * handles the construction of common patterns, like framing.
 */
public class NettyPipelineBuilder
{
    private final ChannelPipeline pipeline;
    private final Log log;
    private final List<ChannelHandler> handlers = new ArrayList<>();

    private NettyPipelineBuilder( ChannelPipeline pipeline, Log log )
    {
        this.pipeline = pipeline;
        this.log = log;
    }

    public static NettyPipelineBuilder with( ChannelPipeline pipeline, Log log )
    {
        return new NettyPipelineBuilder( pipeline, log );
    }

    public NettyPipelineBuilder addFraming()
    {
        add( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
        add( new LengthFieldPrepender( 4 ) );
        return this;
    }

    public NettyPipelineBuilder add( List<ChannelHandler> newHandlers )
    {
        handlers.addAll( newHandlers );
        return this;
    }

    public NettyPipelineBuilder add( ChannelHandler... newHandlers )
    {
        return add( asList( newHandlers ) );
    }

    /**
     * Installs the built pipeline and removes any old pipeline.
     */
    public void install()
    {
        clear();
        handlers.forEach( pipeline::addLast );
        installErrorHandling();
    }

    private void clear()
    {
        pipeline.names().stream()
                .filter( this::isNotDefault )
                .filter( this::isNotUserEvent )
                .forEach( pipeline::remove );
    }

    private boolean isNotUserEvent( String name )
    {
        return !HandshakeGate.HANDSHAKE_GATE.equals( name );
    }

    private boolean isNotDefault( String name )
    {
        return pipeline.get( name ) != null;
    }

    private void installErrorHandling()
    {
        pipeline.addLast( new ChannelDuplexHandler()
        {
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
            {
                log.error( "Exception in inbound", cause );
            }

            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
            {
                log.error( "Unhandled inbound message: " + msg );
            }

            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
            {
                if ( !promise.isVoid() )
                {
                    promise.addListener( (ChannelFutureListener) future ->
                    {
                        if ( !future.isSuccess() )
                        {
                            log.error( "Exception in outbound", future.cause() );
                        }
                    } );
                }
                ctx.write( msg, promise );
            }
        } );

        pipeline.addFirst( new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
            {
                log.error( "Exception in outbound", cause );
            }

            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
            {
                if ( !(msg instanceof ByteBuf) )
                {
                    log.error( "Unhandled outbound message: " + msg );
                }
                else
                {
                    ctx.write( msg );
                }
            }
        } );
    }
}
