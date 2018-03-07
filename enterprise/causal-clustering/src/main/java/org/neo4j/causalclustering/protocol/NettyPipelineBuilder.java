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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.neo4j.causalclustering.messaging.MessageGate;
import org.neo4j.logging.Log;

import static java.util.Arrays.asList;

/**
 * Builder and installer of pipelines.
 * <p>
 * Makes sures to install sane last-resort error handling and
 * handles the construction of common patterns, like framing.
 * <p>
 * Do not modify the names of handlers you install.
 */
public abstract class NettyPipelineBuilder<O extends ProtocolInstaller.Orientation, BUILDER extends NettyPipelineBuilder<O, BUILDER>>
{
    static final String MESSAGE_GATE_NAME = "message_gate";

    private final ChannelPipeline pipeline;
    private final Log log;
    private final List<HandlerInfo> handlerInfos = new ArrayList<>();

    private Predicate<Object> gatePredicate;

    @SuppressWarnings( "unchecked" )
    private BUILDER self = (BUILDER) this;

    NettyPipelineBuilder( ChannelPipeline pipeline, Log log )
    {
        this.pipeline = pipeline;
        this.log = log;
    }

    /**
     * Entry point for the client builder.
     *
     * @param pipeline The pipeline to build for.
     * @param log The log used for last-resort errors occurring in the pipeline.
     * @return The client builder.
     */
    public static ClientNettyPipelineBuilder client( ChannelPipeline pipeline, Log log )
    {
        return new ClientNettyPipelineBuilder( pipeline, log );
    }

    /**
     * Entry point for the server builder.
     *
     * @param pipeline The pipeline to build for.
     * @param log The log used for last-resort errors occurring in the pipeline.
     * @return The server builder.
     */
    public static ServerNettyPipelineBuilder server( ChannelPipeline pipeline, Log log )
    {
        return new ServerNettyPipelineBuilder( pipeline, log );
    }

    /**
     * Adds buffer framing to the pipeline. Useful for pipelines marshalling
     * complete POJOs as an example using {@link MessageToByteEncoder} and
     * {@link ByteToMessageDecoder}.
     */
    public abstract BUILDER addFraming();

    public BUILDER modify( ModifierProtocolInstaller<O> modifier )
    {
        modifier.apply( this );
        return self;
    }

    public BUILDER modify( List<ModifierProtocolInstaller<O>> modifiers )
    {
        modifiers.forEach( this::modify );
        return self;
    }

    /**
     * Adds handlers to the pipeline.
     * <p>
     * The pipeline builder controls the internal names of the handlers in the
     * pipeline and external actors are forbidden from manipulating them.
     *
     * @param name The name of the handler, which must be unique.
     * @param newHandlers The new handlers.
     * @return The builder.
     */
    public BUILDER add( String name, List<ChannelHandler> newHandlers )
    {
        newHandlers.stream().map( handler -> new HandlerInfo( name, handler ) ).forEachOrdered( handlerInfos::add );
        return self;
    }

    /**
     * @see #add(String, List)
     */
    public BUILDER add( String name, ChannelHandler... newHandlers )
    {
        return add( name, asList( newHandlers ) );
    }

    public BUILDER addGate( Predicate<Object> gatePredicate )
    {
        if ( this.gatePredicate != null )
        {
            throw new IllegalStateException( "Cannot have more than one gate." );
        }
        this.gatePredicate = gatePredicate;
        return self;
    }

    /**
     * Installs the built pipeline and removes any old pipeline.
     */
    public void install()
    {
        ChannelHandler oldGateHandler = removeOldGate();
        clear();
        for ( HandlerInfo info : handlerInfos )
        {
            pipeline.addLast( info.name, info.handler );
        }
        installGate( oldGateHandler );
        installErrorHandling();
    }

    private ChannelHandler removeOldGate()
    {
        if ( pipeline.get( MESSAGE_GATE_NAME ) != null )
        {
            return pipeline.remove( MESSAGE_GATE_NAME );
        }
        return null;
    }

    private void installGate( ChannelHandler oldGateHandler )
    {
        if ( oldGateHandler != null && gatePredicate != null )
        {
            throw new IllegalStateException( "Cannot have more than one gate." );
        }
        else if ( gatePredicate != null )
        {
            pipeline.addLast( MESSAGE_GATE_NAME, new MessageGate( gatePredicate ) );
        }
        else if ( oldGateHandler != null )
        {
            pipeline.addLast( MESSAGE_GATE_NAME, oldGateHandler );
        }
    }

    private void clear()
    {
        pipeline.names().stream().filter( this::isNotDefault ).forEach( pipeline::remove );
    }

    private boolean isNotDefault( String name )
    {
        // these are netty internal handlers for head and tail
        return pipeline.get( name ) != null;
    }

    private void installErrorHandling()
    {
        // inbound goes in the direction from first->last
        pipeline.addLast( "error_handler_tail", new ChannelDuplexHandler()
        {
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
                log.error( "Exception in inbound", cause );
            }

            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                log.error( "Unhandled inbound message: " + msg );
            }

            // this is the first handler for an outbound message, and attaches a listener to its promise if possible
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                // if the promise is a void-promise, then exceptions will instead propagate to the
                // exceptionCaught handler on the outbound handler further below

                if ( !promise.isVoid() )
                {
                    promise.addListener( (ChannelFutureListener) future -> {
                        if ( !future.isSuccess() )
                        {
                            log.error( "Exception in outbound", future.cause() );
                        }
                    } );
                }
                ctx.write( msg, promise );
            }
        } );

        pipeline.addFirst( "error_handler_head", new ChannelOutboundHandlerAdapter()
        {
            // exceptions which did not get fulfilled on the promise of a write, etc.
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
                log.error( "Exception in outbound", cause );
            }

            // netty can only handle bytes in the form of ByteBuf, so if you reach this then you are
            // perhaps trying to send a POJO without having a suitable encoder
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                if ( !(msg instanceof ByteBuf) )
                {
                    log.error( "Unhandled outbound message: " + msg );
                }
                else
                {
                    ctx.write( msg, promise );
                }
            }
        } );
    }

    private static class HandlerInfo
    {
        private final String name;
        private final ChannelHandler handler;

        HandlerInfo( String name, ChannelHandler handler )
        {
            this.name = name;
            this.handler = handler;
        }
    }
}
