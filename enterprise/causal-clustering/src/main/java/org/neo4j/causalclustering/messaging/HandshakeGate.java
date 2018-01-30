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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Future;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.neo4j.causalclustering.protocol.handshake.ClientHandshakeException;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.HandshakeFinishedEvent;

/**
 * Gates messages written before the handshake has completed. The handshake is finalized
 * by firing a HandshakeFinishedEvent (as a netty user event) in {@link HandshakeClientInitializer}.
 */
public class HandshakeGate implements ChannelInterceptor
{
    public static final String HANDSHAKE_GATE = "HandshakeGate";

    private final CompletableFuture<Void> handshakePromise = new CompletableFuture<>();

    HandshakeGate( Channel channel )
    {
        channel.pipeline().addFirst( HANDSHAKE_GATE, new ChannelInboundHandlerAdapter()
        {
            @Override
            public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
            {
                if ( HandshakeFinishedEvent.getSuccess().equals( evt ) )
                {
                    handshakePromise.complete( null );
                }
                else if ( HandshakeFinishedEvent.getFailure().equals( evt ) )
                {
                    handshakePromise.completeExceptionally( new ClientHandshakeException( "Handshake failed" ) );
                    channel.close();
                }
                else
                {
                    super.userEventTriggered( ctx, evt );
                }
            }
        } );
    }

    @Override
    public void write( BiFunction<Channel,Object,Future<Void>> writer, Channel channel, Object msg, CompletableFuture<Void> promise )
    {
        handshakePromise.whenComplete( ( ignored, failure ) ->
                writer.apply( channel, msg ).addListener( x -> promise.complete( null ) ) );
    }
}
