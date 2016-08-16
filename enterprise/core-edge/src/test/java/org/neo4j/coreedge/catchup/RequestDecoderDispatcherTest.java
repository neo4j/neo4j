/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class RequestDecoderDispatcherTest
{
    private final Protocol<Type> protocol = new Protocol<Type>( Type.two ) {};
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private enum Type
    {
        one, two, three
    }

    @Test
    public void shouldDispatchToRegisteredDecoder() throws Exception
    {
        // given
        RequestDecoderDispatcher<Type> dispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        ChannelInboundHandler delegate = mock( ChannelInboundHandler.class );
        dispatcher.register( Type.one, mock( ChannelInboundHandler.class ) );
        dispatcher.register( Type.two, delegate );
        dispatcher.register( Type.three, mock( ChannelInboundHandler.class ) );

        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        Object msg = new Object();

        // when
        dispatcher.channelRead( ctx, msg );

        // then
        verify( delegate ).channelRead( ctx, msg );
        verifyNoMoreInteractions( delegate );
    }

    @Test
    public void shouldLogAWarningIfThereIsNoDecoderForTheMessageType() throws Exception
    {
        // given
        RequestDecoderDispatcher<Type> dispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        dispatcher.register( Type.one, mock( ChannelInboundHandler.class ) );
        dispatcher.register( Type.three, mock( ChannelInboundHandler.class ) );

        // when
        dispatcher.channelRead( mock( ChannelHandlerContext.class ), new Object() );

        // then
        AssertableLogProvider.LogMatcher matcher =
                inLog( RequestDecoderDispatcher.class ).warn( "Unregistered handler for message type %s", Type.two );

        logProvider.assertExactly( matcher );
    }
}
