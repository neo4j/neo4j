/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import org.junit.Test;

import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class RequestDecoderDispatcherTest
{
    private final Protocol<State> protocol = new Protocol<State>( State.two )
    {
    };
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private enum State
    {
        one, two, three
    }

    @Test
    public void shouldDispatchToRegisteredDecoder() throws Exception
    {
        // given
        RequestDecoderDispatcher<State> dispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        ChannelInboundHandler delegateOne = mock( ChannelInboundHandler.class );
        ChannelInboundHandler delegateTwo = mock( ChannelInboundHandler.class );
        ChannelInboundHandler delegateThree = mock( ChannelInboundHandler.class );
        dispatcher.register( State.one, delegateOne );
        dispatcher.register( State.two, delegateTwo );
        dispatcher.register( State.three, delegateThree );

        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        Object msg = new Object();

        // when
        dispatcher.channelRead( ctx, msg );

        // then
        verify( delegateTwo ).channelRead( ctx, msg );
        verifyNoMoreInteractions( delegateTwo );
        verifyZeroInteractions( delegateOne, delegateThree );
    }

    @Test
    public void shouldLogAWarningIfThereIsNoDecoderForTheMessageType() throws Exception
    {
        // given
        RequestDecoderDispatcher<State> dispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        ChannelInboundHandler delegateOne = mock( ChannelInboundHandler.class );
        ChannelInboundHandler delegateThree = mock( ChannelInboundHandler.class );
        dispatcher.register( State.one, delegateOne );
        dispatcher.register( State.three, delegateThree );

        // when
        dispatcher.channelRead( mock( ChannelHandlerContext.class ), new Object() );

        // then
        AssertableLogProvider.LogMatcher matcher =
                inLog( RequestDecoderDispatcher.class ).warn( "Unregistered handler for protocol %s", protocol );

        logProvider.assertExactly( matcher );
        verifyZeroInteractions( delegateOne, delegateThree );
    }
}
