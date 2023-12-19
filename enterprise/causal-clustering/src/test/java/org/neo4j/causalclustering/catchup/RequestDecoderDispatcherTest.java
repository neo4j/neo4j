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
