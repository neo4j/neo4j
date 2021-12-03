/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport.pipeline;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;

import org.neo4j.memory.MemoryTracker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class UnauthenticatedChannelProtectorTest
{
    @Test
    void shouldInstallAuthenticationHandlersAfterChannelCreated()
    {
        var channel = mock( Channel.class );
        var pipeline = mock( ChannelPipeline.class );
        when( channel.pipeline() ).thenReturn( pipeline );
        var memoryTracker = mock( MemoryTracker.class );

        var protector =
                new UnauthenticatedChannelProtector( channel, Duration.ZERO, -1, memoryTracker );

        InOrder inOrder = inOrder( pipeline, memoryTracker );
        protector.afterChannelCreated();
        inOrder.verify( memoryTracker ).allocateHeap( AuthenticationTimeoutHandler.SHALLOW_SIZE );
        inOrder.verify( pipeline ).addLast( any( AuthenticationTimeoutHandler.class ) );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldInstallByteAccumulatorBeforeBoltProtocolInstalled()
    {
        var channel = mock( Channel.class );
        var pipeline = mock( ChannelPipeline.class );
        when( channel.pipeline() ).thenReturn( pipeline );
        var memoryTracker = mock( MemoryTracker.class );

        var protector = new UnauthenticatedChannelProtector( channel, Duration.ZERO, 0, memoryTracker );

        var inOrder = inOrder( pipeline, memoryTracker );
        protector.beforeBoltProtocolInstalled();
        inOrder.verify( memoryTracker ).allocateHeap( BytesAccumulator.SHALLOW_SIZE );
        inOrder.verify( pipeline ).addLast( any( BytesAccumulator.class ) );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldRemoveHandlersWhenIsDisabled()
    {
        var channel = mock( Channel.class );
        when( channel.isActive() ).thenReturn( true );
        var pipeline = mock( ChannelPipeline.class );
        when( channel.pipeline() ).thenReturn( pipeline );
        var memoryTracker = mock( MemoryTracker.class );

        var protector = new UnauthenticatedChannelProtector( channel, Duration.ZERO, 0, memoryTracker );

        InOrder inOrder = inOrder( pipeline, memoryTracker );
        protector.disable();
        inOrder.verify( pipeline ).remove( AuthenticationTimeoutHandler.class );
        inOrder.verify( pipeline ).remove( BytesAccumulator.class );
        inOrder.verify( memoryTracker ).releaseHeap( AuthenticationTimeoutHandler.SHALLOW_SIZE + BytesAccumulator.SHALLOW_SIZE );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldIgnoreDisableCallWhenChannelIsInactive()
    {
        var channel = mock( Channel.class );
        when( channel.isActive() ).thenReturn( false );
        var pipeline = mock( ChannelPipeline.class );
        when( channel.pipeline() ).thenReturn( pipeline );
        var memoryTracker = mock( MemoryTracker.class );

        var protector = new UnauthenticatedChannelProtector( channel, Duration.ZERO, 0, memoryTracker );
        protector.disable();

        verify( channel ).isActive();
        verify( memoryTracker ).allocateHeap( AuthenticationTimeoutHandler.SHALLOW_SIZE );
        verifyNoMoreInteractions( channel, pipeline, memoryTracker );
    }
}
