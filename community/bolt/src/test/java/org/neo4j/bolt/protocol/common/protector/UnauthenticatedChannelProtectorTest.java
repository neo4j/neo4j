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
package org.neo4j.bolt.protocol.common.protector;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.bolt.protocol.common.handler.AuthenticationTimeoutHandler;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.packstream.codec.transport.ChunkFrameDecoder;

class UnauthenticatedChannelProtectorTest {
    @Test
    void shouldInstallAuthenticationHandlersAfterChannelCreated() {
        var channel = mock(Channel.class);
        var pipeline = mock(ChannelPipeline.class);
        var memoryTracker = mock(MemoryTracker.class);

        when(channel.pipeline()).thenReturn(pipeline);

        var protector = new UnauthenticatedChannelProtector(channel, Duration.ZERO, memoryTracker);

        var inOrder = inOrder(pipeline, memoryTracker);
        protector.afterChannelCreated();
        inOrder.verify(memoryTracker).allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
        inOrder.verify(pipeline).addLast(any(AuthenticationTimeoutHandler.class));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldRemoveHandlersWhenIsDisabled() {
        var channel = mock(Channel.class);
        var pipeline = mock(ChannelPipeline.class);
        var memoryTracker = mock(MemoryTracker.class);
        var handler = mock(ChunkFrameDecoder.class);

        when(channel.pipeline()).thenReturn(pipeline);
        when(channel.isActive()).thenReturn(true);

        doReturn(handler).when(pipeline).get(ChunkFrameDecoder.class);

        var protector = new UnauthenticatedChannelProtector(channel, Duration.ZERO, memoryTracker);

        InOrder inOrder = inOrder(pipeline, memoryTracker);
        protector.disable();
        inOrder.verify(pipeline).remove(AuthenticationTimeoutHandler.class);
        inOrder.verify(memoryTracker).releaseHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldIgnoreDisableCallWhenChannelIsInactive() {
        var channel = mock(Channel.class);
        var pipeline = mock(ChannelPipeline.class);
        var memoryTracker = mock(MemoryTracker.class);

        when(channel.isActive()).thenReturn(false);
        when(channel.pipeline()).thenReturn(pipeline);

        var protector = new UnauthenticatedChannelProtector(channel, Duration.ZERO, memoryTracker);
        protector.disable();

        verify(channel).isActive();
        verify(memoryTracker).allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
        verifyNoMoreInteractions(channel, pipeline, memoryTracker);
    }
}
