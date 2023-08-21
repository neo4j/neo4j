/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.protector;

class UnauthenticatedConnectionListenerTest {
    //    @Test
    //    void shouldInstallAuthenticationHandlersAfterChannelCreated() {
    //        var channel = mock(Channel.class);
    //        var pipeline = mock(ChannelPipeline.class);
    //        var memoryTracker = mock(MemoryTracker.class);
    //
    //        when(channel.pipeline()).thenReturn(pipeline);
    //
    //        var protector = new UnauthenticatedConnectionListener(channel, Duration.ZERO, memoryTracker);
    //
    //        var inOrder = inOrder(pipeline, memoryTracker);
    //        protector.onConnectionCreated();
    //        inOrder.verify(memoryTracker).allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
    //        inOrder.verify(pipeline).addLast(any(AuthenticationTimeoutHandler.class));
    //        inOrder.verifyNoMoreInteractions();
    //    }
    //
    //    @Test
    //    void shouldRemoveHandlersWhenIsDisabled() {
    //        var channel = mock(Channel.class);
    //        var pipeline = mock(ChannelPipeline.class);
    //        var memoryTracker = mock(MemoryTracker.class);
    //        var handler = mock(ChunkFrameDecoder.class);
    //
    //        when(channel.pipeline()).thenReturn(pipeline);
    //        when(channel.isActive()).thenReturn(true);
    //
    //        doReturn(handler).when(pipeline).get(ChunkFrameDecoder.class);
    //
    //        var protector = new UnauthenticatedConnectionListener(channel, Duration.ZERO, memoryTracker);
    //
    //        InOrder inOrder = inOrder(pipeline, memoryTracker);
    //        protector.onAuthenticated();
    //        inOrder.verify(pipeline).remove(AuthenticationTimeoutHandler.class);
    //        inOrder.verify(memoryTracker).releaseHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
    //        inOrder.verifyNoMoreInteractions();
    //    }
    //
    //    @Test
    //    void shouldIgnoreDisableCallWhenChannelIsInactive() {
    //        var channel = mock(Channel.class);
    //        var pipeline = mock(ChannelPipeline.class);
    //        var memoryTracker = mock(MemoryTracker.class);
    //
    //        when(channel.isActive()).thenReturn(false);
    //        when(channel.pipeline()).thenReturn(pipeline);
    //
    //        var protector = new UnauthenticatedConnectionListener(channel, Duration.ZERO, memoryTracker);
    //        protector.onAuthenticated();
    //
    //        verify(channel).isActive();
    //        verify(memoryTracker).allocateHeap(AuthenticationTimeoutHandler.SHALLOW_SIZE);
    //        verifyNoMoreInteractions(channel, pipeline, memoryTracker);
    //    }
}
