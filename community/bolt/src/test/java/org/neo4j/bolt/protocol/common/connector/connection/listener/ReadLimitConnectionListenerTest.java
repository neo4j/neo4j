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
package org.neo4j.bolt.protocol.common.connector.connection.listener;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.packstream.codec.transport.ChunkFrameDecoder;

class ReadLimitConnectionListenerTest {

    private static final String CONNECTION_ID = "bolt-readlimit";

    private Connection connection;
    private MemoryTracker memoryTracker;
    private DefaultScopedMemoryTracker scopedMemoryTracker;
    private Channel channel;
    private ChannelPipeline pipeline;
    private AssertableLogProvider logProvider;
    private ChunkFrameDecoder chunkFrameDecoder;

    private ReadLimitConnectionListener listener;

    @BeforeEach
    void prepareListener() {
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_MOCKS);
        this.memoryTracker = Mockito.mock(MemoryTracker.class);
        this.scopedMemoryTracker = Mockito.mock(DefaultScopedMemoryTracker.class);
        this.channel = Mockito.mock(Channel.class);
        this.pipeline = Mockito.mock(ChannelPipeline.class, Mockito.RETURNS_SELF);
        this.logProvider = new AssertableLogProvider();
        this.chunkFrameDecoder = new ChunkFrameDecoder(1000L, this.logProvider);

        Mockito.doReturn(CONNECTION_ID).when(this.connection).id();
        Mockito.doReturn(this.memoryTracker).when(this.connection).memoryTracker();
        Mockito.doReturn(this.channel).when(this.connection).channel();
        Mockito.doReturn(this.pipeline).when(this.channel).pipeline();

        Mockito.doReturn(this.scopedMemoryTracker).when(this.memoryTracker).getScopedMemoryTracker();

        Mockito.doReturn(this.chunkFrameDecoder).when(this.pipeline).get(ChunkFrameDecoder.class);

        this.listener = new ReadLimitConnectionListener(connection, this.logProvider, 1000L);
    }

    @Test
    void shouldReplaceChunkFrameDecoderOnAuthenticated() {
        var loginContext = Mockito.mock(LoginContext.class);

        this.listener.onLogon(loginContext);

        var inOrder = Mockito.inOrder(this.connection, this.memoryTracker, this.scopedMemoryTracker, this.pipeline);

        inOrder.verify(this.connection).memoryTracker();
        inOrder.verify(this.memoryTracker).getScopedMemoryTracker();
        inOrder.verify(this.scopedMemoryTracker).allocateHeap(ChunkFrameDecoder.SHALLOW_SIZE);
        inOrder.verify(this.connection).channel();
        inOrder.verify(this.pipeline).get(ChunkFrameDecoder.class);
        inOrder.verify(this.pipeline)
                .replace(
                        ArgumentMatchers.any(ChunkFrameDecoder.class),
                        ArgumentMatchers.eq("chunkFrameDecoder"),
                        ArgumentMatchers.any(ChunkFrameDecoder.class));

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ReadLimitConnectionListener.class)
                .containsMessageWithArgumentsContaining("Removing read limit", CONNECTION_ID);
    }

    @Test
    void shouldReAddLimitedChunkFrameEncoderOnLogoff() {
        this.listener.onLogoff();

        var inOrder = Mockito.inOrder(this.connection, this.memoryTracker, this.scopedMemoryTracker, this.pipeline);

        inOrder.verify(this.connection).memoryTracker();
        inOrder.verify(this.memoryTracker).getScopedMemoryTracker();
        inOrder.verify(this.scopedMemoryTracker).allocateHeap(ChunkFrameDecoder.SHALLOW_SIZE);
        inOrder.verify(this.connection).channel();
        inOrder.verify(this.pipeline).get(ChunkFrameDecoder.class);

        // No easy why to check that the new Decoder has a limit on it.
        // so we will check that we replace it same as above, then check the log.
        inOrder.verify(this.pipeline)
                .replace(
                        ArgumentMatchers.any(ChunkFrameDecoder.class),
                        ArgumentMatchers.eq("chunkFrameDecoder"),
                        ArgumentMatchers.any(ChunkFrameDecoder.class));

        LogAssertions.assertThat(this.logProvider)
                .forLevel(AssertableLogProvider.Level.DEBUG)
                .forClass(ReadLimitConnectionListener.class)
                .containsMessageWithArgumentsContaining("Re-adding read limit of", CONNECTION_ID);
    }

    @Test
    void shouldReleaseMemoryOnRemoval() {
        this.listener.onListenerRemoved();

        Mockito.verify(this.memoryTracker).releaseHeap(ReadLimitConnectionListener.SHALLOW_SIZE);
    }
}
