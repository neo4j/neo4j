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
package org.neo4j.bolt.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.configuration.Config;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;

class AbstractBoltProtocolTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void cleanup() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldInstallChannelHandlersInCorrectOrder() throws Throwable {
        // Given
        BoltChannel boltChannel = newTestBoltChannel(channel);
        BoltConnectionFactory connectionFactory = mock(BoltConnectionFactory.class);
        var memoryTracker = mock(MemoryTracker.class);

        when(connectionFactory.newConnection(eq(boltChannel), any(), any())).thenReturn(mock(BoltConnection.class));
        BoltProtocol boltProtocol = new TestAbstractBoltProtocol(
                boltChannel,
                connectionFactory,
                mock(BoltStateMachineFactory.class),
                Config.defaults(),
                NullLogService.getInstance(),
                mock(TransportThrottleGroup.class),
                mock(ChannelProtector.class),
                memoryTracker);

        // When
        boltProtocol.install();

        Iterator<Map.Entry<String, ChannelHandler>> handlers =
                channel.pipeline().iterator();
        assertThat(handlers.next().getValue()).isInstanceOf(ChunkDecoder.class);
        assertThat(handlers.next().getValue()).isInstanceOf(MessageAccumulator.class);
        assertThat(handlers.next().getValue()).isInstanceOf(MessageDecoder.class);
        assertThat(handlers.next().getValue()).isInstanceOf(HouseKeeper.class);

        assertFalse(handlers.hasNext());
    }

    @Test
    void shouldAllocateMemory() {
        var boltChannel = newTestBoltChannel(channel);
        var connectionFactory = mock(BoltConnectionFactory.class);
        var memoryTracker = mock(MemoryTracker.class, RETURNS_MOCKS);

        when(connectionFactory.newConnection(eq(boltChannel), any(), any())).thenReturn(mock(BoltConnection.class));

        var boltProtocol = new TestAbstractBoltProtocol(
                boltChannel,
                connectionFactory,
                mock(BoltStateMachineFactory.class),
                Config.defaults(),
                NullLogService.getInstance(),
                mock(TransportThrottleGroup.class),
                mock(ChannelProtector.class),
                memoryTracker);

        boltProtocol.install();

        verify(memoryTracker)
                .allocateHeap(ChunkDecoder.SHALLOW_SIZE
                        + MessageAccumulator.SHALLOW_SIZE
                        + MessageDecoder.SHALLOW_SIZE
                        + HouseKeeper.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    private static class TestAbstractBoltProtocol extends AbstractBoltProtocol {
        private static final BoltProtocolVersion DUMMY_VERSION = new BoltProtocolVersion(0, 0);

        TestAbstractBoltProtocol(
                BoltChannel channel,
                BoltConnectionFactory connectionFactory,
                BoltStateMachineFactory stateMachineFactory,
                Config config,
                LogService logging,
                TransportThrottleGroup throttleGroup,
                ChannelProtector channelProtector,
                MemoryTracker memoryTracker) {
            super(
                    channel,
                    connectionFactory,
                    stateMachineFactory,
                    config,
                    logging,
                    throttleGroup,
                    channelProtector,
                    memoryTracker);
        }

        @Override
        public Neo4jPack createPack(MemoryTracker memoryTracker) {
            return mock(Neo4jPack.class);
        }

        @Override
        protected BoltRequestMessageReader createMessageReader(
                BoltConnection connection,
                BoltResponseMessageWriter messageWriter,
                BookmarksParser bookmarksParser,
                LogService logging,
                ChannelProtector channelProtector,
                MemoryTracker memoryTracker) {
            return mock(BoltRequestMessageReader.class);
        }

        @Override
        protected BoltResponseMessageWriter createMessageWriter(
                Neo4jPack neo4jPack, LogService logging, MemoryTracker memoryTracker) {
            return mock(BoltResponseMessageWriter.class);
        }

        @Override
        public BoltProtocolVersion version() {
            return DUMMY_VERSION;
        }
    }
}
