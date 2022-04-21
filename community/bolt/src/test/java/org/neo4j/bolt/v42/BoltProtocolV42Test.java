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
package org.neo4j.bolt.v42;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.ChunkedOutput;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarksParserV4;
import org.neo4j.bolt.v41.messaging.BoltRequestMessageReaderV41;
import org.neo4j.bolt.v41.messaging.BoltResponseMessageWriterV41;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.Clocks;

class BoltProtocolV42Test {
    private final BookmarksParserV4 bookmarksParser =
            new BookmarksParserV4(mock(DatabaseIdRepository.class), CustomBookmarkFormatParser.DEFAULT);

    @Test
    void shouldCreatePackForBoltV42() {
        BoltProtocolV42 protocolV42 = createProtocolV42();

        assertThat(protocolV42.createPack(mock(MemoryTracker.class))).isInstanceOf(Neo4jPackV2.class);
    }

    @Test
    void shouldAllocateMemoryForPackForBoltV42() {
        var protocol = createProtocolV42();
        var memoryTracker = mock(MemoryTracker.class);

        assertThat(protocol.createPack(memoryTracker)).isInstanceOf(Neo4jPackV2.class);

        verify(memoryTracker).allocateHeap(Neo4jPackV2.SHALLOW_SIZE);
    }

    @Test
    void shouldVersionReturnBoltV42() {
        BoltProtocolV42 protocolV42 = createProtocolV42();
        assertThat(protocolV42.version()).isEqualTo(new BoltProtocolVersion(4, 2));
    }

    @Test
    void shouldCreateMessageReaderForBoltV42() {
        BoltProtocolV42 protocolV42 = createProtocolV42();

        assertThat(protocolV42.createMessageReader(
                        mock(BoltConnection.class),
                        mock(BoltResponseMessageWriter.class),
                        bookmarksParser,
                        NullLogService.getInstance(),
                        mock(ChannelProtector.class),
                        mock(MemoryTracker.class)))
                .isInstanceOf(BoltRequestMessageReaderV41.class);
    }

    @Test
    void shouldAllocateMemoryForMessageReaderForBoltV42() {
        var protocol = createProtocolV42();
        var memoryTracker = mock(MemoryTracker.class);

        protocol.createMessageReader(
                mock(BoltConnection.class),
                mock(BoltResponseMessageWriter.class),
                bookmarksParser,
                NullLogService.getInstance(),
                mock(ChannelProtector.class),
                memoryTracker);

        verify(memoryTracker).allocateHeap(BoltRequestMessageReaderV41.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    @Test
    void shouldCreateMessageWriterForBoltV42() {
        BoltProtocolV42 protocolV42 = createProtocolV42();

        assertThat(protocolV42.createMessageWriter(
                        mock(Neo4jPack.class), NullLogService.getInstance(), mock(MemoryTracker.class)))
                .isInstanceOf(BoltResponseMessageWriterV41.class);
    }

    @Test
    void shouldAllocateMemoryForMessageWriterForBoltV42() {
        var protocol = createProtocolV42();
        var memoryTracker = mock(MemoryTracker.class);

        protocol.createMessageWriter(mock(Neo4jPack.class), NullLogService.getInstance(), memoryTracker);

        verify(memoryTracker).allocateHeap(ChunkedOutput.SHALLOW_SIZE);
        verify(memoryTracker).allocateHeap(BoltResponseMessageWriterV41.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    private BoltProtocolV42 createProtocolV42() {
        return new BoltProtocolV42(
                newTestBoltChannel(),
                (ch, st, mr) -> mock(BoltConnection.class),
                mock(BoltStateMachineFactory.class),
                Config.defaults(),
                bookmarksParser,
                NullLogService.getInstance(),
                mock(TransportThrottleGroup.class),
                Clocks.fakeClock(),
                Duration.ZERO,
                mock(ChannelProtector.class),
                mock(MemoryTracker.class));
    }
}
