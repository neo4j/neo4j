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
package org.neo4j.bolt.v4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

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
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3;
import org.neo4j.bolt.v4.messaging.BoltRequestMessageReaderV4;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarksParserV4;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;

class BoltProtocolV4Test {
    private final BookmarksParserV4 bookmarksParser =
            new BookmarksParserV4(mock(DatabaseIdRepository.class), CustomBookmarkFormatParser.DEFAULT);

    @Test
    void shouldCreatePackForBoltV4() {
        BoltProtocolV4 protocolV4 = createProtocolV4();

        assertThat(protocolV4.createPack(mock(MemoryTracker.class))).isInstanceOf(Neo4jPackV2.class);
    }

    @Test
    void shouldAllocateMemoryForPackForBoltV4() {
        var protocol = createProtocolV4();
        var memoryTracker = mock(MemoryTracker.class);

        protocol.createPack(memoryTracker);

        verify(memoryTracker).allocateHeap(Neo4jPackV2.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    @Test
    void shouldVersionReturnBoltV4() {
        BoltProtocolV4 protocolV4 = createProtocolV4();

        assertThat(protocolV4.version()).isEqualTo(new BoltProtocolVersion(4, 0));
    }

    @Test
    void shouldCreateMessageReaderForBoltV4() {
        BoltProtocolV4 protocolV4 = createProtocolV4();

        assertThat(protocolV4.createMessageReader(
                        mock(BoltConnection.class),
                        mock(BoltResponseMessageWriter.class),
                        bookmarksParser,
                        NullLogService.getInstance(),
                        mock(ChannelProtector.class),
                        mock(MemoryTracker.class)))
                .isInstanceOf(BoltRequestMessageReaderV4.class);
    }

    @Test
    void shouldAllocateMemoryForMessageReaderForBoltV4() {
        var protocol = createProtocolV4();
        var memoryTracker = mock(MemoryTracker.class);

        protocol.createMessageReader(
                mock(BoltConnection.class),
                mock(BoltResponseMessageWriter.class),
                bookmarksParser,
                NullLogService.getInstance(),
                mock(ChannelProtector.class),
                memoryTracker);

        verify(memoryTracker).allocateHeap(BoltRequestMessageReaderV4.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    @Test
    void shouldCreateMessageWriterForBoltV4() {
        BoltProtocolV4 protocolV4 = createProtocolV4();

        assertThat(protocolV4.createMessageWriter(
                        mock(Neo4jPack.class), NullLogService.getInstance(), mock(MemoryTracker.class)))
                .isInstanceOf(BoltResponseMessageWriterV3.class);
    }

    @Test
    void shouldAllocateMemoryForMessageWriterForBoltV4() {
        BoltProtocolV4 protocol = createProtocolV4();
        var memoryTracker = mock(MemoryTracker.class);

        protocol.createMessageWriter(mock(Neo4jPack.class), NullLogService.getInstance(), memoryTracker);

        verify(memoryTracker).allocateHeap(ChunkedOutput.SHALLOW_SIZE);
        verify(memoryTracker).allocateHeap(BoltResponseMessageWriterV3.SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryTracker);
    }

    private BoltProtocolV4 createProtocolV4() {
        return new BoltProtocolV4(
                newTestBoltChannel(),
                (ch, st, messageWriter) -> mock(BoltConnection.class),
                mock(BoltStateMachineFactory.class),
                Config.defaults(),
                bookmarksParser,
                NullLogService.getInstance(),
                mock(TransportThrottleGroup.class),
                mock(ChannelProtector.class),
                mock(MemoryTracker.class));
    }
}
