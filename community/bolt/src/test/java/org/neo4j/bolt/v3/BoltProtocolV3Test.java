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
package org.neo4j.bolt.v3;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.ChunkedOutput;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.bolt.v3.messaging.BoltRequestMessageReaderV3;
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3;
import org.neo4j.configuration.Config;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

class BoltProtocolV3Test
{
    @Test
    void shouldCreatePackForBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                createProtocolV3();

        assertThat( protocolV3.createPack( mock( MemoryTracker.class ) ) ).isInstanceOf( Neo4jPackV2.class );
    }

    @Test
    void shouldAllocateMemoryForPackForBoltV3()
    {
        var protocol =
                createProtocolV3();
        var memoryTracker = mock( MemoryTracker.class );

        protocol.createPack( memoryTracker );

        verify( memoryTracker ).allocateHeap( Neo4jPackV2.SHALLOW_SIZE );
        verifyNoMoreInteractions( memoryTracker );
    }

    @Test
    void shouldVersionReturnBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                createProtocolV3();

        assertThat( protocolV3.version() ).isEqualTo( new BoltProtocolVersion( 3, 0 ) );
    }

    @Test
    void shouldCreateMessageReaderForBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                createProtocolV3();

        assertThat(
                protocolV3.createMessageReader( mock( BoltConnection.class ), mock( BoltResponseMessageWriter.class ),
                                                mock( BookmarksParser.class ), NullLogService.getInstance(), mock( ChannelProtector.class ),
                                                mock( MemoryTracker.class ) ) )
                .isInstanceOf( BoltRequestMessageReaderV3.class );
    }

    @Test
    void shouldAllocateMemoryForMessageReaderForBoltV3()
    {
        var protocol = createProtocolV3();
        var memoryTracker = mock( MemoryTracker.class );

        protocol.createMessageReader( mock( BoltConnection.class ), mock( BoltResponseMessageWriter.class ),
                                      mock( BookmarksParser.class ), NullLogService.getInstance(), mock( ChannelProtector.class ), memoryTracker );

        verify( memoryTracker ).allocateHeap( BoltRequestMessageReaderV3.SHALLOW_SIZE );
        verifyNoMoreInteractions( memoryTracker );
    }

    @Test
    void shouldCreateMessageWriterForBoltV3() throws Throwable
    {
        BoltProtocolV3 protocolV3 =
                createProtocolV3();

        assertThat(
                protocolV3.createMessageWriter( mock( Neo4jPack.class ), NullLogService.getInstance(), mock( MemoryTracker.class ) ) )
                .isInstanceOf( BoltResponseMessageWriterV3.class );
    }

    @Test
    void shouldAllocateMemoryForMessageWriterForBoltV3()
    {
        var protocol = createProtocolV3();
        var memoryTracker = mock( MemoryTracker.class );

        protocol.createMessageWriter( mock( Neo4jPack.class ), NullLogService.getInstance(), memoryTracker );

        verify( memoryTracker ).allocateHeap( ChunkedOutput.SHALLOW_SIZE );
        verify( memoryTracker ).allocateHeap( BoltResponseMessageWriterV3.SHALLOW_SIZE );
        verifyNoMoreInteractions( memoryTracker );
    }

    private static BoltProtocolV3 createProtocolV3()
    {
        return new BoltProtocolV3( newTestBoltChannel(), ( ch, st, mw ) -> mock( BoltConnection.class ),
                                   mock( BoltStateMachineFactory.class ), Config.defaults(),
                                   NullLogService.getInstance(), mock( TransportThrottleGroup.class ), mock( ChannelProtector.class ),
                                   mock( MemoryTracker.class ) );
    }
}
