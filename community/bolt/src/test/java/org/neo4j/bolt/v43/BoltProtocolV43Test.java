/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v43;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarksParserV4;
import org.neo4j.bolt.v41.messaging.BoltResponseMessageWriterV41;
import org.neo4j.bolt.v42.BoltProtocolV42;
import org.neo4j.bolt.v43.messaging.BoltRequestMessageReaderV43;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.Clocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

class BoltProtocolV43Test
{
    private final BookmarksParserV4 bookmarksParser = new BookmarksParserV4( new TestDatabaseIdRepository(), CustomBookmarkFormatParser.DEFAULT );

    @Test
    void shouldCreatePackForBoltV43()
    {
        BoltProtocolV43 protocolV43 = createProtocolV43();

        assertThat( protocolV43.createPack() ).isInstanceOf( Neo4jPackV2.class );
    }

    @Test
    void shouldVersionReturnBoltV43()
    {
        BoltProtocolV43 protocolV43 = createProtocolV43();
        assertThat( protocolV43.version() ).isEqualTo( new BoltProtocolVersion( 4, 3 ) );
    }

    @Test
    void shouldCreateMessageReaderForBoltV42()
    {
        BoltProtocolV43 protocolV43 = createProtocolV43();

        assertThat( protocolV43.createMessageReader( mock( BoltConnection.class ),
                mock( BoltResponseMessageWriter.class ),
                bookmarksParser, NullLogService.getInstance() ) ).isInstanceOf( BoltRequestMessageReaderV43.class );
    }

    @Test
    void shouldCreateMessageWriterForBoltV43()
    {
        BoltProtocolV43 protocolV43 = createProtocolV43();

        assertThat( protocolV43.createMessageWriter( mock( Neo4jPack.class ), NullLogService.getInstance() ) )
                .isInstanceOf( BoltResponseMessageWriterV41.class );
    }

    private BoltProtocolV43 createProtocolV43()
    {
        return new BoltProtocolV43( newTestBoltChannel(), ( ch, st, mr ) -> mock( BoltConnection.class ),
                                    mock( BoltStateMachineFactory.class ), bookmarksParser, NullLogService.getInstance(),
                                    mock( TransportThrottleGroup.class ), Clocks.fakeClock(), Duration.ZERO );
    }

}
