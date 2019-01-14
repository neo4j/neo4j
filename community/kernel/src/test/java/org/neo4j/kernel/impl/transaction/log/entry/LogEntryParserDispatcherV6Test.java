/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LogEntryParserDispatcherV6Test
{
    private final LogEntryVersion version = LogEntryVersion.CURRENT;
    private final CommandReaderFactory commandReader = new TestCommandReaderFactory();
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition( 0, 29 );

    @Test
    void shouldParserStartEntry() throws IOException
    {
        // given
        final LogEntryStart start = new LogEntryStart( version, 1, 2, 3, 4, new byte[]{5}, position );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putInt( start.getMasterId() );
        channel.putInt( start.getLocalId() );
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );
        channel.putInt( start.getAdditionalHeader().length );
        channel.put( start.getAdditionalHeader(), start.getAdditionalHeader().length );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.TX_START );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( start, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    void shouldParserOnePhaseCommitEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new LogEntryCommit( version, 42, 21 );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.TX_COMMIT );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( commit, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    void shouldParserCommandsUsingAGivenFactory() throws IOException
    {
        // given
        // The record, it will be used as before and after
        TestCommand testCommand = new TestCommand( new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9} );
        final LogEntryCommand command = new LogEntryCommand( version, testCommand );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();
        testCommand.serialize( channel );
        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.COMMAND );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( command, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    void shouldParseCheckPointEntry() throws IOException
    {
        // given
        final CheckPoint checkPoint = new CheckPoint( new LogPosition( 43, 44 ) );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong( checkPoint.getLogPosition().getLogVersion() );
        channel.putLong( checkPoint.getLogPosition().getByteOffset() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.CHECK_POINT );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( checkPoint, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    void shouldThrowWhenParsingUnknownEntry()
    {
        assertThrows( IllegalArgumentException.class, () -> version.entryParser( (byte) 42 ) ); // unused, at lest for now
    }
}
