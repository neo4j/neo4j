/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import static org.neo4j.kernel.impl.transaction.command.NeoCommandType.NODE_COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.CURRENT;

public class LogEntryParserDispatcherV6Test
{
    private final LogEntryVersion version = CURRENT;
    private final CommandReaderFactory commandReader = new RecordStorageCommandReaderFactory();
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition( 0, 29 );

    @Test
    public void shouldParserStartEntry() throws IOException
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
        final LogEntryParser parser = version.entryParser( TX_START );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( start, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParserOnePhaseCommitEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new LogEntryCommit( version, 42, 21 );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( TX_COMMIT );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( commit, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParserCommandsUsingAGivenFactory() throws IOException
    {
        // given
        // The record, it will be used as before and after
        NodeRecord theRecord = new NodeRecord( 1 );
        NodeCommand nodeCommand = new NodeCommand( theRecord, theRecord );

        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( NODE_COMMAND );
        channel.putLong( theRecord.getId() );

        // record image before
        channel.put( (byte) 0 ); // not in use
        channel.putInt( 0 ); // number of dynamic records in use
        // record image after
        channel.put( (byte) 0 ); // not in use
        channel.putInt( 0 ); // number of dynamic records in use

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( COMMAND );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( command, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParseCheckPointEntry() throws IOException
    {
        // given
        final CheckPoint checkPoint = new CheckPoint( new LogPosition( 43, 44 ) );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.putLong( checkPoint.getLogPosition().getLogVersion() );
        channel.putLong( checkPoint.getLogPosition().getByteOffset() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( CHECK_POINT );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( checkPoint, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldThrowWhenParsingUnknownEntry()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // when
            version.entryParser( (byte) 42 ); // unused, at lest for now

            // then
            // it should throw exception

        } );
    }
}
