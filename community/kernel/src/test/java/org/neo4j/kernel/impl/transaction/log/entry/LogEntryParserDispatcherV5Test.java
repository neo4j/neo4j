/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;

import org.junit.Test;

import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandReaderFactory;
import org.neo4j.kernel.impl.transaction.command.NeoCommandType;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParser;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserDispatcher;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParsersV5;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class LogEntryParserDispatcherV5Test
{
    private final LogEntryParserDispatcher<LogEntryParsersV5> dispatcher =
            new LogEntryParserDispatcher<>( LogEntryParsersV5.values() );
    private final CommandReaderFactory.Default commandReaderFactory = new CommandReaderFactory.Default();
    private final byte version = LogEntryVersions.CURRENT_LOG_ENTRY_VERSION;
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition( 0, 29 );

    @Test
    public void shouldParserStartEntry() throws IOException
    {
        // given
        final LogEntryStart start = new LogEntryStart( version, 1, 2, 3, 4, new byte[]{5}, position );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.putInt( start.getMasterId() );
        channel.putInt( start.getLocalId() );
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );
        channel.putInt( start.getAdditionalHeader().length );
        channel.put( start.getAdditionalHeader(), start.getAdditionalHeader().length );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.TX_START );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( start, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParserOnePhaseCommitEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new OnePhaseCommit( version, 42, 21 );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.TX_1P_COMMIT );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( commit, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParserCommandsUsingAGivenFactory() throws IOException
    {
        // given
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( NeoCommandType.NODE_COMMAND );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.COMMAND );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( command, logEntry );
        assertFalse( parser.skip() );
    }


    @Test
    public void shouldParseEmptyEntry() throws IOException
    {
        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.EMPTY );
        final LogEntry logEntry = parser.parse( version, new InMemoryLogChannel(), marker, commandReaderFactory );

        // then
        assertNull( logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldThrowWhenParsingPrepareEntry() throws IOException
    {
        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.TX_PREPARE );

        // then
        assertNull( parser );
    }

    @Test
    public void shouldThrowWhenParsingTwoPhaseCommitEntry() throws IOException
    {
        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.TX_2P_COMMIT );

        // then
        assertNull( parser );
    }

    @Test
    public void shouldThrowWhenParsingDoneEntry() throws IOException
    {
        // when
        final LogEntryParser parser = dispatcher.dispatch( LogEntryByteCodes.DONE );

        // then
        assertNull( parser );
    }
}
