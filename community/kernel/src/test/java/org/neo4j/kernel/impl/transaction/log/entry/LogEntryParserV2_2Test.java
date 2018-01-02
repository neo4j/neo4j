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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class LogEntryParserV2_2Test
{
    private final LogEntryVersion version = LogEntryVersion.V2_2;
    private final CommandReader commandReader = version.newCommandReader();
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition( 0, 29 );

    @Test
    public void shouldParseStartEntry() throws IOException
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
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.TX_START );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( start, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParseOnePhaseCommitEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new OnePhaseCommit( version, 42, 21 );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.TX_1P_COMMIT );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( commit, logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParseCommandsUsingAGivenFactory() throws IOException
    {
        // given
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( new NodeRecord( 0 ), new NodeRecord( 0 ) );
        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        new CommandWriter( channel ).visitNodeCommand( nodeCommand );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.COMMAND );
        final LogEntry logEntry = parser.parse( version, channel, marker, commandReader );

        // then
        assertEquals( command, logEntry );
        assertFalse( parser.skip() );
    }


    @Test
    public void shouldParseEmptyEntry() throws IOException
    {
        // when
        final LogEntryParser parser = version.entryParser( LogEntryByteCodes.EMPTY );
        final LogEntry logEntry = parser.parse( version, new InMemoryLogChannel(), marker, commandReader );

        // then
        assertNull( logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldThrowWhenParsingUnknownEntry()
    {
        // when
        try
        {
            version.entryParser( LogEntryByteCodes.TX_PREPARE );
            fail( "Should have thrown" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }
}
