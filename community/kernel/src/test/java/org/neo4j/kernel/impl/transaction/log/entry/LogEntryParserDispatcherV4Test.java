/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParser;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserDispatcher;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParsersV4;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersions;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LogEntryParserDispatcherV4Test
{
    private final LogEntryParserDispatcher<LogEntryParsersV4> dispatcher =
            new LogEntryParserDispatcher<>( LogEntryParsersV4.values() );
    private final CommandReaderFactory.Default commandReaderFactory = new CommandReaderFactory.Default();
    private final byte version = LogEntryVersions.CURRENT_LOG_ENTRY_VERSION;
    private final LogPositionMarker marker = new LogPositionMarker();
    private final LogPosition position = new LogPosition( 0, 37 );

    @Test
    public void shouldParserStartEntry() throws IOException
    {
        // given
        final LogEntryStart start = new LogEntryStart( version, 1, 2, 3, 4, new byte[]{}, position );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // ignored part
        channel.put( (byte) 1 ); // globalId length
        channel.put( (byte) 2 ); // branchId length
        channel.put( new byte[]{42}, 1 ); // globalId
        channel.put( new byte[]{21, 12}, 2 ); // branchId
        channel.putInt( 123 ); // identifier
        channel.putInt( 456 ); // formatId
        // actual read data
        channel.putInt( start.getMasterId() );
        channel.putInt( start.getLocalId() );
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.TX_START );
        final IdentifiableLogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( start, logEntry.getEntry() );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParserOnePhaseCommitEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new OnePhaseCommit( version, 42, 21 );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // ignored data
        channel.putInt( 123 ); // identifier
        // actual read data
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.TX_1P_COMMIT );
        final IdentifiableLogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( commit, logEntry.getEntry() );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParseTwoPhaseCommitEntryAndMapThemIntoOnePhaseCommit() throws IOException
    {
        // given
        final LogEntryCommit commit = new OnePhaseCommit( version, 42, 21 );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // ignored data
        channel.putInt( 123 ); // identifier
        // actual read data
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.TX_2P_COMMIT );
        final IdentifiableLogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( commit, logEntry.getEntry() );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParserCommandsUsingAGivenFactory() throws IOException
    {
        // given
        final Command.NodeCommand nodeCommand = new Command.NodeCommand();
        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // ignored data
        channel.putInt( 123 ); // identifier
        // actual read data
        channel.put( NeoCommandType.NODE_COMMAND );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.COMMAND );
        final IdentifiableLogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertEquals( command, logEntry.getEntry() );
        assertFalse( parser.skip() );
    }


    @Test
    public void shouldParseEmptyEntry() throws IOException
    {
        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.EMPTY );
        final IdentifiableLogEntry logEntry = parser.parse( version, new InMemoryLogChannel(), marker, commandReaderFactory );

        // then
        assertNull( logEntry );
        assertFalse( parser.skip() );
    }

    @Test
    public void shouldParsePrepareEntry() throws IOException
    {
        // given
        final byte nextByte = (byte) 7;
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        // ignored
        channel.putInt( 123 ); // identifier
        channel.putLong( 456 ); // timeWritten
        // data available after
        channel.put( nextByte );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.TX_PREPARE );
        final IdentifiableLogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertNull( logEntry );
        assertTrue( parser.skip() );
        assertEquals( nextByte, channel.get() );
    }

    @Test
    public void shouldParseDoneEntry() throws IOException
    {
        // given
        final byte nextByte = (byte) 7;
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        // ignored
        channel.putInt( 123 ); // identifier
        // data available after
        channel.put( nextByte );

        channel.getCurrentPosition( marker );

        // when
        final LogEntryParser<IdentifiableLogEntry> parser = dispatcher.dispatch( LogEntryByteCodes.DONE );
        final IdentifiableLogEntry logEntry = parser.parse( version, channel, marker, commandReaderFactory );

        // then
        assertNull( logEntry );
        assertTrue( parser.skip() );
        assertEquals( nextByte, channel.get() );
    }
}
