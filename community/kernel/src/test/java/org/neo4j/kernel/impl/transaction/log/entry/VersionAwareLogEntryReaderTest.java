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
import org.neo4j.kernel.impl.transaction.log.entry.DefaultLogEntryParserFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryParserFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_2_1;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_VERSION_2_2;

public class VersionAwareLogEntryReaderTest
{
    private final LogEntryParserFactory logEntryParserFactory = new DefaultLogEntryParserFactory();
    private final CommandReaderFactory commandReaderFactory = new CommandReaderFactory.Default();
    private final VersionAwareLogEntryReader logEntryReader =
            new VersionAwareLogEntryReader( logEntryParserFactory, commandReaderFactory );

    private final byte version = (byte) -1;

    @Test
    public void shouldReadAStartLogEntry() throws IOException
    {
        // given
        final LogEntryStart start = new LogEntryStart( version, 1, 2, 3, 4, new byte[]{5}, new LogPosition( 0, 31 ) );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version ); // version
        channel.put( LogEntryByteCodes.TX_START ); // type
        channel.putInt( start.getMasterId() );
        channel.putInt( start.getLocalId() );
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );
        channel.putInt( start.getAdditionalHeader().length );
        channel.put( start.getAdditionalHeader(), start.getAdditionalHeader().length );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_2 );

        // then
        assertEquals( start, logEntry );
    }

    @Test
    public void shouldReadACommitLogEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new OnePhaseCommit( version, 42, 21 );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntryByteCodes.TX_1P_COMMIT );
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_2 );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    public void shouldReadACommandLogEntry() throws IOException
    {
        // given
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntryByteCodes.COMMAND );
        channel.put( NeoCommandType.NODE_COMMAND );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_2 );

        // then
        assertEquals( command, logEntry );
    }

    @Test
    public void shouldReturnNullWhenThereIsNoCommand() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntryByteCodes.COMMAND );
        channel.put( NeoCommandType.NONE );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_2 );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldReturnNullWhenLogEntryIsEmpty() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        channel.put( version );
        channel.put( LogEntryByteCodes.EMPTY );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_2 );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldReturnNullWhenNotEnoughDataInTheChannel() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_2 );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldParseOldStartEntry() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        final LogEntryStart start = new LogEntryStart( 1, 2, 3, 4, new byte[]{}, new LogPosition( 0, 37 ) );

        channel.put( version );
        channel.put( LogEntryByteCodes.TX_START );
        // ignored data
        channel.put( (byte) 1 ); // globalId length
        channel.put( (byte) 0 ); // branchId length
        channel.put( new byte[]{7}, 1 ); // globalId
        channel.put( new byte[]{}, 0 ); // branchId
        channel.putInt( 123 ); // identifier
        channel.putInt( 456 ); // formatId
        // actually used data
        channel.putInt( start.getMasterId() );
        channel.putInt( start.getLocalId() );
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertEquals( start, logEntry );
    }

    @Test
    public void shouldParseOldOnePhaseCommit() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        final OnePhaseCommit commit = new OnePhaseCommit( 42, 456 );

        channel.put( version );
        channel.put( LogEntryByteCodes.TX_1P_COMMIT );
        // ignored data
        channel.putInt( 123 ); // identifier
        // actually used data
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    public void shouldParseOldTwoPhaseCommit() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        final OnePhaseCommit commit = new OnePhaseCommit( 42, 456 );

        channel.put( version );
        channel.put( LogEntryByteCodes.TX_2P_COMMIT );
        // ignored data
        channel.putInt( 123 ); // identifier
        // actually used data
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    public void shouldParseOldPrepareSkipItAndReadTheOneAfter() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        final OnePhaseCommit commit = new OnePhaseCommit( 42, 456 );

        // PREPARE
        channel.put( version );
        channel.put( LogEntryByteCodes.TX_PREPARE );
        // ignored data
        channel.putInt( 123 ); // identifier
        channel.putLong( 456 ); // timewritten
        // 2P COMMIT
        channel.put( version );
        channel.put( LogEntryByteCodes.TX_2P_COMMIT );
        // ignored data
        channel.putInt( 123 ); // identifier
        // actually used data
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    public void shouldParseOldDoneSkipItAndReadTheOneAfter() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        final OnePhaseCommit commit = new OnePhaseCommit( 42, 456 );

        // PREPARE
        channel.put( version );
        channel.put( LogEntryByteCodes.DONE );
        // ignored data
        channel.putInt( 123 ); // identifier
        // 2P COMMIT
        channel.put( version );
        channel.put( LogEntryByteCodes.TX_2P_COMMIT );
        // ignored data
        channel.putInt( 123 ); // identifier
        // actually used data
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    public void shouldParseAnOldCommandLogEntry() throws IOException
    {
        // given
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntryByteCodes.COMMAND );
        // ignored data
        channel.putInt( 42 ); // identifier ignored
        // actual used data
        channel.put( NeoCommandType.NODE_COMMAND );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertEquals( command, logEntry );
    }

    @Test
    public void shouldReturnNullWhenThereIsNoCommandOldVersion() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntryByteCodes.COMMAND );
        channel.put( NeoCommandType.NONE );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldParseOldLogEntryEmptyANdReturnNull() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        channel.put( version );
        channel.put( LogEntryByteCodes.EMPTY );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldReturnNullWhenNotEnoughDataInTheChannelOnOldFormatToo() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel, LOG_VERSION_2_1 );

        // then
        assertNull( logEntry );
    }
}
