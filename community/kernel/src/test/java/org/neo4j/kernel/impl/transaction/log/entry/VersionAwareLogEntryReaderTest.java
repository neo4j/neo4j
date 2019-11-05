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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.CommandReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion.LATEST_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

class VersionAwareLogEntryReaderTest
{
    private final LogEntryReader logEntryReader = new VersionAwareLogEntryReader( new TestCommandReaderFactory() );

    @Test
    void shouldReadAStartLogEntry() throws IOException
    {
        // given
        final LogEntryStart start = new LogEntryStart( LATEST_VERSION, 1, 2, BASE_TX_CHECKSUM, new byte[]{4}, new LogPosition( 0, 27 ) );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        writeStartEntry( channel, start );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( start, logEntry );
    }

    @Test
    void shouldReadACommitLogEntry() throws IOException
    {
        // given
        final LogEntryCommit commit = new LogEntryCommit( LATEST_VERSION, 42, 21, 2039335142 );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        writeCommitEntry( channel, commit );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    void shouldReadACommandLogEntry() throws IOException
    {
        // given
        LogEntryVersion version = LATEST_VERSION;
        TestCommand testCommand = new TestCommand( new byte[] {100, 101, 102} );
        final LogEntryCommand command = new LogEntryCommand( version, testCommand );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( version.version() );
        channel.put( LogEntryByteCodes.COMMAND );
        testCommand.serialize( channel );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( command, logEntry );
    }

    @Test
    void shouldReadACheckPointLogEntry() throws IOException
    {
        // given
        final CheckPoint checkPoint = new CheckPoint( LATEST_VERSION, new LogPosition( 42, 43 ) );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( checkPoint.getVersion().version() );
        channel.put( LogEntryByteCodes.CHECK_POINT );
        channel.putLong( checkPoint.getLogPosition().getLogVersion() );
        channel.putLong( checkPoint.getLogPosition().getByteOffset() );
        channel.putChecksum();

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( checkPoint, logEntry );
    }

    @Test
    void shouldReturnNullWhenThereIsNoCommand() throws IOException
    {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( LATEST_VERSION.version() );
        channel.put( LogEntryByteCodes.COMMAND );
        channel.put( CommandReader.NONE );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    @Test
    void shouldReturnNullWhenNotEnoughDataInTheChannel() throws IOException
    {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    @Disabled // TODO it's not clear what the benefit verifying the chain will give us, so it's disable for now
    @Test
    void shouldValidateChecksumChain() throws IOException
    {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();
        LogPosition startPosition = new LogPosition( 0, 174 );

        int checksum1 = 1021763356;
        final LogEntryStart start1 = new LogEntryStart( LATEST_VERSION, 1, 2, BASE_TX_CHECKSUM, new byte[]{4}, startPosition );
        final LogEntryCommit commit1 = new LogEntryCommit( LATEST_VERSION, 42, 21, checksum1 );

        int checksum2 = 2120750830;
        final LogEntryStart start2 = new LogEntryStart( LATEST_VERSION, 35,  30, checksum1, new byte[]{5}, startPosition );
        final LogEntryCommit commit2 = new LogEntryCommit( LATEST_VERSION, 76, 35, checksum2 );

        int checksum3 = -1462443939;
        final LogEntryStart start3 = new LogEntryStart( LATEST_VERSION, 58,  80, checksum2, new byte[]{6}, startPosition );
        final LogEntryCommit commit3 = new LogEntryCommit( LATEST_VERSION, 83, 47, checksum3 );

        int notChecksum3 = checksum3 + 1;
        final LogEntryStart start4 = new LogEntryStart( LATEST_VERSION, 68,  83, notChecksum3, new byte[]{7}, startPosition );

        writeStartEntry( channel, start1 );
        writeCommitEntry( channel, commit1 );
        writeStartEntry( channel, start2 );
        writeCommitEntry( channel, commit2 );
        writeStartEntry( channel, start3 );
        writeCommitEntry( channel, commit3 );
        writeStartEntry( channel, start4 );

        assertEquals( start1, logEntryReader.readLogEntry( channel ) );
        assertEquals( commit1, logEntryReader.readLogEntry( channel ) );
        assertEquals( start2, logEntryReader.readLogEntry( channel ) );
        assertEquals( commit2, logEntryReader.readLogEntry( channel ) );
        assertEquals( start3, logEntryReader.readLogEntry( channel ) );
        assertEquals( commit3, logEntryReader.readLogEntry( channel ) );
        IllegalStateException e = assertThrows( IllegalStateException.class, () -> logEntryReader.readLogEntry( channel ) );
        assertTrue( e.getMessage().contains( "The checksum chain is broken" ) );
    }

    private static void writeStartEntry( InMemoryClosableChannel channel, LogEntryStart start )
    {
        channel.beginChecksum();
        channel.put( start.getVersion().version() ); // version
        channel.put( LogEntryByteCodes.TX_START ); // type
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );
        channel.putInt( start.getPreviousChecksum() );
        channel.putInt( start.getAdditionalHeader().length );
        channel.put( start.getAdditionalHeader(), start.getAdditionalHeader().length );
    }

    private static void writeCommitEntry( InMemoryClosableChannel channel, LogEntryCommit commit )
    {
        channel.put( commit.getVersion().version() );
        channel.put( LogEntryByteCodes.TX_COMMIT );
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );
        channel.putChecksum();
    }
}
