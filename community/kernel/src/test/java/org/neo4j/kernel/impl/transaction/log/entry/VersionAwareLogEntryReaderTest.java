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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.NeoCommandType;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class VersionAwareLogEntryReaderTest
{
    private final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();

    @Test
    public void shouldReadAStartLogEntry() throws IOException
    {
        // given
        LogEntryVersion version = LogEntryVersion.CURRENT;
        final LogEntryStart start = new LogEntryStart( version, 1, 2, 3, 4, new byte[]{5}, new LogPosition( 0, 31 ) );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( version.byteCode() ); // version
        channel.put( LogEntryByteCodes.TX_START ); // type
        channel.putInt( start.getMasterId() );
        channel.putInt( start.getLocalId() );
        channel.putLong( start.getTimeWritten() );
        channel.putLong( start.getLastCommittedTxWhenTransactionStarted() );
        channel.putInt( start.getAdditionalHeader().length );
        channel.put( start.getAdditionalHeader(), start.getAdditionalHeader().length );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( start, logEntry );
    }

    @Test
    public void shouldReadACommitLogEntry() throws IOException
    {
        // given
        LogEntryVersion version = LogEntryVersion.CURRENT;
        final LogEntryCommit commit = new LogEntryCommit( version, 42, 21 );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( version.byteCode() );
        channel.put( LogEntryByteCodes.TX_COMMIT );
        channel.putLong( commit.getTxId() );
        channel.putLong( commit.getTimeWritten() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( commit, logEntry );
    }

    @Test
    public void shouldReadACommandLogEntry() throws IOException
    {
        // given
        LogEntryVersion version = LogEntryVersion.CURRENT;
        Command.NodeCommand nodeCommand = new Command.NodeCommand( new NodeRecord( 11 ), new NodeRecord( 11 ) );
        final LogEntryCommand command = new LogEntryCommand( version, nodeCommand );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( version.byteCode() );
        channel.put( LogEntryByteCodes.COMMAND );
        nodeCommand.serialize( channel );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( command, logEntry );
    }

    @Test
    public void shouldReadACheckPointLogEntry() throws IOException
    {
        // given
        LogEntryVersion version = LogEntryVersion.CURRENT;
        final LogPosition logPosition = new LogPosition( 42, 43 );
        final CheckPoint checkPoint = new CheckPoint( version, logPosition );
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( version.byteCode() );
        channel.put( LogEntryByteCodes.CHECK_POINT );
        channel.putLong( logPosition.getLogVersion() );
        channel.putLong( logPosition.getByteOffset() );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( checkPoint, logEntry );
    }

    @Test
    public void shouldReturnNullWhenThereIsNoCommand() throws IOException
    {
        // given
        LogEntryVersion version = LogEntryVersion.CURRENT;
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        channel.put( version.byteCode() );
        channel.put( LogEntryByteCodes.COMMAND );
        channel.put( NeoCommandType.NONE );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldReturnNullWhenNotEnoughDataInTheChannel() throws IOException
    {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldBeAbleToSkipBadVersionAndTypeBytesInBetweenLogEntries() throws Exception
    {
        // GIVEN
        AcceptingInvalidLogEntryHandler invalidLogEntryHandler = new AcceptingInvalidLogEntryHandler();
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory(), invalidLogEntryHandler );
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 1_000 );
        LogEntryWriter writer = new LogEntryWriter( channel.writer() );
        long startTime = currentTimeMillis();
        long commitTime = startTime + 10;
        writer.writeStartEntry( 1, 2, startTime, 3, new byte[0] );
        writer.writeCommitEntry( 4, commitTime );
        channel.put( (byte) 127 );
        channel.put( (byte) 126 );
        channel.put( (byte) 125 );
        long secondStartTime = startTime + 100;
        writer.writeStartEntry( 1, 2, secondStartTime, 4, new byte[0] );

        // WHEN
        LogEntryStart readStartEntry = reader.readLogEntry( channel.reader() ).as();
        LogEntryCommit readCommitEntry = reader.readLogEntry( channel.reader() ).as();
        LogEntryStart readSecondStartEntry = reader.readLogEntry( channel.reader() ).as();

        // THEN
        assertEquals( 1, readStartEntry.getMasterId() );
        assertEquals( 2, readStartEntry.getLocalId() );
        assertEquals( startTime, readStartEntry.getTimeWritten() );

        assertEquals( 4, readCommitEntry.getTxId() );
        assertEquals( commitTime, readCommitEntry.getTimeWritten() );

        assertEquals( 3, invalidLogEntryHandler.bytesSkipped );
        assertEquals( 3, invalidLogEntryHandler.invalidEntryCalls );

        assertEquals( 1, readSecondStartEntry.getMasterId() );
        assertEquals( 2, readSecondStartEntry.getLocalId() );
        assertEquals( secondStartTime, readSecondStartEntry.getTimeWritten() );
    }

    @Test
    public void shouldBeAbleToSkipBadLogEntries() throws Exception
    {
        // GIVEN
        AcceptingInvalidLogEntryHandler invalidLogEntryHandler = new AcceptingInvalidLogEntryHandler();
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>(
                new RecordStorageCommandReaderFactory(), invalidLogEntryHandler );
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 1_000 );
        LogEntryWriter writer = new LogEntryWriter( channel.writer() );
        long startTime = currentTimeMillis();
        long commitTime = startTime + 10;
        writer.writeStartEntry( 1, 2, startTime, 3, new byte[0] );

        // Write command ...
        int posBefore = channel.writerPosition();
        writer.serialize( singletonList( new Command.NodeCommand( new NodeRecord( 1 ),
                new NodeRecord( 1 ).initialize( true, 1, false, 2, 0 ) ) ) );
        int posAfter = channel.writerPosition();
        // ... which then gets overwritten with invalid data
        channel.positionWriter( posBefore );
        while ( channel.writerPosition() < posAfter )
        {
            channel.put( (byte) 0xFF );
        }

        writer.writeCommitEntry( 4, commitTime );
        long secondStartTime = startTime + 100;
        writer.writeStartEntry( 1, 2, secondStartTime, 4, new byte[0] );

        // WHEN
        LogEntryStart readStartEntry = reader.readLogEntry( channel.reader() ).as();
        LogEntryCommit readCommitEntry = reader.readLogEntry( channel.reader() ).as();
        LogEntryStart readSecondStartEntry = reader.readLogEntry( channel.reader() ).as();

        // THEN
        assertEquals( 1, readStartEntry.getMasterId() );
        assertEquals( 2, readStartEntry.getLocalId() );
        assertEquals( startTime, readStartEntry.getTimeWritten() );

        assertEquals( 4, readCommitEntry.getTxId() );
        assertEquals( commitTime, readCommitEntry.getTimeWritten() );

        assertEquals( posAfter - posBefore, invalidLogEntryHandler.bytesSkipped );
        assertEquals( posAfter - posBefore, invalidLogEntryHandler.invalidEntryCalls );

        assertEquals( 1, readSecondStartEntry.getMasterId() );
        assertEquals( 2, readSecondStartEntry.getLocalId() );
        assertEquals( secondStartTime, readSecondStartEntry.getTimeWritten() );
    }

    static class AcceptingInvalidLogEntryHandler extends InvalidLogEntryHandler
    {
        long bytesSkipped;
        Exception e;
        LogPosition position;
        int invalidEntryCalls;

        @Override
        public boolean handleInvalidEntry( Exception e, LogPosition position )
        {
            this.e = e;
            this.position = position;
            invalidEntryCalls++;
            return true;
        }

        @Override
        public void bytesSkipped( long bytesSkipped )
        {
            this.bytesSkipped += bytesSkipped;
        }
    }
}
