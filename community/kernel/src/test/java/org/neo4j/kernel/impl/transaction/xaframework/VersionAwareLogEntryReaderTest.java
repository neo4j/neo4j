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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.decodeLogVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader.writeLogHeader;

public class VersionAwareLogEntryReaderTest
{
    private final CommandReaderFactory commandReaderFactory = CommandReaderFactory.DEFAULT;
    private final byte version = (byte) -1;

    @Test
    public void shouldReadAStartLogEntry() throws IOException
    {
        // given
        final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );

        final LogEntry.Start start = new LogEntry.Start( version, 1, 2, 3, 4, new byte[]{5}, new LogPosition( 0, 31 ) );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version ); // version
        channel.put( LogEntry.TX_START ); // type
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
        final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );

        final LogEntry.Commit commit = new LogEntry.OnePhaseCommit( version, 42, 21 );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntry.TX_1P_COMMIT );
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
        final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );

        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        final LogEntry.Command command = new LogEntry.Command( version, nodeCommand );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntry.COMMAND );
        channel.put( NeoCommandType.NODE_COMMAND );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertEquals( command, logEntry );
    }

    @Test
    public void shouldReturnNullWhenThereIsNoCommand() throws IOException
    {
        // given
        final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );

        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.put( version );
        channel.put( LogEntry.COMMAND );
        channel.put( NeoCommandType.NONE );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldReturnNullWhenLogEntryIsEmpty() throws IOException
    {
        // given
        final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
        final InMemoryLogChannel channel = new InMemoryLogChannel();
        channel.put( version );
        channel.put( LogEntry.EMPTY );

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    @Test
    public void shouldReturnNullWhenNotEnoughDataInTheChannel() throws IOException
    {
        // given
        final VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // when
        final LogEntry logEntry = logEntryReader.readLogEntry( channel );

        // then
        assertNull( logEntry );
    }

    // STATIC PART

    @Test
    public void shouldWriteALogHeaderInTheGivenBuffer() throws IllegalLogFormatException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );

        final long logVersion = 1;
        final long txId = 42;

        // when
        final ByteBuffer result = writeLogHeader( buffer, logVersion, txId );

        // then
        assertTrue( buffer == result );
        assertEquals( logVersion, decodeLogVersion( result.getLong(), true ) );
        assertEquals( txId, result.getLong() );
    }

    @Test
    public void shouldWriteALogHeaderInAFile() throws IOException
    {
        // given
        final File file = File.createTempFile( "WriteLogHeader", getClass().getSimpleName() );
        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

        final long logVersion = 1;
        final long txId = 42;

        // when
        writeLogHeader( fs, file, logVersion, txId );

        // then
        final byte[] array = new byte[LOG_HEADER_SIZE];
        try ( InputStream stream = fs.openAsInputStream( file ) )
        {
            stream.read( array );
        }
        final ByteBuffer result = ByteBuffer.wrap( array );

        assertEquals( logVersion, decodeLogVersion( result.getLong(), true ) );
        assertEquals( txId, result.getLong() );
    }

    @Test
    public void shouldReadALogHeaderFromAChannel() throws IOException
    {
        // given
        final long logVersion = 1;
        final long txId = 42;

        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocation ) throws Throwable
            {
                buffer.putLong( encodeLogVersion( logVersion ) );
                buffer.putLong( txId );
                return 8 + 8;
            }
        } );

        // when
        final long[] result = readLogHeader( buffer, channel, true );

        // then
        assertEquals( logVersion, result[0] );
        assertEquals( txId, result[1] );
    }

    @Test
    public void shouldFailWhenUnableToReadALogHeaderFromAChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenReturn( 1 );

        try
        {
            // when
            readLogHeader( buffer, channel, true );
            fail( "should have thrown" );
        }
        catch ( IOException ex )
        {
            // then
            assertEquals( "Unable to read log version and last committed tx", ex.getMessage() );
        }
    }

    @Test
    public void shouldReadALogHeaderFromAFile() throws IOException
    {
        // given
        final long logVersion = 1;
        final long txId = 42;

        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final File file = File.createTempFile( "ReadLogHeader", getClass().getSimpleName() );

        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        buffer.putLong( encodeLogVersion( logVersion ) );
        buffer.putLong( txId );

        try ( OutputStream stream = fs.openAsOutputStream( file, false ) )
        {
            stream.write( buffer.array() );
        }

        // when
        final long[] result = readLogHeader( fs, file );

        // then
        assertEquals( logVersion, result[0] );
        assertEquals( txId, result[1] );
    }

    @Test
    public void shouldFailWhenUnablleToReadALogHeaderFromAFile() throws IOException
    {
        // given
        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final File file = File.createTempFile( "ReadLogHeaderFail", getClass().getSimpleName() );
        try
        {
            // when
            readLogHeader( fs, file );
            fail( "should have thrown" );
        }
        catch ( IOException ex )
        {
            // then
            assertEquals( "Unable to read log version and last committed tx", ex.getMessage() );
        }
    }
}
