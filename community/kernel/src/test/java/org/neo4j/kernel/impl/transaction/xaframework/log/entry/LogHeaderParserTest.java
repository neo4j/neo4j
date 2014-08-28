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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

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
import org.neo4j.kernel.impl.transaction.xaframework.IllegalLogFormatException;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.decodeLogFormatVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.decodeLogVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.readLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class LogHeaderParserTest
{
    private final long expectedLogVersion = 1;
    private final long expectedTxId = 42;

    @Test
    public void shouldWriteALogHeaderInTheGivenChannel() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        // when
        writeLogHeader( channel, expectedLogVersion, expectedTxId );

        // then
        long encodedLogVersions = channel.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( logFormatVersion, encodedLogVersions, true );
        assertEquals( expectedLogVersion, logVersion );

        long txId = channel.getLong();
        assertEquals( expectedTxId, txId );
    }

    @Test
    public void shouldWriteALogHeaderInTheGivenBuffer() throws IllegalLogFormatException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );

        // when
        final ByteBuffer result = writeLogHeader( buffer, expectedLogVersion, expectedTxId );

        // then
        assertSame( buffer, result );

        long encodedLogVersions = result.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( logFormatVersion, encodedLogVersions, true );
        assertEquals( expectedLogVersion, logVersion );

        long txId = result.getLong();
        assertEquals( expectedTxId, txId );
    }

    @Test
    public void shouldWriteALogHeaderInAFile() throws IOException
    {
        // given
        final File file = File.createTempFile( "WriteLogHeader", getClass().getSimpleName() );
        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

        // when
        writeLogHeader( fs, file, expectedLogVersion, expectedTxId );

        // then
        final byte[] array = new byte[LOG_HEADER_SIZE];
        try ( InputStream stream = fs.openAsInputStream( file ) )
        {
            int read = stream.read( array );
            assertEquals( LOG_HEADER_SIZE, read );
        }
        final ByteBuffer result = ByteBuffer.wrap( array );

        long encodedLogVersions = result.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( logFormatVersion, encodedLogVersions, true );
        assertEquals( expectedLogVersion, logVersion );

        long txId = result.getLong();
        assertEquals( expectedTxId, txId );
    }

    @Test
    public void shouldReadALogHeaderFromALogChannel() throws IOException
    {
        // given
        final InMemoryLogChannel channel = new InMemoryLogChannel();

        channel.putLong( encodeLogVersion( expectedLogVersion ) );
        channel.putLong( expectedTxId );

        // when
        final LogHeader result = readLogHeader( channel );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, expectedLogVersion, expectedTxId ), result );
    }

    @Test
    public void shouldReadALogHeaderFromAByteChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocation ) throws Throwable
            {
                buffer.putLong( encodeLogVersion( expectedLogVersion ) );
                buffer.putLong( expectedTxId );
                return 8 + 8;
            }
        } );

        // when
        final LogHeader result = readLogHeader( buffer, channel, true );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, expectedLogVersion, expectedTxId ), result );
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
        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final File file = File.createTempFile( "ReadLogHeader", getClass().getSimpleName() );

        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        buffer.putLong( encodeLogVersion( expectedLogVersion ) );
        buffer.putLong( expectedTxId );

        try ( OutputStream stream = fs.openAsOutputStream( file, false ) )
        {
            stream.write( buffer.array() );
        }

        // when
        final LogHeader result = readLogHeader( fs, file );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, expectedLogVersion, expectedTxId ), result );

    }

    @Test
    public void shouldFailWhenUnableToReadALogHeaderFromAFile() throws IOException
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
