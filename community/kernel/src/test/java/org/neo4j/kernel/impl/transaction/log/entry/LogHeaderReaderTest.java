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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

public class LogHeaderReaderTest
{
    private final long expectedLogVersion = CURRENT_LOG_VERSION;
    private final long expectedTxId = 42;

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

    @Test
    public void shouldReadALongString() throws IOException
    {
        // given

        // build a string longer than 32k
        int stringSize = 32 * 1024 + 1;
        StringBuilder sb = new StringBuilder(  );
        for ( int i = 0; i < stringSize; i++) {
            sb.append("x");
        }
        String lengthyString = sb.toString();

        // we need 3 more bytes for writing the string length
        final InMemoryLogChannel channel = new InMemoryLogChannel(stringSize + 3);

        IoPrimitiveUtils.write3bLengthAndString( channel, lengthyString);

        // when
        String stringFromChannel = IoPrimitiveUtils.read3bLengthAndString( channel );

        // then
        assertEquals( lengthyString, stringFromChannel );
    }
}
