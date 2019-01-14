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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldReadALogHeaderFromAByteChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( invocation ->
        {
            buffer.putLong( encodeLogVersion( expectedLogVersion ) );
            buffer.putLong( expectedTxId );
            return 8 + 8;
        } );

        // when
        final LogHeader result = readLogHeader( buffer, channel, true, null );

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
            readLogHeader( buffer, channel, true, null );
            fail( "should have thrown" );
        }
        catch ( IncompleteLogHeaderException ex )
        {
            // then good
        }
    }

    @Test
    public void shouldReadALogHeaderFromAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "ReadLogHeader" );

        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        buffer.putLong( encodeLogVersion( expectedLogVersion ) );
        buffer.putLong( expectedTxId );

        try ( OutputStream stream = fileSystemRule.get().openAsOutputStream( file, false ) )
        {
            stream.write( buffer.array() );
        }

        // when
        final LogHeader result = readLogHeader( fileSystemRule.get(), file );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, expectedLogVersion, expectedTxId ), result );

    }

    @Test
    public void shouldFailWhenUnableToReadALogHeaderFromAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "ReadLogHeader" );
        fileSystemRule.create( file ).close();
        try
        {
            // when
            readLogHeader( fileSystemRule.get(), file );
            fail( "should have thrown" );
        }
        catch ( IncompleteLogHeaderException ex )
        {
            // then
            assertTrue( ex.getMessage(), ex.getMessage().contains( file.getName() ) );
        }
    }

    @Test
    public void shouldReadALongString() throws IOException
    {
        // given

        // build a string longer than 32k
        int stringSize = 32 * 1024 + 1;
        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < stringSize; i++ )
        {
            sb.append( "x" );
        }
        String lengthyString = sb.toString();

        // we need 3 more bytes for writing the string length
        InMemoryClosableChannel channel = new InMemoryClosableChannel( stringSize + 3 );

        IoPrimitiveUtils.write3bLengthAndString( channel, lengthyString );

        // when
        String stringFromChannel = IoPrimitiveUtils.read3bLengthAndString( channel );

        // then
        assertEquals( lengthyString, stringFromChannel );
    }
}
