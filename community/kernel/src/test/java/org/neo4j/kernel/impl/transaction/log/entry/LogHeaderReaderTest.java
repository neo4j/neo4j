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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.IoPrimitiveUtils;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

@TestDirectoryExtension
class LogHeaderReaderTest
{
    private final long expectedLogVersion = CURRENT_LOG_VERSION;
    private final long expectedTxId = 42;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldReadALogHeaderFromAByteChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( invocation ->
        {
            buffer.putLong( encodeLogVersion( expectedLogVersion ) );
            buffer.putLong( expectedTxId );
            return Long.BYTES * 2;
        } );

        // when
        final LogHeader result = readLogHeader( buffer, channel, true, null );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, expectedLogVersion, expectedTxId ), result );
    }

    @Test
    void shouldFailWhenUnableToReadALogHeaderFromAChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenReturn( 1 );

        assertThrows( IncompleteLogHeaderException.class, () -> readLogHeader( buffer, channel, true, null ) );
    }

    @Test
    void shouldReadALogHeaderFromAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "ReadLogHeader" );

        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        buffer.putLong( encodeLogVersion( expectedLogVersion ) );
        buffer.putLong( expectedTxId );

        try ( OutputStream stream = fileSystem.openAsOutputStream( file, false ) )
        {
            stream.write( buffer.array() );
        }

        // when
        final LogHeader result = readLogHeader( fileSystem, file );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_VERSION, expectedLogVersion, expectedTxId ), result );

    }

    @Test
    void shouldFailWhenUnableToReadALogHeaderFromAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "ReadLogHeader" );
        fileSystem.write( file ).close();
        IncompleteLogHeaderException exception = assertThrows( IncompleteLogHeaderException.class, () -> readLogHeader( fileSystem, file ) );
        assertThat( exception.getMessage(), containsString( file.getName() ) );
    }

    @Test
    void shouldReadALongString() throws IOException
    {
        // given

        // build a string longer than 32k
        int stringSize = (int) (ByteUnit.kibiBytes( 32 )  + 1);
        String lengthyString = "x".repeat( stringSize );

        // we need 3 more bytes for writing the string length
        InMemoryClosableChannel channel = new InMemoryClosableChannel( stringSize + 3 );

        IoPrimitiveUtils.write3bLengthAndString( channel, lengthyString );

        // when
        String stringFromChannel = IoPrimitiveUtils.read3bLengthAndString( channel );

        // then
        assertEquals( lengthyString, stringFromChannel );
    }

    @Test
    void readEmptyPreallocatedFileHeaderAsNoHeader() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( invocation ->
        {
            buffer.putLong( 0L );
            buffer.putLong( 0L );
            return Long.BYTES * 2;
        } );

        LogHeader result = readLogHeader( buffer, channel, true, null );

        assertNull( result );
        verify( channel ).read( buffer );
    }
}
