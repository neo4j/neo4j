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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.IoPrimitiveUtils;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.LOG_VERSION_MASK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.LOG_HEADER_SIZE_3_5;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class LogHeaderReaderTest
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;

    private long expectedLogVersion;
    private long expectedTxId;
    private StoreId expectedStoreId;

    @BeforeEach
    void setUp()
    {
        expectedLogVersion = random.nextLong( 0, LOG_VERSION_MASK );
        expectedTxId = random.nextLong();
        expectedStoreId = new StoreId( random.nextLong(), random.nextLong(), random.nextLong(), random.nextLong(), random.nextLong() );
    }

    @Test
    void shouldReadAnOldLogHeaderFromAByteChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );
        byte oldVersion = 6;

        when( channel.read( buffer ) ).thenAnswer( new Answer<>()
        {
            private int count;
            public Integer answer( InvocationOnMock invocation )
            {
                count++;
                if ( count == 1 )
                {
                    buffer.putLong( encodeLogVersion( expectedLogVersion, oldVersion ) );
                    return Long.BYTES;
                }
                if ( count == 2 )
                {
                    buffer.putLong( expectedTxId );
                    return Long.BYTES;
                }
                throw new AssertionError( "Should only be called twice" );
            }
        } );

        // when
        final LogHeader result = readLogHeader( buffer, channel, true, null );

        // then
        assertEquals( new LogHeader( oldVersion, expectedLogVersion, expectedTxId, LOG_HEADER_SIZE_3_5 ), result );
    }

    @Test
    void shouldReadALogHeaderFromAByteChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( new Answer<>()
        {
            private int count;
            public Integer answer( InvocationOnMock invocation )
            {
                count++;
                if ( count == 1 )
                {
                    buffer.putLong( encodeLogVersion( expectedLogVersion, CURRENT_LOG_FORMAT_VERSION ) );
                    return Long.BYTES;
                }
                if ( count == 2 )
                {
                    buffer.putLong( expectedTxId );
                    buffer.putLong( expectedStoreId.getCreationTime() );
                    buffer.putLong( expectedStoreId.getRandomId() );
                    buffer.putLong( expectedStoreId.getStoreVersion() );
                    buffer.putLong( expectedStoreId.getUpgradeTime() );
                    buffer.putLong( expectedStoreId.getUpgradeTxId() );
                    buffer.putLong( 0 ); // reserved
                    return Long.BYTES * 7;
                }
                throw new AssertionError( "Should only be called 3 times" );
            }
        } );

        // when
        final LogHeader result = readLogHeader( buffer, channel, true, null );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_FORMAT_VERSION, expectedLogVersion, expectedTxId, expectedStoreId, CURRENT_FORMAT_LOG_HEADER_SIZE ), result );
    }

    @Test
    void shouldFailWhenUnableToReadALogHeaderFromAChannel() throws IOException
    {
        // given
        final ByteBuffer buffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
        final ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenReturn( 1 );

        assertThrows( IncompleteLogHeaderException.class, () -> readLogHeader( buffer, channel, true, null ) );
    }

    @Test
    void shouldReadALogHeaderFromAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "ReadLogHeader" );

        final ByteBuffer buffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
        buffer.putLong( encodeLogVersion( expectedLogVersion, CURRENT_LOG_FORMAT_VERSION ) );
        buffer.putLong( expectedTxId );
        buffer.putLong( expectedStoreId.getCreationTime() );
        buffer.putLong( expectedStoreId.getRandomId() );
        buffer.putLong( expectedStoreId.getStoreVersion() );
        buffer.putLong( expectedStoreId.getUpgradeTime() );
        buffer.putLong( expectedStoreId.getUpgradeTxId() );

        try ( OutputStream stream = fileSystem.openAsOutputStream( file, false ) )
        {
            stream.write( buffer.array() );
        }

        // when
        final LogHeader result = readLogHeader( fileSystem, file );

        // then
        assertEquals( new LogHeader( CURRENT_LOG_FORMAT_VERSION, expectedLogVersion, expectedTxId, expectedStoreId, CURRENT_FORMAT_LOG_HEADER_SIZE ), result );
    }

    @Test
    void shouldFailWhenUnableToReadALogHeaderFromAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "ReadLogHeader" );
        fileSystem.write( file ).close();
        IncompleteLogHeaderException exception = assertThrows( IncompleteLogHeaderException.class, () -> readLogHeader( fileSystem, file ) );
        assertThat( exception.getMessage() ).contains( file.getName() );
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
        ByteBuffer buffer = ByteBuffers.allocate( CURRENT_FORMAT_LOG_HEADER_SIZE );
        ReadableByteChannel channel = mock( ReadableByteChannel.class );

        when( channel.read( buffer ) ).thenAnswer( invocation ->
        {
            buffer.putLong( 0L );
            return Long.BYTES;
        } );

        LogHeader result = readLogHeader( buffer, channel, true, null );

        assertNull( result );
        verify( channel ).read( buffer );
    }
}
