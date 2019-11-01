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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Random;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.memory.ByteBuffers.allocateDirect;

@TestDirectoryExtension
class PhysicalFlushableChannelTest
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory directory;
    private final LogFileChannelNativeAccessor nativeChannelAccessor = mock( LogFileChannelNativeAccessor.class );

    @Test
    void shouldBeAbleToWriteSmallNumberOfBytes() throws IOException
    {
        final File firstFile = new File( directory.homeDir(), "file1" );
        StoreChannel storeChannel = fileSystem.write( firstFile );
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, firstFile,
                nativeChannelAccessor );
        int length = 26_145;
        byte[] bytes;
        try ( PhysicalFlushableChannel channel = new PhysicalFlushableChannel( versionedStoreChannel ) )
        {
            bytes = generateBytes( length );
            channel.put( bytes, length );
        }

        byte[] writtenBytes = new byte[length];
        try ( InputStream in = new FileInputStream( firstFile ) )
        {
            in.read( writtenBytes );
        }

        assertArrayEquals( bytes, writtenBytes );
    }

    @Test
    void shouldBeAbleToWriteValuesGreaterThanHalfTheBufferSize() throws IOException
    {
        final File firstFile = new File( directory.homeDir(), "file1" );
        StoreChannel storeChannel = fileSystem.write( firstFile );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor );
        int length = 262_145;
        byte[] bytes;
        try ( PhysicalFlushableChannel channel = new PhysicalFlushableChannel( versionedStoreChannel ) )
        {
            bytes = generateBytes( length );
            channel.put( bytes, length );
        }

        byte[] writtenBytes = new byte[length];
        try ( InputStream in = new FileInputStream( firstFile ) )
        {
            in.read( writtenBytes );
        }

        assertArrayEquals( bytes, writtenBytes );
    }

    @Test
    void shouldBeAbleToWriteValuesGreaterThanTheBufferSize() throws IOException
    {
        final File firstFile = new File( directory.homeDir(), "file1" );
        StoreChannel storeChannel = fileSystem.write( firstFile );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor );
        int length = 1_000_000;
        byte[] bytes;
        try ( PhysicalFlushableChannel channel = new PhysicalFlushableChannel( versionedStoreChannel ) )
        {
            bytes = generateBytes( length );
            channel.put( bytes, length );
        }

        byte[] writtenBytes = new byte[length];
        try ( InputStream in = new FileInputStream( firstFile ) )
        {
            in.read( writtenBytes );
        }

        assertArrayEquals( bytes, writtenBytes );
    }

    private static byte[] generateBytes( int length )
    {
        Random random = new Random();
        char[] validCharacters = new char[] { 'a', 'b', 'c', 'd', 'e','f', 'g', 'h','i', 'j', 'k', 'l', 'm', 'n', 'o' };
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) validCharacters[random.nextInt(validCharacters.length)];
        }
        return bytes;
    }

    @Test
    void shouldWriteThroughRotation() throws Exception
    {
        // GIVEN
        final File firstFile = new File( directory.homeDir(), "file1" );
        final File secondFile = new File( directory.homeDir(), "file2" );
        StoreChannel storeChannel = fileSystem.write( firstFile );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor );
        PhysicalFlushableLogChannel channel = new PhysicalFlushableLogChannel( versionedStoreChannel );

        // WHEN writing a transaction, of sorts
        byte byteValue = (byte) 4;
        short shortValue = (short) 10;
        int intValue = 3545;
        long longValue = 45849589L;
        float floatValue = 45849.332f;
        double doubleValue = 458493343D;
        byte[] byteArrayValue = new byte[] {1,4,2,5,3,6};

        channel.put( byteValue );
        channel.putShort( shortValue );
        channel.putInt( intValue );
        channel.putLong( longValue );
        channel.prepareForFlush().flush();
        channel.close();

        // "Rotate" and continue
        storeChannel = fileSystem.write( secondFile );
        channel.setChannel( new PhysicalLogVersionedStoreChannel( storeChannel, 2, (byte) -1, secondFile, nativeChannelAccessor ) );
        channel.putFloat( floatValue );
        channel.putDouble( doubleValue );
        channel.put( byteArrayValue, byteArrayValue.length );
        channel.close();

        // The two chunks of values should end up in two different files
        ByteBuffer firstFileContents = readFile( firstFile );
        assertEquals( byteValue, firstFileContents.get() );
        assertEquals( shortValue, firstFileContents.getShort() );
        assertEquals( intValue, firstFileContents.getInt() );
        assertEquals( longValue, firstFileContents.getLong() );
        ByteBuffer secondFileContents = readFile( secondFile );
        assertEquals( floatValue, secondFileContents.getFloat(), 0.001f );
        assertEquals( doubleValue, secondFileContents.getDouble(), 0.001d );

        byte[] readByteArray = new byte[byteArrayValue.length];
        secondFileContents.get( readByteArray );
        assertArrayEquals( byteArrayValue, readByteArray );
    }

    @Test
    void shouldSeeCorrectPositionEvenBeforeEmptyingDataIntoChannel() throws Exception
    {
        // GIVEN
        final File file = new File( directory.homeDir(), "file" );
        StoreChannel storeChannel = fileSystem.write( file );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, file, nativeChannelAccessor );
        PositionAwarePhysicalFlushableChecksumChannel channel =
                new PositionAwarePhysicalFlushableChecksumChannel( versionedStoreChannel, allocateDirect( 1024 ) );
        LogPositionMarker positionMarker = new LogPositionMarker();
        LogPosition initialPosition = channel.getCurrentPosition( positionMarker ).newPosition();

        // WHEN
        channel.putLong( 67 );
        channel.putInt( 1234 );
        LogPosition positionAfterSomeData = channel.getCurrentPosition( positionMarker ).newPosition();

        // THEN
        assertEquals( 12, positionAfterSomeData.getByteOffset() - initialPosition.getByteOffset() );
        channel.close();
    }

    @Test
    void shouldThrowIllegalStateExceptionAfterClosed() throws Exception
    {
        // GIVEN
        final File file = new File( directory.homeDir(), "file" );
        StoreChannel storeChannel = fileSystem.write( file );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, file, nativeChannelAccessor );
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel( versionedStoreChannel );

        // closing the WritableLogChannel, then the underlying channel is what PhysicalLogFile does
        channel.close();
        storeChannel.close();

        // WHEN just appending something to the buffer
        channel.put( (byte) 0 );
        // and wanting to empty that into the channel
        assertThrows( IllegalStateException.class, channel::prepareForFlush );
    }

    @Test
    void shouldThrowClosedChannelExceptionWhenChannelUnexpectedlyClosed() throws Exception
    {
        // GIVEN
        final File file = new File( directory.homeDir(), "file" );
        StoreChannel storeChannel = fileSystem.write( file );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, 1, (byte) -1, file, nativeChannelAccessor );
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel( versionedStoreChannel );

        // just close the underlying channel
        storeChannel.close();

        // WHEN just appending something to the buffer
        channel.put( (byte) 0 );
        // and wanting to empty that into the channel
        assertThrows( ClosedChannelException.class, channel::prepareForFlush );
    }

    private ByteBuffer readFile( File file ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.read( file ) )
        {
            ByteBuffer buffer = ByteBuffers.allocate( (int) channel.size() );
            channel.readAll( buffer );
            buffer.flip();
            return buffer;
        }
    }

}
