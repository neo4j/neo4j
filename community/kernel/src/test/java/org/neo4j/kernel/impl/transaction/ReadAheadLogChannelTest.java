/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class ReadAheadLogChannelTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory directory;
    private final LogFileChannelNativeAccessor nativeChannelAccessor = mock( LogFileChannelNativeAccessor.class );

    @Test
    void shouldReadFromSingleChannel() throws Exception
    {
        // GIVEN
        Path file = file( 0 );
        final byte byteValue = (byte) 5;
        final short shortValue = (short) 56;
        final int intValue = 32145;
        final long longValue = 5689456895869L;
        final float floatValue = 12.12345f;
        final double doubleValue = 3548.45748D;
        final byte[] byteArrayValue = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
        writeSomeData( file, element ->
        {
            element.put( byteValue );
            element.putShort( shortValue );
            element.putInt( intValue );
            element.putLong( longValue );
            element.putFloat( floatValue );
            element.putDouble( doubleValue );
            element.put( byteArrayValue );
            return true;
        } );

        StoreChannel storeChannel = fileSystem.read( file );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, -1 /* ignored */, (byte) -1, file, nativeChannelAccessor );
        try ( ReadAheadLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel, INSTANCE ) )
        {
            // THEN
            assertEquals( byteValue, channel.get() );
            assertEquals( shortValue, channel.getShort() );
            assertEquals( intValue, channel.getInt() );
            assertEquals( longValue, channel.getLong() );
            assertEquals( floatValue, channel.getFloat(), 0.1f );
            assertEquals( doubleValue, channel.getDouble(), 0.1d );

            byte[] bytes = new byte[byteArrayValue.length];
            channel.get( bytes, byteArrayValue.length );
            assertArrayEquals( byteArrayValue, bytes );
        }
    }

    @Test
    void rawReadAheadChannelOpensRawChannelOnNext() throws IOException
    {
        Path path = file( 0 );
        directory.createFile( path.getFileName().toString() );
        var storeChannel = fileSystem.read( path );
        var versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, -1 , (byte) -1, path, nativeChannelAccessor );
        var capturingLogVersionBridge = new RawCapturingLogVersionBridge();
        assertThrows( ReadPastEndException.class, () ->
        {
            try ( ReadAheadLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel, capturingLogVersionBridge, INSTANCE, true ) )
            {
                channel.get();
            }
        } );
        assertTrue( capturingLogVersionBridge.isRaw() );
    }

    @Test
    void shouldReadFromMultipleChannels() throws Exception
    {
        // GIVEN
        writeSomeData( file( 0 ), element ->
        {
            for ( int i = 0; i < 10; i++ )
            {
                element.putLong( i );
            }
            return true;
        } );
        writeSomeData( file( 1 ), element ->
        {
            for ( int i = 10; i < 20; i++ )
            {
                element.putLong( i );
            }
            return true;
        } );

        StoreChannel storeChannel = fileSystem.read( file( 0 ) );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, -1 /* ignored */, (byte) -1, file( 0 ), nativeChannelAccessor );
        try ( ReadAheadLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel, new LogVersionBridge()
        {
            private boolean returned;

            @Override
            public LogVersionedStoreChannel next( LogVersionedStoreChannel channel, boolean raw ) throws IOException
            {
                if ( !returned )
                {
                    returned = true;
                    channel.close();
                    return new PhysicalLogVersionedStoreChannel( fileSystem.read( file( 1 ) ),
                            -1 /* ignored */, (byte) -1, file( 1 ), nativeChannelAccessor );
                }
                return channel;
            }
        }, INSTANCE ) )
        {
            // THEN
            for ( long i = 0; i < 20; i++ )
            {
                assertEquals( i, channel.getLong() );
            }
        }
    }

    private void writeSomeData( Path file, Visitor<ByteBuffer, IOException> visitor ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.write( file ) )
        {
            ByteBuffer buffer = ByteBuffers.allocate( 1, KibiByte, INSTANCE );
            visitor.visit( buffer );
            buffer.flip();
            channel.writeAll( buffer );
        }
    }

    private Path file( int index )
    {
        return directory.homePath().resolve( "" + index );
    }

    private static class RawCapturingLogVersionBridge implements LogVersionBridge
    {
        private final MutableBoolean rawWitness = new MutableBoolean( false );

        @Override
        public LogVersionedStoreChannel next( LogVersionedStoreChannel channel, boolean raw ) throws IOException
        {
            rawWitness.setValue( raw );
            return channel;
        }

        public boolean isRaw()
        {
            return rawWitness.booleanValue();
        }
    }
}
