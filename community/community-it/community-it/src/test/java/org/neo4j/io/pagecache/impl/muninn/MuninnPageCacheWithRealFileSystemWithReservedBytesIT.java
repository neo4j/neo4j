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
package org.neo4j.io.pagecache.impl.muninn;

import org.bouncycastle.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.PageCursor.UNBOUND_PAYLOAD_SIZE;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@TestDirectoryExtension
class MuninnPageCacheWithRealFileSystemWithReservedBytesIT extends MuninnPageCacheTest
{
    private static final int TEST_RESERVED_BYTES = Long.BYTES * 3;

    @Inject
    TestDirectory directory;

    MuninnPageCacheWithRealFileSystemWithReservedBytesIT()
    {
        SHORT_TIMEOUT_MILLIS = 240_000;
        SEMI_LONG_TIMEOUT_MILLIS = 720_000;
        LONG_TIMEOUT_MILLIS = 2_400_000;
    }

    @Override
    protected Fixture<MuninnPageCache> createFixture()
    {
        return super.createFixture()
                    .withFileSystemAbstraction( DefaultFileSystemAbstraction::new )
                    .withReservedBytes( TEST_RESERVED_BYTES )
                    .withFileConstructor( directory::file );
    }

    @Test
    void pageCursorsHaveCorrectPayloadSize() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
              var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );
                assertEquals( pageCache.payloadSize(), writer.getCurrentPayloadSize() );
                assertEquals( pageCache.pageSize() - TEST_RESERVED_BYTES, writer.getCurrentPayloadSize() );
                assertEquals( pageCache.pageSize(), writer.getCurrentPageSize() );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );
                assertEquals( pageCache.payloadSize(), reader.getCurrentPayloadSize() );
                assertEquals( pageCache.pageSize() - TEST_RESERVED_BYTES, reader.getCurrentPayloadSize() );
                assertEquals( pageCache.pageSize(), reader.getCurrentPageSize() );
            }
        }
    }

    @Test
    void unboundPageCursorPayload() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertEquals( UNBOUND_PAYLOAD_SIZE, writer.getCurrentPayloadSize() );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertEquals( UNBOUND_PAYLOAD_SIZE, reader.getCurrentPayloadSize() );
            }
        }
    }

    @Test
    void linkedCursorsPreservePayloadSize() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() ) )
        {
            var file = file( "a" );
            generateFileWithRecords( file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes );

            try ( var pagedFile = map( file( "a" ), pageCache.pageSize() );
                  var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );
                try ( var linedReader = reader.openLinkedCursor( 1 ) )
                {
                    assertTrue( linedReader.next() );
                    assertEquals( pageCache.pageSize() - TEST_RESERVED_BYTES, linedReader.getCurrentPayloadSize() );
                }
            }
        }
    }

    @Test
    void writeAndReadFullPageWithReservedBytesWithoutOutOfBounds() throws IOException
    {
        byte data = 5;
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
              var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );

                int counter = 0;
                while ( writer.getOffset() < writer.getCurrentPayloadSize() )
                {
                    writer.putByte( data );
                    counter++;
                }
                assertFalse( writer.checkAndClearBoundsFlag() );
                assertEquals( counter, writer.getCurrentPayloadSize() );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );
                int counter = 0;
                while ( reader.getOffset() < reader.getCurrentPayloadSize() )
                {
                    assertEquals( data, reader.getByte() );
                    counter++;
                }
                assertFalse( reader.checkAndClearBoundsFlag() );
                assertEquals( counter, reader.getCurrentPayloadSize() );
            }
        }
    }

    @Test
    void outOfBoundsOnAttemptToWriteOrReadWholePageData() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );
                long value = 1;
                while ( writer.getOffset() < pageCache.pageSize() )
                {
                    writer.putLong( value++ );
                }
                assertTrue( writer.checkAndClearBoundsFlag() );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( reader.next() );
                while ( reader.getOffset() < pageCache.pageSize() )
                {
                    assertThat( reader.getLong() ).isGreaterThan( 0 );
                }
                assertTrue( reader.checkAndClearBoundsFlag() );
            }
        }
    }

    @Test
    void offsetIsLogicalAndDoesNotDependFromNumberOfReservedBytesForInts() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            int offset = 0;
            int typeBytes = Integer.BYTES;
            int expectedIterations = pageCache.payloadSize() / typeBytes;
            assertThat( expectedIterations ).isNotZero();

            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );
                int writes = 0;
                while ( writer.getOffset() < writer.getCurrentPayloadSize() )
                {
                    assertEquals( offset, writer.getOffset() );
                    writer.putInt( offset );
                    offset += typeBytes;
                    writes++;
                }
                assertFalse( writer.checkAndClearBoundsFlag() );
                assertEquals( expectedIterations, writes );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );
                int reads = 0;
                while ( offset > 0 )
                {
                    int value = offset - typeBytes;
                    assertEquals( value, reader.getInt( value ) );
                    offset = value;
                    reads++;
                }
                assertFalse( reader.checkAndClearBoundsFlag() );
                assertEquals( expectedIterations, reads );
            }
        }
    }

    @Test
    void offsetIsLogicalAndDoesNotDependFromNumberOfReservedBytesForLongs() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            int offset = 0;
            int typeBytes = Long.BYTES;
            int expectedIterations = pageCache.payloadSize() / typeBytes;
            assertThat( expectedIterations ).isNotZero();

            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );
                int writes = 0;
                while ( writer.getOffset() < writer.getCurrentPayloadSize() )
                {
                    assertEquals( offset, writer.getOffset() );
                    writer.putLong( offset );
                    offset += typeBytes;
                    writes++;
                }
                assertFalse( writer.checkAndClearBoundsFlag() );
                assertEquals( expectedIterations, writes );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );
                int reads = 0;
                while ( offset > 0 )
                {
                    int value = offset - typeBytes;
                    assertEquals( value, reader.getLong( value ) );
                    offset = value;
                    reads++;
                }
                assertFalse( reader.checkAndClearBoundsFlag() );
                assertEquals( expectedIterations, reads );
            }
        }
    }

    @Test
    void outOfBoundsWhenReadDataOutsideOfPayloadWindow() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() ) )
        {
            var file = file( "a" );
            generateFileWithRecords( file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes );
            try ( var pagedFile = map( file, pageCache.pageSize() );
                  var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );

                checkReadOob( reader, pagedFile.payloadSize() );
                checkReadOob( reader, -1 );
                checkReadOob( reader, -5 );
                checkReadOob( reader, -10 );

                // we can read from this cursor if offsets are ok
                reader.getByte();
                assertFalse( reader.checkAndClearBoundsFlag() );
            }
        }
    }

    @Test
    void outOfBoundsWhenWriteDataOutsideOfPayloadWindow() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() ) )
        {
            var file = file( "a" );
            generateFileWithRecords( file, recordsPerFilePage * 2, recordSize, recordsPerFilePage, reservedBytes );
            try ( var pagedFile = map( file, pageCache.pageSize() );
                    var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );

                checkWriteOob( writer, pagedFile.payloadSize() );
                checkWriteOob( writer, -1 );
                checkWriteOob( writer, -5 );
                checkWriteOob( writer, -10 );

                // we can read from this cursor if offsets are ok
                writer.putByte( (byte) 7 );
                assertFalse( writer.checkAndClearBoundsFlag() );
            }
        }
    }

    @Test
    void offsetAccessAndMutationTakeReservedBytesIntoAccount() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
              var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            int offset = 0;
            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );

                writer.putByte( offset, (byte) 1 );
                offset += Byte.BYTES;

                writer.putShort( offset, (short) 2 );
                offset += Short.BYTES;

                writer.putInt( offset, 3 );
                offset += Integer.BYTES;

                writer.putLong( offset, 4 );
                offset += Long.BYTES;
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( reader.next() );

                offset -= Long.BYTES;
                assertEquals( 4, reader.getLong( offset ) );

                offset -= Integer.BYTES;
                assertEquals( 3, reader.getInt( offset ) );

                offset -= Short.BYTES;
                assertEquals( 2, reader.getShort( offset ) );

                offset -= Byte.BYTES;
                assertEquals( 1, reader.getByte( offset ) );

                assertEquals( 0, reader.getOffset() );

                assertEquals( 1, reader.getByte() );
                assertEquals( Bytes.BYTES, reader.getOffset() );

                assertEquals( 2, reader.getShort() );
                assertEquals( Bytes.BYTES + Short.BYTES, reader.getOffset() );

                assertEquals( 3, reader.getInt() );
                assertEquals( Bytes.BYTES + Short.BYTES + Integer.BYTES, reader.getOffset() );

                assertEquals( 4, reader.getLong() );
                assertEquals( Bytes.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES, reader.getOffset() );
            }
        }
    }

    @Test
    void copyToCursorFailToCopyWholePageSizeAndCopyOnlyPayload() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() );
                var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL );
                var writer2 = pagedFile.io( 1, PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( writer.next() );
            assertTrue( writer2.next() );

            int value = 1;
            while ( writer.getOffset() < writer.getCurrentPayloadSize() )
            {
                writer.putInt( value );
            }

            int copiedBytes = writer.copyTo( 0, writer2, 0, writer.getCurrentPageSize() );
            assertEquals( writer.getCurrentPayloadSize(), copiedBytes );
            assertFalse( writer.checkAndClearBoundsFlag() );

            int expectedValue = 1;
            while ( writer2.getOffset() < writer2.getCurrentPayloadSize() )
            {
                assertEquals( expectedValue, writer2.getInt() );
            }
        }
    }

    @Test
    void copyToByteBufferTakeReservedBytesIntoAccount() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() );
                var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( writer.next() );

            int value = 1;
            int writtenValues = 0;
            while ( writer.getOffset() < writer.getCurrentPayloadSize() )
            {
                writer.putInt( value++ );
                writtenValues++;
            }
            assertThat( writtenValues ).isNotZero();

            var heapBuffer = ByteBuffers.allocate( writer.getCurrentPageSize(), EmptyMemoryTracker.INSTANCE );
            var nativeBuffer = ByteBuffers.allocateDirect( writer.getCurrentPageSize(), EmptyMemoryTracker.INSTANCE );

            checkCopiedBuffer( writer, writtenValues, heapBuffer, writer.copyTo( 0, heapBuffer ) );
            checkCopiedBuffer( writer, writtenValues, nativeBuffer, writer.copyTo( 0, nativeBuffer ) );
        }
    }

    @Test
    void shiftBytesOverflowsWhenShiftingOverPayloadWindow() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
              var pagedFile = map( file( "a" ), pageCache.pageSize() );
              var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( writer.next() );

            int value = 1;
            while ( writer.getOffset() < writer.getCurrentPayloadSize() )
            {
                writer.putInt( value++ );
            }

            writer.shiftBytes( 0, writer.getCurrentPayloadSize(), 1 );
            assertTrue( writer.checkAndClearBoundsFlag() );
        }
    }

    @Test
    void shiftBytesTakesReservedBytesIntoAccount() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
                var pagedFile = map( file( "a" ), pageCache.pageSize() );
                var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( writer.next() );

            int value = 1;
            while ( writer.getOffset() < writer.getCurrentPayloadSize() )
            {
                writer.putInt( value++ );
            }

            int windowSize = Integer.BYTES * 4;
            writer.shiftBytes( 0, windowSize, 4 );
            assertFalse( writer.checkAndClearBoundsFlag() );

            writer.shiftBytes( 4, windowSize, -4 );
            assertFalse( writer.checkAndClearBoundsFlag() );

            assertEquals( 1, writer.getInt( 0 ) );
            assertEquals( 2, writer.getInt( Integer.BYTES ) );
            assertEquals( 3, writer.getInt( 2 * Integer.BYTES ) );
            assertEquals( 4, writer.getInt( 3 * Integer.BYTES ) );
        }
    }

    @Test
    void overflowOnAccessingDataWithOffsetGreaterThanPayload() throws IOException
    {
        try ( var pageCache = getPageCache( fs, 1024, new DefaultPageCacheTracer() );
              var pagedFile = map( file( "a" ), pageCache.pageSize() ) )
        {
            try ( var writer = pagedFile.io( 0, PF_SHARED_WRITE_LOCK, NULL ) )
            {
                assertTrue( writer.next() );

                writer.getByte( writer.getCurrentPayloadSize() - 1 );
                assertFalse( writer.checkAndClearBoundsFlag() );

                writer.getByte( writer.getCurrentPayloadSize() );
                assertTrue( writer.checkAndClearBoundsFlag() );
            }

            try ( var reader = pagedFile.io( 0, PF_SHARED_READ_LOCK, NULL ) )
            {
                assertTrue( reader.next() );

                reader.getByte( reader.getCurrentPayloadSize() - 1 );
                assertFalse( reader.checkAndClearBoundsFlag() );

                reader.getByte( reader.getCurrentPayloadSize() );
                assertTrue( reader.checkAndClearBoundsFlag() );
            }
        }
    }

    private void checkCopiedBuffer( PageCursor writer, int writtenValues, ByteBuffer buffer, int copiedBytes )
    {
        assertEquals( writer.getCurrentPayloadSize(), copiedBytes );
        buffer.flip();
        int copiedValue = 1;
        int numberOfItems = 0;
        while ( buffer.hasRemaining() )
        {
            assertEquals( copiedValue++, buffer.getInt() );
            numberOfItems++;
        }
        assertEquals( numberOfItems, writtenValues );
    }

    private void checkReadOob( PageCursor reader, int offset )
    {
        reader.getLong( offset );
        assertTrue( reader.checkAndClearBoundsFlag() );

        reader.getInt( offset );
        assertTrue( reader.checkAndClearBoundsFlag() );

        reader.getShort( offset );
        assertTrue( reader.checkAndClearBoundsFlag() );

        reader.getByte( offset );
        assertTrue( reader.checkAndClearBoundsFlag() );
    }

    private void checkWriteOob( PageCursor writer, int offset )
    {
        writer.putLong( offset, 1 );
        assertTrue( writer.checkAndClearBoundsFlag() );

        writer.putInt( offset, 1 );
        assertTrue( writer.checkAndClearBoundsFlag() );

        writer.putShort( offset, (short) 1 );
        assertTrue( writer.checkAndClearBoundsFlag() );

        writer.putByte( offset, (byte) 1 );
        assertTrue( writer.checkAndClearBoundsFlag() );
    }
}
