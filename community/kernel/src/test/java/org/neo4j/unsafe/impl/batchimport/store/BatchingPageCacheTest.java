/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;
import static org.neo4j.unsafe.impl.batchimport.store.io.Monitor.NO_MONITOR;

public class BatchingPageCacheTest
{
    @Test
    public void shouldAppendThroughMultiplePages() throws Exception
    {
        // GIVEN
        int numberOfRecords = 100;
        PageCache pageCache = new BatchingPageCache( FS, numberOfRecords, 1, SYNCHRONOUS, NO_MONITOR );
        File file = directory.file( "store" );
        PagedFile pagedFile = pageCache.map( file, recordSize*recordsPerPage /* =90 */ );

        // WHEN
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            cursor.next();
            for ( int i = 0; i < numberOfRecords; i++ )
            {
                if ( i > 0 && i%recordsPerPage == 0 )
                {
                    cursor.next(); // no need for looping here in the batching thingie
                }
                writeRecord( cursor, i );
            }
        }
        pageCache.close();

        // THEN
        assertRecordsAreCorrect( file, numberOfRecords );
    }

    @Test
    public void shouldReadAndUpdateExistingContents() throws Exception
    {
        // GIVEN
        int pageSize = 100;
        File file = directory.file( "store" );
        fillFileWithByteContents( file );
        PageCache pageCache = new BatchingPageCache( FS, pageSize, 1, SYNCHRONOUS, NO_MONITOR );
        PagedFile pagedFile = pageCache.map( file, pageSize );

        // WHEN
        for ( int p = 0; p <= 1; p++ )
        {
            try ( PageCursor reader = pagedFile.io( p, PagedFile.PF_SHARED_LOCK );
                  PageCursor writer = pagedFile.io( p, PagedFile.PF_EXCLUSIVE_LOCK ) )
            {
                reader.next();
                writer.next();
                for ( int i = 0; i < pageSize; i++ )
                {
                    int offset = reader.getOffset();
                    byte value = reader.getByte();
                    if ( i%3 == 0 )
                    {
                        value++;
                    }
                    writer.setOffset( offset );
                    writer.putByte( value );
                }
            }
        }
        pageCache.close();

        // THEN
        assertByteContentsAreCorrect( file );
    }

    @Test
    public void shouldWriteChangesBeforeMovingWindow() throws Exception
    {
        // GIVEN
        byte[] someBytes = new byte[] { 1, 2, 3, 4, 5 };
        int pageSize = 100;
        Monitor monitor = mock( Monitor.class );
        File file = directory.file( "store" );
        fillFileWithByteContents( file );

        PageCache pageCache = new BatchingPageCache( FS, pageSize, 1, SYNCHRONOUS, monitor );
        PagedFile pagedFile = pageCache.map( file, pageSize );

        // WHEN
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            cursor.putBytes( someBytes );
        }
        verify( monitor, times(0) ).dataWritten( 0 );

        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            cursor.setOffset( 16 );
            cursor.putBytes( someBytes );
        }
        verify( monitor, times(0) ).dataWritten( 0 );

        // THEN
        try ( PageCursor ignored = pagedFile.io( 1, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            verify( monitor, times(1) ).dataWritten( anyInt() );
        }
    }

    @Test
    public void shouldZeroOutBufferBetweenUses() throws Exception
    {
        // GIVEN
        int pageSize = 100;
        File file = directory.file( "store" );
        fillFileWithByteContents( file );

        byte[] someBytes = new byte[]{1, 2, 3, 4, 5};
        PageCache pageCache = new BatchingPageCache( FS, pageSize, 1, SYNCHRONOUS, NO_MONITOR );
        PagedFile pagedFile = pageCache.map( file, pageSize );

        // And Given I've used the cursor a bit
        try ( PageCursor cursor = pagedFile.io( 1, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            cursor.putBytes( someBytes );
        }


        // WHEN I read empty data from the file
        try ( PageCursor cursor = pagedFile.io( 2, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            byte[] readBack = new byte[someBytes.length];
            cursor.getBytes( readBack );


            // THEN the buffer should be zero-filled
            byte[] zeros = new byte[someBytes.length];
            Arrays.fill( zeros, (byte) 0 );
            assertArrayEquals( zeros, readBack );
        }
    }

    private void assertByteContentsAreCorrect( File file ) throws IOException
    {
        try ( StoreChannel channel = FS.open( file, "r" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 255 );
            int read = channel.read( buffer );
            assertEquals( buffer.capacity(), read );
            buffer.flip();
            int counter = 0;
            for ( int i = 0; i <= 1; i++ )
            {
                for ( int j = 0; j < 100; j++ )
                {
                    byte value = buffer.get();
                    byte expectedValue = (byte) counter++;
                    if ( j%3 == 0 )
                    {
                        expectedValue++;
                    }
                    assertEquals( expectedValue, value );
                }
            }
        }
    }

    private void fillFileWithByteContents( File file ) throws IOException
    {
        try ( StoreChannel channel = FS.open( file, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 256 );
            for ( int i = 0; i < buffer.capacity(); i++ )
            {
                buffer.put( (byte) i );
            }
            buffer.flip();
            channel.write( buffer );
        }
    }

    private void assertRecordsAreCorrect( File file, int numberOfRecords ) throws IOException
    {
        try ( StoreChannel channel = FS.open( file, "r" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( recordSize );
            for ( int i = 0; i < numberOfRecords; i++ )
            {
                buffer.clear();
                channel.read( buffer );
                buffer.flip();
                assertRecord( i, buffer );
            }
        }
    }

    private void assertRecord( int i, ByteBuffer buffer )
    {
        assertEquals( i, buffer.getLong() );
        int length = recordSize-8;
        byte[] readBytes = new byte[length];
        buffer.get( readBytes );
        assertArrayEquals( bytesStartingAt( length, i ), readBytes );
    }

    private void writeRecord( PageCursor cursor, int index )
    {
        cursor.putLong( index );
        cursor.putBytes( bytesStartingAt( recordSize-8, index ) );
        // = 15
    }

    private byte[] bytesStartingAt( int length, int start )
    {
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) (start+i);
        }
        return bytes;
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final FileSystemAbstraction FS = new DefaultFileSystemAbstraction();
    private final int recordSize = 15;
    private final int recordsPerPage = 6;
}
