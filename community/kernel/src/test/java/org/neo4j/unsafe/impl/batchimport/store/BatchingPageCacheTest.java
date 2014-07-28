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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Mode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;
import static org.neo4j.unsafe.impl.batchimport.store.io.Monitor.NO_MONITOR;

public class BatchingPageCacheTest
{
    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    private static final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    private static final int RECORD_SIZE = 15;
    private static final int RECORDS_PER_PAGE = 6;

    @Test
    public void shouldAppendThroughMultiplePages() throws Exception
    {
        // GIVEN
        int numberOfRecords = 100;
        PageCache pageCache = new BatchingPageCache(
                fs, configWithPageSize( numberOfRecords ), SYNCHRONOUS, NO_MONITOR, Mode.APPEND_ONLY );
        File file = directory.file( "store" );
        PagedFile pagedFile = pageCache.map( file, RECORD_SIZE * RECORDS_PER_PAGE /* =90 */ );

        // WHEN
        try ( PageCursor cursor = pagedFile.io( 0, 0 ) )
        {
            cursor.next();
            for ( int i = 0; i < numberOfRecords; i++ )
            {
                if ( i > 0 && i % RECORDS_PER_PAGE == 0 )
                {
                    cursor.next(); // no need for looping here in the batching thingy
                }
                writeRecord( cursor, i );
            }
        }
        pageCache.close();

        // THEN
        assertRecordsAreCorrect( file, numberOfRecords );
    }

    @Test
    public void shouldReadAndUpdateExistingContentsInUpdateMode() throws Exception
    {
        // GIVEN
        int pageSize = 100;
        File file = directory.file( "store" );
        fillFileWithByteContents( file );
        PageCache pageCache =
                new BatchingPageCache( fs, configWithPageSize( pageSize ), SYNCHRONOUS, NO_MONITOR, Mode.UPDATE );
        PagedFile pagedFile = pageCache.map( file, pageSize );

        // WHEN
        for ( int p = 0; p <= 1; p++ )
        {
            try ( PageCursor cursor = pagedFile.io( p, 0 ) )
            {
                cursor.next();
                for ( int i = 0; i < pageSize; i++ )
                {
                    int offset = cursor.getOffset();
                    byte value = cursor.getByte();
                    if ( i % 3 == 0 )
                    {
                        cursor.setOffset( offset );
                        value++;
                        cursor.putByte( value );
                    }
                }
            }
        }
        pageCache.close();

        // THEN
        assertByteContentsAreCorrect( file );
    }

    private void assertByteContentsAreCorrect( File file ) throws IOException
    {
        try ( StoreChannel channel = fs.open( file, "r" ) )
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
                    if ( j % 3 == 0 )
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
        try ( StoreChannel channel = fs.open( file, "rw" ) )
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
        try ( StoreChannel channel = fs.open( file, "r" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( RECORD_SIZE );
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
        int length = RECORD_SIZE - 8;
        byte[] readBytes = new byte[length];
        buffer.get( readBytes );
        assertArrayEquals( bytesStartingAt( length, i ), readBytes );
    }

    private void writeRecord( PageCursor cursor, int index )
    {
        cursor.putLong( index );
        cursor.putBytes( bytesStartingAt( RECORD_SIZE - 8, index ) );
        // = 15
    }

    private byte[] bytesStartingAt( int length, int start )
    {
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = (byte) (start + i);
        }
        return bytes;
    }

    private static Configuration configWithPageSize( final int size )
    {
        return new Configuration.Default()
        {
            @Override
            public int fileChannelBufferSize()
            {
                return size;
            }
        };
    }
}
