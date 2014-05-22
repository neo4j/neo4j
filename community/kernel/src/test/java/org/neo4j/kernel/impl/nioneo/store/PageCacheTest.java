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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.standard.StandardPageCache;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PageCacheTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private final File storeFile = new File( "myStore" );

    @Test
    public void shouldMapAndDoIO() throws Exception
    {
        // Given
        PageCache cache = newPageCache();

        // When
        PagedFile mappedFile = cache.map( storeFile, 1024 );

        // Then I should be able to write to the file
        PageCursor cursor = cache.newCursor();
        mappedFile.pin( cursor, PageLock.EXCLUSIVE, 4 );

        cursor.setOffset( 33 );
        byte[] expected = "Hello, cruel world".getBytes( "UTF-8" );
        cursor.putBytes( expected );
        cursor.putByte( (byte) 13 );
        cursor.putInt( 1337 );
        cursor.putLong( 7331 );

        // Then I should be able to read from the file
        cursor.setOffset( 33 );
        byte[] actual = new byte[expected.length];
        cursor.getBytes( actual );

        assertThat( actual, equalTo( expected ) );
        assertThat(cursor.getByte(), equalTo((byte)13));
        assertThat(cursor.getInt(), equalTo(1337));
        assertThat(cursor.getLong(), equalTo(7331l));
    }

    @Test
    public void shouldCloseAllFiles() throws Exception
    {
        // Given
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        StandardPageCache cache = new StandardPageCache( fs, 16, 512 );
        File file1Name = new File( "file1" );
        File file2Name = new File( "file2" );

        StoreChannel channel1 = mock(StoreChannel.class);
        StoreChannel channel2 = mock(StoreChannel.class);
        when( fs.open( file1Name, "rw" ) ).thenReturn( channel1 );
        when( fs.open( file2Name, "rw" ) ).thenReturn( channel2 );


        // When
        PagedFile file1 = cache.map( file1Name, 64 );
        PagedFile file1Again = cache.map( file1Name, 64 );
        PagedFile file2 = cache.map( file2Name, 64 );
        cache.unmap( file2Name );

        // Then
        verify( fs ).open( file1Name, "rw" );
        verify( fs ).open( file2Name, "rw" );
        verify( channel2 ).close();
        verifyNoMoreInteractions( channel1, channel2, fs );

        // And When
        cache.close();

        // Then
        verify( channel1 ).close();
        verifyNoMoreInteractions( channel1, channel2, fs );
    }

    @Test
    public void shouldRemoveEvictedPages() throws Exception
    {
        // Given
        StandardPageCache cache = newPageCache();
        PagedFile mappedFile = cache.map( storeFile, 1024 );
        PageCursor cursor = cache.newCursor();

        Thread evictionThread = new Thread( cache );
        evictionThread.start();

        // When I pin and unpin a series of pages
        for ( int i = 0; i < 128; i++ )
        {
            mappedFile.pin( cursor, PageLock.SHARED, i );
            mappedFile.unpin( cursor );
        }

        // Then
        Thread.sleep( 50 );
        assertThat( mappedFile.numberOfCachedPages(), equalTo( 61 ) );
        evictionThread.interrupt();
    }

    private StandardPageCache newPageCache() throws IOException
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        return new StandardPageCache( fs, 64, 1024 );
    }
}
