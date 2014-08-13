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
package org.neo4j.io.pagecache.impl.standard;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

public class StandardPagedFileTest
{

    private final PageTable table = mock( PageTable.class );
    private final PinnablePage page = mock( PinnablePage.class );
    private final StoreChannel channel = mock( StoreChannel.class);
    private SingleFilePageSwapper swapper = new SingleFilePageSwapper( null, channel, 512, null );
    private final PageSwapperFactory swapperFactory = new PageSwapperFactory()
    {
        @Override
        public PageSwapper createPageSwapper( File file, int filePageSize, PageEvictionCallback onEviction ) throws IOException
        {
            return swapper;
        }
    };

    @Test
    public void shouldLoadPage() throws Exception
    {
        // Given
        int filePageSize = 28;
        long fileSize = 2048L;
        long pageId = 12;
        swapper = new SingleFilePageSwapper( null, channel, filePageSize, null );

        when( table.load( swapper, pageId, PagedFile.PF_NO_GROW | PagedFile.PF_SHARED_LOCK ) ).thenReturn( page );
        when( page.pin( swapper, pageId, PagedFile.PF_SHARED_LOCK ) ).thenReturn( true );
        when( page.pageId() ).thenReturn( pageId );
        when( channel.size() ).thenReturn( fileSize );

        StandardPagedFile file = new StandardPagedFile( table, null, swapperFactory, filePageSize, PageCacheMonitor.NULL );

        // When
        try ( PageCursor cursor = file.io( pageId, PF_SHARED_LOCK ) )
        {
            // Then
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( pageId ) );
        }

        // And then
        verify( table ).load( swapper, pageId, PagedFile.PF_NO_GROW | PagedFile.PF_SHARED_LOCK );
    }

    @Test
    public void shouldUnpinWithCorrectLockType() throws Exception
    {
        // Given
        when( table.load( swapper, 12, PagedFile.PF_EXCLUSIVE_LOCK ) ).thenReturn( page );
        when( page.pin( swapper, 12, PagedFile.PF_EXCLUSIVE_LOCK ) ).thenReturn( true );
        when( page.pageId() ).thenReturn( 12L );
        when( swapper.getLastPageId() ).thenReturn( 512L );

        StandardPagedFile file = new StandardPagedFile(table, null, swapperFactory, 512, PageCacheMonitor.NULL );

        // When
        try ( PageCursor cursor = file.io( 12, PF_EXCLUSIVE_LOCK ) )
        {
            // Then
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 12L ) );
        }
        verify( page ).unpin( PagedFile.PF_EXCLUSIVE_LOCK );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfNoLockSpecified() throws Exception
    {
        // Given
        when( table.load( swapper, 12, PagedFile.PF_SHARED_LOCK ) ).thenReturn( page );
        when( page.pin( swapper, 12, PagedFile.PF_SHARED_LOCK ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile(table, null, swapperFactory, 512, PageCacheMonitor.NULL );

        // When
        try ( PageCursor cursor = file.io( 12, 0 ) )
        {
            // Then
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 12L ) );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfSpecifyingBothSharedAndExclusiveLock() throws IOException
    {
        // Given
        when( table.load( swapper, 12, PagedFile.PF_SHARED_LOCK ) ).thenReturn( page );
        when( page.pin( swapper, 12, PagedFile.PF_SHARED_LOCK ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile( table, null, swapperFactory, 512, PageCacheMonitor.NULL );

        // When
        int pf_flags = PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK;
        try ( PageCursor cursor = file.io( 12, pf_flags ) )
        {
            // Then
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 12L ) );
        }
    }
}
