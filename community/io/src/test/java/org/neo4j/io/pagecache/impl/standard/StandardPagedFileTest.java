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

import java.io.IOException;

import org.junit.Test;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageIO;
import org.neo4j.io.pagecache.PageLock;

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
    private final PageIO pageIO = mock( PageIO.class );
    private final StandardPageSwapper swapper = new StandardPageSwapper( null, channel, 512, null );

    @Test
    public void shouldLoadPage() throws Exception
    {
        // Given
        when( table.load( swapper, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( swapper, 12, PageLock.SHARED ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile(table, null, channel, 512, PageCacheMonitor.NULL );

        // When
        try ( PageCursor cursor = file.io( 12, PF_SHARED_LOCK, pageIO, 13, 14 ) )
        {
            cursor.next();
        }

        // Then
        verify( table ).load( swapper, 12, PageLock.SHARED );
        verify( pageIO ).apply( 12, page, 13, 14 );
    }

    @Test
    public void shouldUnpinWithCorrectLockType() throws Exception
    {
        // Given
        when( table.load( swapper, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( swapper, 12, PageLock.SHARED ) ).thenReturn( true );
        when( swapper.getLastPageId() ).thenReturn( 512L );

        StandardPagedFile file = new StandardPagedFile(table, null, channel, 512, PageCacheMonitor.NULL );

        // When
        try ( PageCursor cursor = file.io( 12, PF_SHARED_LOCK, pageIO, 13, 14 ) )
        {
            cursor.next();
        }

        // Then
        verify( page ).unpin( PageLock.SHARED );
    }

    @Test( expected = IOException.class )
    public void shouldThrowIfNoLockSpecified() throws Exception
    {
        // Given
        when( table.load( swapper, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( swapper, 12, PageLock.SHARED ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile(table, null, channel, 512, PageCacheMonitor.NULL );

        // When
        try ( PageCursor cursor = file.io( 12, 0, pageIO, 13, 14 ) )
        {
            cursor.next();
        }
    }

    @Test( expected = IOException.class )
    public void shouldThrowIfSpecifyingBothSharedAndExclusiveLock() throws IOException
    {
        // Given
        when( table.load( swapper, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( swapper, 12, PageLock.SHARED ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile( table, null, channel, 512, PageCacheMonitor.NULL );

        // When
        int pf_flags = PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK;
        try ( PageCursor cursor = file.io( 12, pf_flags, pageIO , 13, 14 ) )
        {
            cursor.next();
        }
    }
}
