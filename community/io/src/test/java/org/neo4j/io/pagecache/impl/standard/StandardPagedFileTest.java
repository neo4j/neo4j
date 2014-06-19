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
import org.neo4j.io.pagecache.PageLock;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.*;

public class StandardPagedFileTest
{

    private final StandardPageCursor cursor = new StandardPageCursor();
    private final PageTable table = mock(PageTable.class);
    private final PinnablePage page = mock( PinnablePage.class );
    private final StoreChannel channel = mock( StoreChannel.class);
    private final StandardPageIO io = new StandardPageIO( null, channel, 512, null );

    @Test
    public void shouldLoadPage() throws Exception
    {
        // Given
        when( table.load( io, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( io, 12, PageLock.SHARED ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile(table, null, channel, 512, PageCacheMonitor.NULL );

        // When
        file.pin( cursor, PageLock.SHARED, 12 );

        // Then
        verify(table).load( io, 12, PageLock.SHARED );
        assertThat(cursor.page(), equalTo( page ));
    }

    @Test
    public void shouldUnpinWithCorrectLockType() throws Exception
    {
        // Given
        when( table.load( io, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( io, 12, PageLock.SHARED ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile(table, null, channel, 512, PageCacheMonitor.NULL );

        // When
        file.pin( cursor, PageLock.SHARED, 12 );
        file.unpin( cursor );

        // Then
        verify(page).unpin( PageLock.SHARED );
    }

    @Test
    public void shouldThrowIfCursorIsAlreadyUsed() throws Exception
    {
        // Given
        when( table.load( io, 12, PageLock.SHARED ) ).thenReturn( page );
        when( page.pin( io, 12, PageLock.SHARED ) ).thenReturn( true );

        StandardPagedFile file = new StandardPagedFile(table, null, channel, 512, PageCacheMonitor.NULL );

        // And given I've pinned a page already
        file.pin( cursor, PageLock.SHARED, 12 );

        // When
        try
        {
            file.pin( cursor, PageLock.SHARED, 12 );
            fail("Should have thrown when re-using an active cursor");

        // Then
        } catch(IOException e)
        {
            assertThat(e.getMessage(), equalTo("The cursor is already in use, you need to unpin the cursor before using it again."));
        }
    }
}
