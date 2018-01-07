/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PagedIndexInputCloningTest
{
    // Lucene extensively uses #clone() and #slice() on inputs,
    // but it only properly closes the "root" inputs, the ones it
    // got from Directory#openInput(..). Hence, we need to ensure
    // resources held by slices and clones are cleaned up when the
    // roots are closed.

    private PageCursor rootCursor;
    private final PageCursor[] cloneCursors = new PageCursor[4];

    @Before
    public void setup()
    {
        rootCursor = mock( PageCursor.class );
        for ( int i = 0; i < cloneCursors.length; i++ )
        {
            cloneCursors[i] = mock( PageCursor.class );
        }
    }

    @Test
    public void shouldCloseCloneCursorOnRootClose() throws Exception
    {
        // Given
        PagedFile pagedFile = mock( PagedFile.class );
        when( pagedFile.pageSize() ).thenReturn( 4096 );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenReturn( rootCursor,
                cloneCursors );
        PagedIndexInput root =
                new PagedIndexInput( "Root", pagedFile, 0, 4096 );

        // When
        root.clone();
        root.close();

        // Then two cursors should have been allocated
        verify( pagedFile, times( 2 ) ).io( 0, PagedFile.PF_SHARED_READ_LOCK );

        // And both should be closed
        verify( rootCursor ).close();
        verify( cloneCursors[0] ).close();

        // And the paged file should be closed
        verify( pagedFile, times( 1 ) ).close();

        // And nothing more should be going on
        verifyNoMoreInteractions( rootCursor );
        verifyNoMoreInteractions( cloneCursors[0] );
    }

    @Test
    public void cloneOfASliceLooksLikeTheSlice() throws Exception
    {
        // Given
        PagedFile pagedFile = mock( PagedFile.class );
        when( pagedFile.pageSize() ).thenReturn( 4096 );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenReturn( rootCursor,
                cloneCursors );
        PagedIndexInput root =
                new PagedIndexInput( "Root", pagedFile, 0, 4096 );

        // When
        PagedIndexInput slice = root.slice( "Slice", 1337, 16 );
        slice.skipBytes( 7 );
        PagedIndexInput clone = slice.clone();

        // Then
        assertEquals( slice.toString(), clone.toString() );
        assertEquals( slice.getFilePointer(), clone.getFilePointer() );
        assertEquals( slice.length(), clone.length() );
    }

    @Test
    public void cloneOfRootLooksLikeTheRoot() throws Exception
    {
        // Given
        PagedFile pagedFile = mock( PagedFile.class );
        when( pagedFile.pageSize() ).thenReturn( 4096 );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenReturn( rootCursor,
                cloneCursors );
        PagedIndexInput root =
                new PagedIndexInput( "Root", pagedFile, 0, 4096 );

        // When
        root.skipBytes( 7 );
        PagedIndexInput clone = root.clone();

        // Then
        assertEquals( root.toString(), clone.toString() );
        assertEquals( root.getFilePointer(), clone.getFilePointer() );
        assertEquals( root.length(), clone.length() );
    }
}
