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
import static org.mockito.Mockito.when;

public class PagedIndexInputSlicingTest
{
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
    public void sliceShouldReportActualFileOffsetAsFilePointer()
            throws Exception
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
        PagedIndexInput subSlice = slice.slice( "Slice", 1, 8 );

        // Then
        assertEquals( 0, slice.getFilePointer() );
        assertEquals( 0, subSlice.getFilePointer() );
    }

    @Test
    public void shouldMoveFilePointerAsReadsAreDone() throws Exception
    {
        // Given
        PagedFile pagedFile = mock( PagedFile.class );
        when( pagedFile.pageSize() ).thenReturn( 4096 );
        when( pagedFile.io( anyLong(), anyInt() ) ).thenReturn( rootCursor,
                cloneCursors );
        when( cloneCursors[0].next( anyLong() ) ).thenReturn( true );
        PagedIndexInput root =
                new PagedIndexInput( "Root", pagedFile, 0, 4096 );

        // When/Then
        PagedIndexInput slice = root.slice( "Slice", 1337, 16 );
        assertEquals( 0, slice.getFilePointer() );

        // When/Then
        slice.readByte();
        assertEquals( 1, slice.getFilePointer() );

        // When/Then
        slice.readBytes( new byte[5], 0, 4 );
        assertEquals( 1 + 4, slice.getFilePointer() );

        // When/Then
        slice.seek( 0 );
        assertEquals( 0, slice.getFilePointer() );
    }
}
