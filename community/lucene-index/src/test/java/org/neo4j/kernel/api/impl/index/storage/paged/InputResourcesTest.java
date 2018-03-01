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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InputResourcesTest
{
    @Test
    public void shouldClosePagedFileEvenIfCursorFailsToClose() throws Exception
    {
        // Given
        PagedFile file = mock( PagedFile.class );
        FailsToClosePageCursor firstCursor = new FailsToClosePageCursor();
        when( file.io( anyLong(), anyInt() ) ).thenReturn( firstCursor );

        InputResources.RootInputResources resources = new InputResources.RootInputResources( file );
        PagedIndexInput rootInput = new PagedIndexInput( resources, "RootInput", 0, 16, 1337 );

        // When
        try
        {
            rootInput.close();
            fail( "Should have thrown IOException" );
        }
        catch ( IOException e )
        {
            assertEquals( e.getMessage(), "Emulated error to close.." );
        }

        // Then
        assertEquals( firstCursor.closeCalls, 1 );
        verify( file ).close();
    }

    @Test
    public void shouldCloseRootOnce() throws Exception
    {
        // Given
        PagedFile file = mock( PagedFile.class );
        CloseTrackingPageCursor cursor = new CloseTrackingPageCursor();
        when( file.io( anyLong(), anyInt() ) ).thenReturn( cursor );

        InputResources.RootInputResources resources = new InputResources.RootInputResources( file );

        PagedIndexInput rootInput = new PagedIndexInput( resources, "RootInput", 0, 16, 1337 );

        // When
        rootInput.close();

        // Then
        assertEquals( cursor.closeCalls, 1 );
        verify( file ).close();
    }

    @Test
    public void shouldHandleIndefiniteClonesCreatedAndTrashed() throws Exception
    {
        // Given
        int cursorBulkSize = 1024 * 1024;
        PagedFile file = mock( PagedFile.class );
        when( file.pageSize() ).thenReturn( 4096 );
        when( file.io( anyLong(), anyInt() ) ).then( call -> new BigBulkyPageCursor( cursorBulkSize ) );

        PagedIndexInput rootInput = new PagedIndexInput( "RootInput", file, 0, 1337 );

        // When we access more inputs than there is space on the heap, but don't retain references to them
        int blackHole = 0;
        for ( int i = 0; i < Runtime.getRuntime().maxMemory() / cursorBulkSize * 2; i++ )
        {
            blackHole *= rootInput.clone().hashCode();
        }

        // Then the test should pass without OOM
        System.out.println( blackHole );
    }

    static class BigBulkyPageCursor extends DelegatingPageCursor
    {
        private final byte[] bulk;

        BigBulkyPageCursor( int size )
        {
            super( mock( PageCursor.class ) );
            this.bulk = new byte[size];
        }

        @Override
        public int hashCode()
        {
            return super.hashCode() * System.identityHashCode( bulk );
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj == this;
        }
    }

    static class CloseTrackingPageCursor extends DelegatingPageCursor
    {
        int closeCalls;

        CloseTrackingPageCursor()
        {
            super( mock( PageCursor.class ) );
        }

        @Override
        public void close()
        {
            closeCalls++;
        }
    }

    static class FailsToClosePageCursor extends CloseTrackingPageCursor
    {
        @Override
        public void close()
        {
            super.close();
            throw new RuntimeException( "Emulated error to close.." );
        }
    }
}
