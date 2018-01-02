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
package org.neo4j.io.pagecache.checking;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccessCheckingPageCacheTest
{
    private PageCache pageCache;
    private PageCursor cursor;

    @Before
    public void getPageCursor() throws IOException
    {
        PageCache mockedPageCache = mock( PageCache.class );
        PagedFile mockedPagedFile = mock( PagedFile.class );
        PageCursor mockedCursor = mock( PageCursor.class );
        when( mockedPagedFile.io( anyLong(), anyInt() ) ).thenReturn( mockedCursor );
        when( mockedPageCache.map( any( File.class ), anyInt(), anyVararg() ) ).thenReturn( mockedPagedFile );
        pageCache = new AccessCheckingPageCache( mockedPageCache );
        PagedFile file = pageCache.map( new File( "some file" ), 512 );
        cursor = file.io( 0, PagedFile.PF_SHARED_READ_LOCK );
    }

    @Test
    public void shouldGrant_read_shouldRetry_close() throws Exception
    {
        // GIVEN
        cursor.getByte();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.close();
    }

    @Test
    public void shouldGrant_read_shouldRetry_next() throws Exception
    {
        // GIVEN
        cursor.getByte( 0 );

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next();
    }

    @Test
    public void shouldGrant_read_shouldRetry_next_with_id() throws Exception
    {
        // GIVEN
        cursor.getShort();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next( 1 );
    }

    @Test
    public void shouldGrant_read_shouldRetry_read_shouldRetry_close() throws Exception
    {
        // GIVEN
        cursor.getShort( 0 );
        cursor.shouldRetry();
        cursor.getInt();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.close();
    }

    @Test
    public void shouldGrant_read_shouldRetry_read_shouldRetry_next() throws Exception
    {
        // GIVEN
        cursor.getInt( 0 );
        cursor.shouldRetry();
        cursor.getLong();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next();
    }

    @Test
    public void shouldGrant_read_shouldRetry_read_shouldRetry_next_with_id() throws Exception
    {
        // GIVEN
        cursor.getLong( 0 );
        cursor.shouldRetry();
        cursor.getBytes( new byte[2] );

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next( 1 );
    }

    @Test
    public void shouldFail_read_close() throws Exception
    {
        // GIVEN
        cursor.getByte();

        try
        {
            // WHEN
            cursor.close();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "shouldRetry" ) );
        }
    }

    @Test
    public void shouldFail_read_next() throws Exception
    {
        // GIVEN
        cursor.getByte( 0 );

        try
        {
            // WHEN
            cursor.next();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "shouldRetry" ) );
        }
    }

    @Test
    public void shouldFail_read_next_with_id() throws Exception
    {
        // GIVEN
        cursor.getShort();

        try
        {
            // WHEN
            cursor.next( 1 );
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "shouldRetry" ) );
        }
    }

    @Test
    public void shouldFail_read_shouldRetry_read_close() throws Exception
    {
        // GIVEN
        cursor.getShort( 0 );
        cursor.shouldRetry();
        cursor.getInt();

        try
        {
            // WHEN
            cursor.close();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "shouldRetry" ) );
        }
    }

    @Test
    public void shouldFail_read_shouldRetry_read_next() throws Exception
    {
        // GIVEN
        cursor.getInt( 0 );
        cursor.shouldRetry();
        cursor.getLong();

        try
        {
            // WHEN
            cursor.next();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "shouldRetry" ) );
        }
    }

    @Test
    public void shouldFail_read_shouldRetry_read_next_with_id() throws Exception
    {
        // GIVEN
        cursor.getLong( 0 );
        cursor.shouldRetry();
        cursor.getBytes( new byte[2] );

        try
        {
            // WHEN
            cursor.next( 1 );
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "shouldRetry" ) );
        }
    }
}
