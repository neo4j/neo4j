/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.bookmarking;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class BookmarkTest
{
    @Test
    public void shouldFormatAndParseSingleBookmarkContainingTransactionId() throws Exception
    {
        // given
        long txId = 1234;
        Map<String,Object> params = singletonMap( "bookmark", new Bookmark( txId ).toString() );

        // when
        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        // then
        assertEquals( txId, bookmark.txId() );
    }

    @Test
    public void shouldFormatAndParseMultipleBookmarksContainingTransactionId() throws Exception
    {
        // given
        long txId1 = 1234;
        long txId2 = 12345;
        Map<String,Object> params = singletonMap( "bookmarks",
                asList( new Bookmark( txId1 ).toString(), new Bookmark( txId2 ).toString() )
        );

        // when
        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        // then
        assertEquals( txId2, bookmark.txId() );
    }

    @Test
    public void shouldParseAndFormatSingleBookmarkContainingTransactionId() throws Exception
    {
        // given
        String expected = "neo4j:bookmark:v1:tx1234";
        Map<String,Object> params = singletonMap( "bookmark", expected );

        // when
        String actual = new Bookmark( Bookmark.fromParamsOrNull( params ).txId() ).toString();

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldParseAndFormatMultipleBookmarkContainingTransactionId() throws Exception
    {
        // given
        String txId1 = "neo4j:bookmark:v1:tx1234";
        String txId2 = "neo4j:bookmark:v1:tx12345";
        Map<String,Object> params = singletonMap( "bookmarks", asList( txId1, txId2 ) );

        // when
        String actual = new Bookmark( Bookmark.fromParamsOrNull( params ).txId() ).toString();

        // then
        assertEquals( txId2, actual );
    }

    @Test
    public void shouldFailWhenParsingBadlyFormattedSingleBookmark() throws Exception
    {
        // given
        String bookmarkString = "neo4q:markbook:v9:xt998";

        // when
        try
        {
            Bookmark.fromParamsOrNull( singletonMap( "bookmark", bookmarkString ) );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenParsingBadlyFormattedMultipleBookmarks() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx998";
        String wrongBookmarkString = "neo4q:markbook:v9:xt998";

        // when
        try
        {
            Bookmark.fromParamsOrNull( singletonMap( "bookmarks", asList( bookmarkString, wrongBookmarkString ) ) );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenNoNumberFollowsThePrefixInSingleBookmark() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx";

        // when
        try
        {
            Bookmark.fromParamsOrNull( singletonMap( "bookmark", bookmarkString ) );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenNoNumberFollowsThePrefixInMultipleBookmarks() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx10";
        String wrongBookmarkString = "neo4j:bookmark:v1:tx";

        // when
        try
        {
            Bookmark.fromParamsOrNull( singletonMap( "bookmarks", asList( bookmarkString, wrongBookmarkString ) ) );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenSingleBookmarkHasExtraneousTrailingCharacters() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx1234supercalifragilisticexpialidocious";

        // when
        try
        {
            Bookmark.fromParamsOrNull( singletonMap( "bookmark", bookmarkString ) );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenMultipleBookmarksHaveExtraneousTrailingCharacters() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx1234";
        String wrongBookmarkString = "neo4j:bookmark:v1:tx1234supercalifragilisticexpialidocious";

        // when
        try
        {
            Bookmark.fromParamsOrNull( singletonMap( "bookmarks", asList( bookmarkString, wrongBookmarkString ) ) );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldUseMultipleBookmarksWhenGivenBothSingleAndMultiple() throws Exception
    {
        Map<String,Object> params = params(
                "neo4j:bookmark:v1:tx42",
                asList( "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx99", "neo4j:bookmark:v1:tx3" ) );

        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        assertEquals( 99, bookmark.txId() );
    }

    @Test
    public void shouldUseMultipleBookmarksWhenGivenOnlyMultiple() throws Exception
    {
        Map<String,Object> params = params( null, asList( "neo4j:bookmark:v1:tx85", "neo4j:bookmark:v1:tx47",
                "neo4j:bookmark:v1:tx15", "neo4j:bookmark:v1:tx6" ) );

        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        assertEquals( 85, bookmark.txId() );
    }

    @Test
    public void shouldUseSingleBookmarkWhenGivenOnlySingle() throws Exception
    {
        Map<String,Object> params = params( "neo4j:bookmark:v1:tx82", null );

        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        assertEquals( 82, bookmark.txId() );
    }

    @Test
    public void shouldUseSingleBookmarkWhenGivenBothSingleAndNullAsMultiple() throws Exception
    {
        Map<String,Object> params = params( "neo4j:bookmark:v1:tx58", null );

        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        assertEquals( 58, bookmark.txId() );
    }

    @Test
    public void shouldUseSingleBookmarkWhenGivenBothSingleAndEmptyListAsMultiple() throws Exception
    {
        Map<String,Object> params = params( "neo4j:bookmark:v1:tx67", emptyList() );

        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        assertEquals( 67, bookmark.txId() );
    }

    @Test
    public void shouldThrowWhenMultipleBookmarksIsNotAList() throws Exception
    {
        Map<String,Object> params = params( "neo4j:bookmark:v1:tx67", new String[]{"neo4j:bookmark:v1:tx68"} );

        try
        {
            Bookmark.fromParamsOrNull( params );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( Bookmark.BookmarkFormatException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenMultipleBookmarksIsNotAListOfStrings() throws Exception
    {
        Map<String,Object> params = params(
                "neo4j:bookmark:v1:tx67",
                asList( new String[]{"neo4j:bookmark:v1:tx50"}, new Object[]{"neo4j:bookmark:v1:tx89"} ) );

        try
        {
            Bookmark.fromParamsOrNull( params );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( Bookmark.BookmarkFormatException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenOneOfMultipleBookmarksIsMalformed()
    {
        Map<String,Object> params = params(
                "neo4j:bookmark:v1:tx67",
                asList( "neo4j:bookmark:v1:tx99", "neo4j:bookmark:v1:tx12", "neo4j:bookmark:www:tx99" ) );

        try
        {
            Bookmark.fromParamsOrNull( params );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( Bookmark.BookmarkFormatException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenSingleBookmarkIsMalformed()
    {
        Map<String,Object> params = params( "neo4j:strange-bookmark:v1:tx6", null );

        try
        {
            Bookmark.fromParamsOrNull( params );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( Bookmark.BookmarkFormatException.class ) );
        }
    }

    @Test
    public void shouldReturnNullWhenNoBookmarks() throws Exception
    {
        assertNull( Bookmark.fromParamsOrNull( emptyMap() ) );
    }

    @Test
    public void shouldReturnNullWhenGivenEmptyListForMultipleBookmarks() throws Exception
    {
        Map<String,Object> params = params( null, emptyList() );
        assertNull( Bookmark.fromParamsOrNull( params ) );
    }

    @Test
    public void shouldSkipNullsInMultipleBookmarks() throws Exception
    {
        Map<String,Object> params = params( null,
                asList( "neo4j:bookmark:v1:tx3", "neo4j:bookmark:v1:tx5", null, "neo4j:bookmark:v1:tx17" ) );

        Bookmark bookmark = Bookmark.fromParamsOrNull( params );

        assertEquals( 17, bookmark.txId() );
    }

    private static Map<String,Object> params( String bookmark, Object bookmarks )
    {
        Map<String,Object> result = new HashMap<>();
        if ( bookmark != null )
        {
            result.put( "bookmark", bookmark );
        }
        result.put( "bookmarks", bookmarks );
        return result;
    }
}
