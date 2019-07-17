/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookmarksParserV1Test
{
    @Test
    void shouldParseSingleBookmarkContainingTransactionId() throws Exception
    {
        var txId = 1234;
        var metadata = singletonMap( "bookmark", bookmarkString( txId ) );

        var bookmark = parse( metadata );

        assertEquals( txId, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( txId ), bookmark );
    }

    @Test
    void shouldParseMultipleBookmarksContainingTransactionId() throws Exception
    {
        var txId1 = 1234;
        var txId2 = 12345;
        var metadata = singletonMap( "bookmarks", List.of( bookmarkString( txId1 ), bookmarkString( txId2 ) ) );

        var bookmark = parse( metadata );

        assertEquals( txId2, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( txId2 ), bookmark );
    }

    @Test
    void shouldFailWhenParsingBadlyFormattedSingleBookmark()
    {
        var bookmarkString = "neo4q:markbook:v9:xt998";

        var error = assertThrows( BookmarkFormatException.class,
                () -> parse( singletonMap( "bookmark", bookmarkString ) ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenParsingBadlyFormattedMultipleBookmarks()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx998";
        var wrongBookmarkString = "neo4q:markbook:v9:xt998";

        var error = assertThrows( BookmarkFormatException.class,
                () -> parse( singletonMap( "bookmarks", List.of( bookmarkString, wrongBookmarkString ) ) ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenNoNumberFollowsThePrefixInSingleBookmark()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx";

        var error = assertThrows( BookmarkFormatException.class,
                () -> parse( singletonMap( "bookmark", bookmarkString ) ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenNoNumberFollowsThePrefixInMultipleBookmarks()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx10";
        var wrongBookmarkString = "neo4j:bookmark:v1:tx";

        var error = assertThrows( BookmarkFormatException.class,
                () -> parse( singletonMap( "bookmarks", List.of( bookmarkString, wrongBookmarkString ) ) ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenSingleBookmarkHasExtraneousTrailingCharacters()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx1234supercalifragilisticexpialidocious";

        var error = assertThrows( BookmarkFormatException.class,
                () -> parse( singletonMap( "bookmark", bookmarkString ) ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenMultipleBookmarksHaveExtraneousTrailingCharacters()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx1234";
        var wrongBookmarkString = "neo4j:bookmark:v1:tx1234supercalifragilisticexpialidocious";

        var error = assertThrows( BookmarkFormatException.class,
                () -> parse( singletonMap( "bookmarks", List.of( bookmarkString, wrongBookmarkString ) ) ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldUseMultipleBookmarksWhenGivenBothSingleAndMultiple() throws Exception
    {
        var metadata = metadata(
                "neo4j:bookmark:v1:tx42",
                List.of( "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx99", "neo4j:bookmark:v1:tx3" ) );

        var bookmark = parse( metadata );

        assertEquals( 99, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( 99 ), bookmark );
    }

    @Test
    void shouldUseMultipleBookmarksWhenGivenOnlyMultiple() throws Exception
    {
        var metadata = metadata( null,
                List.of( "neo4j:bookmark:v1:tx85", "neo4j:bookmark:v1:tx47", "neo4j:bookmark:v1:tx15", "neo4j:bookmark:v1:tx6" ) );

        var bookmark = parse( metadata );

        assertEquals( 85, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( 85 ), bookmark );
    }

    @Test
    void shouldUseSingleBookmarkWhenGivenOnlySingle() throws Exception
    {
        var metadata = metadata( "neo4j:bookmark:v1:tx82", null );

        var bookmark = parse( metadata );

        assertEquals( 82, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( 82 ), bookmark );
    }

    @Test
    void shouldUseSingleBookmarkWhenGivenBothSingleAndNullAsMultiple() throws Exception
    {
        var metadata = metadata( "neo4j:bookmark:v1:tx58", null );

        var bookmark = parse( metadata );

        assertEquals( 58, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( 58 ), bookmark );
    }

    @Test
    void shouldUseSingleBookmarkWhenGivenBothSingleAndEmptyListAsMultiple() throws Exception
    {
        var metadata = metadata( "neo4j:bookmark:v1:tx67", emptyList() );

        var bookmark = parse( metadata );

        assertEquals( 67, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( 67 ), bookmark );
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAList()
    {
        var metadata = metadata( "neo4j:bookmark:v1:tx67", new String[]{"neo4j:bookmark:v1:tx68"} );

        var error = assertThrows( BookmarkFormatException.class, () -> parse( metadata ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAListOfStrings()
    {
        var metadata = metadata(
                "neo4j:bookmark:v1:tx67",
                List.of( new String[]{"neo4j:bookmark:v1:tx50"}, new Object[]{"neo4j:bookmark:v1:tx89"} ) );

        var error = assertThrows( BookmarkFormatException.class, () -> parse( metadata ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldThrowWhenOneOfMultipleBookmarksIsMalformed()
    {
        var metadata = metadata(
                "neo4j:bookmark:v1:tx67",
                List.of( "neo4j:bookmark:v1:tx99", "neo4j:bookmark:v1:tx12", "neo4j:bookmark:www:tx99" ) );

        var error = assertThrows( BookmarkFormatException.class, () -> parse( metadata ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldThrowWhenSingleBookmarkIsMalformed()
    {
        var metadata = metadata( "neo4j:strange-bookmark:v1:tx6", null );

        var error = assertThrows( BookmarkFormatException.class, () -> parse( metadata ) );

        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldReturnEmptyListWhenNoBookmarks() throws Exception
    {
        assertEquals( List.of(), BookmarksParserV1.INSTANCE.parseBookmarks( VirtualValues.EMPTY_MAP ) );
    }

    @Test
    void shouldReturnEmptyListWhenGivenEmptyListForMultipleBookmarks() throws Exception
    {
        var metadata = metadata( null, emptyList() );

        assertEquals( List.of(), BookmarksParserV1.INSTANCE.parseBookmarks( metadata ) );
    }

    @Test
    void shouldSkipNullsInMultipleBookmarks() throws Exception
    {
        var metadata = metadata( null,
                Arrays.asList( "neo4j:bookmark:v1:tx3", "neo4j:bookmark:v1:tx5", null, "neo4j:bookmark:v1:tx17" ) );

        var bookmark = parse( metadata );

        assertEquals( 17, bookmark.txId() );
        assertEquals( new BookmarkWithPrefix( 17 ), bookmark );
    }

    private static BookmarkWithPrefix parse( MapValue metadata ) throws BookmarkFormatException
    {
        var bookmarks = BookmarksParserV1.INSTANCE.parseBookmarks( metadata );
        assertThat( bookmarks, hasSize( 1 ) );
        var bookmark = bookmarks.get( 0 );
        assertThat( bookmark, instanceOf( BookmarkWithPrefix.class ) );
        return (BookmarkWithPrefix) bookmark;
    }

    private static MapValue metadata( String bookmark, Object bookmarks )
    {
        var builder = new MapValueBuilder();
        if ( bookmark != null )
        {
            builder.add( "bookmark", ValueUtils.of( bookmark ) );
        }
        builder.add( "bookmarks", ValueUtils.of( bookmarks ) );
        return builder.build();
    }

    private static MapValue singletonMap( String key, Object value )
    {
        return VirtualValues.map( new String[]{key}, new AnyValue[]{ValueUtils.of( value )} );
    }

    private static String bookmarkString( long txId )
    {
        return new BookmarkWithPrefix( txId ).toString();
    }
}
