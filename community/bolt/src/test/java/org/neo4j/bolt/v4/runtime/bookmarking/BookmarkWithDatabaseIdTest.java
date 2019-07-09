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

package org.neo4j.bolt.v4.runtime.bookmarking;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId.fromParamsOrNull;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmarkMixture;

class BookmarkWithDatabaseIdTest
{
    @Test
    void shouldIgnoreSingleBookmark() throws Exception
    {
        var txId = 1234;
        var dbId = "molly";
        // no single bookmark anymore
        var params = singletonMap( "bookmark", new BookmarkWithDatabaseId( dbId, txId ).toString() );

        assertNull( fromParamsOrNull( params ) );
    }

    @Test
    void shouldFormatAndParseMultipleBookmarksContainingTransactionId() throws Exception
    {
        // given
        var txId1 = 1234;
        var txId2 = 12345;
        var dbId = "molly";
        var params = params( asList( new BookmarkWithDatabaseId( dbId, txId1 ).toString(), new BookmarkWithDatabaseId( dbId, txId2 ).toString() )
        );

        // when
        BookmarkWithDatabaseId bookmark = fromParamsOrNull( params );

        // then
        assertEquals( dbId, bookmark.databaseId() );
        assertEquals( txId2, bookmark.txId() );
    }

    @Test
    void shouldParseAndFormatMultipleBookmarkContainingTransactionId() throws Exception
    {
        // given
        var txId1 = "molly:1234";
        var txId2 = "molly:12345";
        var params = params( asList( txId1, txId2 ) );

        // when
        var actual = fromParamsOrNull( params ).toString();

        // then
        assertEquals( txId2, actual );
    }

    @Test
    void shouldFailWhenParsingBadlyFormattedBookmark()
    {
        var bookmarkString = "neo4j:1234";
        var wrongBookmarkString = "neo4j:1234:v9:xt998";

        var e = assertThrows( BookmarkParsingException.class,
                () -> fromParamsOrNull( params( asList( bookmarkString, wrongBookmarkString ) ) ) );

        assertThat( e.status(), equalTo( InvalidBookmark ) );
        assertTrue( e.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenProvidingBookmarkInOldFormat()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx10";

        var e = assertThrows( BookmarkParsingException.class,
                () -> fromParamsOrNull( params( List.of( bookmarkString ) ) ) );

        assertThat( e.status(), equalTo( InvalidBookmark ) );
        assertTrue( e.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenMixingOldFormatAndNewFormat()
    {
        var bookmarkString = "neo4j:1234";
        var wrongBookmarkString = "neo4j:bookmark:v1:tx10";

        var e = assertThrows( BookmarkParsingException.class,
                () -> fromParamsOrNull( params( asList( bookmarkString, wrongBookmarkString ) ) ) );
        assertThat( e.status(), equalTo( InvalidBookmark ) );
        assertTrue( e.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenMixingBookmarksFromDifferentDatabases()
    {
        var bookmarkString = "foo:1234";
        var wrongBookmarkString = "neo4j:1234";

        var e = assertThrows( BookmarkParsingException.class,
                () -> fromParamsOrNull( params( asList( bookmarkString, wrongBookmarkString ) ) ) );

        assertThat( e.status(), equalTo( InvalidBookmarkMixture ) );
        assertTrue( e.causesFailureMessage() );
    }

    @Test
    void shouldReturnNullWhenGivenBothSingleAndEmptyListAsMultiple() throws Exception
    {
        var params = params( emptyList() );

        var bookmark = fromParamsOrNull( params );

        assertNull( bookmark );
    }

    @Test
    void shouldReturnNullWhenNoBookmarks() throws Exception
    {
        assertNull( fromParamsOrNull( VirtualValues.EMPTY_MAP ) );
    }

    @Test
    void shouldSkipNullsInMultipleBookmarks() throws Exception
    {
        var params = params( asList( "neo4j:3", "neo4j:5", null, "neo4j:17" ) );

        var bookmark = fromParamsOrNull( params );

        assertEquals( 17, bookmark.txId() );
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAList()
    {
        var params = params( new String[]{"neo4j:68"} );

        var e = assertThrows( BookmarkParsingException.class, () -> fromParamsOrNull( params ) );

        assertThat( e.status(), equalTo( InvalidBookmark ) );
        assertTrue( e.causesFailureMessage() );
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAListOfStrings()
    {
        var params = params( asList( new String[]{"neo4j:50"}, new Object[]{"neo4j:89"} ) );

        var e = assertThrows( BookmarkParsingException.class, () -> fromParamsOrNull( params ) );

        assertThat( e.status(), equalTo( InvalidBookmark ) );
        assertTrue( e.causesFailureMessage() );
    }

    private static MapValue params( Object bookmarks )
    {
        var builder = new MapValueBuilder();
        builder.add( "bookmarks", ValueUtils.of( bookmarks ) );
        return builder.build();
    }

    private static MapValue singletonMap( String key, Object value )
    {
        return VirtualValues.map( new String[]{key}, new AnyValue[]{ValueUtils.of( value )} );
    }
}

