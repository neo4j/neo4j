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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmarkMixture;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

class BookmarksParserV4Test
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final BookmarksParser parser = new BookmarksParserV4( databaseIdRepository );

    @Test
    void shouldIgnoreSingleBookmarkMetadata() throws Exception
    {
        var txId = 1234;
        var dbId = databaseIdRepository.getRaw( "molly" );
        var metadata = singletonMap( "bookmark", bookmarkString( txId, dbId ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of(), bookmarks );
    }

    @Test
    void shouldParseMultipleFormattedBookmarksContainingTransactionId() throws Exception
    {
        var txId1 = 1234;
        var txId2 = 12345;
        var dbId = databaseIdRepository.getRaw( "molly" );
        var metadata = metadata( List.of( bookmarkString( txId1, dbId ), bookmarkString( txId2, dbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( txId2, dbId ) ), bookmarks );
    }

    @Test
    void shouldParseMultipleBookmarksContainingTransactionId() throws Exception
    {
        var dbId = databaseIdRepository.getRaw( "molly" );
        var bookmark1 = bookmarkString( 1234, dbId );
        var bookmark2 = bookmarkString( 12345, dbId );
        var metadata = metadata( List.of( bookmark1, bookmark2 ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 12345, databaseIdRepository.getRaw( "molly" ) ) ), bookmarks );
    }

    @Test
    void shouldFailWhenParsingBadlyFormattedBookmark()
    {
        var bookmarkString = bookmarkString();
        var wrongBookmarkString = "neo4j:1234:v9:xt998";

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( bookmarkString, wrongBookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenProvidingBookmarkInOldFormat()
    {
        var bookmarkString = "neo4j:bookmark:v1:tx10";

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( bookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenMixingOldFormatAndNewFormat()
    {
        var bookmarkString = bookmarkString();
        var wrongBookmarkString = "neo4j:bookmark:v1:tx10";

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( bookmarkString, wrongBookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenMixingBookmarksFromDifferentDatabases()
    {
        var bookmarkString = bookmarkString( 1234, databaseIdRepository.getRaw( "foo" ) );
        var wrongBookmarkString = bookmarkString( 1234, databaseIdRepository.getRaw( "neo4j" ) );

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( bookmarkString, wrongBookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmarkMixture ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldFailWhenBookmarkForUnknownDatabase()
    {
        var unknownDatabaseIdRepo = new DatabaseIdRepository()
        {
            @Override
            public Optional<DatabaseId> getByName( NormalizedDatabaseName databaseName )
            {
                return Optional.empty();
            }

            @Override
            public Optional<DatabaseId> getByUuid( UUID uuid )
            {
                return Optional.empty();
            }
        };

        var parser = new BookmarksParserV4( unknownDatabaseIdRepo );

        var bookmarkString = bookmarkString();

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( bookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldReturnNoBookmarksWhenGivenBothSingleAndEmptyListAsMultiple() throws Exception
    {
        var metadata = metadata( emptyList() );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of(), bookmarks );
    }

    @Test
    void shouldReturnNullWhenNoBookmarks() throws Exception
    {
        assertEquals( List.of(), parser.parseBookmarks( VirtualValues.EMPTY_MAP ) );
    }

    @Test
    void shouldSkipNullsInMultipleBookmarks() throws Exception
    {
        var dbId = databaseIdRepository.getRaw( "neo4j" );
        var metadata = metadata( Arrays.asList( bookmarkString( 3, dbId ), bookmarkString( 5, dbId ), null, bookmarkString( 17, dbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 17, databaseIdRepository.getRaw( "neo4j" ) ) ), bookmarks );
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAList()
    {
        var metadata = metadata( new String[]{bookmarkString()} );

        var error = assertThrows( BookmarkParsingException.class, () -> parser.parseBookmarks( metadata ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAListOfStrings()
    {
        var metadata = metadata( List.of( new String[]{bookmarkString()}, new Object[]{bookmarkString()} ) );

        var error = assertThrows( BookmarkParsingException.class, () -> parser.parseBookmarks( metadata ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldParseSingleSystemDbBookmark() throws Exception
    {
        var systemDbId = SYSTEM_DATABASE_ID;
        var metadata = metadata( List.of( bookmarkString( 42, systemDbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 42, systemDbId ) ), bookmarks );
    }

    @Test
    void shouldParseMultipleSystemDbBookmarks() throws Exception
    {
        var systemDbId = SYSTEM_DATABASE_ID;
        var metadata = metadata( List.of( bookmarkString( 42, systemDbId ), bookmarkString( 1, systemDbId ), bookmarkString( 39, systemDbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 42, systemDbId ) ), bookmarks );
    }

    @Test
    void shouldParseSingleSystemAndSingleUserDbBookmarks() throws Exception
    {
        var systemDbId = SYSTEM_DATABASE_ID;
        var userDbId = databaseIdRepository.getRaw( "foo" );
        var metadata = metadata( List.of( bookmarkString( 33, systemDbId ), bookmarkString( 22, userDbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 33, systemDbId ), new BookmarkWithDatabaseId( 22, userDbId ) ), bookmarks );
    }

    @Test
    void shouldParseMultipleSystemAndSingleUserDbBookmarks() throws Exception
    {
        var systemDbId = SYSTEM_DATABASE_ID;
        var userDbId = databaseIdRepository.getRaw( "foo" );
        var metadata = metadata( List.of( bookmarkString( 33, systemDbId ), bookmarkString( 9, userDbId ), bookmarkString( 44, systemDbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 44, systemDbId ), new BookmarkWithDatabaseId( 9, userDbId ) ), bookmarks );
    }

    @Test
    void shouldParseMultipleSystemAndMultipleUserDbBookmarks() throws Exception
    {
        var systemDbId = SYSTEM_DATABASE_ID;
        var userDbId = databaseIdRepository.getRaw( "foo" );
        var metadata = metadata( List.of(
                bookmarkString( 12, systemDbId ), bookmarkString( 69, userDbId ), bookmarkString( 83, systemDbId ), bookmarkString( 17, userDbId ) ) );

        var bookmarks = parser.parseBookmarks( metadata );

        assertEquals( List.of( new BookmarkWithDatabaseId( 83, systemDbId ), new BookmarkWithDatabaseId( 69, userDbId ) ), bookmarks );
    }

    @Test
    void shouldErrorWhenDatabaseIdContainsInvalidUuid() throws Exception
    {
        var wrongBookmarkString = "neo4j:1234";

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( wrongBookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    @Test
    void shouldErrorWhenDatabaseIdContainsInvalidTxId() throws Exception
    {
        var databaseId = databaseIdRepository.getRaw( "foo" );
        var wrongBookmarkString = String.format( "%s:neo4j", databaseId.uuid() );

        var error = assertThrows( BookmarkParsingException.class,
                () -> parser.parseBookmarks( metadata( List.of( wrongBookmarkString ) ) ) );

        assertThat( error.status(), equalTo( InvalidBookmark ) );
        assertTrue( error.causesFailureMessage() );
    }

    private static MapValue metadata( Object bookmarks )
    {
        return singletonMap( "bookmarks", bookmarks );
    }

    private static MapValue singletonMap( String key, Object value )
    {
        var builder = new MapValueBuilder();
        builder.add( key, ValueUtils.of( value ) );
        return builder.build();
    }

    private static String bookmarkString( long txId, DatabaseId databaseId )
    {
        return new BookmarkWithDatabaseId( txId, databaseId ).toString();
    }

    /**
     * Create a random bookmark
     */
    private String bookmarkString()
    {
        var dbId = databaseIdRepository.getRaw( "molly" );
        return bookmarkString( 123, dbId );
    }
}
