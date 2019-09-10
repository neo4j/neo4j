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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkError;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkForUnknownDatabaseError;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkMixtureError;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidSingleBookmarkError;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public final class BookmarksParserV4 implements BookmarksParser
{
    private static final String BOOKMARKS_KEY = "bookmarks";
    private static final long ABSENT_BOOKMARK_ID = -1L;

    private final DatabaseIdRepository databaseIdRepository;
    private final CustomBookmarkFormatParser customBookmarkFormatParser;

    public BookmarksParserV4( DatabaseIdRepository databaseIdRepository, CustomBookmarkFormatParser customBookmarkFormatParser )
    {
        this.databaseIdRepository = databaseIdRepository;
        this.customBookmarkFormatParser = customBookmarkFormatParser;
    }

    @Override
    public List<Bookmark> parseBookmarks( MapValue metadata ) throws BoltIOException
    {
        var bookmarksObject = metadata.get( BOOKMARKS_KEY );

        if ( bookmarksObject == Values.NO_VALUE )
        {
            return List.of();
        }
        else if ( bookmarksObject instanceof ListValue )
        {
            return parseBookmarks( (ListValue) bookmarksObject );
        }
        else
        {
            throw newInvalidBookmarkError( String.format( "Supplied bookmarks '%s' is not a List.", bookmarksObject ) );
        }
    }

    private List<Bookmark> parseBookmarks( ListValue bookmarks ) throws BookmarkParsingException
    {
        var maxSystemDbTxId = ABSENT_BOOKMARK_ID;

        NamedDatabaseId userDbId = null;
        var maxUserDbTxId = ABSENT_BOOKMARK_ID;

        List<String> customBookmarkStrings = new ArrayList<>();

        for ( var bookmark : bookmarks )
        {
            if ( bookmark != Values.NO_VALUE )
            {
                var bookmarkString = toBookmarkString( bookmark );

                if ( customBookmarkFormatParser.isCustomBookmark( bookmarkString ) )
                {
                    customBookmarkStrings.add( bookmarkString );
                }
                else
                {
                    var parsedBookmark = parse( bookmarkString );

                    if ( NAMED_SYSTEM_DATABASE_ID.equals( parsedBookmark.namedDatabaseId ) )
                    {
                        maxSystemDbTxId = Math.max( maxSystemDbTxId, parsedBookmark.txId );
                    }
                    else
                    {
                        if ( userDbId == null )
                        {
                            userDbId = parsedBookmark.namedDatabaseId;
                        }
                        else
                        {
                            assertSameDatabaseId( userDbId, parsedBookmark.namedDatabaseId, bookmarks );
                        }
                        maxUserDbTxId = Math.max( maxUserDbTxId, parsedBookmark.txId );
                    }
                }
            }
        }

        if ( customBookmarkStrings.isEmpty() )
        {
            return buildBookmarks( NAMED_SYSTEM_DATABASE_ID, maxSystemDbTxId, userDbId, maxUserDbTxId );
        }

        if ( maxUserDbTxId != ABSENT_BOOKMARK_ID )
        {
            throw newInvalidBookmarkMixtureError( bookmarks );
        }

        var customBookmarks =  customBookmarkFormatParser.parse( customBookmarkStrings );

        if ( maxSystemDbTxId != ABSENT_BOOKMARK_ID )
        {
            customBookmarks.add( new BookmarkWithDatabaseId( maxSystemDbTxId, NAMED_SYSTEM_DATABASE_ID ) );
        }

        return customBookmarks;
    }

    private ParsedBookmark parse( String bookmarkString ) throws BookmarkParsingException
    {
        var split = bookmarkString.split( ":" );
        if ( split.length != 2 )
        {
            throw newInvalidSingleBookmarkError( bookmarkString );
        }

        UUID databaseUuid = parseDatabaseId( split[0], bookmarkString );
        var databaseId = databaseIdRepository.getById( DatabaseIdFactory.from( databaseUuid ) ).orElseThrow(
                () -> newInvalidBookmarkForUnknownDatabaseError( databaseUuid ) );
        var txId = parseTxId( split[1], bookmarkString );

        return new ParsedBookmark( databaseId, txId );
    }

    private String toBookmarkString( AnyValue bookmark ) throws BookmarkParsingException
    {
        if ( !(bookmark instanceof TextValue) )
        {
            throw newInvalidSingleBookmarkError( bookmark );
        }
        return  ((TextValue) bookmark).stringValue();
    }

    private static UUID parseDatabaseId( String uuid, String bookmark ) throws BookmarkParsingException
    {
        try
        {
            return UUID.fromString( uuid );
        }
        catch ( IllegalArgumentException e )
        {
            throw BookmarkParsingException.newInvalidSingleBookmarkError( bookmark, String.format( "Unable to parse database id: %s", uuid ), e );
        }
    }

    private static long parseTxId( String txIdString, String bookmark ) throws BookmarkParsingException
    {
        try
        {
            return Long.parseLong( txIdString );
        }
        catch ( NumberFormatException e )
        {
            throw BookmarkParsingException.newInvalidSingleBookmarkError( bookmark, String.format( "Unable to parse transaction id: %s", txIdString ), e );
        }
    }

    private static void assertSameDatabaseId( NamedDatabaseId id1, NamedDatabaseId id2, ListValue bookmarks ) throws BookmarkParsingException
    {
        if ( !id1.equals( id2 ) )
        {
            throw newInvalidBookmarkMixtureError( bookmarks );
        }
    }

    private static List<Bookmark> buildBookmarks( NamedDatabaseId systemDbId, long maxSystemDbTxId, NamedDatabaseId userDbId, long maxUserDbTxId )
    {
        if ( maxSystemDbTxId != ABSENT_BOOKMARK_ID && maxUserDbTxId != ABSENT_BOOKMARK_ID )
        {
            return List.of( new BookmarkWithDatabaseId( maxSystemDbTxId, systemDbId ), new BookmarkWithDatabaseId( maxUserDbTxId, userDbId ) );
        }
        else if ( maxSystemDbTxId != ABSENT_BOOKMARK_ID )
        {
            return List.of( new BookmarkWithDatabaseId( maxSystemDbTxId, systemDbId ) );
        }
        else if ( maxUserDbTxId != ABSENT_BOOKMARK_ID )
        {
            return List.of( new BookmarkWithDatabaseId( maxUserDbTxId, userDbId ) );
        }
        else
        {
            return List.of();
        }
    }

    private static class ParsedBookmark
    {
        final NamedDatabaseId namedDatabaseId;
        final long txId;

        ParsedBookmark( NamedDatabaseId namedDatabaseId, long txId )
        {
            this.namedDatabaseId = namedDatabaseId;
            this.txId = txId;
        }
    }
}
