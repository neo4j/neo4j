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

import java.util.List;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkError;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkMixtureError;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkUnknownDatabaseError;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

public final class BookmarksParserV4 implements BookmarksParser
{
    private static final String BOOKMARKS_KEY = "bookmarks";
    private static final long ABSENT_BOOKMARK_ID = -1L;

    private final DatabaseIdRepository databaseIdRepository;

    public BookmarksParserV4( DatabaseIdRepository databaseIdRepository )
    {
        this.databaseIdRepository = databaseIdRepository;
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
            throw newInvalidBookmarkError( bookmarksObject );
        }
    }

    private List<Bookmark> parseBookmarks( ListValue bookmarks ) throws BookmarkParsingException
    {
        var maxSystemDbTxId = ABSENT_BOOKMARK_ID;

        DatabaseId userDbId = null;
        var maxUserDbTxId = ABSENT_BOOKMARK_ID;

        for ( var bookmark : bookmarks )
        {
            if ( bookmark != Values.NO_VALUE )
            {
                var parsedBookmark = parse( bookmark );

                if ( SYSTEM_DATABASE_ID.equals( parsedBookmark.databaseId ) )
                {
                    maxSystemDbTxId = Math.max( maxSystemDbTxId, parsedBookmark.txId );
                }
                else
                {
                    if ( userDbId == null )
                    {
                        userDbId = parsedBookmark.databaseId;
                    }
                    else
                    {
                        assertSameDatabaseId( userDbId, parsedBookmark.databaseId, bookmarks );
                    }
                    maxUserDbTxId = Math.max( maxUserDbTxId, parsedBookmark.txId );
                }
            }
        }

        return buildBookmarks( SYSTEM_DATABASE_ID, maxSystemDbTxId, userDbId, maxUserDbTxId );
    }

    private ParsedBookmark parse( AnyValue bookmark ) throws BookmarkParsingException
    {
        if ( !(bookmark instanceof TextValue) )
        {
            throw newInvalidBookmarkError( bookmark );
        }
        var bookmarkString = ((TextValue) bookmark).stringValue();
        var split = bookmarkString.split( ":" );
        if ( split.length != 2 )
        {
            throw newInvalidBookmarkError( bookmarkString );
        }

        String databaseName = split[0];
        var databaseId = databaseIdRepository.get( databaseName ).orElseThrow( () -> newInvalidBookmarkUnknownDatabaseError( databaseName ) );
        var txId = parseTxId( split[1], bookmarkString );

        return new ParsedBookmark( databaseId, txId );
    }

    private static long parseTxId( String txIdString, String bookmark ) throws BookmarkParsingException
    {
        try
        {
            return Long.parseLong( txIdString );
        }
        catch ( NumberFormatException e )
        {
            throw newInvalidBookmarkError( bookmark, e );
        }
    }

    private static void assertSameDatabaseId( DatabaseId id1, DatabaseId id2, ListValue bookmarks ) throws BookmarkParsingException
    {
        if ( !id1.equals( id2 ) )
        {
            throw newInvalidBookmarkMixtureError( bookmarks );
        }
    }

    private static List<Bookmark> buildBookmarks( DatabaseId systemDbId, long maxSystemDbTxId, DatabaseId userDbId, long maxUserDbTxId )
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
        final DatabaseId databaseId;
        final long txId;

        ParsedBookmark( DatabaseId databaseId, long txId )
        {
            this.databaseId = databaseId;
            this.txId = txId;
        }
    }
}
