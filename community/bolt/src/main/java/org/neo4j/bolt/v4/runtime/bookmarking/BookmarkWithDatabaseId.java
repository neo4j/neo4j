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

import java.util.Objects;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkError;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkParsingException.newInvalidBookmarkMixtureError;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This bookmark is introduced in bolt v4 with multi-databases support.
 */
public class BookmarkWithDatabaseId implements Bookmark
{
    private static final String BOOKMARK_KEY = "bookmark"; // used in response messages
    private static final String BOOKMARKS_KEY = "bookmarks"; // used in request messages

    private static final long ABSENT_BOOKMARK_ID = -1L;

    private final long txId;
    private final String dbId; // this is the string representation of database uuid.

    public BookmarkWithDatabaseId( String dbId, long txId )
    {
        this.dbId = dbId;
        this.txId = txId;
    }

    public static BookmarkWithDatabaseId fromParamsOrNull( MapValue params ) throws BookmarkParsingException
    {
        return parseMultipleBookmarks( params );
    }

    @Override
    public long txId()
    {
        return txId;
    }

    @Override
    public String databaseId()
    {
        return dbId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        var bookmark = (BookmarkWithDatabaseId) o;
        return txId == bookmark.txId && Objects.equals( dbId, bookmark.dbId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( dbId, txId );
    }

    @Override
    public String toString()
    {
        return format( "%s:%d", dbId, txId );
    }

    private static BookmarkWithDatabaseId parseMultipleBookmarks( MapValue params ) throws BookmarkParsingException
    {
        var bookmarksObject = params.get( BOOKMARKS_KEY );

        if ( bookmarksObject == Values.NO_VALUE )
        {
            return null;
        }
        else if ( bookmarksObject instanceof ListValue )
        {
            var bookmarks = (ListValue) bookmarksObject;

            String dbId = null;
            var maxTxId = ABSENT_BOOKMARK_ID;

            for ( var bookmark : bookmarks )
            {
                if ( bookmark != Values.NO_VALUE )
                {
                    var pair = dbIdAndTxIdFrom( bookmark );

                    if ( dbId == null )
                    {
                        dbId = pair.first();
                    }
                    else
                    {
                        assertSameDbId( dbId, pair.first(), bookmarks );
                    }

                    if ( pair.other() > maxTxId )
                    {
                        maxTxId = pair.other();
                    }
                }
            }
            return maxTxId == ABSENT_BOOKMARK_ID ? null : new BookmarkWithDatabaseId( dbId, maxTxId );
        }
        else
        {
            throw newInvalidBookmarkError( bookmarksObject );
        }
    }

    private static void assertSameDbId( String id1, String id2, ListValue bookmarks ) throws BookmarkParsingException
    {
        if ( !id1.equals( id2 ) )
        {
            throw newInvalidBookmarkMixtureError( bookmarks );
        }
    }

    private static Pair<String,Long> dbIdAndTxIdFrom( AnyValue bookmark ) throws BookmarkParsingException
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

        try
        {
            return Pair.of( split[0], Long.parseLong( split[1] ) );
        }
        catch ( NumberFormatException e )
        {
            throw newInvalidBookmarkError( bookmarkString, e );
        }
    }

    @Override
    public void attachTo( BoltResponseHandler state )
    {
        if ( !equals( EMPTY_BOOKMARK ) )
        {
            state.onMetadata( BOOKMARK_KEY, stringValue( toString() ) );
        }
    }
}
