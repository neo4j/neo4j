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

import java.util.Objects;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId.EMPTY_BOOKMARK;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This is the bookmark used in Bolt V1-V3. It is prefixed to identify this string is a bookmark.
 * This bookmark cannot be used with multi-databases as we cannot identify the database where the bookmark is originally generated.
 */
public class BookmarkWithPrefix implements Bookmark
{
    private static final String BOOKMARK_KEY = "bookmark";
    private static final String BOOKMARKS_KEY = "bookmarks";
    static final String BOOKMARK_TX_PREFIX = "neo4j:bookmark:v1:tx";

    private static final Long ABSENT_BOOKMARK_ID = -1L;

    private final long txId;

    public BookmarkWithPrefix( long txId )
    {
        this.txId = txId;
    }

    public static BookmarkWithPrefix fromParamsOrNull( MapValue params ) throws BookmarkFormatException
    {
        // try to parse multiple bookmarks, if available
        BookmarkWithPrefix bookmark = parseMultipleBookmarks( params );
        if ( bookmark == null )
        {
            // fallback to parsing single bookmark, if available, for backwards compatibility reasons
            // some older drivers can only send a single bookmark
            return parseSingleBookmark( params );
        }
        return bookmark;
    }

    public long txId()
    {
        return txId;
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
        BookmarkWithPrefix bookmark = (BookmarkWithPrefix) o;
        return txId == bookmark.txId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( txId );
    }

    @Override
    public String toString()
    {
        return format( BOOKMARK_TX_PREFIX + "%d", txId );
    }

    private static BookmarkWithPrefix parseMultipleBookmarks( MapValue params ) throws BookmarkFormatException
    {
        AnyValue bookmarksObject = params.get( BOOKMARKS_KEY );

        if ( bookmarksObject == Values.NO_VALUE )
        {
            return null;
        }
        else if ( bookmarksObject instanceof ListValue )
        {
            ListValue bookmarks = (ListValue) bookmarksObject;

            long maxTxId = ABSENT_BOOKMARK_ID;
            for ( AnyValue bookmark : bookmarks )
            {
                if ( bookmark != Values.NO_VALUE )
                {
                    long txId = txIdFrom( bookmark );
                    if ( txId > maxTxId )
                    {
                        maxTxId = txId;
                    }
                }
            }
            return maxTxId == ABSENT_BOOKMARK_ID ? null : new BookmarkWithPrefix( maxTxId );
        }
        else
        {
            throw new BookmarkFormatException( bookmarksObject );
        }
    }

    private static BookmarkWithPrefix parseSingleBookmark( MapValue params ) throws BookmarkFormatException
    {
        AnyValue bookmarkObject = params.get( BOOKMARK_KEY );
        if ( bookmarkObject == Values.NO_VALUE )
        {
            return null;
        }

        return new BookmarkWithPrefix( txIdFrom( bookmarkObject ) );
    }

    private static long txIdFrom( AnyValue bookmark ) throws BookmarkFormatException
    {
        if ( !(bookmark instanceof TextValue) )
        {
            throw new BookmarkFormatException( bookmark );
        }
        String bookmarkString = ((TextValue) bookmark).stringValue();
        if ( !bookmarkString.startsWith( BOOKMARK_TX_PREFIX ) )
        {
            throw new BookmarkFormatException( bookmarkString );
        }

        try
        {
            return Long.parseLong( bookmarkString.substring( BOOKMARK_TX_PREFIX.length() ) );
        }
        catch ( NumberFormatException e )
        {
            throw new BookmarkFormatException( bookmarkString, e );
        }
    }

    public void attachTo( BoltResponseHandler state )
    {
        if ( !equals( EMPTY_BOOKMARK ) )
        {
            state.onMetadata( BOOKMARK_KEY, stringValue( toString() ) );
        }
    }
}
