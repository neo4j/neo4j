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
package org.neo4j.bolt.v3.runtime.bookmarking;

import java.util.List;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v3.runtime.bookmarking.BookmarkWithPrefix.BOOKMARK_KEY;
import static org.neo4j.bolt.v3.runtime.bookmarking.BookmarkWithPrefix.BOOKMARK_TX_PREFIX;

public final class BookmarksParserV3 implements BookmarksParser
{
    public static final BookmarksParserV3 INSTANCE = new BookmarksParserV3();

    private static final String BOOKMARKS_KEY = "bookmarks";
    private static final Long ABSENT_BOOKMARK_ID = -1L;

    private BookmarksParserV3()
    {
    }

    @Override
    public List<Bookmark> parseBookmarks( MapValue metadata ) throws BookmarkFormatException
    {
        // try to parse multiple bookmarks, if available
        var bookmark = parseMultipleBookmarks( metadata );
        if ( bookmark == null )
        {
            // fallback to parsing single bookmark, if available, for backwards compatibility reasons
            // some older drivers can only send a single bookmark
            bookmark = parseSingleBookmark( metadata );
        }
        return bookmark == null ? List.of() : List.of( bookmark );
    }

    private static BookmarkWithPrefix parseMultipleBookmarks( MapValue params ) throws BookmarkFormatException
    {
        var bookmarksObject = params.get( BOOKMARKS_KEY );

        if ( bookmarksObject == Values.NO_VALUE )
        {
            return null;
        }
        else if ( bookmarksObject instanceof ListValue )
        {
            var bookmarks = (ListValue) bookmarksObject;

            long maxTxId = ABSENT_BOOKMARK_ID;
            for ( var bookmark : bookmarks )
            {
                if ( bookmark != Values.NO_VALUE )
                {
                    var txId = txIdFrom( bookmark );
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
        var bookmarkObject = params.get( BOOKMARK_KEY );
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
        var bookmarkString = ((TextValue) bookmark).stringValue();
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
}
