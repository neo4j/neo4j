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

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.ListValue;

class BookmarkParsingException extends BoltIOException
{
    private BookmarkParsingException( Status status, String message, Throwable cause )
    {
        super( status, message, cause );
    }

    private BookmarkParsingException( Status status, String message )
    {
        super( status, message );
    }

    static BookmarkParsingException newInvalidBookmarkError( String bookmarkString, NumberFormatException cause )
    {
        return new BookmarkParsingException( Status.Transaction.InvalidBookmark,
                String.format( "Supplied bookmark [%s] does not conform to pattern {database_id}:{tx_id}; unable to parse transaction id", bookmarkString ),
                cause );
    }

    static BookmarkParsingException newInvalidBookmarkError( Object bookmarkObject )
    {
        return new BookmarkParsingException( Status.Transaction.InvalidBookmark,
                String.format( "Supplied bookmark [%s] does not conform to pattern {database_id}:{tx_id}", bookmarkObject ) );
    }

    static BookmarkParsingException newInvalidBookmarkMixtureError( ListValue bookmarks )
    {
        return new BookmarkParsingException( Status.Transaction.InvalidBookmarkMixture,
                String.format( "Supplied bookmark list contains bookmarks from multiple databases. Bookmark list: %s ", bookmarks ) );
    }

    static BookmarkParsingException newInvalidBookmarkUnknownDatabaseError( String databaseName )
    {
        return new BookmarkParsingException( Status.Transaction.InvalidBookmark,
                String.format( "Supplied bookmark is for unknown database: %s", databaseName ) );
    }
}
