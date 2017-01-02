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

import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class Bookmark
{
    private static final String BOOKMARK_TX_PREFIX = "neo4j:bookmark:v1:tx";

    private final long txId;

    public Bookmark( long txId )
    {
        this.txId = txId;
    }

    @Override
    public String toString()
    {
        return format( BOOKMARK_TX_PREFIX + "%d", txId );
    }

    public static Bookmark fromString( String bookmarkString) throws BookmarkFormatException
    {
        if ( bookmarkString != null && bookmarkString.startsWith( BOOKMARK_TX_PREFIX ) )
        {
            try
            {
                return new Bookmark( Long.parseLong( bookmarkString.substring( BOOKMARK_TX_PREFIX.length() ) ) );
            }
            catch ( NumberFormatException e )
            {
                throw new BookmarkFormatException( bookmarkString, e );
            }
        }
        throw new BookmarkFormatException( bookmarkString );
    }

    public long txId()
    {
        return txId;
    }

    static class BookmarkFormatException extends KernelException
    {
        BookmarkFormatException( String bookmarkString, NumberFormatException e )
        {
            super( Status.Transaction.InvalidBookmark, e, "Supplied bookmark [%s] does not conform to pattern %s; " +
                    "unable to parse transaction id", bookmarkString, BOOKMARK_TX_PREFIX );
        }

        BookmarkFormatException( String bookmarkString )
        {
            super( Status.Transaction.InvalidBookmark, "Supplied bookmark [%s] does not conform to pattern %s",
                    bookmarkString, BOOKMARK_TX_PREFIX );
        }
    }
}
