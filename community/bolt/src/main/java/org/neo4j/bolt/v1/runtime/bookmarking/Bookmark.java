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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.cypher.internal.frontend.v3_2.ast.RegexMatch;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class Bookmark
{
    private static final String BOOKMARK_TX_PREFIX = "neo4j:bookmark:v1:tx";

    private final long txId;
    private static Pattern validBookmarkPattern = Pattern.compile( "neo4j\\:bookmark\\:v1\\:(tx\\d+,{0,1})+" );

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
        if ( validBookmark( bookmarkString ) )
        {
            try
            {
                return new Bookmark( getHighestTransactionIdFromSuppliedBookmarks( bookmarkString ) );

            }
            catch ( NumberFormatException e )
            {
                throw new BookmarkFormatException( bookmarkString );
            }

        }
        throw new BookmarkFormatException( bookmarkString );
    }

    private static boolean validBookmark( String bookmarkString )
    {
        return validBookmarkPattern.matcher( bookmarkString ).matches() && bookmarkString != null &&
                bookmarkString.startsWith( BOOKMARK_TX_PREFIX );
    }

    private static long getHighestTransactionIdFromSuppliedBookmarks( String bookmarkString )
    {
        String[] stringBookmarks = bookmarkString.replace( BOOKMARK_TX_PREFIX, "" ).replace( "tx", "" ).split( "," );

        long highest = -1;

        for ( String stringBookmark : stringBookmarks )
        {
            long current = Long.valueOf( stringBookmark );

            if ( current > highest )
            {
                highest = current;
            }
        }
        return highest;
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
