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

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This is the bookmark used in Bolt V1-V3. It is prefixed to identify this string is a bookmark.
 * This bookmark cannot be used with multi-databases as we cannot identify the database where the bookmark is originally generated.
 */
public class BookmarkWithPrefix implements Bookmark
{
    static final String BOOKMARK_KEY = "bookmark";
    static final String BOOKMARK_TX_PREFIX = "neo4j:bookmark:v1:tx";

    private final long txId;

    public BookmarkWithPrefix( long txId )
    {
        this.txId = txId;
    }

    @Override
    public long txId()
    {
        return txId;
    }

    @Override
    public DatabaseId databaseId()
    {
        return null;
    }

    @Override
    public void attachTo( BoltResponseHandler state )
    {
        state.onMetadata( BOOKMARK_KEY, stringValue( toString() ) );
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
}
