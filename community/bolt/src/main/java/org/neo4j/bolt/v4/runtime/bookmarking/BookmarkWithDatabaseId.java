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

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This bookmark is introduced in bolt v4 with multi-databases support.
 */
public class BookmarkWithDatabaseId implements Bookmark
{
    private static final String BOOKMARK_KEY = "bookmark"; // used in response messages

    private final long txId;
    private final DatabaseId databaseId;

    public BookmarkWithDatabaseId( long txId, DatabaseId databaseId )
    {
        this.txId = txId;
        this.databaseId = databaseId;
    }

    @Override
    public long txId()
    {
        return txId;
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
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
        var that = (BookmarkWithDatabaseId) o;
        return txId == that.txId && Objects.equals( databaseId, that.databaseId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( txId, databaseId );
    }

    @Override
    public String toString()
    {
        return format( "%s:%d", databaseId.name(), txId );
    }
}
