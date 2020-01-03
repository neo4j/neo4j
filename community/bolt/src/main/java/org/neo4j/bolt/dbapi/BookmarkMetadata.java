/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.dbapi;

import java.util.function.BiFunction;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * A serialization-format-independent representation of data carried by a bookmark.
 * {@link Bookmark} is format dependent representation of the same data.
 */
public class BookmarkMetadata
{
    private final NamedDatabaseId namedDatabaseId;
    private final long txId;

    public BookmarkMetadata( long txId, NamedDatabaseId namedDatabaseId )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.txId = txId;
    }

    public BookmarkMetadata( long txId )
    {
        this( txId, null );
    }

    /**
     * Converts this serialization-format-independent representation of a bookmark into a serialization-format-dependent one.
     */
    public Bookmark toBookmark( BiFunction<Long,NamedDatabaseId,Bookmark> defaultBookmarkFormat )
    {
        return defaultBookmarkFormat.apply( txId, namedDatabaseId );
    }

    public NamedDatabaseId getNamedDatabaseId()
    {
        return namedDatabaseId;
    }

    public long getTxId()
    {
        return txId;
    }
}
