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
package org.neo4j.bolt.v1.runtime.spi;

import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;

import static org.neo4j.values.storable.Values.stringValue;

public class BookmarkResult extends BoltResult
{
    private final Bookmark bookmark;

    public BookmarkResult( Bookmark bookmark )
    {
        this.bookmark = bookmark;
    }

    @Override
    public String[] fieldNames()
    {
        return new String[0];
    }

    @Override
    public void accept( Visitor visitor )
    {
        visitor.addMetadata( "bookmark", stringValue( bookmark.toString() ) );
    }

    @Override
    public void close()
    {
    }
}
