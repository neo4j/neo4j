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

import java.util.List;

import org.neo4j.bolt.runtime.Bookmark;

/**
 * A parser of custom bookmark format. It can parse a serialized bookmark into a subclass of {@link Bookmark}.
 * <p>
 * The state-carrying part of a bookmark consists of a long representing a transaction ID, if a transaction state cannot be represented as a single long,
 * {@link BoltGraphDatabaseManagementServiceSPI} can use a custom format for its bookmarks. This class represents a parsing part of the custom bookmark logic.
 * The serialization part is represented by {@link BookmarkMetadata#toBookmark(java.util.function.BiFunction)}
 * and {@link Bookmark#attachTo(org.neo4j.bolt.runtime.BoltResponseHandler)}
 */
public interface CustomBookmarkFormatParser
{
    boolean isCustomBookmark( String string );

    List<Bookmark> parse( List<String> customBookmarks );

    CustomBookmarkFormatParser DEFAULT = new CustomBookmarkFormatParser()
    {

        @Override
        public boolean isCustomBookmark( String string )
        {
            return false;
        }

        @Override
        public List<Bookmark> parse( List<String> customBookmarks )
        {
            throw new IllegalStateException( "Custom parser invoked for unsupported bookmarks" );
        }
    };
}
