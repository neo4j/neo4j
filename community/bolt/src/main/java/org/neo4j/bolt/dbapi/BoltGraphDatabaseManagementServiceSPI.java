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
package org.neo4j.bolt.dbapi;

import java.util.Optional;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.availability.UnavailableException;

/**
 * A service used for database look up. This is the main entry point of the bolt - DB facade.
 */
public interface BoltGraphDatabaseManagementServiceSPI
{
    BoltGraphDatabaseServiceSPI database( String databaseName ) throws UnavailableException, DatabaseNotFoundException;

    /**
     * The state-carrying part of a bookmark consists of a long representing a transaction ID, if a transaction state cannot be represented as a single long,
     * a custom format can be used for bookmarks. This method returns a parsing part of the custom bookmark logic.
     * The serialization part is represented by {@link BookmarkMetadata#toBookmark(java.util.function.BiFunction)}
     * and {@link org.neo4j.bolt.runtime.Bookmark#attachTo(org.neo4j.bolt.runtime.BoltResponseHandler)}
     */
    default Optional<CustomBookmarkFormatParser> getCustomBookmarkFormatParser()
    {
        return Optional.empty();
    }
}
