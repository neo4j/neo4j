/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.dbms.database;

import java.util.List;
import java.util.Set;

public interface DatabaseInfoService
{
    /**
     * Returns information about the given databases. For single instance one list item is returned per
     * database. In clustered setups one list item is returned per database per server.
     *
     * Information returned should be accessible and be able to return a quick result.
     *
     * @param databaseNames databases the request is about
     * @return a list containing one item per database per server
     */
    List<DatabaseInfo> lookupCachedInfo( Set<String> databaseNames );

    /**
     * Similar to {@link #lookupCachedInfo(Set)}  but with additional information that might require network calls.
     *
     * @param databaseNames databases the request is about
     * @return a list containing one item per database per server
     */
    List<ExtendedDatabaseInfo> requestDetailedInfo( Set<String> databaseNames );
}
