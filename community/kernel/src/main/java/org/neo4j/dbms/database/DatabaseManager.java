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
package org.neo4j.dbms.database;

import java.util.List;
import java.util.Optional;

import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.Lifecycle;

public interface DatabaseManager extends Lifecycle
{

    Optional<GraphDatabaseFacade> getDatabaseFacade( String name );

    GraphDatabaseFacade createDatabase( String name );

    /**
     * Shutdown database with specified name.
     * @param name database name to shutdown
     */
    void shutdownDatabase( String name );

    /**
     * Return sorted list of known database names
     * @return sorted list of known database names
     */
    List<String> listDatabases();
}
