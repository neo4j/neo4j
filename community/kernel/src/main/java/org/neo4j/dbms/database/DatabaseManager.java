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

import java.util.Optional;
import java.util.SortedMap;

import org.neo4j.kernel.lifecycle.Lifecycle;

public interface DatabaseManager<DB extends DatabaseContext> extends Lifecycle
{

    /**
     * Returns a given {@link DatabaseContext} object by name, or `Optional.empty()` if the database does not exist
     *
     * @param databaseName the name of the database to be returned
     * @return optionally, the database context instance with name databaseName
     */
    Optional<DB> getDatabaseContext( String databaseName );

    /**
     * Create database with specified name.
     * Database name should be unique.
     * @param databaseName name of database to create
     * @throws DatabaseExistsException In case if database with specified name already exists
     * @return database context for newly created database
     */
    DB createDatabase( String databaseName ) throws DatabaseExistsException;

    /**
     * Drop database with specified name.
     * Database that was requested to be dropped will be stopped first, and then completely removed.
     * If database with requested name does not exist exception will be thrown.
     * @param databaseName name of database to drop.
     */
    void dropDatabase( String databaseName ) throws DatabaseNotFoundException;

    /**
     * Stop database with specified name.
     * Stopping already stopped database does not have any effect.
     * @param databaseName database name to stop
     */
    void stopDatabase( String databaseName ) throws DatabaseNotFoundException;

    /**
     * Start database with specified name.
     * Starting already started database does not have any effect.
     * @param databaseName database name to start
     */
    void startDatabase( String databaseName ) throws DatabaseNotFoundException;

    /**
     * Return all {@link DatabaseContext} instances created by this service, associated with their database names.
     *
     * The collection returned from this method must be an immutable view over the registered database and sorted by database name.
     *
     * @return a Map from database names to database objects.
     */
    SortedMap<String,DB> registeredDatabases();

}
