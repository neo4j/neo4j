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

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

public interface DatabaseManager<DB extends DatabaseContext> extends Lifecycle
{
    /**
     * Creates the system database. This must be created and started before
     * any other databases can be created. If the system database is already
     * initialised, nothing happens
     */
    default void initialiseSystemDatabase()
    {
        // no-op
    }

    /**
     * Creates those databases which are required by default in a Neo4j install
     * as per the system {@link Config}. If default databases are already
     * initialised, nothing happens
     */
    default void initialiseDefaultDatabase()
    {
        // no-op
    }

    /**
     * Returns a given {@link DatabaseContext} object by identifier, or `Optional.empty()` if the database does not exist
     *
     * @param namedDatabaseId the ID of the database to be returned
     * @return optionally, the database context instance with ID databaseId
     */
    Optional<DB> getDatabaseContext( NamedDatabaseId namedDatabaseId );

    /**
     * Returns a given {@link DatabaseContext} object by name, or `Optional.empty()` if the database does not exist
     *
     * @param databaseName the name of the database to be returned
     * @return optionally, the database context instance with name databaseName
     */
    default Optional<DB> getDatabaseContext( String databaseName )
    {
        return databaseIdRepository().getByName( databaseName ).flatMap( this::getDatabaseContext );
    }

    /**
     * Returns a given {@link DatabaseContext} object by name, or `Optional.empty()` if the database does not exist
     *
     * @param databaseId the identifier of the database to be returned
     * @return optionally, the database context instance with identifier databaseId
     */
    default Optional<DB> getDatabaseContext( DatabaseId databaseId )
    {
        return databaseIdRepository().getById( databaseId ).flatMap( this::getDatabaseContext );
    }

    /**
     * Create database with specified name.
     * Database name should be unique.
     * By default a database is in a started state when it is initially created.
     * @param namedDatabaseId ID of database to create
     * @throws DatabaseManagementException if database with specified name already exists, or the limited number of databases has been reached.
     * @return database context for newly created database
     */
    DB createDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseManagementException;

    /**
     * Drop database with specified name.
     * Database that was requested to be dropped will be stopped first, and then completely removed.
     * If database with requested name does not exist exception will be thrown.
     * @param namedDatabaseId ID of database to drop.
     */
    void dropDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException;

    /**
     * Stop database with specified name.
     * Stopping already stopped database does not have any effect.
     * @param namedDatabaseId database ID to stop
     */
    void stopDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException;

    /**
     * Start database with specified name.
     * Starting already started database does not have any effect.
     * @param namedDatabaseId database ID to start
     */
    void startDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException;

    /**
     * Return all {@link DatabaseContext} instances created by this service, associated with their database names.
     *
     * The collection returned from this method must be an immutable view over the registered database and sorted by database name.
     *
     * @return a Map from database names to database objects.
     */
    SortedMap<NamedDatabaseId,DB> registeredDatabases();

    /**
     * Return the {@link DatabaseIdRepository.Caching} constructed with this database manager. Use this to retrieve a
     * {@link NamedDatabaseId} for a given database name.
     *
     * @return database ID repository for use with this database manager
     */
    DatabaseIdRepository.Caching databaseIdRepository();
}
