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

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * The {@link DatabaseManagementService} provides an API to manage databases and provided access to the managed database services.
 */
public interface DatabaseManagementService
{
    /**
     * Retrieve a database service by name.
     * @param name of the database.
     * @return the database service with the provided name
     * @throws DatabaseNotFoundException if no database service with the given name is found.
     */
    GraphDatabaseService database( String name ) throws DatabaseNotFoundException;

    /**
     * Create a new database.
     * @param name of the new database.
     * @throws DatabaseExistsException if a database with the provided name already exists
     */
    void createDatabase( String name ) throws DatabaseExistsException;

    /**
     * Drop a database by name. All data stored in the database will be deleted as well.
     * @param name of the database to drop.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void dropDatabase( String name ) throws DatabaseNotFoundException;

    /**
     * Starts a already existing database.
     * @param name of the database to start.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void startDatabase( String name ) throws DatabaseNotFoundException;

    /**
     * Shutdown database with provided name.
     * @param name of the database.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void stopDatabase( String name ) throws DatabaseNotFoundException;

    /**
     * @return an alphabetically sorted list of all database names this database server manages.
     */
    List<String> listDatabases();

    /**
     * Shutdown database server.
     */
    void shutdown();
}
