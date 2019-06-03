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
package org.neo4j.dbms.api;

import java.util.List;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionEventListener;

/**
 * The {@link DatabaseManagementService} provides an API to manage databases and provided access to the managed database services.
 */
@PublicApi
public interface DatabaseManagementService
{
    /**
     * Retrieve a database service by name.
     * @param databaseName name of the database.
     * @return the database service with the provided name
     * @throws DatabaseNotFoundException if no database service with the given name is found.
     */
    GraphDatabaseService database( String databaseName ) throws DatabaseNotFoundException;

    /**
     * Create a new database.
     * @param databaseName name of the database.
     * @throws DatabaseExistsException if a database with the provided name already exists
     */
    void createDatabase( String databaseName ) throws DatabaseExistsException;

    /**
     * Drop a database by name. All data stored in the database will be deleted as well.
     * @param databaseName name of the database to drop.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void dropDatabase( String databaseName ) throws DatabaseNotFoundException;

    /**
     * Starts a already existing database.
     * @param databaseName name of the database to start.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void startDatabase( String databaseName ) throws DatabaseNotFoundException;

    /**
     * Shutdown database with provided name.
     * @param databaseName name of the database.
     * @throws DatabaseNotFoundException if no database with the given name is found.
     */
    void shutdownDatabase( String databaseName ) throws DatabaseNotFoundException;

    /**
     * @return an alphabetically sorted list of all database names this database server manages.
     */
    List<String> listDatabases();

    /**
     * Registers {@code listener} as a listener for database events.
     * If the specified listener instance has already been registered this method will do nothing.
     *
     * @param listener the listener to receive events about different states
     *                in the database lifecycle.
     */
    void registerDatabaseEventListener( DatabaseEventListener listener );

    /**
     * Unregisters {@code listener} from the list of database event handlers.
     * If {@code listener} hasn't been registered with
     * {@link #registerDatabaseEventListener(DatabaseEventListener)} prior to calling
     * this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code listener} will no
     * longer receive any database events.
     *
     * @param listener the listener to receive events about database lifecycle.
     * @throws IllegalStateException if {@code listener} wasn't registered prior
     *                               to calling this method.
     */
    void unregisterDatabaseEventListener( DatabaseEventListener listener );

    /**
     * Registers {@code listener} as a listener for transaction events which
     * are generated from different places in the lifecycle of each
     * transaction in particular database. To guarantee that the handler gets all events properly
     * it shouldn't be registered when the application is running (i.e. in the
     * middle of one or more transactions). If the specified handler instance
     * has already been registered this method will do nothing.
     *
     * @param databaseName name of the database to listener transactions
     * @param listener the listener to receive events about different states
     *                in transaction lifecycle.
     */
    void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener );

    /**
     * Unregisters {@code listener} from the list of transaction event listeners.
     * If {@code handler} hasn't been registered with
     * {@link #registerTransactionEventListener(String, TransactionEventListener)} prior
     * to calling this method an {@link IllegalStateException} will be thrown.
     * After a successful call to this method the {@code listener} will no
     * longer receive any transaction events.
     *
     * @param databaseName name of the database to listener transactions
     * @param listener the listener to receive events about different states
     *                in transaction lifecycles.
     * @throws IllegalStateException if {@code listener} wasn't registered prior
     *                               to calling this method.
     */
    void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener );

    /**
     * Shutdown database server.
     */
    void shutdown();
}
