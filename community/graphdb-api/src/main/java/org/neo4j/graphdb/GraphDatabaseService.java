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
package org.neo4j.graphdb;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.annotations.api.PublicApi;

/**
 * <p>
 * GraphDatabaseService represents a graph database and is used to create
 * new transactions with {@link #beginTx()}.
 */
@PublicApi
public interface GraphDatabaseService
{
    /**
     * Use this method to check if the database is currently in a usable state.
     *
     * @param timeout timeout (in milliseconds) to wait for the database to become available.
     *   If the database has been shut down {@code false} is returned immediately.
     * @return the state of the database: {@code true} if it is available, otherwise {@code false}
     */
    boolean isAvailable( long timeout );

    /**
     * Starts a new {@link Transaction transaction} and associates it with the current thread.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside of a transaction, those operations will throw
     * {@link NotInTransactionException}.
     * <p>
     * Please ensure that any returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return a new transaction instance
     */
    Transaction beginTx();

    /**
     * Starts a new {@link Transaction transaction} with custom timeout and associates it with the current thread.
     * Timeout will be taken into account <b>only</b> when execution guard is enabled.
     * <p>
     * <em>All database operations must be wrapped in a transaction.</em>
     * <p>
     * If you attempt to access the graph outside of a transaction, those operations will throw
     * {@link NotInTransactionException}.
     * <p>
     * Please ensure that any returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param timeout transaction timeout
     * @param unit time unit of timeout argument
     * @return a new transaction instance
     */
    Transaction beginTx( long timeout, TimeUnit unit );

    /**
     * Executes query in a separate transaction.
     * Capable to execute periodic commit queries.
     *
     * @param query The query to execute
     * @throws QueryExecutionException If the Query contains errors
     */
    void executeTransactionally( String query ) throws QueryExecutionException;

    /**
     * Executes query in a separate transaction.
     * Capable to execute periodic commit queries.
     *
     * @param query The query to execute
     * @param parameters Parameters for the query
     * @throws QueryExecutionException If the Query contains errors
     */
    void executeTransactionally( String query, Map<String,Object> parameters ) throws QueryExecutionException;

    /**
     * Executes query in a separate transaction and allow to query result to be consumed by provided {@link ResultTransformer}.
     * Capable to execute periodic commit queries.
     *
     * @param query The query to execute
     * @param parameters Parameters for the query
     * @param resultTransformer Query results consumer
     * @throws QueryExecutionException If the query contains errors
     */
    <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer ) throws QueryExecutionException;

    /**
     * Executes query in a separate transaction and allows query result to be consumed by provided {@link ResultTransformer}.
     * If query will not gonna be able to complete within provided timeout time interval it will be terminated.
     *
     * Capable to execute periodic commit queries.
     *
     * @param query The query to execute
     * @param parameters Parameters for the query
     * @param resultTransformer Query results consumer
     * @param timeout Maximum duration of underlying transaction
     * @throws QueryExecutionException If the query contains errors
     */
    <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer,
            Duration timeout ) throws QueryExecutionException;

    /**
     * Return name of underlying database
     * @return database name
     */
    String databaseName();
}
