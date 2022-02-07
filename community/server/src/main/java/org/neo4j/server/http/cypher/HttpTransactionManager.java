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
package org.neo4j.server.http.cypher;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

/**
 * An entry point for managing transaction in HTTP API.
 */
public class HttpTransactionManager
{
    private final TransactionHandleRegistry transactionRegistry;
    private final DatabaseManagementService managementService;
    private final JobScheduler jobScheduler;
    private final TransactionManager transactionManager;
    private final BoltGraphDatabaseManagementServiceSPI boltSPI;
    private final LogProvider userLogProvider;
    private final MemoryPool memoryPool;
    private final AuthManager authManager;
    private final Clock clock;
    private final boolean readByDefault;

    public HttpTransactionManager( DatabaseManagementService managementService, MemoryPool memoryPool,
                                   JobScheduler jobScheduler, Clock clock, Duration transactionTimeout,
                                   LogProvider userLogProvider, TransactionManager transactionManager, BoltGraphDatabaseManagementServiceSPI boltSPI,
                                   AuthManager authManager, boolean readByDefault )
    {
        this.managementService = managementService;
        this.jobScheduler = jobScheduler;
        this.transactionManager = transactionManager;
        this.boltSPI = boltSPI;
        this.userLogProvider = userLogProvider;
        this.memoryPool = memoryPool;
        this.authManager = authManager;
        this.clock = clock;
        this.readByDefault = readByDefault;

        transactionRegistry = new TransactionHandleRegistry( clock, transactionTimeout, userLogProvider, memoryPool );
        scheduleTransactionTimeout( transactionTimeout );
    }

    /**
     * Creates and returns a transaction facade for a given database.
     *
     * @param databaseName database name.
     * @return a transaction facade or {@code null} if a database with the supplied database name does not exist.
     */
    public Optional<GraphDatabaseAPI> getGraphDatabaseAPI( String databaseName )
    {
        Optional<GraphDatabaseAPI> database;
        try
        {
            database = Optional.of( (GraphDatabaseAPI) managementService.database( databaseName ) );
        }
        catch ( DatabaseNotFoundException e )
        {
            database = Optional.empty();
        }
        return database;
    }

    public TransactionHandleRegistry getTransactionHandleRegistry()
    {
        return transactionRegistry;
    }

    public TransactionFacade createTransactionFacade( GraphDatabaseAPI databaseAPI, MemoryTracker memoryTracker, String databaseName )
    {
        var dependencyResolver = databaseAPI.getDependencyResolver();

        memoryTracker.allocateHeap( TransactionFacade.SHALLOW_SIZE );
        return new TransactionFacade( databaseName,
                dependencyResolver.resolveDependency( QueryExecutionEngine.class ), transactionRegistry,
                                      transactionManager, userLogProvider, boltSPI, authManager, readByDefault );
    }

    public TransactionFacade createTransactionFacade( GraphDatabaseAPI databaseAPI, MemoryTracker memoryTracker, String databaseName,
                                                      boolean isReadOnlyTransaction )
    {
        var dependencyResolver = databaseAPI.getDependencyResolver();

        memoryTracker.allocateHeap( TransactionFacade.SHALLOW_SIZE );
        return new TransactionFacade( databaseName,
                                      dependencyResolver.resolveDependency( QueryExecutionEngine.class ), transactionRegistry,
                                      transactionManager, userLogProvider, boltSPI, authManager, isReadOnlyTransaction );
    }

    private void scheduleTransactionTimeout( Duration timeout )
    {
        long timeoutMillis = timeout.toMillis();
        long runEvery = round( timeoutMillis / 2.0 );
        jobScheduler.scheduleRecurring( Group.SERVER_TRANSACTION_TIMEOUT, systemJob( "Timeout of HTTP transactions" ), () ->
        {
            long maxAge = clock.millis() - timeoutMillis;
            transactionRegistry.rollbackSuspendedTransactionsIdleSince( maxAge );
        }, runEvery, MILLISECONDS );
    }
}
