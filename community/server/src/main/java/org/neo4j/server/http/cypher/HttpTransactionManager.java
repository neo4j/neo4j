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
package org.neo4j.server.http.cypher;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An entry point for managing transaction in HTTP API.
 */
public class HttpTransactionManager
{
    private final TransactionHandleRegistry transactionRegistry;
    private final DatabaseManagementService managementService;
    private final JobScheduler jobScheduler;

    public HttpTransactionManager( DatabaseManagementService managementService, JobScheduler jobScheduler, Clock clock, Duration transactionTimeout,
            LogProvider userLogProvider )
    {
        this.managementService = managementService;
        this.jobScheduler = jobScheduler;

        transactionRegistry = new TransactionHandleRegistry( clock, transactionTimeout, userLogProvider );
        scheduleTransactionTimeout( transactionTimeout );
    }

    /**
     * Creates and returns a transaction facade for a given database.
     *
     * @param databaseName database name.
     * @return a transaction facade or {@code null} if a database with the supplied database name does not exist.
     */
    public Optional<GraphDatabaseFacade> getGraphDatabaseFacade( String databaseName )
    {
        Optional<GraphDatabaseFacade> graph;
        try
        {
            graph = Optional.of( (GraphDatabaseFacade) managementService.database( databaseName ) );
        }
        catch ( DatabaseNotFoundException e )
        {
            graph = Optional.empty();
        }
        return graph;
    }

    public TransactionHandleRegistry getTransactionHandleRegistry()
    {
        return transactionRegistry;
    }

    public TransactionFacade createTransactionFacade( GraphDatabaseFacade databaseFacade )
    {
        DependencyResolver dependencyResolver = databaseFacade.getDependencyResolver();

        return new TransactionFacade( databaseFacade,
                dependencyResolver.resolveDependency( QueryExecutionEngine.class ), transactionRegistry );
    }

    private void scheduleTransactionTimeout( Duration timeout )
    {
        Clock clock = Clocks.systemClock();

        long timeoutMillis = timeout.toMillis();
        long runEvery = round( timeoutMillis / 2.0 );
        jobScheduler.scheduleRecurring( Group.SERVER_TRANSACTION_TIMEOUT, () ->
        {
            long maxAge = clock.millis() - timeoutMillis;
            transactionRegistry.rollbackSuspendedTransactionsIdleSince( maxAge );
        }, runEvery, MILLISECONDS );
    }
}
