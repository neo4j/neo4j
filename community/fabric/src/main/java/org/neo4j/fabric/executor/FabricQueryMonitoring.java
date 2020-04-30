/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.executor;

import java.util.Map;

import org.neo4j.common.DependencyResolver;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.util.MonotonicCounter;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

public class FabricQueryMonitoring
{
    private final DependencyResolver dependencyResolver;
    private final Monitors monitors;
    private DatabaseIdRepository databaseIdRepository;

    public FabricQueryMonitoring( DependencyResolver dependencyResolver, Monitors monitors )
    {
        this.dependencyResolver = dependencyResolver;
        this.monitors = monitors;
    }

    QueryMonitor queryMonitor( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread )
    {
        return new QueryMonitor(
                executingQuery( transactionInfo, statement, params, thread ),
                monitors.newMonitor( QueryExecutionMonitor.class )
        );
    }

    static class QueryMonitor
    {
        private final ExecutingQuery executingQuery;
        private final QueryExecutionMonitor monitor;

        private QueryMonitor( ExecutingQuery executingQuery, QueryExecutionMonitor monitor )
        {
            this.executingQuery = executingQuery;
            this.monitor = monitor;
        }

        void start()
        {
            monitor.start( executingQuery );
        }

        void startExecution()
        {
            executingQuery.onCompilationCompleted( null, null, null );
            executingQuery.onExecutionStarted( OptionalMemoryTracker.NONE );

            monitor.start( executingQuery );
        }

        void endSuccess()
        {
            monitor.beforeEnd( executingQuery, true );
            monitor.endSuccess( executingQuery );
        }

        void endFailure( Throwable failure )
        {
            monitor.beforeEnd( executingQuery, false );
            monitor.endFailure( executingQuery, failure );
        }

        ExecutingQuery getMonitoredQuery()
        {
            return executingQuery;
        }
    }

    private ExecutingQuery executingQuery( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread )
    {

        NamedDatabaseId namedDatabaseId = getDatabaseIdRepository().getByName( transactionInfo.getDatabaseName() ).get();
        return new FabricExecutingQuery( transactionInfo, statement, params, thread, namedDatabaseId );
    }

    private DatabaseIdRepository getDatabaseIdRepository()
    {
        if ( databaseIdRepository == null )
        {
            databaseIdRepository = dependencyResolver.resolveDependency( FabricDatabaseManager.class ).databaseIdRepository();
        }
        return databaseIdRepository;
    }

    private static class FabricExecutingQuery extends ExecutingQuery
    {
        private static final MonotonicCounter internalFabricQueryIdGenerator = MonotonicCounter.newAtomicMonotonicCounter();
        private String internalFabricId;

        private FabricExecutingQuery( FabricTransactionInfo transactionInfo, String statement, MapValue params, Thread thread, NamedDatabaseId namedDatabaseId )
        {
            super( -1, //The actual query id should never be used(leaked) to the dbms
                    transactionInfo.getClientConnectionInfo(),
                    namedDatabaseId,
                    transactionInfo.getLoginContext().subject().username(),
                    statement,
                    params,
                    Map.of(),
                    () -> 0L,
                    () -> 0L,
                    () -> 0L,
                    thread.getId(),
                    thread.getName(),
                    Clocks.nanoClock(),
                    CpuClock.NOT_AVAILABLE
            );
            internalFabricId = "Fabric-" + internalFabricQueryIdGenerator.incrementAndGet();
        }

        @Override
        public String id()
        {
            return internalFabricId;
        }
    }
}
