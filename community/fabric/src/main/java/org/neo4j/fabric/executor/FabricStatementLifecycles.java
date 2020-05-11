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

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cypher.internal.CypherQueryObfuscator;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.ExecutingQueryFactory;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

public class FabricStatementLifecycles
{
    private final Monitors monitors;
    private final AtomicReference<CpuClock> cpuClockReference;
    private final ExecutingQueryFactory executingQueryFactory;

    public FabricStatementLifecycles( Monitors monitors )
    {
        this.monitors = monitors;
        // TODO: Fix clock setup here
        this.cpuClockReference = new AtomicReference<>( CpuClock.NOT_AVAILABLE );
        this.executingQueryFactory = new ExecutingQueryFactory(
                Clocks.nanoClock(),
                cpuClockReference );
    }

    StatementLifecycle create( FabricTransactionInfo transactionInfo, String statement, MapValue params )
    {
        var executingQuery = executingQueryFactory.createUnbound(
                statement, params,
                transactionInfo.getClientConnectionInfo(),
                transactionInfo.getLoginContext().subject().username(),
                transactionInfo.getTxMetadata() );
        var queryExecutionMonitor = monitors.newMonitor( QueryExecutionMonitor.class );

        return new StatementLifecycle( executingQuery, queryExecutionMonitor );
    }

    static class StatementLifecycle
    {
        protected final ExecutingQuery executingQuery;
        protected final QueryExecutionMonitor monitor;

        private MonitoringMode monitoringMode;

        private StatementLifecycle( ExecutingQuery executingQuery, QueryExecutionMonitor monitor )
        {
            this.executingQuery = executingQuery;
            this.monitor = monitor;
        }

        void startProcessing()
        {
            monitor.startProcessing( executingQuery );
        }

        void doneFabricProcessing( FabricPlan plan )
        {
            executingQuery.onObfuscatorReady( CypherQueryObfuscator.apply( plan.obfuscationMetadata() ) );

            if ( plan.inFabricContext() )
            {
                monitoringMode = new ParentChildMonitoringMode();
            }
            else
            {
                monitoringMode = new SingleQueryMonitoringMode();
            }
        }

        void startExecution()
        {
            monitor.startExecution( executingQuery );
            monitoringMode.startExecution();
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

        QueryExecutionMonitor getChildQueryMonitor()
        {
            return monitoringMode.getChildQueryMonitor();
        }

        boolean isParentChildMonitoringMode()
        {
            return monitoringMode.isParentChildMonitoringMode();
        }

        private abstract static class MonitoringMode
        {
            abstract boolean isParentChildMonitoringMode();

            abstract QueryExecutionMonitor getChildQueryMonitor();

            abstract void startExecution();
        }

        private static class SingleQueryMonitoringMode extends MonitoringMode
        {
            @Override
            boolean isParentChildMonitoringMode()
            {
                return false;
            }

            @Override
            void startExecution()
            {
                // Query state events triggered by cypher engine
            }

            @Override
            QueryExecutionMonitor getChildQueryMonitor()
            {
                // Query monitoring events handled by fabric
                return QueryExecutionMonitor.NO_OP;
            }
        }

        private class ParentChildMonitoringMode extends MonitoringMode
        {
            @Override
            boolean isParentChildMonitoringMode()
            {
                return true;
            }

            @Override
            void startExecution()
            {
                executingQuery.onCompilationCompleted( null, null, null );
                executingQuery.onExecutionStarted( OptionalMemoryTracker.NONE );
            }

            @Override
            QueryExecutionMonitor getChildQueryMonitor()
            {
                return monitor;
            }
        }
    }
}
