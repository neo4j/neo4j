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
package org.neo4j.fabric.executor;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.cypher.internal.CypherQueryObfuscator;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.planning.FabricPlan;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.api.ExecutingQueryFactory;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.memory.HeapHighWaterMarkTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class FabricStatementLifecycles
{
    private final DatabaseManager<? extends DatabaseContext> databaseManager;
    private final QueryExecutionMonitor dbmsMonitor;
    private final ExecutingQueryFactory executingQueryFactory;

    public FabricStatementLifecycles( DatabaseManager<? extends DatabaseContext> databaseManager,
            Monitors dbmsMonitors, Config config, SystemNanoClock systemNanoClock )
    {
        this.databaseManager = databaseManager;
        this.dbmsMonitor = dbmsMonitors.newMonitor( QueryExecutionMonitor.class );
        this.executingQueryFactory = new ExecutingQueryFactory(
                systemNanoClock,
                setupCpuClockAtomicReference( config ),
                config );
    }

    private static AtomicReference<CpuClock> setupCpuClockAtomicReference( Config config )
    {
        AtomicReference<CpuClock> cpuClock = new AtomicReference<>( CpuClock.NOT_AVAILABLE );
        SettingChangeListener<Boolean> cpuClockUpdater = ( before, after ) ->
        {
            if ( after )
            {
                cpuClock.set( CpuClock.CPU_CLOCK );
            }
            else
            {
                cpuClock.set( CpuClock.NOT_AVAILABLE );
            }
        };
        cpuClockUpdater.accept( null, config.get( GraphDatabaseSettings.track_query_cpu_time ) );
        config.addListener( GraphDatabaseSettings.track_query_cpu_time, cpuClockUpdater );
        return cpuClock;
    }

    StatementLifecycle create( FabricTransactionInfo transactionInfo, String statement, MapValue params )
    {
        var executingQuery = executingQueryFactory.createUnbound(
                statement, params,
                transactionInfo.getClientConnectionInfo(),
                transactionInfo.getLoginContext().subject().executingUser(),
                transactionInfo.getLoginContext().subject().authenticatedUser(),
                transactionInfo.getTxMetadata() );

        return new StatementLifecycle( executingQuery );
    }

    public enum StatementPhase
    {
        FABRIC, CYPHER, ENDED
    }

    public class StatementLifecycle
    {
        private final ExecutingQuery executingQuery;

        private QueryExecutionMonitor dbMonitor;
        private StatementPhase phase;
        private MonitoringMode monitoringMode;

        private StatementLifecycle( ExecutingQuery executingQuery )
        {
            this.executingQuery = executingQuery;
            this.phase = StatementPhase.FABRIC;
        }

        void startProcessing()
        {
            getQueryExecutionMonitor().startProcessing( executingQuery );
        }

        void doneFabricProcessing( FabricPlan plan )
        {
            executingQuery.onObfuscatorReady( CypherQueryObfuscator.apply( plan.obfuscationMetadata() ) );
            executingQuery.onFabricDeprecationNotificationsProviderReady( plan.deprecationNotificationsProvider() );

            if ( plan.inFabricContext() )
            {
                monitoringMode = new ParentChildMonitoringMode();
            }
            else
            {
                monitoringMode = new SingleQueryMonitoringMode();
            }
        }

        void startExecution( Boolean shouldLogIfSingleQuery )
        {
                monitoringMode.startExecution(shouldLogIfSingleQuery);
        }

        void doneFabricPhase()
        {
            phase = StatementPhase.CYPHER;
        }

        void endSuccess()
        {
            phase = StatementPhase.ENDED;
            QueryExecutionMonitor monitor = getQueryExecutionMonitor();
            monitor.beforeEnd( executingQuery, true );
            monitor.endSuccess( executingQuery );
        }

        void endFailure( Throwable failure )
        {
            phase = StatementPhase.ENDED;
            QueryExecutionMonitor monitor = getQueryExecutionMonitor();
            monitor.beforeEnd( executingQuery, false );
            monitor.endFailure( executingQuery, failure.getMessage() );
        }

        private QueryExecutionMonitor getQueryExecutionMonitor()
        {
            return getDbMonitor().orElse( dbmsMonitor );
        }

        private Optional<QueryExecutionMonitor> getDbMonitor()
        {
            if ( dbMonitor == null )
            {
                executingQuery.databaseId()
                              .flatMap( databaseManager::getDatabaseContext )
                              .map( dbm -> dbm.dependencies().resolveDependency( Monitors.class ) )
                              .map( monitors -> monitors.newMonitor( QueryExecutionMonitor.class ) )
                              .ifPresent( monitor -> dbMonitor = monitor );
            }

            return Optional.ofNullable( dbMonitor );
        }

        public boolean inFabricPhase()
        {
            return phase == StatementPhase.FABRIC;
        }

        public ExecutingQuery getMonitoredQuery()
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

        private abstract class MonitoringMode
        {
            abstract boolean isParentChildMonitoringMode();

            abstract QueryExecutionMonitor getChildQueryMonitor();

            abstract void startExecution( Boolean shouldLogIfSingleQuery );
        }

        private class SingleQueryMonitoringMode extends MonitoringMode
        {
            @Override
            boolean isParentChildMonitoringMode()
            {
                return false;
            }

            @Override
            void startExecution( Boolean shouldLogIfSingleQuery )
            {
                // Query state events triggered by cypher engine
                if ( shouldLogIfSingleQuery )
                {
                    getQueryExecutionMonitor().startExecution( executingQuery );
                }
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
            void startExecution( Boolean shouldLogIfSingleQuery )
            {
                if ( !shouldLogIfSingleQuery )
                {
                    getQueryExecutionMonitor().startExecution( executingQuery );
                    executingQuery.onCompilationCompleted( null, null, null );
                    executingQuery.onExecutionStarted( HeapHighWaterMarkTracker.NONE );
                }
            }

            @Override
            QueryExecutionMonitor getChildQueryMonitor()
            {
                return getQueryExecutionMonitor();
            }
        }
    }
}
