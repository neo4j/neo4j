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
package org.neo4j.cypher.internal.javacompat;

import java.time.Clock;

import org.neo4j.cypher.CypherException;
import org.neo4j.cypher.internal.CacheTracer;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.cypher.internal.StringCacheMonitor;
import org.neo4j.cypher.internal.tracing.CompilationTracer;
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.ResultBuffer;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.virtual.MapValue;

/**
 * To run a Cypher query, use this class.
 *
 * This class construct and initialize both the cypher compiler and the cypher runtime, which is a very expensive
 * operation so please make sure this will be constructed only once and properly reused.
 *
 */
public class ExecutionEngine implements QueryExecutionEngine
{
    private org.neo4j.cypher.internal.ExecutionEngine inner;

    /**
     * Creates an execution engine around the give graph database
     * @param queryService The database to wrap
     * @param logProvider A {@link LogProvider} for cypher-statements
     */
    public ExecutionEngine( GraphDatabaseQueryService queryService, LogProvider logProvider, CompilerFactory compilerFactory )
    {
        DependencyResolver resolver = queryService.getDependencyResolver();
        Monitors monitors = resolver.resolveDependency( Monitors.class );
        CacheTracer cacheTracer = new MonitoringCacheTracer( monitors.newMonitor( StringCacheMonitor.class ) );
        Config config = resolver.resolveDependency( Config.class );
        CypherConfiguration cypherConfiguration = CypherConfiguration.fromConfig( config );
        CompilationTracer tracer =
                new TimingCompilationTracer( monitors.newMonitor( TimingCompilationTracer.EventListener.class ) );
        inner = new org.neo4j.cypher.internal.ExecutionEngine( queryService,
                                                               monitors,
                                                               tracer,
                                                               cacheTracer,
                                                               cypherConfiguration,
                                                               compilerFactory,
                                                               logProvider,
                                                               Clock.systemUTC() );
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        try
        {
            return inner.execute( query, parameters, context, false );
        }
        catch ( CypherException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }

    @Override
    public Result profileQuery( String query, MapValue parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        try
        {
            return inner.execute( query, parameters, context, true );
        }
        catch ( CypherException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return inner.isPeriodicCommit( query );
    }

    @Override
    public long clearQueryCaches()
    {
        return inner.clearQueryCaches();
    }
}
