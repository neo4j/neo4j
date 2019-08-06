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
package org.neo4j.cypher.internal.javacompat;

import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.virtual.MapValue;

/**
 * {@link ExecutionEngine} engine that will try to run cypher query with guarantee that query will never see any data
 * that coming from transaction that are newer then transaction that was the last closed on a moment when
 * {@link VersionContext} was initialised. Observed behaviour is the same as executing query on top data snapshot for
 * that version.
 */
public class SnapshotExecutionEngine extends ExecutionEngine
{
    private final int maxQueryExecutionAttempts;

    SnapshotExecutionEngine( GraphDatabaseQueryService queryService, Config config, LogProvider logProvider,
                             CompilerFactory compilerFactory )
    {
        super( queryService, logProvider, compilerFactory );
        this.maxQueryExecutionAttempts = config.get( GraphDatabaseSettings.snapshot_query_retries );
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext context, boolean prePopulate )
            throws QueryExecutionKernelException
    {
        return executeWithRetries( query, parameters, context, super::executeQuery, prePopulate ).other();
    }

    @Override
    public QueryExecution executeQuery( String query, MapValue parameters, TransactionalContext context, boolean prePopulate, QuerySubscriber subscriber )
            throws QueryExecutionKernelException
    {
        var pair = executeWithRetries( query, parameters, context, super::executeQuery, prePopulate );
        return pair.other().streamToSubscriber( subscriber, pair.first() );
    }

    protected <T> Pair<QueryExecution, EagerResult> executeWithRetries( String query,
                                                                   T parameters,
                                                                   TransactionalContext context,
                                                                   ParametrizedQueryExecutor<T> executor,
                                                                   boolean prePopulate ) throws QueryExecutionKernelException
    {
        VersionContext versionContext = getCursorContext( context );
        QueryExecution queryExecution;
        EagerResult eagerResult;
        int attempt = 0;
        boolean dirtySnapshot;
        do
        {
            if ( attempt == maxQueryExecutionAttempts )
            {
                throw new QueryExecutionKernelException( new UnstableSnapshotException( "Unable to get clean data snapshot for query '%s' after %d attempts.",
                                                                                        query, attempt ) );
            }
            attempt++;
            versionContext.initRead();

            ResultSubscriber resultSubscriber = getResultSubscriber( context );

            queryExecution = executor.execute( query, parameters, context, prePopulate, resultSubscriber );
            resultSubscriber.init( queryExecution );

            eagerResult = getEagerResult( versionContext, resultSubscriber );
            eagerResult.consume();
            dirtySnapshot = versionContext.isDirty();
            if ( dirtySnapshot && resultSubscriber.getQueryStatistics().containsUpdates() )
            {
                throw new QueryExecutionKernelException( new UnstableSnapshotException(
                        "Unable to get clean data snapshot for query '%s' that performs updates.", query, attempt ) );
            }
        }
        while ( dirtySnapshot );

        return Pair.of(queryExecution, eagerResult);
    }

    protected EagerResult getEagerResult( VersionContext versionContext, ResultSubscriber resultSubscriber )
    {
        return new EagerResult( resultSubscriber, versionContext );
    }

    protected ResultSubscriber getResultSubscriber( TransactionalContext context )
    {
        return new ResultSubscriber( context );
    }

    private static VersionContext getCursorContext( TransactionalContext context )
    {
        return ((KernelStatement) context.statement()).getVersionContext();
    }

    @FunctionalInterface
    protected interface ParametrizedQueryExecutor<T>
    {
        QueryExecution execute( String query, T parameters, TransactionalContext context, boolean prePopulate, QuerySubscriber subscriber )
                throws QueryExecutionKernelException;
    }
}
