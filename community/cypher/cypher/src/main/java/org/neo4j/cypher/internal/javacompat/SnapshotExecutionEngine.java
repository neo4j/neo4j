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

import java.util.Map;

import org.neo4j.cypher.internal.CompatibilityFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
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
            CompatibilityFactory compatibilityFactory )
    {
        super( queryService, logProvider, compatibilityFactory );
        this.maxQueryExecutionAttempts = config.get( GraphDatabaseSettings.snapshot_query_retries );
    }

    @Override
    public Result executeQuery( String query, MapValue parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        return executeWithRetries( query, parameters, context, super::executeQuery );
    }

    @Override
    public Result executeQuery( String query, Map<String,Object> parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        return executeWithRetries( query, parameters, context, super::executeQuery );
    }

    @Override
    public Result profileQuery( String query, Map<String,Object> parameters, TransactionalContext context )
            throws QueryExecutionKernelException
    {
        return executeWithRetries( query, parameters, context, super::profileQuery );
    }

    protected <T> Result executeWithRetries( String query, T parameters, TransactionalContext context,
            ParametrizedQueryExecutor<T> executor ) throws QueryExecutionKernelException
    {
        VersionContext versionContext = getCursorContext( context );
        EagerResult eagerResult;
        int attempt = 0;
        boolean dirtySnapshot;
        do
        {
            if ( attempt == maxQueryExecutionAttempts )
            {
                return throwQueryExecutionException(
                        "Unable to get clean data snapshot for query '%s' after %d attempts.", query, attempt );
            }
            attempt++;
            versionContext.initRead();
            Result result = executor.execute( query, parameters, context );
            eagerResult = new EagerResult( result, versionContext );
            eagerResult.consume();
            dirtySnapshot = versionContext.isDirty();
            if ( dirtySnapshot && result.getQueryStatistics().containsUpdates() )
            {
                return throwQueryExecutionException(
                        "Unable to get clean data snapshot for query '%s' that perform updates.", query, attempt );
            }
        }
        while ( dirtySnapshot );
        return eagerResult;
    }

    private Result throwQueryExecutionException( String message, Object... parameters ) throws
            QueryExecutionKernelException
    {
        throw new QueryExecutionKernelException( new UnstableSnapshotException( message, parameters ) );
    }

    private static VersionContext getCursorContext( TransactionalContext context )
    {
        return ((KernelStatement) context.statement()).getVersionContext();
    }

    @FunctionalInterface
    protected interface ParametrizedQueryExecutor<T>
    {
        Result execute( String query, T parameters, TransactionalContext context ) throws QueryExecutionKernelException;
    }

}
