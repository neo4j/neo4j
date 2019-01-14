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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.cypher.internal.CompatibilityFactory;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SnapshotExecutionEngineTest
{
    @Rule
    public final DatabaseRule database = new ImpermanentDatabaseRule();

    private CompatibilityFactory compatibilityFactory;
    private TestSnapshotExecutionEngine executionEngine;
    private VersionContext versionContext;
    private SnapshotExecutionEngine.ParametrizedQueryExecutor executor;
    private TransactionalContext transactionalContext;
    private final Config config = Config.defaults();

    @Before
    public void setUp() throws Exception
    {
        GraphDatabaseQueryService cypherService = new GraphDatabaseCypherService( this.database.getGraphDatabaseAPI() );

        compatibilityFactory = mock( CompatibilityFactory.class );
        transactionalContext = mock( TransactionalContext.class );
        KernelStatement kernelStatement = mock( KernelStatement.class );
        executor = mock( SnapshotExecutionEngine.ParametrizedQueryExecutor.class );
        versionContext = mock( VersionContext.class );

        executionEngine = createExecutionEngine(cypherService);
        when( kernelStatement.getVersionContext() ).thenReturn( versionContext );
        when( transactionalContext.statement() ).thenReturn( kernelStatement );
        Result result = mock( Result.class );
        QueryStatistics statistics = mock( QueryStatistics.class );
        when( result.getQueryStatistics() ).thenReturn( statistics );
        when( executor.execute( any(), anyMap(), any() ) ).thenReturn( result );
    }

    @Test
    public void executeQueryWithoutRetries() throws QueryExecutionKernelException
    {
        executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor );

        verify( executor, times( 1 ) ).execute( any(), anyMap(), any() );
        verify( versionContext, times( 1 ) ).initRead();
    }

    @Test
    public void executeQueryAfterSeveralRetries() throws QueryExecutionKernelException
    {
        when( versionContext.isDirty() ).thenReturn( true, true, false );

        executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor );

        verify( executor, times( 3 ) ).execute( any(), anyMap(), any() );
        verify( versionContext, times( 3 ) ).initRead();
    }

    @Test
    public void failQueryAfterMaxRetriesReached() throws QueryExecutionKernelException
    {
        when( versionContext.isDirty() ).thenReturn( true );

        try
        {
            executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor );
        }
        catch ( QueryExecutionKernelException e )
        {
            assertEquals( "Unable to get clean data snapshot for query 'query' after 5 attempts.", e.getMessage() );
        }

        verify( executor, times( 5 ) ).execute( any(), anyMap(), any() );
        verify( versionContext, times( 5 ) ).initRead();
    }

    private class TestSnapshotExecutionEngine extends SnapshotExecutionEngine
    {

        TestSnapshotExecutionEngine( GraphDatabaseQueryService queryService, Config config, LogProvider logProvider,
                CompatibilityFactory compatibilityFactory )
        {
            super( queryService, config, logProvider, compatibilityFactory );
        }

        @Override
        public <T> Result executeWithRetries( String query, T parameters, TransactionalContext context,
                ParametrizedQueryExecutor<T> executor ) throws QueryExecutionKernelException
        {
            return super.executeWithRetries( query, parameters, context, executor );
        }
    }

    private TestSnapshotExecutionEngine createExecutionEngine( GraphDatabaseQueryService cypherService )
    {
        return new TestSnapshotExecutionEngine( cypherService, config, NullLogProvider.getInstance(),
                compatibilityFactory );
    }
}
