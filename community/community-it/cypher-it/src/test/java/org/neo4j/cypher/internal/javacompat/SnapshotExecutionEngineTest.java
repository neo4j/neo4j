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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings( "unchecked" )
@ImpermanentDbmsExtension
class SnapshotExecutionEngineTest
{
    @Inject
    private GraphDatabaseService db;

    private GraphDatabaseQueryService cypherService;
    private CompilerFactory compilerFactory;
    private SnapshotExecutionEngine executionEngine;
    private VersionContext versionContext;
    private SnapshotExecutionEngine.ParametrizedQueryExecutor executor;
    private TransactionalContext transactionalContext;
    private final Config config = Config.defaults();

    @BeforeEach
    void setUp() throws Exception
    {
        cypherService = new GraphDatabaseCypherService( db );
        compilerFactory = mock( CompilerFactory.class );
        transactionalContext = mock( TransactionalContext.class, RETURNS_DEEP_STUBS );
        KernelStatement kernelStatement = mock( KernelStatement.class );
        executor = mock( SnapshotExecutionEngine.ParametrizedQueryExecutor.class );
        versionContext = mock( VersionContext.class );

        executionEngine = createExecutionEngine( cypherService, false );
        when( kernelStatement.getVersionContext() ).thenReturn( versionContext );
        when( transactionalContext.statement() ).thenReturn( kernelStatement );
        Result originalResult = mock( Result.class );
        QueryStatistics statistics = mock( QueryStatistics.class );
        when( originalResult.getQueryStatistics() ).thenReturn( statistics );

        var innerExecution = mock( QueryExecution.class );
        when( executor.execute( any(), anyMap(), any(), anyBoolean(), any() ) ).thenReturn( innerExecution );
    }

    @Test
    void executeQueryWithoutRetries() throws QueryExecutionKernelException
    {
        executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor, false );

        verify( executor ).execute( any(), anyMap(), any(), anyBoolean(), any() );
        verify( versionContext ).initRead();
    }

    @Test
    void executeQueryAfterSeveralRetries() throws QueryExecutionKernelException
    {
        when( versionContext.isDirty() ).thenReturn( true, true, false );

        executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor, false );

        verify( executor, times( 3 ) ).execute( any(), anyMap(), any(), anyBoolean(), any() );
        verify( versionContext, times( 3 ) ).initRead();
    }

    @Test
    void failWriteQueryAfterFirstRetry() throws QueryExecutionKernelException
    {
        executionEngine = createExecutionEngine( cypherService, true );

        when( versionContext.isDirty() ).thenReturn( true, true, false );

        QueryExecutionKernelException e = assertThrows( QueryExecutionKernelException.class, () ->
                executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor, false ) );
        assertEquals( "Unable to get clean data snapshot for query 'query' that performs updates.", e.getMessage() );

        verify( executor, times( 1 ) ).execute( any(), anyMap(), any(), anyBoolean(), any() );
        verify( versionContext, times( 1 ) ).initRead();
    }

    @Test
    void failQueryAfterMaxRetriesReached() throws QueryExecutionKernelException
    {
        when( versionContext.isDirty() ).thenReturn( true );

        QueryExecutionKernelException e = assertThrows( QueryExecutionKernelException.class, () ->
                executionEngine.executeWithRetries( "query", Collections.emptyMap(), transactionalContext, executor, false ) );
        assertEquals( "Unable to get clean data snapshot for query 'query' after 5 attempts.", e.getMessage() );

        verify( executor, times( 5 ) ).execute( any(), anyMap(), any(), anyBoolean(), any() );
        verify( versionContext, times( 5 ) ).initRead();
    }

    private SnapshotExecutionEngine createExecutionEngine( GraphDatabaseQueryService cypherService, boolean containsUpdates )
    {
        return new MockingSnapshotExecutionEngine( cypherService, config, NullLogProvider.getInstance(),
                                                   compilerFactory, containsUpdates );
    }

    private static class MockingSnapshotExecutionEngine extends SnapshotExecutionEngine
    {
        private final boolean containsUpdates;

        MockingSnapshotExecutionEngine( GraphDatabaseQueryService queryService,
                                        Config config,
                                        LogProvider logProvider,
                                        CompilerFactory compilerFactory,
                                        boolean containsUpdates )
        {
            super( queryService, config, logProvider, compilerFactory );
            this.containsUpdates = containsUpdates;
        }

        @Override
        protected EagerResult getEagerResult( VersionContext versionContext, ResultSubscriber resultSubscriber )
        {
            return mock( EagerResult.class );
        }

        @Override
        protected ResultSubscriber getResultSubscriber( TransactionalContext context )
        {
            ResultSubscriber mock = mock( ResultSubscriber.class, RETURNS_DEEP_STUBS );
            when( mock.getQueryStatistics().containsUpdates() ).thenReturn( containsUpdates );
            return mock;
        }
    }
}
