/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.javacompat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class SnapshotExecutionEngineTest {
    @Inject
    private GraphDatabaseService db;

    private SnapshotExecutionEngine executionEngine;
    private VersionContext versionContext;
    private SnapshotExecutionEngine.QueryExecutor executor;
    private QueryStatistics statistics;
    private TransactionalContext transactionalContext;
    private final Config config = Config.defaults();

    @BeforeEach
    void setUp() throws Exception {
        transactionalContext = mock(TransactionalContext.class, RETURNS_DEEP_STUBS);
        KernelStatement kernelStatement = mock(KernelStatement.class);
        executor = mock(SnapshotExecutionEngine.QueryExecutor.class);
        versionContext = mock(VersionContext.class);
        statistics = mock(QueryStatistics.class);

        executionEngine = new SnapshotExecutionEngine(
                new GraphDatabaseCypherService(db),
                config,
                mock(CypherQueryCaches.class),
                NullLogProvider.getInstance(),
                mock(CompilerFactory.class));
        CursorContextFactory contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new VersionContextSupplier() {
                    @Override
                    public void init(
                            TransactionIdSnapshotFactory transactionIdSnapshotFactory,
                            OldestTransactionIdFactory oldestTransactionIdFactory) {}

                    @Override
                    public VersionContext createVersionContext() {
                        return versionContext;
                    }
                });
        CursorContext cursorContext = contextFactory.create("SnapshotExecutionEngineTest");
        when(transactionalContext.kernelTransaction().cursorContext()).thenReturn(cursorContext);
        when(transactionalContext.statement()).thenReturn(kernelStatement);
        var innerExecution = mock(QueryExecution.class);
        when(executor.execute(any())).thenAnswer((Answer<QueryExecution>) invocationOnMock -> {
            MaterialisedResult materialisedResult = invocationOnMock.getArgument(0);
            materialisedResult.onResultCompleted(statistics);
            return innerExecution;
        });
    }

    @Test
    void executeQueryWithoutRetries() throws QueryExecutionKernelException {
        executionEngine.executeWithRetries("query", transactionalContext, executor);

        verify(executor).execute(any());
        verify(versionContext).initRead();
    }

    @Test
    void executeQueryAfterSeveralRetries() throws QueryExecutionKernelException {
        when(versionContext.isDirty()).thenReturn(true, true, false);

        executionEngine.executeWithRetries("query", transactionalContext, executor);

        verify(executor, times(3)).execute(any());
        verify(versionContext, times(3)).initRead();
    }

    @Test
    void failWriteQueryAfterFirstRetry() throws QueryExecutionKernelException {
        when(statistics.containsUpdates()).thenReturn(true);

        when(versionContext.isDirty()).thenReturn(true, true, false);

        QueryExecutionKernelException e = assertThrows(
                QueryExecutionKernelException.class,
                () -> executionEngine.executeWithRetries("query", transactionalContext, executor));
        assertEquals("Unable to get clean data snapshot for query 'query' that performs updates.", e.getMessage());

        verify(executor, times(1)).execute(any());
        verify(versionContext, times(1)).initRead();
    }

    @Test
    void failQueryAfterMaxRetriesReached() throws QueryExecutionKernelException {
        when(versionContext.isDirty()).thenReturn(true);

        QueryExecutionKernelException e = assertThrows(
                QueryExecutionKernelException.class,
                () -> executionEngine.executeWithRetries("query", transactionalContext, executor));
        assertEquals("Unable to get clean data snapshot for query 'query' after 5 attempts.", e.getMessage());

        verify(executor, times(5)).execute(any());
        verify(versionContext, times(5)).initRead();
    }
}
