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
package org.neo4j.kernel.impl.query;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.ExecutionStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;

class Neo4jTransactionalContextTest {
    private GraphDatabaseQueryService queryService;
    private KernelStatement statement;
    private ConfiguredExecutionStatistics statistics;
    private final KernelTransactionFactory transactionFactory = mock(KernelTransactionFactory.class);
    private final NamedDatabaseId namedDatabaseId = from(DEFAULT_DATABASE_NAME, UUID.randomUUID());
    private QueryRegistry queryRegistry;

    @BeforeEach
    void setUp() {
        setUpMocks();
    }

    @Test
    void contextRollbackClosesAndRollbackTransaction() {
        ExecutingQuery executingQuery = mock(ExecutingQuery.class);
        InternalTransaction internalTransaction = mock(InternalTransaction.class, new ReturnsDeepStubs());
        KernelTransaction kernelTransaction = mockTransaction(statement);
        when(internalTransaction.kernelTransaction()).thenReturn(kernelTransaction);

        Neo4jTransactionalContext transactionalContext = new Neo4jTransactionalContext(
                null,
                internalTransaction,
                statement,
                executingQuery,
                transactionFactory,
                QueryExecutionConfiguration.DEFAULT_CONFIG);

        transactionalContext.rollback();

        verify(internalTransaction).rollback();
        assertFalse(transactionalContext.isOpen());
    }

    @Test
    void rollsBackNewlyCreatedTransactionIfTerminationDetectedOnCloseDuringPeriodicCommit()
            throws TransactionFailureException {
        // Given
        InternalTransaction userTransaction = mock(InternalTransaction.class, new ReturnsDeepStubs());
        KernelTransaction.Type transactionType = KernelTransaction.Type.IMPLICIT;
        SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        ClientConnectionInfo connectionInfo = ClientConnectionInfo.EMBEDDED_CONNECTION;
        when(userTransaction.transactionType()).thenReturn(transactionType);
        when(userTransaction.clientInfo()).thenReturn(connectionInfo);
        when(userTransaction.securityContext()).thenReturn(securityContext);
        when(userTransaction.terminationReason()).thenReturn(Optional.empty());

        GraphDatabaseQueryService queryService = mock(GraphDatabaseQueryService.class);
        KernelStatement initialStatement = mock(KernelStatement.class);
        KernelTransaction initialKTX = mockTransaction(initialStatement);
        QueryRegistry initialQueryRegistry = mock(QueryRegistry.class);
        ExecutingQuery executingQuery = mock(ExecutingQuery.class);

        KernelStatement secondStatement = mock(KernelStatement.class);
        KernelTransaction secondKTX = mockTransaction(secondStatement);
        QueryRegistry secondQueryRegistry = mock(QueryRegistry.class);

        when(transactionFactory.beginKernelTransaction(transactionType, securityContext, connectionInfo, null))
                .thenReturn(secondKTX);
        when(executingQuery.databaseId()).thenReturn(Optional.of(namedDatabaseId));
        Mockito.doThrow(RuntimeException.class).when(initialKTX).commit(any());
        when(initialStatement.queryRegistry()).thenReturn(initialQueryRegistry);
        when(userTransaction.kernelTransaction()).thenReturn(initialKTX, initialKTX, secondKTX);
        when(secondStatement.queryRegistry()).thenReturn(secondQueryRegistry);

        Neo4jTransactionalContext context = new Neo4jTransactionalContext(
                queryService,
                userTransaction,
                initialStatement,
                executingQuery,
                transactionFactory,
                QueryExecutionConfiguration.DEFAULT_CONFIG);

        // When
        assertThrows(RuntimeException.class, context::commitAndRestartTx);

        Mockito.verify(userTransaction).rollback();
    }

    @Test
    void shouldCloseInnerTransactionOnOuterTermination() {
        // Given
        ExecutingQuery executingQuery = mock(ExecutingQuery.class);
        InternalTransaction transaction = mock(InternalTransaction.class, new ReturnsDeepStubs());
        InternalTransaction innerTransaction = mock(InternalTransaction.class, new ReturnsDeepStubs());

        KernelTransaction kernelTransaction = mockTransaction(statement);
        when(transaction.kernelTransaction()).thenReturn(kernelTransaction);
        when(transaction.transactionType()).thenReturn(KernelTransaction.Type.IMPLICIT);
        GraphDatabaseQueryService graph = mock(GraphDatabaseQueryService.class);
        when(graph.beginTransaction(any(), any(), any())).thenReturn(innerTransaction);
        TransactionTerminatedException error = new TransactionTerminatedException(Status.Transaction.Terminated);
        when(innerTransaction.kernelTransaction()).thenThrow(error);

        // When
        Neo4jTransactionalContext transactionalContext = new Neo4jTransactionalContext(
                graph,
                transaction,
                statement,
                executingQuery,
                transactionFactory,
                QueryExecutionConfiguration.DEFAULT_CONFIG);

        // Then
        assertThatThrownBy(transactionalContext::contextWithNewTransaction).isSameAs(error);
        verify(innerTransaction).close();
    }

    @Test
    void shouldBeOpenAfterCreation() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);

        Neo4jTransactionalContext context = newContext(tx);

        assertTrue(context.isOpen());
    }

    @Test
    void shouldBeTopLevelWithImplicitTx() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        when(tx.transactionType()).thenReturn(KernelTransaction.Type.IMPLICIT);

        Neo4jTransactionalContext context = newContext(tx);

        assertTrue(context.isTopLevelTx());
    }

    @Test
    void shouldNotBeTopLevelWithExplicitTx() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        when(tx.transactionType()).thenReturn(KernelTransaction.Type.EXPLICIT);

        Neo4jTransactionalContext context = newContext(tx);

        assertFalse(context.isTopLevelTx());
    }

    @Test
    void shouldNotCloseTransactionDuringTermination() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        when(tx.transactionType()).thenReturn(KernelTransaction.Type.IMPLICIT);

        Neo4jTransactionalContext context = newContext(tx);

        context.terminate();

        verify(tx).terminate();
        verify(tx, never()).close();
    }

    @Test
    void shouldBePossibleToCloseAfterTermination() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        when(tx.transactionType()).thenReturn(KernelTransaction.Type.IMPLICIT);

        Neo4jTransactionalContext context = newContext(tx);

        context.terminate();

        verify(tx).terminate();
        verify(tx, never()).close();

        context.close();
    }

    @Test
    void shouldBePossibleToTerminateWithoutActiveTransaction() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        Neo4jTransactionalContext context = newContext(tx);

        context.close();

        context.terminate();
        verify(tx, never()).terminate();
    }

    @Test
    void shouldThrowWhenRestartedAfterTermination() {
        MutableObject<Status> terminationReason = new MutableObject<>();
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        doAnswer(invocation -> {
                    terminationReason.setValue(Status.Transaction.Terminated);
                    return null;
                })
                .when(tx)
                .terminate();
        when(tx.terminationReason()).then(invocation -> Optional.ofNullable(terminationReason.getValue()));

        Neo4jTransactionalContext context = newContext(tx);

        context.terminate();

        assertThrows(TransactionTerminatedException.class, context::commitAndRestartTx);
    }

    @Test
    void shouldThrowWhenAssertIsOpenAfterTermination() {
        MutableObject<Status> terminationReason = new MutableObject<>();
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        doAnswer(invocation -> {
                    terminationReason.setValue(Status.Transaction.Terminated);
                    return null;
                })
                .when(tx)
                .terminate();
        when(tx.terminationReason()).then(invocation -> Optional.ofNullable(terminationReason.getValue()));

        Neo4jTransactionalContext context = newContext(tx);

        context.terminate();

        assertThrows(TransactionTerminatedException.class, context::getOrBeginNewIfClosed);
    }

    @Test
    void shouldBePossibleToCloseMultipleTimes() {
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        Neo4jTransactionalContext context = newContext(tx);

        assertDoesNotThrow(() -> {
            context.close();
            context.close();
            context.close();
        });
    }

    @Test
    void shouldUnbindExecutingQueryAfterCommit() {
        // Given
        InternalTransaction tx = mock(InternalTransaction.class, RETURNS_DEEP_STUBS);
        Neo4jTransactionalContext context = newContext(tx);

        // When
        context.commit();

        // Then
        InOrder inOrder = inOrder(statement, tx, queryRegistry);
        inOrder.verify(statement).close();
        inOrder.verify(tx).commit(any());
        inOrder.verify(queryRegistry).unbindExecutingQuery(any(), anyLong());
    }

    private void setUpMocks() {
        queryService = mock(GraphDatabaseQueryService.class);
        DependencyResolver resolver = mock(DependencyResolver.class);
        statement = mock(KernelStatement.class);

        statistics = new ConfiguredExecutionStatistics();
        queryRegistry = mock(QueryRegistry.class);
        InternalTransaction internalTransaction = mock(InternalTransaction.class);
        when(internalTransaction.terminationReason()).thenReturn(Optional.empty());

        when(statement.queryRegistry()).thenReturn(queryRegistry);
        when(queryService.getDependencyResolver()).thenReturn(resolver);
        when(queryService.beginTransaction(any(), any(), any())).thenReturn(internalTransaction);
    }

    private Neo4jTransactionalContext newContext(InternalTransaction initialTx) {
        ExecutingQuery executingQuery = mock(ExecutingQuery.class);
        when(executingQuery.databaseId()).thenReturn(Optional.of(namedDatabaseId));
        return new Neo4jTransactionalContext(
                queryService,
                initialTx,
                statement,
                executingQuery,
                transactionFactory,
                QueryExecutionConfiguration.DEFAULT_CONFIG);
    }

    private KernelTransaction mockTransaction(Statement statement) {
        KernelTransaction kernelTransaction = mock(KernelTransaction.class, new ReturnsDeepStubs());
        when(kernelTransaction.executionStatistics()).thenReturn(statistics);
        when(kernelTransaction.acquireStatement()).thenReturn(statement);
        return kernelTransaction;
    }

    private static class ConfiguredExecutionStatistics implements ExecutionStatistics {
        private long hits;
        private long faults;

        @Override
        public long pageHits() {
            return hits;
        }

        @Override
        public long pageFaults() {
            return faults;
        }
    }
}
