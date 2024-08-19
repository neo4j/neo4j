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
package org.neo4j.kernel;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.KernelTransactionMonitor;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ResourceCloseFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.DefaultTransactionExceptionMapper;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;

class TransactionImplTest {
    private final TokenHolders tokenHolders = mock(TokenHolders.class);
    private final QueryExecutionEngine engine = mock(QueryExecutionEngine.class);
    private final TransactionalContextFactory contextFactory = mock(TransactionalContextFactory.class);
    private final DatabaseAvailabilityGuard availabilityGuard = mock(DatabaseAvailabilityGuard.class);
    private final ResourceTracker resourceTracker = mock(ResourceTracker.class);

    @Test
    void shouldThrowTransientExceptionOnTransientKernelException() throws Exception {
        // GIVEN
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        when(kernelTransaction.isOpen()).thenReturn(true);
        doThrow(new TransactionFailureException(
                        Status.Transaction.ConstraintsChanged, "Proving that transaction does the right thing"))
                .when(kernelTransaction)
                .commit();
        TransactionImpl transaction = createTransaction(kernelTransaction);

        // WHEN
        transaction.commit();
        verify(resourceTracker, times(1)).closeAllCloseableResources();
    }

    @Test
    void shouldThrowTransactionExceptionOnTransientKernelException() throws Exception {
        // GIVEN
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        when(kernelTransaction.isOpen()).thenReturn(true);
        doThrow(new RuntimeException("Just a random failure"))
                .when(kernelTransaction)
                .commit();
        TransactionImpl transaction = createTransaction(kernelTransaction);

        // WHEN
        transaction.commit();
        verify(resourceTracker, times(1)).closeAllCloseableResources();
    }

    @Test
    void shouldLetThroughTransientFailureException() throws Exception {
        // GIVEN
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        when(kernelTransaction.isOpen()).thenReturn(true);
        doThrow(new TransientFailureException("Just a random failure") {
                    @Override
                    public Status status() {
                        return null;
                    }
                })
                .when(kernelTransaction)
                .commit();
        TransactionImpl transaction = createTransaction(kernelTransaction);

        // WHEN
        transaction.commit();
        verify(resourceTracker, times(1)).closeAllCloseableResources();
    }

    @Test
    void shouldShowTransactionTerminatedExceptionAsTransient() throws Exception {
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        doReturn(true).when(kernelTransaction).isOpen();
        RuntimeException error = new TransactionTerminatedException(Status.Transaction.Terminated);
        doThrow(error).when(kernelTransaction).commit();
        TransactionImpl transaction = createTransaction(kernelTransaction);

        transaction.commit();
        verify(resourceTracker, times(1)).closeAllCloseableResources();
    }

    @Test
    void shouldReturnTerminationReason() {
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        when(kernelTransaction.getReasonIfTerminated())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(Status.Transaction.Terminated));

        TransactionImpl tx = createTransaction(kernelTransaction);

        Optional<Status> terminationReason1 = tx.terminationReason();
        Optional<Status> terminationReason2 = tx.terminationReason();

        assertFalse(terminationReason1.isPresent());
        assertTrue(terminationReason2.isPresent());
        assertEquals(Status.Transaction.Terminated, terminationReason2.get());

        verify(resourceTracker, never()).closeAllCloseableResources();
    }

    @Test
    void testFailingClosableResource() throws TransactionFailureException {
        var kernelTransaction = mock(KernelTransaction.class);
        var tx = createTransaction(kernelTransaction);
        doThrow(new ResourceCloseFailureException("not so fast", null))
                .when(resourceTracker)
                .closeAllCloseableResources();
        assertThatThrownBy(tx::close).isInstanceOf(org.neo4j.graphdb.TransactionFailureException.class);
        verify(kernelTransaction, times(1)).close();
    }

    @Test
    void testFailingClosableResourceAndTxClose() throws TransactionFailureException {
        var kernelTransaction = mock(KernelTransaction.class);
        var tx = createTransaction(kernelTransaction);
        doThrow(new ResourceCloseFailureException("not so fast", null))
                .when(resourceTracker)
                .closeAllCloseableResources();
        var exceptionFromClose = new TransactionFailureException("This transaction can't be closed", null);
        doThrow(exceptionFromClose).when(kernelTransaction).close();

        assertThatThrownBy(tx::close)
                .isInstanceOf(org.neo4j.graphdb.TransactionFailureException.class)
                .hasCauseInstanceOf(ResourceCloseFailureException.class)
                .getCause()
                .hasSuppressedException(exceptionFromClose);

        tx.close(); // second close should be no-op
        verify(kernelTransaction, times(1)).close();
    }

    @Test
    void testFindNodesValidation() {
        checkForIAE(tx -> tx.findNodes(null), "Label");

        checkForIAE(tx -> tx.findNodes(null, "key", "value"), "Label");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), null, null), "Property key");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), "key", null), "property value");

        checkForIAE(tx -> tx.findNodes(null, "key", "template", null), "Label");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), null, "template", null), "Property key");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), "key", null, null), "Template");

        checkForIAE(tx -> tx.findNodes(null, "key", "value", "key", "value"), "Label");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), null, "value", "key", "value"), "Property key");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), "key", null, "key", "value"), "property value");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), "key", "value", null, "value"), "Property key");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), "key", "value", "key", null), "property value");

        checkForIAE(tx -> tx.findNodes(null, "key", "value", "key", "value", "key", "value"), "Label");
        checkForIAE(
                tx -> tx.findNodes(Label.label("test"), null, "value", "key", "value", "key", "value"), "Property key");
        checkForIAE(
                tx -> tx.findNodes(Label.label("test"), "key", null, "key", "value", "key", "value"), "property value");
        checkForIAE(
                tx -> tx.findNodes(Label.label("test"), "key", "value", "key", "value", null, "value"), "Property key");
        checkForIAE(
                tx -> tx.findNodes(Label.label("test"), "key", "value", "key", "value", "key", null), "property value");

        checkForIAE(tx -> tx.findNodes(null, emptyMap()), "Label");
        checkForIAE(tx -> tx.findNodes(Label.label("test"), null), "Property values");
    }

    @Test
    void testFindRelationshipsValidation() {
        checkForIAE(tx -> tx.findRelationships(null), "Relationship type");

        checkForIAE(tx -> tx.findRelationships(null, "key", "value"), "Relationship type");
        checkForIAE(tx -> tx.findRelationships(RelationshipType.withName("test"), null, null), "Property key");
        checkForIAE(tx -> tx.findRelationships(RelationshipType.withName("test"), "key", null), "property value");

        checkForIAE(tx -> tx.findRelationships(null, "key", "template", null), "Relationship type");
        checkForIAE(
                tx -> tx.findRelationships(RelationshipType.withName("test"), null, "template", null), "Property key");
        checkForIAE(tx -> tx.findRelationships(RelationshipType.withName("test"), "key", null, null), "Template");

        checkForIAE(tx -> tx.findRelationships(null, "key", "value", "key", "value"), "Relationship type");
        checkForIAE(
                tx -> tx.findRelationships(RelationshipType.withName("test"), null, "value", "key", "value"),
                "Property key");
        checkForIAE(
                tx -> tx.findRelationships(RelationshipType.withName("test"), "key", null, "key", "value"),
                "property value");
        checkForIAE(
                tx -> tx.findRelationships(RelationshipType.withName("test"), "key", "value", null, "value"),
                "Property key");
        checkForIAE(
                tx -> tx.findRelationships(RelationshipType.withName("test"), "key", "value", "key", null),
                "property value");

        checkForIAE(
                tx -> tx.findRelationships(null, "key", "value", "key", "value", "key", "value"), "Relationship type");
        checkForIAE(
                tx -> tx.findRelationships(
                        RelationshipType.withName("test"), null, "value", "key", "value", "key", "value"),
                "Property key");
        checkForIAE(
                tx -> tx.findRelationships(
                        RelationshipType.withName("test"), "key", null, "key", "value", "key", "value"),
                "property value");
        checkForIAE(
                tx -> tx.findRelationships(
                        RelationshipType.withName("test"), "key", "value", "key", "value", null, "value"),
                "Property key");
        checkForIAE(
                tx -> tx.findRelationships(
                        RelationshipType.withName("test"), "key", "value", "key", "value", "key", null),
                "property value");

        checkForIAE(tx -> tx.findRelationships(null, emptyMap()), "Relationship type");
        checkForIAE(tx -> tx.findRelationships(RelationshipType.withName("test"), null), "Property values");
    }

    @Test
    void getAllNodesShouldRegisterAndUnregisterAsResource() {
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);

        // commit
        try (TransactionImpl tx = createTransaction(kernelTransaction);
                ResourceIterable<Node> nodes = tx.getAllNodes()) {
            verify(resourceTracker, times(1)).registerCloseableResource(eq(nodes));
            verify(resourceTracker, never()).unregisterCloseableResource(any());

            nodes.close();
            verify(resourceTracker, times(1)).registerCloseableResource(eq(nodes));
            verify(resourceTracker, times(1)).unregisterCloseableResource(eq(nodes));

            tx.commit();
            verify(resourceTracker, times(1)).closeAllCloseableResources();
        }
    }

    @Test
    void getAllRelationshipsShouldRegisterAndUnregisterAsResource() {
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);

        // commit
        try (TransactionImpl tx = createTransaction(kernelTransaction);
                ResourceIterable<Relationship> nodes = tx.getAllRelationships()) {
            verify(resourceTracker, times(1)).registerCloseableResource(eq(nodes));
            verify(resourceTracker, never()).unregisterCloseableResource(any());

            nodes.close();
            verify(resourceTracker, times(1)).registerCloseableResource(eq(nodes));
            verify(resourceTracker, times(1)).unregisterCloseableResource(eq(nodes));

            tx.commit();
            verify(resourceTracker, times(1)).closeAllCloseableResources();
        }
    }

    @Test
    void shouldOnlyCloseKtiOnceWhenRollbackInsideCommit() throws Exception {
        // GIVEN
        // Mock that forward commit calls to the given monitor
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        when(kernelTransaction.commit(any(KernelTransactionMonitor.class)))
                .thenAnswer((Answer<Long>) invocationOnMock -> {
                    var monitor = (KernelTransactionMonitor) invocationOnMock.getArgument(0);
                    monitor.beforeApply();
                    return -1L;
                });

        // a TransactionImpl
        TransactionImpl transaction = createTransaction(kernelTransaction);

        // and a monitor that simulates a tx-failure + rollback inside of commit
        var monitorCalled = new MutableBoolean();
        var monitor = KernelTransactionMonitor.withBeforeApply(() -> {
            monitorCalled.setTrue();
            transaction.rollback();
        });

        // WHEN
        transaction.commit(monitor);

        // THEN
        assertThat(monitorCalled.getValue()).isTrue();
        verify(kernelTransaction, times(1)).close();
    }

    private void checkForIAE(Consumer<Transaction> consumer, String message) {
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        SchemaRead mock = mock(SchemaRead.class);
        when(mock.index(any())).thenReturn(Collections.emptyIterator());
        when(kernelTransaction.tokenRead()).thenReturn(mock(TokenRead.class));
        when(kernelTransaction.schemaRead()).thenReturn(mock);

        try (TransactionImpl tx = createTransaction(kernelTransaction)) {
            assertThatThrownBy(() -> consumer.accept(tx))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(message);
        }
    }

    private TransactionImpl createTransaction(KernelTransaction kernelTransaction) {
        return new TransactionImpl(
                tokenHolders,
                contextFactory,
                availabilityGuard,
                engine,
                kernelTransaction,
                resourceTracker,
                null,
                DefaultTransactionExceptionMapper.INSTANCE,
                mock(ElementIdMapper.class),
                null);
    }
}
