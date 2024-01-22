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
package org.neo4j.kernel.internal.event;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

class DatabaseTransactionEventListenersTest {
    @Test
    void shouldUnregisterRemainingListenerOnShutdown() {
        // Given
        GlobalTransactionEventListeners globalListeners = mock(GlobalTransactionEventListeners.class);
        NamedDatabaseId databaseId = DatabaseIdFactory.from("foo", UUID.randomUUID());
        DatabaseTransactionEventListeners listeners =
                new DatabaseTransactionEventListeners(mock(GraphDatabaseFacade.class), globalListeners, databaseId);
        TransactionEventListener<?> firstListener = mock(TransactionEventListener.class);
        TransactionEventListener<?> secondListener = mock(TransactionEventListener.class);

        // When
        listeners.registerTransactionEventListener(firstListener);
        listeners.registerTransactionEventListener(secondListener);

        // Then
        verify(globalListeners).registerTransactionEventListener(databaseId.name(), firstListener);
        verify(globalListeners).registerTransactionEventListener(databaseId.name(), secondListener);
        verifyNoMoreInteractions(globalListeners);

        // When
        listeners.unregisterTransactionEventListener(firstListener);

        // Then
        verify(globalListeners).unregisterTransactionEventListener(databaseId.name(), firstListener);
        verifyNoMoreInteractions(globalListeners);

        // When
        listeners.shutdown();

        // Then
        verify(globalListeners).unregisterTransactionEventListener(databaseId.name(), secondListener);
        verifyNoMoreInteractions(globalListeners);
    }

    @Test
    void shouldCloseTxSnapshotAfterCommit() {
        shouldCloseTxSnapshot(TransactionEventListeners::afterCommit);
    }

    @Test
    void shouldCloseTxSnapshotAfterRollback() {
        shouldCloseTxSnapshot(TransactionEventListeners::afterRollback);
    }

    @Test
    void shouldCallBeforeCommitOnAllListenersRegardlessOfExceptions() throws Exception {
        // given
        var databaseId = DatabaseIdFactory.from("db", UUID.randomUUID());
        var db = mock(GraphDatabaseFacade.class);
        when(db.databaseId()).thenReturn(databaseId);
        var dbListeners =
                new DatabaseTransactionEventListeners(db, new DefaultGlobalTransactionEventListeners(), databaseId);
        var firstSuccessfulListener = mock(TransactionEventListener.class);
        var secondFailingListener = mock(TransactionEventListener.class);
        var thirdSuccessfulListener = mock(TransactionEventListener.class);
        when(secondFailingListener.beforeCommit(any(), any(), any())).thenThrow(FailingListenerException.class);
        dbListeners.registerTransactionEventListener(firstSuccessfulListener);
        dbListeners.registerTransactionEventListener(secondFailingListener);
        dbListeners.registerTransactionEventListener(thirdSuccessfulListener);

        // when listeners called for tx state w/ some changes in it
        var txState = new TxState();
        txState.nodeDoCreate(1);
        var tx = mock(KernelTransaction.class);
        when(tx.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);

        var transactionEventListeners = new TransactionEventListeners(dbListeners, tx, mockedStorageReader());

        transactionEventListeners.beforeCommit(
                txState,
                tx,
                mock(StorageReader.class),
                dbListeners.getCurrentRegisteredTransactionEventListeners(),
                true);

        // then
        verify(firstSuccessfulListener).beforeCommit(any(), any(), any());
        verify(secondFailingListener).beforeCommit(any(), any(), any());
        verify(thirdSuccessfulListener).beforeCommit(any(), any(), any());
    }

    @Test
    void shouldCallAfterCommitOnAllListenersRegardlessOfExceptions() {
        // given
        var databaseId = DatabaseIdFactory.from("db", UUID.randomUUID());
        var db = mock(GraphDatabaseFacade.class);
        when(db.databaseId()).thenReturn(databaseId);
        var dbListeners =
                new DatabaseTransactionEventListeners(db, new DefaultGlobalTransactionEventListeners(), databaseId);
        TransactionEventListener<?> firstSuccessfulListener = mock(TransactionEventListener.class);
        TransactionEventListener<?> secondFailingListener = mock(TransactionEventListener.class);
        TransactionEventListener<?> thirdSuccessfulListener = mock(TransactionEventListener.class);
        doThrow(FailingListenerException.class).when(secondFailingListener).afterCommit(any(), any(), any());
        dbListeners.registerTransactionEventListener(firstSuccessfulListener);
        dbListeners.registerTransactionEventListener(secondFailingListener);
        dbListeners.registerTransactionEventListener(thirdSuccessfulListener);
        var txState = new TxState();
        txState.nodeDoCreate(1);
        var tx = mock(KernelTransaction.class);
        when(tx.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);

        var transactionEventListeners = new TransactionEventListeners(dbListeners, tx, mockedStorageReader());

        var state = transactionEventListeners.beforeCommit(
                txState, tx, mockedStorageReader(), dbListeners.getCurrentRegisteredTransactionEventListeners(), true);

        // when
        assertThatThrownBy(() -> transactionEventListeners.afterCommit(state))
                .isInstanceOf(FailingListenerException.class);

        // then
        verify(firstSuccessfulListener).afterCommit(any(), any(), any());
        verify(secondFailingListener).afterCommit(any(), any(), any());
        verify(thirdSuccessfulListener).afterCommit(any(), any(), any());
    }

    @Test
    void shouldCallAfterRollbackOnAllListenersRegardlessOfExceptions() {
        // given
        var databaseId = DatabaseIdFactory.from("db", UUID.randomUUID());
        var db = mock(GraphDatabaseFacade.class);
        when(db.databaseId()).thenReturn(databaseId);
        var dbListeners =
                new DatabaseTransactionEventListeners(db, new DefaultGlobalTransactionEventListeners(), databaseId);
        TransactionEventListener<?> firstSuccessfulListener = mock(TransactionEventListener.class);
        TransactionEventListener<?> secondFailingListener = mock(TransactionEventListener.class);
        TransactionEventListener<?> thirdSuccessfulListener = mock(TransactionEventListener.class);
        doThrow(FailingListenerException.class).when(secondFailingListener).afterRollback(any(), any(), any());
        dbListeners.registerTransactionEventListener(firstSuccessfulListener);
        dbListeners.registerTransactionEventListener(secondFailingListener);
        dbListeners.registerTransactionEventListener(thirdSuccessfulListener);
        var txState = new TxState();
        txState.nodeDoCreate(1);
        var tx = mock(KernelTransaction.class);
        when(tx.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);

        var transactionEventListeners = new TransactionEventListeners(dbListeners, tx, mockedStorageReader());

        var state = transactionEventListeners.beforeCommit(
                txState, tx, mockedStorageReader(), dbListeners.getCurrentRegisteredTransactionEventListeners(), true);

        // when
        assertThatThrownBy(() -> transactionEventListeners.afterRollback(state))
                .isInstanceOf(FailingListenerException.class);

        // then
        verify(firstSuccessfulListener).afterRollback(any(), any(), any());
        verify(secondFailingListener).afterRollback(any(), any(), any());
        verify(thirdSuccessfulListener).afterRollback(any(), any(), any());
    }

    private StorageReader mockedStorageReader() {
        var reader = mock(StorageReader.class);
        var relationshipScanCursor = mock(StorageRelationshipScanCursor.class);
        when(reader.allocateRelationshipScanCursor(any(), any())).thenReturn(relationshipScanCursor);
        return reader;
    }

    private void shouldCloseTxSnapshot(BiConsumer<TransactionEventListeners, TransactionListenersState> txAction) {
        // Given
        var databaseId = DatabaseIdFactory.from("db", UUID.randomUUID());
        var db = mock(GraphDatabaseFacade.class);
        when(db.databaseId()).thenReturn(databaseId);
        GlobalTransactionEventListeners globalListeners = new DefaultGlobalTransactionEventListeners();
        DatabaseTransactionEventListeners listeners =
                new DatabaseTransactionEventListeners(db, globalListeners, databaseId);
        TransactionEventListener<?> listener = mock(TransactionEventListener.class);
        listeners.registerTransactionEventListener(listener);

        TxState txState = new TxState();
        txState.relationshipDoCreate(1, 2, 3, 4);
        KernelTransaction kernelTransaction = mock(KernelTransaction.class);
        InternalTransaction internalTransaction = mock(InternalTransaction.class);
        when(kernelTransaction.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);
        when(kernelTransaction.internalTransaction()).thenReturn(internalTransaction);
        StorageReader storageReader = mock(StorageReader.class);
        StorageRelationshipScanCursor relationshipScanCursor = mock(StorageRelationshipScanCursor.class);
        when(storageReader.allocateRelationshipScanCursor(any(), any())).thenReturn(relationshipScanCursor);
        when(internalTransaction.newRelationshipEntity(anyLong()))
                .then(invocationOnMock ->
                        new RelationshipEntity(internalTransaction, invocationOnMock.getArgument(0, Long.class)));

        TransactionEventListeners transactionEventListeners =
                new TransactionEventListeners(listeners, kernelTransaction, storageReader);

        // When
        TransactionListenersState state = transactionEventListeners.beforeCommit(
                txState,
                kernelTransaction,
                storageReader,
                listeners.getCurrentRegisteredTransactionEventListeners(),
                false);
        txAction.accept(transactionEventListeners, state);

        // Then
        verify(relationshipScanCursor).close();
    }

    private static class FailingListenerException extends RuntimeException {}
}
