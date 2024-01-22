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

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionHookFailed;

import java.util.Collection;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public class TransactionEventListeners {
    private final DatabaseTransactionEventListeners databaseEventListeners;
    private final KernelTransaction ktx;
    private final StorageReader storageReader;
    private final GraphDatabaseFacade databaseFacade;
    public TransactionListenersState listenersState;
    private Collection<TransactionEventListener<?>> listenersSnapshot;

    public TransactionEventListeners(
            DatabaseTransactionEventListeners databaseEventListeners,
            KernelTransaction ktx,
            StorageReader storageReader) {
        this.databaseEventListeners = databaseEventListeners;
        this.databaseFacade = databaseEventListeners.getDatabaseFacade();
        this.ktx = ktx;
        this.storageReader = storageReader;
    }

    public void beforeCommit(TxState txState, boolean isCommitCall) throws TransactionFailureException {
        if (listenersSnapshot == null) {
            listenersSnapshot = databaseEventListeners.getCurrentRegisteredTransactionEventListeners();
        }
        var newState = beforeCommit(txState, ktx, storageReader, listenersSnapshot, isCommitCall);
        try {
            if (newState != null && newState.isFailed()) {
                Throwable cause = newState.failure();
                if (cause instanceof TransientFailureException tfe) {
                    throw tfe;
                }
                if (cause instanceof Status.HasStatus se) {
                    throw new TransactionFailureException(se.status(), cause, cause.getMessage());
                }
                throw new TransactionFailureException(TransactionHookFailed, cause, cause.getMessage());
            }
        } finally {
            this.listenersState = newState;
        }
    }

    public void reset() {
        listenersSnapshot = null;
        listenersState = null;
    }

    public void afterCommit() {
        afterCommit(listenersState);
    }

    public void afterRollback() {
        afterRollback(listenersState);
    }

    public Throwable failure() {
        if (listenersState == null) {
            return null;
        }
        return listenersState.failure();
    }

    TransactionListenersState beforeCommit(
            ReadableTransactionState state,
            KernelTransaction transaction,
            StorageReader storageReader,
            Collection<TransactionEventListener<?>> eventListeners,
            boolean isCommitCall) {
        if (!canInvokeBeforeCommitListeners(eventListeners, state)) {
            // Use 'null' as a signal that no event listenerIterator were registered at beforeCommit time
            return null;
        }

        TransactionData txData = new TxStateTransactionDataSnapshot(state, storageReader, transaction, isCommitCall);
        TransactionListenersState listenersStates = new TransactionListenersState(txData);

        boolean hasDataChanges = state.hasDataChanges();

        boolean isSystem = databaseFacade.databaseId().isSystemDatabase();

        for (TransactionEventListener<?> listener : eventListeners) {
            boolean internal = listener instanceof InternalTransactionEventListener;
            if (hasDataChanges || internal || isSystem) {
                Object listenerState = null;
                try {
                    listenerState = !internal
                            ? listener.beforeCommit(txData, transaction.internalTransaction(), databaseFacade)
                            : ((InternalTransactionEventListener) listener)
                                    .beforeCommit(txData, transaction, databaseFacade);
                } catch (Throwable t) {
                    listenersStates.failed(t);
                }
                listenersStates.addListenerState(listener, listenerState);
            }
        }

        return listenersStates;
    }

    void afterCommit(TransactionListenersState listeners) {
        if (listeners == null) {
            // As per beforeCommit, 'null' means no listeners were registered in time for this transaction to
            // observe them.
            return;
        }

        TransactionData txData = listeners.getTxData();
        Throwable error = null;
        try {
            for (TransactionListenersState.ListenerState listenerState : listeners.getStates()) {
                TransactionEventListener listener = listenerState.listener();
                try {
                    listener.afterCommit(txData, listenerState.state(), databaseFacade);
                } catch (Throwable t) {
                    error = Exceptions.chain(error, t);
                }
            }
            if (error != null) {
                Exceptions.throwIfUnchecked(error);
                throw new RuntimeException(error);
            }
        } finally {
            closeTxDataSnapshot(txData);
        }
    }

    void afterRollback(TransactionListenersState listenersState) {
        if (listenersState == null) {
            // For legacy reasons, we don't call transaction listeners on implicit rollback.
            return;
        }

        TransactionData txData = listenersState.getTxData();
        Throwable error = null;
        try {
            for (TransactionListenersState.ListenerState listenerState : listenersState.getStates()) {
                TransactionEventListener listener = listenerState.listener();
                try {
                    listener.afterRollback(txData, listenerState.state(), databaseFacade);
                } catch (Throwable t) {
                    error = Exceptions.chain(error, t);
                }
            }
            if (error != null) {
                Exceptions.throwIfUnchecked(error);
                throw new RuntimeException(error);
            }
        } finally {
            closeTxDataSnapshot(txData);
        }
    }

    private void closeTxDataSnapshot(TransactionData txData) {
        // We don't want the user-facing TransactionData interface to have close() on it, which is why the exposed
        // object is
        // of type TransactionData, but internally we know that it's a specific implementation of it
        assert txData instanceof TxStateTransactionDataSnapshot;
        ((TxStateTransactionDataSnapshot) txData).close();
    }

    private static boolean canInvokeBeforeCommitListeners(
            Collection<TransactionEventListener<?>> listeners, ReadableTransactionState state) {
        return !listeners.isEmpty() && canInvokeListenersWithTransactionState(state);
    }

    private static boolean canInvokeListenersWithTransactionState(ReadableTransactionState state) {
        return state != null && state.hasChanges();
    }
}
