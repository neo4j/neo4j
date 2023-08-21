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
package org.neo4j.server.http.cypher;

import static java.lang.String.format;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryPool;
import org.neo4j.util.VisibleForTesting;

public class TransactionHandleRegistry implements TransactionRegistry {
    @VisibleForTesting
    public static final long ACTIVE_TRANSACTION_SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(ActiveTransaction.class);

    @VisibleForTesting
    public static final long SUSPENDED_TRANSACTION_SHALLOW_SIZE =
            HeapEstimator.shallowSizeOf(SuspendedTransaction.class);

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final ConcurrentHashMap<Long, TransactionMarker> registry = new ConcurrentHashMap<>(64);

    private final Clock clock;

    private final InternalLog log;
    private final Duration transactionTimeout;

    private final MemoryPool memoryPool;

    public TransactionHandleRegistry(
            Clock clock, Duration transactionTimeout, InternalLogProvider logProvider, MemoryPool memoryPool) {
        this.clock = clock;
        this.transactionTimeout = transactionTimeout;
        this.log = logProvider.getLog(getClass());
        this.memoryPool = memoryPool;
    }

    private abstract static class TransactionMarker {
        protected TransactionHandle transactionHandle;

        protected TransactionMarker(TransactionHandle transactionHandle) {
            this.transactionHandle = transactionHandle;
        }

        abstract ActiveTransaction getActiveTransaction();

        abstract SuspendedTransaction getSuspendedTransaction() throws InvalidConcurrentTransactionAccess;

        abstract boolean isSuspended();

        LoginContext getLoginContext() {
            return transactionHandle.loginContext();
        }
    }

    private static class ActiveTransaction extends TransactionMarker {
        final TransactionTerminationHandle terminationHandle;

        private ActiveTransaction(TransactionHandle terminationHandle) {
            super(terminationHandle);
            this.terminationHandle = terminationHandle;
        }

        TransactionTerminationHandle getTerminationHandle() {
            return terminationHandle;
        }

        @Override
        ActiveTransaction getActiveTransaction() {
            return this;
        }

        @Override
        SuspendedTransaction getSuspendedTransaction() throws InvalidConcurrentTransactionAccess {
            throw new InvalidConcurrentTransactionAccess();
        }

        @Override
        boolean isSuspended() {
            return false;
        }
    }

    private class SuspendedTransaction extends TransactionMarker {
        final ActiveTransaction activeMarker;
        final long lastActiveTimestamp;

        private SuspendedTransaction(ActiveTransaction activeMarker, TransactionHandle transactionHandle) {
            super(transactionHandle);
            this.activeMarker = activeMarker;
            this.lastActiveTimestamp = clock.millis();
        }

        @Override
        ActiveTransaction getActiveTransaction() {
            return activeMarker;
        }

        @Override
        SuspendedTransaction getSuspendedTransaction() {
            return this;
        }

        @Override
        boolean isSuspended() {
            return true;
        }

        @Override
        LoginContext getLoginContext() {
            return transactionHandle.loginContext();
        }

        long getLastActiveTimestamp() {
            return lastActiveTimestamp;
        }
    }

    @Override
    public long begin(TransactionHandle handle) {
        memoryPool.reserveHeap(ACTIVE_TRANSACTION_SHALLOW_SIZE);

        long id = idGenerator.incrementAndGet();
        if (null == registry.putIfAbsent(id, new ActiveTransaction(handle))) {
            return id;
        } else {
            memoryPool.releaseHeap(ACTIVE_TRANSACTION_SHALLOW_SIZE);
            throw new IllegalStateException("Attempt to begin transaction for id that was already registered");
        }
    }

    @Override
    public long release(long id, TransactionHandle transactionHandle) {
        TransactionMarker marker = registry.get(id);

        if (null == marker) {
            throw new IllegalStateException("Trying to suspend unregistered transaction");
        }

        if (marker.isSuspended()) {
            throw new IllegalStateException("Trying to suspend transaction that was already suspended");
        }

        memoryPool.reserveHeap(SUSPENDED_TRANSACTION_SHALLOW_SIZE);

        SuspendedTransaction suspendedTx = new SuspendedTransaction(marker.getActiveTransaction(), transactionHandle);
        if (!registry.replace(id, marker, suspendedTx)) {
            memoryPool.releaseHeap(SUSPENDED_TRANSACTION_SHALLOW_SIZE);
            throw new IllegalStateException("Trying to suspend transaction that has been concurrently suspended");
        }

        return computeNewExpiryTime(suspendedTx.getLastActiveTimestamp());
    }

    private long computeNewExpiryTime(long lastActiveTimestamp) {
        return lastActiveTimestamp + transactionTimeout.toMillis();
    }

    @Override
    public LoginContext getLoginContextForTransaction(long id) throws InvalidTransactionId {
        var marker = registry.get(id);
        if (marker == null) {
            throw new InvalidTransactionId();
        }
        return marker.getLoginContext();
    }

    @Override
    public TransactionHandle acquire(long id) throws TransactionLifecycleException {
        TransactionMarker marker = registry.get(id);

        if (null == marker) {
            throw new InvalidTransactionId();
        }

        SuspendedTransaction transaction = marker.getSuspendedTransaction();
        if (registry.replace(id, marker, marker.getActiveTransaction())) {
            memoryPool.releaseHeap(SUSPENDED_TRANSACTION_SHALLOW_SIZE);
            return transaction.transactionHandle;
        } else {
            throw new InvalidConcurrentTransactionAccess();
        }
    }

    @Override
    public void forget(long id) {
        TransactionMarker marker = registry.get(id);

        if (null == marker) {
            throw new IllegalStateException("Could not finish unregistered transaction");
        }

        if (marker.isSuspended()) {
            throw new IllegalStateException("Cannot finish suspended registered transaction");
        }

        if (!registry.remove(id, marker)) {
            throw new IllegalStateException(
                    "Trying to finish transaction that has been concurrently finished or suspended");
        }

        memoryPool.releaseHeap(ACTIVE_TRANSACTION_SHALLOW_SIZE);
    }

    @Override
    public TransactionHandle terminate(long id) throws TransactionLifecycleException {
        TransactionMarker marker = registry.get(id);
        if (null == marker) {
            throw new InvalidTransactionId();
        }

        memoryPool.releaseHeap(ACTIVE_TRANSACTION_SHALLOW_SIZE);

        TransactionTerminationHandle handle = marker.getActiveTransaction().getTerminationHandle();
        handle.terminate();

        try {
            SuspendedTransaction transaction = marker.getSuspendedTransaction();
            if (registry.replace(id, marker, marker.getActiveTransaction())) {
                memoryPool.releaseHeap(SUSPENDED_TRANSACTION_SHALLOW_SIZE);
                return transaction.transactionHandle;
            }
        } catch (InvalidConcurrentTransactionAccess exception) {
            // We could not acquire the transaction. Let the other request clean up.
        }
        return null;
    }

    @Override
    public void rollbackAllSuspendedTransactions() {
        rollbackSuspended(Predicates.alwaysTrue());
    }

    @Override
    public void rollbackSuspendedTransactionsIdleSince(final long oldestLastActiveTime) {
        rollbackSuspended(item -> {
            try {
                SuspendedTransaction transaction = item.getSuspendedTransaction();
                return transaction.lastActiveTimestamp < oldestLastActiveTime;
            } catch (InvalidConcurrentTransactionAccess concurrentTransactionAccessError) {
                throw new RuntimeException(concurrentTransactionAccessError);
            }
        });
    }

    private void rollbackSuspended(Predicate<TransactionMarker> predicate) {
        Set<Long> candidateTransactionIdsToRollback = new HashSet<>();

        for (Map.Entry<Long, TransactionMarker> entry : registry.entrySet()) {
            TransactionMarker marker = entry.getValue();
            if (marker.isSuspended() && predicate.test(marker)) {
                candidateTransactionIdsToRollback.add(entry.getKey());
            }
        }

        for (long id : candidateTransactionIdsToRollback) {
            TransactionHandle handle;
            try {
                handle = acquire(id);
            } catch (TransactionLifecycleException invalidTransactionId) {
                // Allow this - someone snatched the transaction from under our feet,
                continue;
            }
            try {
                handle.forceRollback();
                log.info(format(
                        "Transaction with id %d has been automatically rolled back due to transaction timeout.", id));
            } catch (Throwable e) {
                log.error(format("Transaction with id %d failed to roll back.", id), e);
            } finally {
                forget(id);
            }
        }
    }
}
