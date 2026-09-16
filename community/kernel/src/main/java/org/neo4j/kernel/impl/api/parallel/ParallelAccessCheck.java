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
package org.neo4j.kernel.impl.api.parallel;

import static org.neo4j.scheduler.Group.CYPHER_WORKER;
import static org.neo4j.util.FeatureToggles.flag;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;

public class ParallelAccessCheck {

    public static final boolean CHECK_PARALLEL_ACCESS = flag(ParallelAccessCheck.class, "CHECK_PARALLEL_ACCESS", false);
    private static final ThreadLocal<Boolean> DISABLED = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> ALLOWED_TX_BEGIN = new ThreadLocal<>();

    public static boolean shouldPerformCheck() {
        return CHECK_PARALLEL_ACCESS && DISABLED.get() == null;
    }

    public static <T> T performWithCheckDisabled(Supplier<T> operation) {
        try {
            if (DISABLED.get() != null) {
                throw new IllegalStateException("Parallel access check is already disabled");
            }
            DISABLED.set(true);
            return operation.get();
        } finally {
            DISABLED.remove();
        }
    }

    /**
     * Allows Cypher worker threads to access any kernel transaction begun by the current thread inside the
     * given operation, for the transaction's entire lifetime. The transaction is still not thread-safe, so
     * threads must execute in it one at a time. Unlike {@link #performWithCheckDisabled}, this applies to
     * the transaction, not the thread.
     */
    public static <T, E extends Exception> T performTransactionBeginAllowingParallelAccess(
            ThrowingSupplier<T, E> operation) throws E {
        try {
            if (ALLOWED_TX_BEGIN.get() != null) {
                throw new IllegalStateException("A transaction begin allowing parallel access is already in progress");
            }
            ALLOWED_TX_BEGIN.set(true);
            return operation.get();
        } finally {
            ALLOWED_TX_BEGIN.remove();
        }
    }

    public static boolean isParallelAccessAllowedForNewTransactions() {
        return ALLOWED_TX_BEGIN.get() != null;
    }

    public static void checkNotCypherWorkerThread() {
        if (Thread.currentThread().getThreadGroup().getName().equals(CYPHER_WORKER.groupName())) {
            throw new IllegalStateException(
                    "A resource that does not support parallel access is being accessed by a Cypher worker thread");
        }
    }

    /**
     * The wrap decision is per pooled transaction instance and depends only on the static flag.
     * Whether checks fire is decided per call through the allowed supplier.
     */
    public static LockManager.Client maybeWrapLockClient(
            LockManager.Client wrappedLockClient, BooleanSupplier allowed) {
        if (!CHECK_PARALLEL_ACCESS) {
            return wrappedLockClient;
        }
        return new ParallelAccessCheckClient(wrappedLockClient, allowed);
    }

    public static final class ParallelAccessCheckClient implements LockManager.Client {
        private final LockManager.Client wrappedLockClient;
        private final BooleanSupplier allowed;

        public ParallelAccessCheckClient(LockManager.Client wrappedLockClient, BooleanSupplier allowed) {
            this.wrappedLockClient = wrappedLockClient;
            this.allowed = allowed;
        }

        private void checkAccess() {
            if (!allowed.getAsBoolean()) {
                checkNotCypherWorkerThread();
            }
        }

        @Override
        public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
            checkAccess();
            return wrappedLockClient.tryExclusiveLock(resourceType, resourceId);
        }

        @Override
        public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
            checkAccess();
            wrappedLockClient.acquireExclusive(tracer, resourceType, resourceIds);
        }

        @Override
        public void releaseExclusive(ResourceType resourceType, long... resourceIds) {
            checkAccess();
            wrappedLockClient.releaseExclusive(resourceType, resourceIds);
        }

        @Override
        public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
            checkAccess();
            wrappedLockClient.acquireShared(tracer, resourceType, resourceIds);
        }

        @Override
        public void releaseShared(ResourceType resourceType, long... resourceIds) {
            checkAccess();
            wrappedLockClient.releaseShared(resourceType, resourceIds);
        }

        @Override
        public Collection<ActiveLock> activeLocks(MemoryTracker memoryTracker) {
            checkAccess();
            return wrappedLockClient.activeLocks(memoryTracker);
        }

        @Override
        public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
            checkAccess();
            return wrappedLockClient.holdsLock(id, resource, lockType);
        }

        @Override
        public void initialize(
                LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config) {
            checkAccess();
            wrappedLockClient.initialize(leaseClient, transactionId, memoryTracker, config);
        }

        @Override
        public boolean trySharedLock(ResourceType resourceType, long resourceId) {
            checkAccess();
            return wrappedLockClient.trySharedLock(resourceType, resourceId);
        }

        @Override
        public void prepareForCommit() {
            checkAccess();
            wrappedLockClient.prepareForCommit();
        }

        @Override
        public void stop() {
            checkAccess();
            wrappedLockClient.stop();
        }

        @Override
        public void close() {
            checkAccess();
            wrappedLockClient.close();
        }

        @Override
        public long getTransactionId() {
            checkAccess();
            return wrappedLockClient.getTransactionId();
        }

        @Override
        public long activeLockCount() {
            /*
            NOTE, unlike other methods here this method has no thread guard (allows calls from other threads) because:
            (1) all current implementations of activeLockCount() are thread-safe, i.e., it is safe
            (2) method gets called by Cypher Worker via ExecutingQuery.snapshot()
             */
            return wrappedLockClient.activeLockCount();
        }

        @Override
        public void reset() {
            checkAccess();
            wrappedLockClient.reset();
        }

        public LockManager.Client getWrappedLockClient() {
            return wrappedLockClient;
        }
    }
}
