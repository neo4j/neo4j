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
package org.neo4j.cypher.internal.kernel.api.helpers;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.helpers.TransactionDependenciesResolver;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.VirtualValues;

class TransactionDependenciesResolverTest {
    @Test
    void detectIndependentTransactionsAsNotBlocked() {
        HashMap<KernelTransactionHandle, Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));

        map.put(handle1, Optional.of(createQuerySnapshot(1)));
        map.put(handle2, Optional.of(createQuerySnapshot(2)));
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver(map);

        assertFalse(resolver.isBlocked(handle1));
        assertFalse(resolver.isBlocked(handle2));
    }

    @Test
    void detectBlockedTransactionsByExclusiveLock() {
        HashMap<KernelTransactionHandle, Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks(
                mock(KernelTransaction.class), 0, singletonList(new ActiveLock(ResourceType.NODE, EXCLUSIVE, 1, 1)));
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));

        map.put(handle1, Optional.of(createQuerySnapshot(1)));
        map.put(handle2, Optional.of(createQuerySnapshotWaitingForLock(2, SHARED, ResourceType.NODE, 1, 1)));
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver(map);

        assertFalse(resolver.isBlocked(handle1));
        assertTrue(resolver.isBlocked(handle2));
    }

    @Test
    void detectBlockedTransactionsBySharedLock() {
        HashMap<KernelTransactionHandle, Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks(
                mock(KernelTransaction.class), 0, singletonList(new ActiveLock(ResourceType.NODE, SHARED, 1, 1)));
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));

        map.put(handle1, Optional.of(createQuerySnapshot(1)));
        map.put(handle2, Optional.of(createQuerySnapshotWaitingForLock(2, EXCLUSIVE, ResourceType.NODE, 1, 1)));
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver(map);

        assertFalse(resolver.isBlocked(handle1));
        assertTrue(resolver.isBlocked(handle2));
    }

    @Test
    void blockingChainDescriptionForIndependentTransactionsIsEmpty() {
        HashMap<KernelTransactionHandle, Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));

        map.put(handle1, Optional.of(createQuerySnapshot(1)));
        map.put(handle2, Optional.of(createQuerySnapshot(2)));
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver(map);

        assertThat(resolver.describeBlockingTransactions(handle1)).isEmpty();
        assertThat(resolver.describeBlockingTransactions(handle2)).isEmpty();
    }

    @Test
    void blockingChainDescriptionForDirectlyBlockedTransaction() {
        HashMap<KernelTransactionHandle, Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks(
                mock(KernelTransaction.class), 3, singletonList(new ActiveLock(ResourceType.NODE, EXCLUSIVE, 1, 1)));
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class));

        map.put(handle1, Optional.of(createQuerySnapshot(1)));
        map.put(handle2, Optional.of(createQuerySnapshotWaitingForLock(2, SHARED, ResourceType.NODE, 1, 1)));
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver(map);

        assertThat(resolver.describeBlockingTransactions(handle1)).isEmpty();
        assertEquals("[transaction-3]", resolver.describeBlockingTransactions(handle2));
    }

    @Test
    void blockingChainDescriptionForChainedBlockedTransaction() {
        HashMap<KernelTransactionHandle, Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks(
                mock(KernelTransaction.class), 4, singletonList(new ActiveLock(ResourceType.NODE, EXCLUSIVE, 4, 1)));
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks(
                mock(KernelTransaction.class), 5, singletonList(new ActiveLock(ResourceType.NODE, SHARED, 5, 2)));
        TestKernelTransactionHandle handle3 =
                new TestKernelTransactionHandleWithLocks(mock(KernelTransaction.class), 6);

        map.put(handle1, Optional.of(createQuerySnapshot(1)));
        map.put(handle2, Optional.of(createQuerySnapshotWaitingForLock(2, EXCLUSIVE, ResourceType.NODE, 5, 1)));
        map.put(handle3, Optional.of(createQuerySnapshotWaitingForLock(3, EXCLUSIVE, ResourceType.NODE, 6, 2)));
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver(map);

        assertThat(resolver.describeBlockingTransactions(handle1)).isEmpty();
        assertEquals("[transaction-4]", resolver.describeBlockingTransactions(handle2));
        assertEquals("[transaction-4, transaction-5]", resolver.describeBlockingTransactions(handle3));
    }

    private static QuerySnapshot createQuerySnapshot(long queryId) {
        return createExecutingQuery(queryId).snapshot();
    }

    private static QuerySnapshot createQuerySnapshotWaitingForLock(
            long queryId, LockType lockType, ResourceType resourceType, long transactionId, long id) {
        ExecutingQuery executingQuery = createExecutingQuery(queryId);
        executingQuery.lockTracer().waitForLock(lockType, resourceType, transactionId, id);
        return executingQuery.snapshot();
    }

    private static ExecutingQuery createExecutingQuery(long queryId) {
        return new ExecutingQuery(
                queryId,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                from(DEFAULT_DATABASE_NAME, UUID.randomUUID()),
                "test",
                "test",
                "testQuery",
                VirtualValues.EMPTY_MAP,
                Collections.emptyMap(),
                () -> 1L,
                () -> 1,
                () -> 2,
                Thread.currentThread().getId(),
                Thread.currentThread().getName(),
                LockTracer.NONE,
                Clocks.nanoClock(),
                CpuClock.NOT_AVAILABLE);
    }

    private static class TestKernelTransactionHandleWithLocks extends TestKernelTransactionHandle {

        private final long userTxId;
        private final List<ActiveLock> locks;

        TestKernelTransactionHandleWithLocks(KernelTransaction tx) {
            this(tx, 0, Collections.emptyList());
        }

        TestKernelTransactionHandleWithLocks(KernelTransaction tx, long userTxId) {
            this(tx, userTxId, Collections.emptyList());
        }

        TestKernelTransactionHandleWithLocks(KernelTransaction tx, long userTxId, List<ActiveLock> locks) {
            super(tx);
            this.userTxId = userTxId;
            this.locks = locks;
        }

        @Override
        public Collection<ActiveLock> activeLocks() {
            return locks;
        }

        @Override
        public long getTransactionSequenceNumber() {
            return userTxId;
        }
    }
}
