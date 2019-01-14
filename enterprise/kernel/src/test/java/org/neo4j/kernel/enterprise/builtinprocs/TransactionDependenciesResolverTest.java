/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TransactionDependenciesResolverTest
{
    @Test
    public void detectIndependentTransactionsAsNotBlocked()
    {
        HashMap<KernelTransactionHandle,List<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, singletonList( createQuerySnapshot( 1 ) ) );
        map.put( handle2, singletonList( createQuerySnapshot( 2 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertFalse( resolver.isBlocked( handle1 ) );
        assertFalse( resolver.isBlocked( handle2 ) );
    }

    @Test
    public void detectBlockedTransactionsByExclusiveLock()
    {
        HashMap<KernelTransactionHandle,List<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 0,
                singletonList( ActiveLock.exclusiveLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, singletonList( createQuerySnapshot( 1 ) ) );
        map.put( handle2, singletonList( createQuerySnapshotWaitingForLock( 2, false, ResourceTypes.NODE, 1 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertFalse( resolver.isBlocked( handle1 ) );
        assertTrue( resolver.isBlocked( handle2 ) );
    }

    @Test
    public void detectBlockedTransactionsBySharedLock()
    {
        HashMap<KernelTransactionHandle,List<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 0,
                singletonList( ActiveLock.sharedLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, singletonList( createQuerySnapshot( 1 ) ) );
        map.put( handle2, singletonList( createQuerySnapshotWaitingForLock( 2, true, ResourceTypes.NODE, 1 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertFalse( resolver.isBlocked( handle1 ) );
        assertTrue( resolver.isBlocked( handle2 ) );
    }

    @Test
    public void blockingChainDescriptionForIndependentTransactionsIsEmpty()
    {
        HashMap<KernelTransactionHandle,List<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, singletonList( createQuerySnapshot( 1 ) ) );
        map.put( handle2, singletonList( createQuerySnapshot( 2 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertThat( resolver.describeBlockingTransactions( handle1 ), isEmptyString() );
        assertThat( resolver.describeBlockingTransactions( handle2 ), isEmptyString() );
    }

    @Test
    public void blockingChainDescriptionForDirectlyBlockedTransaction()
    {
        HashMap<KernelTransactionHandle,List<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 3,
                singletonList( ActiveLock.exclusiveLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, singletonList( createQuerySnapshot( 1 ) ) );
        map.put( handle2, singletonList( createQuerySnapshotWaitingForLock( 2, false, ResourceTypes.NODE, 1 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertThat( resolver.describeBlockingTransactions( handle1 ), isEmptyString() );
        assertEquals( "[transaction-3]", resolver.describeBlockingTransactions( handle2 ) );
    }

    @Test
    public void blockingChainDescriptionForChainedBlockedTransaction()
    {
        HashMap<KernelTransactionHandle,List<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 4,
                singletonList( ActiveLock.exclusiveLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(),
                5, singletonList( ActiveLock.sharedLock( ResourceTypes.NODE, 2) ) );
        TestKernelTransactionHandle handle3 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 6 );

        map.put( handle1, singletonList( createQuerySnapshot( 1 ) ) );
        map.put( handle2, singletonList( createQuerySnapshotWaitingForLock( 2, false, ResourceTypes.NODE, 1 ) ) );
        map.put( handle3, singletonList( createQuerySnapshotWaitingForLock( 3, true, ResourceTypes.NODE, 2 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertThat( resolver.describeBlockingTransactions( handle1 ), isEmptyString() );
        assertEquals( "[transaction-4]", resolver.describeBlockingTransactions( handle2 ) );
        assertEquals( "[transaction-4, transaction-5]", resolver.describeBlockingTransactions( handle3 ) );
    }

    private QuerySnapshot createQuerySnapshot( long queryId )
    {
        return createExecutingQuery( queryId ).snapshot();
    }

    private QuerySnapshot createQuerySnapshotWaitingForLock( long queryId, boolean exclusive, ResourceType resourceType, long id )
    {
        ExecutingQuery executingQuery = createExecutingQuery( queryId );
        executingQuery.lockTracer().waitForLock( exclusive, resourceType, id );
        return executingQuery.snapshot();
    }

    private ExecutingQuery createExecutingQuery( long queryId )
    {
        return new ExecutingQuery( queryId, ClientConnectionInfo.EMBEDDED_CONNECTION, "test", "testQuey",
                VirtualValues.EMPTY_MAP, Collections.emptyMap(), () -> 1L, PageCursorTracer.NULL,
                Thread.currentThread().getId(), Thread.currentThread().getName(),
                Clocks.nanoClock(), CpuClock.NOT_AVAILABLE, HeapAllocation.NOT_AVAILABLE );
    }

    private static class TestKernelTransactionHandleWithLocks extends TestKernelTransactionHandle
    {

        private final long userTxId;
        private final List<ActiveLock> locks;

        TestKernelTransactionHandleWithLocks( KernelTransaction tx )
        {
            this( tx, 0, Collections.emptyList() );
        }

        TestKernelTransactionHandleWithLocks( KernelTransaction tx, long userTxId )
        {
            this( tx, userTxId, Collections.emptyList() );
        }

        TestKernelTransactionHandleWithLocks( KernelTransaction tx, long userTxId, List<ActiveLock> locks )
        {
            super( tx );
            this.userTxId = userTxId;
            this.locks = locks;
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return locks.stream();
        }

        @Override
        public long getUserTransactionId()
        {
            return userTxId;
        }
    }
}
