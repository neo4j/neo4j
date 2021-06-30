/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.locking.forseti;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.lock.ResourceTypes.NODE;

class ForsetiMemoryTrackingTest
{
    private static final AtomicLong TRANSACTION_ID = new AtomicLong();
    private static final int ONE_LOCK_SIZE_ESTIMATE = 56;
    private GlobalMemoryGroupTracker memoryPool;
    private MemoryTracker memoryTracker;
    private ForsetiLockManager forsetiLockManager;

    @BeforeEach
    void setUp()
    {
        memoryPool = new MemoryPools().pool( MemoryGroup.TRANSACTION, 0L, null );
        memoryTracker = new LocalMemoryTracker( memoryPool );
        forsetiLockManager = new ForsetiLockManager( Config.defaults(), Clocks.nanoClock(), ResourceTypes.values() );
    }

    @AfterEach
    void tearDown()
    {
        assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
        memoryTracker.close();
        assertThat( memoryPool.getPoolMemoryTracker().estimatedHeapMemory() ).isEqualTo( 0 );
    }

    @Test
    void trackMemoryOnSharedLockAcquire()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( oneLockAllocatedMemory ).isGreaterThan( 0 );

            client.acquireShared( LockTracer.NONE, NODE, 2 );
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( twoLocksAllocatedMemory )
                    .isGreaterThan( 0 )
                    .isEqualTo( oneLockAllocatedMemory + ONE_LOCK_SIZE_ESTIMATE );
        }
    }

    @Test
    void trackMemoryOnExclusiveLockAcquire()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( oneLockAllocatedMemory ).isGreaterThan( 0 );

            client.acquireExclusive( LockTracer.NONE, NODE, 2 );
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( twoLocksAllocatedMemory )
                    .isGreaterThan( 0 )
                    .isEqualTo( oneLockAllocatedMemory + ONE_LOCK_SIZE_ESTIMATE );
        }
    }

    @Test
    void sharedLockReAcquireDoesNotAllocateMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( oneLockAllocatedMemory ).isGreaterThan( 0 );

            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertEquals( oneLockAllocatedMemory, twoLocksAllocatedMemory );
        }
    }

    @Test
    void exclusiveLockReAcquireDoesNotAllocateMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( oneLockAllocatedMemory ).isGreaterThan( 0 );

            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertEquals( oneLockAllocatedMemory, twoLocksAllocatedMemory );
        }
    }

    @Test
    void exclusiveLockOverSharedDoesNotAllocateMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var sharedAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( sharedAllocatedMemory ).isGreaterThan( 0 );

            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertEquals( sharedAllocatedMemory, twoLocksAllocatedMemory );
        }
    }

    @Test
    void sharedLockOverExclusiveAllocateMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            var exclusiveAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( exclusiveAllocatedMemory ).isGreaterThan( 0 );

            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( twoLocksAllocatedMemory ).isGreaterThan( exclusiveAllocatedMemory );

            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var threeLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();

            assertThat( threeLocksAllocatedMemory ).isEqualTo( twoLocksAllocatedMemory );
        }
    }

    @Test
    void releaseMemoryOfSharedLock()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            var sharedAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( sharedAllocatedMemory ).isGreaterThan( 0 );

            client.releaseShared(  NODE, 1 );
            var noLocksClientMemory = memoryTracker.estimatedHeapMemory();
            assertThat( noLocksClientMemory ).isGreaterThan( 0 )
                    .isEqualTo( sharedAllocatedMemory - ONE_LOCK_SIZE_ESTIMATE );
        }
    }

    @Test
    void releaseMemoryOfExclusiveLock()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );

            // we take shared lock here as well to create internal maps and report them into tracker before release call
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.releaseShared( NODE, 1 );

            var exclusiveAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat( exclusiveAllocatedMemory ).isGreaterThan( 0 );

            client.releaseExclusive(  NODE, 1 );
            var noLocksClientMemory = memoryTracker.estimatedHeapMemory();
            assertThat( noLocksClientMemory ).isGreaterThan( 0 )
                    .isEqualTo( exclusiveAllocatedMemory - ONE_LOCK_SIZE_ESTIMATE );
        }
    }

    @Test
    void releaseExclusiveLockWhyHoldingSharedDoNotReleaseAnyMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );

            var locksMemory = memoryTracker.estimatedHeapMemory();
            assertThat( locksMemory ).isGreaterThan( 0 );

            client.releaseExclusive(  NODE, 1 );
            var noExclusiveLockMemory = memoryTracker.estimatedHeapMemory();
            assertThat( noExclusiveLockMemory ).isGreaterThan( 0 )
                    .isEqualTo( locksMemory );
        }
    }

    @Test
    void releaseLocksReleasingMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.releaseExclusive( NODE, 1 );
            client.releaseShared( NODE, 1 );

            var noLocksMemory = memoryTracker.estimatedHeapMemory();
            assertThat( noLocksMemory ).isGreaterThan( 0 );

            int lockNumber = 10;
            for ( int i = 0; i < lockNumber; i++ )
            {
                client.acquireExclusive( LockTracer.NONE, NODE, i );
            }
            long exclusiveLocksMemory = memoryTracker.estimatedHeapMemory();
            assertThat( exclusiveLocksMemory ).isEqualTo( noLocksMemory + lockNumber * ONE_LOCK_SIZE_ESTIMATE );

            for ( int i = 0; i < lockNumber; i++ )
            {
                client.acquireShared( LockTracer.NONE, NODE, i );
            }
            long sharedLocksMemory = memoryTracker.estimatedHeapMemory();
            assertThat( sharedLocksMemory ).isEqualTo( exclusiveLocksMemory );

            for ( int i = 0; i < lockNumber; i++ )
            {
                client.releaseShared( NODE, i );
                client.releaseExclusive( NODE, i );
            }

            assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( noLocksMemory );
        }
    }

    @Test
    void trackMemoryOnLocksAcquire()
    {
        try ( Locks.Client client = getClient() )
        {
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.acquireExclusive( LockTracer.NONE, NODE, 2 );
            assertThat( memoryTracker.estimatedHeapMemory() ).isGreaterThan( 0 );
        }
    }

    @Test
    void releaseMemoryOnUnlock()
    {
        try ( Locks.Client client = getClient() )
        {
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.releaseShared( NODE, 1 );
            client.acquireExclusive( LockTracer.NONE, NODE, 2 );
            long lockedSize = memoryTracker.estimatedHeapMemory();
            assertThat( lockedSize ).isGreaterThan( 0 );
            client.releaseExclusive( NODE, 2 );
            assertThat( memoryTracker.estimatedHeapMemory() ).isLessThan( lockedSize );
        }
    }

    @Test
    void upgradingLockShouldNotLeakMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 ); // Should be upgraded
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
            client.releaseExclusive( NODE, 1 );
            client.releaseExclusive( NODE, 1 );
            client.releaseShared( NODE, 1 );
            client.releaseShared( NODE, 1 );
        }
    }

    @Test
    void closeShouldReleaseAllMemory()
    {
        try ( Locks.Client client = getClient() )
        {
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.acquireShared( LockTracer.NONE, NODE, 1 );
            client.acquireExclusive( LockTracer.NONE, NODE, 1 ); // Should be upgraded
            client.acquireExclusive( LockTracer.NONE, NODE, 1 );
        }
    }

    @Test
    void concurrentMemoryShouldEndUpZero() throws Throwable
    {
        Race race = new Race();
        int numThreads = 4;
        LocalMemoryTracker[] trackers = new LocalMemoryTracker[numThreads];
        for ( int i = 0; i < numThreads; i++ )
        {
            trackers[i] = new LocalMemoryTracker( memoryPool );
            Locks.Client client = forsetiLockManager.newClient();
            client.initialize( LeaseService.NoLeaseClient.INSTANCE, i, trackers[i], Config.defaults() );
            race.addContestant( new SimulatedTransaction( client ) );
        }
        race.go();
        for ( int i = 0; i < numThreads; i++ )
        {
            try ( LocalMemoryTracker tracker = trackers[i] )
            {
                assertThat( tracker.estimatedHeapMemory() ).describedAs( "Tracker " + tracker ).isGreaterThanOrEqualTo( 0 );
            }
        }
    }

    private static class SimulatedTransaction implements Runnable
    {
        private final Deque<LockEvent> heldLocks = new ArrayDeque<>();
        private final Locks.Client client;

        SimulatedTransaction( Locks.Client client )
        {
            this.client = client;
        }

        @Override
        public void run()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            try
            {
                for ( int i = 0; i < 100; i++ )
                {
                    if ( heldLocks.isEmpty() || random.nextFloat() > 0.33 )
                    {
                        // Acquire new lock
                        int nodeId = random.nextInt( 10 );
                        if ( random.nextBoolean() )
                        {
                            // Exclusive
                            if ( random.nextBoolean() )
                            {
                                client.acquireExclusive( LockTracer.NONE, NODE, nodeId );
                                heldLocks.push( new LockEvent( true, nodeId ) );
                            }
                            else
                            {
                                if ( client.tryExclusiveLock( NODE, nodeId ) )
                                {
                                    heldLocks.push( new LockEvent( true, nodeId ) );
                                }
                            }
                        }
                        else
                        {
                            // Shared
                            if ( random.nextBoolean() )
                            {
                                client.acquireShared( LockTracer.NONE, NODE, nodeId );
                                heldLocks.push( new LockEvent( false, nodeId ) );
                            }
                            else
                            {
                                if ( client.trySharedLock( NODE, nodeId ) )
                                {
                                    heldLocks.push( new LockEvent( false, nodeId ) );
                                }
                            }
                        }
                    }
                    else
                    {
                        // Release old lock
                        LockEvent pop = heldLocks.pop();
                        if ( pop.isExclusive )
                        {
                            client.releaseExclusive( NODE, pop.nodeId );
                        }
                        else
                        {
                            client.releaseShared( NODE, pop.nodeId );
                        }
                    }
                }
            }
            catch ( DeadlockDetectedException ignore )
            {
            }
            finally
            {
                client.close(); // Should release all of the locks, end resolve deadlock
            }
        }
        private static class LockEvent
        {
            final boolean isExclusive;
            final long nodeId;
            LockEvent( boolean isExclusive, long nodeId )
            {
                this.isExclusive = isExclusive;
                this.nodeId = nodeId;
            }
        }
    }

    private Locks.Client getClient()
    {
        Locks.Client client = forsetiLockManager.newClient();
        client.initialize( LeaseService.NoLeaseClient.INSTANCE, TRANSACTION_ID.getAndIncrement(), memoryTracker, Config.defaults() );
        return client;
    }
}
