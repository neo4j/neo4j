/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.AfterClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class OptiLockTest
{
    private static final long TIMEOUT = 5000;
    private static final ExecutorService executor = Executors.newCachedThreadPool(new DaemonThreadFactory());

    @AfterClass
    public static void shutDownExecutor()
    {
        executor.shutdown();
    }

    OptiLock lock = new OptiLock();

    @Test
    public void uncontendedOptimisticLockMustValidate() throws Exception
    {
        long stamp = lock.tryOptimisticReadLock();
        assertTrue( lock.validateReadLock( stamp ) );
    }

    @Test
    public void mustNotValidateRandomStamp() throws Exception
    {
        assertFalse( lock.validateReadLock( 4242 ) );
    }

    @Test
    public void writeLockMustInvalidateOptimisticLock() throws Exception
    {
        long r = lock.tryOptimisticReadLock();
        lock.tryWriteLock();
        lock.unlockWrite();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void takingWriteLockMustInvalidateOptimisticLock() throws Exception
    {
        long r = lock.tryOptimisticReadLock();
        lock.tryWriteLock();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void optimisticReadLockMustNotValidateUnderWriteLock() throws Exception
    {
        lock.tryWriteLock();
        long r = lock.tryOptimisticReadLock();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void writeLockReleaseMustInvalidateOptimisticReadLock() throws Exception
    {
        lock.tryWriteLock();
        long r = lock.tryOptimisticReadLock();
        lock.unlockWrite();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void uncontendedWriteLockMustBeAvailable() throws Exception
    {
        assertTrue( lock.tryWriteLock() );
    }

    @Test
    public void uncontendedOptimisticReadLockMustValidateAfterWriteLockRelease() throws Exception
    {
        lock.tryWriteLock();
        lock.unlockWrite();
        long r = lock.tryOptimisticReadLock();
        assertTrue( lock.validateReadLock( r ) );
    }

    @Test( timeout = TIMEOUT )
    public void writeLocksMustNotBlockOtherWriteLocks() throws Exception
    {
        assertTrue( lock.tryWriteLock() );
        assertTrue( lock.tryWriteLock() );
    }

    @Test( timeout = TIMEOUT )
    public void writeLocksMustNotBlockOtherWriteLocksInOtherThreads() throws Exception
    {
        int threads = 10;
        CountDownLatch end = new CountDownLatch( threads );
        Runnable runnable = () -> {
            assertTrue( lock.tryWriteLock() );
            end.countDown();
        };
        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 0; i < threads; i++ )
        {
            futures.add( executor.submit( runnable ) );
        }
        end.await();
        for ( Future<?> future : futures )
        {
            future.get();
        }
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockWriteLockMustThrow() throws Exception
    {
        lock.unlockWrite();
    }

    @Test( expected = IllegalMonitorStateException.class, timeout = TIMEOUT )
    public void writeLockCountOverflowMustThrow() throws Exception
    {
        // TODO its possible we might want to spin-yield or fail instead of throwing, hoping someone will give us a lock
        for ( ;; )
        {
            assertTrue( lock.tryWriteLock() );
        }
    }

    @Test
    public void exclusiveLockMustInvalidateOptimisticLock() throws Exception
    {
        long r = lock.tryOptimisticReadLock();
        lock.tryExclusiveLock();
        lock.unlockExclusive();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void takingExclusiveLockMustInvalidateOptimisticLock() throws Exception
    {
        long r = lock.tryOptimisticReadLock();
        lock.tryExclusiveLock();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void optimisticReadLockMustNotValidateUnderExclusiveLock() throws Exception
    {
        lock.tryExclusiveLock();
        long r = lock.tryOptimisticReadLock();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void exclusiveLockReleaseMustInvalidateOptimisticReadLock() throws Exception
    {
        lock.tryExclusiveLock();
        long r = lock.tryOptimisticReadLock();
        lock.unlockExclusive();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void uncontendedOptimisticReadLockMustValidateAfterExclusiveLockRelease() throws Exception
    {
        lock.tryExclusiveLock();
        lock.unlockExclusive();
        long r = lock.tryOptimisticReadLock();
        assertTrue( lock.validateReadLock( r ) );
    }

    @Test
    public void canTakeUncontendedExclusiveLocks() throws Exception
    {
        assertTrue( lock.tryExclusiveLock() );
    }

    @Test
    public void writeLocksMustFailExclusiveLocks() throws Exception
    {
        lock.tryWriteLock();
        assertFalse( lock.tryExclusiveLock() );
    }

    @Test
    public void exclusiveLockMustBeAvailableAfterWriteLock() throws Exception
    {
        lock.tryWriteLock();
        lock.unlockWrite();
        assertTrue( lock.tryExclusiveLock() );
    }

    @Test
    public void cannotTakeExclusiveLockIfAlreadyTaken() throws Exception
    {
        assertTrue( lock.tryExclusiveLock() );
        assertFalse( lock.tryExclusiveLock() );
    }

    @Test
    public void exclusiveLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        assertTrue( lock.tryExclusiveLock() );
        lock.unlockExclusive();
        assertTrue( lock.tryExclusiveLock() );
    }

    @Test( timeout = TIMEOUT )
    public void exclusiveLockMustFailWriteLocks() throws Exception
    {
        lock.tryExclusiveLock();
        assertFalse( lock.tryWriteLock() );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockExclusiveLockMustThrow() throws Exception
    {
        lock.unlockExclusive();
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockWriteAfterTakingExclusiveLockMustThrow() throws Exception
    {
        lock.tryExclusiveLock();
        lock.unlockWrite();
    }

    @Test( timeout = TIMEOUT )
    public void writeLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        lock.tryExclusiveLock();
        lock.unlockExclusive();
        assertTrue( lock.tryWriteLock() );
        lock.unlockWrite();
    }

    @Test
    public void unlockExclusiveMustReturnStampForOptimisticReadLock() throws Exception
    {
        lock.tryExclusiveLock();
        long r = lock.unlockExclusive();
        assertTrue( lock.validateReadLock( r ) );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustInvalidateOptimisticReadLocks() throws Exception
    {
        lock.tryExclusiveLock();
        lock.unlockExclusiveAndTakeWriteLock();
        long r = lock.tryOptimisticReadLock();
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustPreventExclusiveLocks() throws Exception
    {
        lock.tryExclusiveLock();
        lock.unlockExclusiveAndTakeWriteLock();
        assertFalse( lock.tryExclusiveLock() );
    }

    @Test( timeout = TIMEOUT )
    public void unlockExclusiveAndTakeWriteLockMustAllowConcurrentWriteLocks() throws Exception
    {
        lock.tryExclusiveLock();
        lock.unlockExclusiveAndTakeWriteLock();
        assertTrue( lock.tryWriteLock() );
    }

    @Test( timeout = TIMEOUT )
    public void unlockExclusiveAndTakeWriteLockMustBeAtomic() throws Exception
    {
        int threads = 12;
        CountDownLatch start = new CountDownLatch( threads );
        AtomicBoolean stop = new AtomicBoolean();
        lock.tryExclusiveLock();
        Runnable runnable = () -> {
            while ( !stop.get() )
            {
                if ( lock.tryExclusiveLock() )
                {
                    lock.unlockExclusive();
                    throw new RuntimeException( "I should not have gotten that lock" );
                }
                start.countDown();
            }
        };

        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 0; i < threads; i++ )
        {
            futures.add( executor.submit( runnable ) );
        }

        start.await();
        lock.unlockExclusiveAndTakeWriteLock();
        stop.set( true );
        for ( Future<?> future : futures )
        {
            future.get(); // Assert that this does not throw
        }
    }

    @Test
    public void stampFromUnlockExclusiveMustNotBeValidIfThereAreWriteLocks() throws Exception
    {
        lock.tryExclusiveLock();
        long r = lock.unlockExclusive();
        assertTrue( lock.tryWriteLock() );
        assertFalse( lock.validateReadLock( r ) );
    }

    @Test
    public void toStringMustDescribeState() throws Exception
    {
        assertThat( lock.toString(), is( "OptiLock[E:0, W:0:0, S:0:0]" ) );
        lock.tryWriteLock();
        assertThat( lock.toString(), is( "OptiLock[E:0, W:1:1, S:0:0]" ) );
        lock.unlockWrite();
        assertThat( lock.toString(), is( "OptiLock[E:0, W:0:0, S:1:1]" ) );
    }
}
