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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.MemoryManager;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class PageListTest
{
    private static final long TIMEOUT = 5000;
    private static final int ALIGNMENT = 8;

    private static final long[] pageIds = new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final DummyPageSwapper DUMMY_SWAPPER = new DummyPageSwapper( "" );

    @Parameterized.Parameters( name = "pageRef = {0}")
    public static Iterable<Object[]> parameters()
    {
        LongFunction<Object[]> toArray = x -> new Object[]{x};
//        return () -> Arrays.stream( pageIds ).mapToObj( toArray ).iterator();
        return () -> Arrays.stream( new long[]{0} ).mapToObj( toArray ).iterator();
    }

    private static ExecutorService executor;
    private static MemoryManager mman;

    @BeforeClass
    public static void setUpStatics()
    {
        executor = Executors.newCachedThreadPool( new DaemonThreadFactory() );
        mman = new MemoryManager( ByteUnit.mebiBytes( 1 ), ALIGNMENT );
    }

    @AfterClass
    public static void tearDownStatics()
    {
        mman = null;
        executor.shutdown();
        executor = null;
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final long pageId;
    private final long prevPageId;
    private final long nextPageId;
    private long pageRef;
    private long prevPageRef;
    private long nextPageRef;
    private final int pageSize;
    private PageList pageList;

    public PageListTest( long pageId )
    {
        this.pageId = pageId;
        this.prevPageId = pageId == 0 ? pageIds.length - 1 : (pageId - 1) % pageIds.length;
        this.nextPageId = (pageId + 1) % pageIds.length;
        pageSize = UnsafeUtil.pageSize();
    }

    @Before
    public void setUp()
    {
        pageList = new PageList( pageIds.length, pageSize, mman, VictimPageReference.getVictimPage( pageSize ) );
        pageRef = pageList.deref( pageId );
        prevPageRef = pageList.deref( prevPageId );
        nextPageRef = pageList.deref( nextPageId );
    }

    // xxx ---[ Sequence lock tests ]---

    @Test
    public void uncontendedOptimisticLockMustValidate() throws Exception
    {
        long stamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, stamp ) );
    }

    @Test
    public void mustNotValidateRandomStamp() throws Exception
    {
        assertFalse( pageList.validateReadLock( pageRef, 4242 ) );
    }

    @Test
    public void writeLockMustInvalidateOptimisticReadLock() throws Exception
    {
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void takingWriteLockMustInvalidateOptimisticReadLock() throws Exception
    {
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryWriteLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotValidateUnderWriteLock() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void writeLockReleaseMustInvalidateOptimisticReadLock() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedWriteLockMustBeAvailable() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test
    public void uncontendedOptimisticReadLockMustValidateAfterWriteLockRelease() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test( timeout = TIMEOUT )
    public void writeLocksMustNotBlockOtherWriteLocks() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void writeLocksMustNotBlockOtherWriteLocksInOtherThreads() throws Exception
    {
        int threads = 10;
        CountDownLatch end = new CountDownLatch( threads );
        Runnable runnable = () ->
        {
            assertTrue( pageList.tryWriteLock( pageRef ) );
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
        pageList.unlockWrite( pageRef );
    }

    @Test( expected = IllegalMonitorStateException.class, timeout = TIMEOUT )
    public void writeLockCountOverflowMustThrow() throws Exception
    {
        //noinspection InfiniteLoopStatement
        for (; ; )
        {
            assertTrue( pageList.tryWriteLock( pageRef ) );
        }
    }

    @Test
    public void exclusiveLockMustInvalidateOptimisticLock() throws Exception
    {
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void takingExclusiveLockMustInvalidateOptimisticLock() throws Exception
    {
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryExclusiveLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotValidateUnderExclusiveLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void exclusiveLockReleaseMustInvalidateOptimisticReadLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedOptimisticReadLockMustValidateAfterExclusiveLockRelease() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void canTakeUncontendedExclusiveLocks() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void writeLocksMustFailExclusiveLocks() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void concurrentWriteLocksMustFailExclusiveLocks() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void exclusiveLockMustBeAvailableAfterWriteLock() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void cannotTakeExclusiveLockIfAlreadyTaken() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void exclusiveLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void exclusiveLockMustFailWriteLocks() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        assertFalse( pageList.tryWriteLock( pageRef ) );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockExclusiveLockMustThrow() throws Exception
    {
        pageList.unlockExclusive( pageRef );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockWriteAfterTakingExclusiveLockMustThrow() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockWrite( pageRef );
    }

    @Test( timeout = TIMEOUT )
    public void writeLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @Test
    public void unlockExclusiveMustReturnStampForOptimisticReadLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        long r = pageList.unlockExclusive( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustInvalidateOptimisticReadLocks() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustPreventExclusiveLocks() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void unlockExclusiveAndTakeWriteLockMustAllowConcurrentWriteLocks() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void unlockExclusiveAndTakeWriteLockMustBeAtomic() throws Exception
    {
        int threads = Runtime.getRuntime().availableProcessors() - 1;
        CountDownLatch start = new CountDownLatch( threads );
        AtomicBoolean stop = new AtomicBoolean();
        pageList.tryExclusiveLock( pageRef );
        Runnable runnable = () ->
        {
            while ( !stop.get() )
            {
                if ( pageList.tryExclusiveLock( pageRef ) )
                {
                    pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        stop.set( true );
        for ( Future<?> future : futures )
        {
            future.get(); // Assert that this does not throw
        }
    }

    @Test
    public void stampFromUnlockExclusiveMustNotBeValidIfThereAreWriteLocks() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        long r = pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailable() throws Exception
    {
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void flushLockMustNotInvalidateOptimisticReadLock() throws Exception
    {
        long r = pageList.tryOptimisticReadLock( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void flushLockMustNotFailWriteLock() throws Exception
    {
        pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test
    public void flushLockMustFailExclusiveLock() throws Exception
    {
        pageList.tryFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void cannotTakeFlushLockIfAlreadyTaken() throws Exception
    {
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
        assertFalse( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void writeLockMustNotFailFlushLock() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void exclusiveLockMustFailFlushLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        assertFalse( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustNotFailFlushLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void flushUnlockMustNotInvalidateOptimisticReadLock() throws Exception
    {
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustValidateUnderFlushLock() throws Exception
    {
        pageList.tryFlushLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void flushLockReleaseMustNotInvalidateOptimisticReadLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockFlushMustThrow() throws Exception
    {
        pageList.unlockFlush( pageRef, pageList.tryOptimisticReadLock( pageRef ), true );
    }

    @Test
    public void uncontendedOptimisticReadLockMustBeAvailableAfterFlushLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedWriteLockMustBeAvailableAfterFlushLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test
    public void uncontendedExclusiveLockMustBeAvailableAfterFlushLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailableAfterWriteLock() throws Exception
    {
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailableAfterFlushLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void stampFromUnlockExclusiveMustBeValidUnderFlushLock() throws Exception
    {
        pageList.tryExclusiveLock( pageRef );
        long r = pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentWriteLocks() throws Exception
    {
        assertTrue( pageList.tryWriteLock( prevPageRef ) );
        assertTrue( pageList.tryWriteLock( nextPageRef ) );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
        pageList.unlockWrite( prevPageRef );
        pageList.unlockWrite( nextPageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentExclusiveLocks() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentExclusiveAndWriteLocks() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
        pageList.unlockExclusiveAndTakeWriteLock( prevPageRef );
        pageList.unlockExclusiveAndTakeWriteLock( nextPageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
        pageList.unlockWrite( prevPageRef );
        pageList.unlockWrite( nextPageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void writeLockMustNotGetInterferenceFromAdjacentExclusiveLocks() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
    }

    @Test
    public void flushLockMustNotGetInterferenceFromAdjacentExclusiveLocks() throws Exception
    {
        long s;
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( (s = pageList.tryFlushLock( pageRef )) != 0 );
        pageList.unlockFlush( pageRef, s, true );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
    }

    @Test
    public void flushLockMustNotGetInterferenceFromAdjacentFlushLocks() throws Exception
    {
        long ps, ns, s;
        assertTrue( (ps = pageList.tryFlushLock( prevPageRef )) != 0 );
        assertTrue( (ns = pageList.tryFlushLock( nextPageRef )) != 0 );
        assertTrue( (s = pageList.tryFlushLock( pageRef )) != 0 );
        pageList.unlockFlush( pageRef, s, true );
        pageList.unlockFlush( prevPageRef, ps, true );
        pageList.unlockFlush( nextPageRef, ns, true );
    }

    @Test
    public void exclusiveLockMustNotGetInterferenceFromAdjacentExclusiveLocks() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
    }

    @Test
    public void exclusiveLockMustNotGetInterferenceFromAdjacentWriteLocks() throws Exception
    {
        assertTrue( pageList.tryWriteLock( prevPageRef ) );
        assertTrue( pageList.tryWriteLock( nextPageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockWrite( prevPageRef );
        pageList.unlockWrite( nextPageRef );
    }

    @Test
    public void exclusiveLockMustNotGetInterferenceFromAdjacentExclusiveAndWriteLocks() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusiveAndTakeWriteLock( prevPageRef );
        pageList.unlockExclusiveAndTakeWriteLock( nextPageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockWrite( prevPageRef );
        pageList.unlockWrite( nextPageRef );

        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        pageList.unlockExclusiveAndTakeWriteLock( prevPageRef );
        pageList.unlockExclusiveAndTakeWriteLock( nextPageRef );
        pageList.unlockWrite( prevPageRef );
        pageList.unlockWrite( nextPageRef );
        pageList.unlockExclusive( pageRef );
    }

    @Test
    public void exclusiveLockMustNotGetInterferenceFromAdjacentFlushLocks() throws Exception
    {
        long ps, ns;
        assertTrue( (ps = pageList.tryFlushLock( prevPageRef )) != 0 );
        assertTrue( (ns = pageList.tryFlushLock( nextPageRef )) != 0);
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockFlush( prevPageRef, ps, true );
        pageList.unlockFlush( nextPageRef, ns, true );
    }

    @Test
    public void takingWriteLockMustRaiseModifiedFlag() throws Exception
    {
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @Test
    public void turningExclusiveLockIntoWriteLockMustRaiseModifiedFlag() throws Exception
    {
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertFalse( pageList.isModified( pageRef ) );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @Test
    public void releasingFlushLockMustLowerModifiedFlagIfSuccessful() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfUnsuccessful() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, false );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockWasWithinFlushFlushLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedTakingFlushLock() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockWrite( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedReleasingFlushLock() throws Exception
    {
        long s = pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedFlushLock() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotInterfereWithAdjacentModifiedFlags() throws Exception
    {
        assertTrue( pageList.tryWriteLock( prevPageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertTrue( pageList.tryWriteLock( nextPageRef ) );
        pageList.unlockWrite( prevPageRef );
        pageList.unlockWrite( pageRef );
        pageList.unlockWrite( nextPageRef );
        assertTrue( pageList.isModified( prevPageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.isModified( nextPageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( prevPageRef ) );
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.isModified( nextPageRef ) );
    }

    @Test
    public void writeLockMustNotInterfereWithAdjacentModifiedFlags() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.isModified( prevPageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        assertFalse( pageList.isModified( nextPageRef ) );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowUnlockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowReadLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.tryOptimisticReadLock( pageRef );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowFlushLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        assertThat( pageList.tryFlushLock( pageRef ), is( not( 0L ) ) );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowWriteLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test
    public void allowExclusiveLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
        assertFalse( pageList.isModified( pageRef ) );
        pageList.unlockExclusive( pageRef );
    }

    // xxx ---[ Page state tests ]---

    @Test
    public void mustExposeCachePageSize() throws Exception
    {
        PageList list = new PageList( 0, 42, mman, VictimPageReference.getVictimPage( 42 ) );
        assertThat( list.getCachePageSize(), is( 42 ) );
    }

    @Test
    public void addressesMustBeZeroBeforeInitialisation() throws Exception
    {
        assertThat( pageList.address( pageRef ), is( 0L ) );
    }

    @Test
    public void initialisingBufferMustConsumeMemoryFromMemoryManager() throws Exception
    {
        long initialUsedMemory = mman.sumUsedMemory();
        pageList.initBuffer( pageRef );
        long resultingUsedMemory = mman.sumUsedMemory();
        int allocatedMemory = (int) (resultingUsedMemory - initialUsedMemory);
        assertThat( allocatedMemory, greaterThanOrEqualTo( pageSize ) );
        assertThat( allocatedMemory, lessThanOrEqualTo( pageSize + ALIGNMENT ) );
    }

    @Test
    public void addressMustNotBeZeroAfterInitialisation() throws Exception
    {
        pageList.initBuffer( pageRef );
        assertThat( pageList.address( pageRef ), is( not( equalTo( 0L ) ) ) );
    }

    @Test
    public void pageListMustBeCopyableViaConstructor() throws Exception
    {
        assertThat( pageList.address( pageRef ), is( equalTo( 0L ) ) );
        PageList pl = new PageList( pageList );
        assertThat( pl.address( pageRef ), is( equalTo( 0L ) ) );

        pageList.initBuffer( pageRef );
        assertThat( pageList.address( pageRef ), is( not( equalTo( 0L ) ) ) );
        assertThat( pl.address( pageRef ), is( not( equalTo( 0L ) ) ) );
    }

    @Test
    public void usageCounterMustBeZeroByDefault() throws Exception
    {
        assertTrue( pageList.decrementUsage( pageRef ) );
    }

    @Test
    public void usageCounterMustGoUpToFour() throws Exception
    {
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertTrue( pageList.decrementUsage( pageRef ) );
    }

    @Test
    public void usageCounterMustTruncateAtFour() throws Exception
    {
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertTrue( pageList.decrementUsage( pageRef ) );
        assertTrue( pageList.decrementUsage( pageRef ) );
    }

    @Test
    public void incrementingUsageCounterMustNotInterfereWithAdjacentUsageCounters() throws Exception
    {
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        assertTrue( pageList.decrementUsage( prevPageRef ) );
        assertTrue( pageList.decrementUsage( nextPageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
    }

    @Test
    public void decrementingUsageCounterMustNotInterfereWithAdjacentUsageCounters() throws Exception
    {
        for ( long id : pageIds )
        {
            long ref = pageList.deref( id );
            pageList.incrementUsage( ref );
            pageList.incrementUsage( ref );
        }

        assertFalse( pageList.decrementUsage( pageRef ) );
        assertTrue( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( prevPageRef ) );
        assertFalse( pageList.decrementUsage( nextPageRef ) );
    }

    @Test
    public void filePageIdIsUnboundByDefault() throws Exception
    {
        assertThat( pageList.getFilePageId( pageRef ), is( PageCursor.UNBOUND_PAGE_ID ) );
    }

    // xxx ---[ Page fault tests ]---

    @Test
    public void faultMustThrowWithoutExclusiveLock() throws Exception
    {
        pageList.initBuffer( pageRef );
        exception.expect( IllegalStateException.class );
        pageList.fault( pageRef, DUMMY_SWAPPER, 0, 0, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustThrowIfSwapperIsNull() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        exception.expect( IllegalArgumentException.class );
        pageList.fault( pageRef, null, 0, 0, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustThrowIfFilePageIdIsUnbound() throws Exception
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        exception.expect( IllegalStateException.class );
        pageList.fault( pageRef, DUMMY_SWAPPER, 0, PageCursor.UNBOUND_PAGE_ID, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustReadIntoPage() throws Exception
    {
        byte pageByteContents = (byte) 0xF7;
        int swapperId = 1;
        long filePageId = 2;
        PageSwapper swapper = new DummyPageSwapper( "some file" )
        {
            @Override
            public long read( long fpId, long bufferAddress, int bufferSize ) throws IOException
            {
                if ( fpId == filePageId )
                {
                    UnsafeUtil.setMemory( bufferAddress, bufferSize, pageByteContents );
                    return bufferSize;
                }
                throw new IOException( "Did not expect this file page id = " + fpId );
            }
        };
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, swapper, swapperId, filePageId, PageFaultEvent.NULL );

        long address = pageList.address( pageRef );
        assertThat( address, is( not( 0L ) ) );
        for ( int i = 0; i < pageSize; i++ )
        {
            byte actualByteContents = UnsafeUtil.getByte( address + i );
            if ( actualByteContents != pageByteContents )
            {
                fail( String.format(
                        "Page contents where different at address %x + %s, expected %x but was %x",
                        address, i, pageByteContents, actualByteContents ) );
            }
        }
    }

    @Test
    public void pageMustBeLoadedAndBoundAfterFault() throws Exception
    {
        int swapperId = 1;
        long filePageId = 42;
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
        assertThat( pageList.getFilePageId( pageRef ), is( filePageId ) );
        assertThat( pageList.getSwapperId( pageRef ), is( swapperId ) );
        assertTrue( pageList.isLoaded( pageRef ) );
        assertTrue( pageList.isBoundTo( pageRef, swapperId, filePageId ) );
    }

    @Test
    public void pageMustBeLoadedAndNotBoundIfFaultThrows() throws Exception
    {
        PageSwapper swapper = new DummyPageSwapper( "file" )
        {
            @Override
            public long read( long filePageId, long bufferAddress, int bufferSize ) throws IOException
            {
                throw new IOException( "boo" );
            }
        };
        int swapperId = 1;
        long filePageId = 42;
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        try
        {
            pageList.fault( pageRef, swapper, swapperId, filePageId, PageFaultEvent.NULL );
        }
        catch ( IOException e )
        {
            assertThat( e.getMessage(), is( "boo" ) );
        }
        assertThat( pageList.getFilePageId( pageRef ), is( filePageId ) );
        assertThat( pageList.getSwapperId( pageRef ), is( 0 ) ); // 0 means not bound
        assertTrue( pageList.isLoaded( pageRef ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId, filePageId ) );
    }

    @Test
    public void faultMustThrowIfPageIsAlreadyBound() throws Exception
    {
        int swapperId = 1;
        long filePageId = 42;
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );

        exception.expect( IllegalStateException.class );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustThrowIfPageIsLoadedButNotBound() throws Exception
    {
        int swapperId = 1;
        long filePageId = 42;
        doFailedFault( swapperId, filePageId );

        // After the failed page fault, the page is loaded but not bound.
        // We still can't fault into a loaded page, though.
        exception.expect( IllegalStateException.class );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
    }

    private void doFailedFault( int swapperId, long filePageId )
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        DummyPageSwapper swapper = new DummyPageSwapper( "" )
        {
            @Override
            public long read( long filePageId, long bufferAddress, int bufferSize ) throws IOException
            {
                throw new IOException( "boom" );
            }
        };
        try
        {
            pageList.fault( pageRef, swapper, swapperId, filePageId, PageFaultEvent.NULL );
            fail( "fault should have thrown" );
        }
        catch ( IOException e )
        {
            assertThat( e.getMessage(), is( "boom" ) );
        }
    }

    @Test
    public void faultMustPopulatePageFaultEvent() throws Exception
    {
        int swapperId = 1;
        long filePageId = 42;
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        DummyPageSwapper swapper = new DummyPageSwapper( "" )
        {
            @Override
            public long read( long filePageId, long bufferAddress, int bufferSize ) throws IOException
            {
                return 333;
            }
        };
        StubPageFaultEvent event = new StubPageFaultEvent();
        pageList.fault( pageRef, swapper, swapperId, filePageId, event );
        assertThat( event.bytesRead, is( 333L ) );
        assertThat( event.cachePageId, is( not( 0 ) ) );
    }

    @Test
    public void unboundPageMustNotBeLoaded() throws Exception
    {
        assertFalse( pageList.isLoaded( pageRef ) );
    }

    @Test
    public void unboundPageMustNotBeBoundToAnything() throws Exception
    {
        assertFalse( pageList.isBoundTo( pageRef, 0, 0 ) );
    }

    @Test
    public void boundPagesAreNotBoundToOtherPagesWithSameSwapper() throws Exception
    {
        long filePageId = 42;
        doFault( 2, filePageId );

        assertTrue( pageList.isBoundTo( pageRef, 2, filePageId ) );
        assertFalse( pageList.isBoundTo( pageRef, 2, filePageId + 1 ) );
        assertFalse( pageList.isBoundTo( pageRef, 2, filePageId - 1 ) );
    }

    private void doFault( int swapperId, long filePageId ) throws IOException
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
    }

    @Test
    public void boundPagesAreNotBoundToOtherPagesWithSameFilePageId() throws Exception
    {
        int swapperId = 2;
        doFault( swapperId, 42 );

        assertTrue( pageList.isBoundTo( pageRef, swapperId, 42 ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId + 1, 42 ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId - 1, 42 ) );
    }

    @Test
    public void faultMustNotInterfereWithAdjacentPages() throws Exception
    {
        doFault( 1, 42 );

        assertFalse( pageList.isLoaded( prevPageRef ) );
        assertFalse( pageList.isLoaded( nextPageRef ) );
        assertFalse( pageList.isBoundTo( prevPageRef, 1, 42 ) );
        assertFalse( pageList.isBoundTo( prevPageRef, 0, 0 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, 1, 42 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, 0, 0 ) );
    }

    @Test
    public void failedFaultMustNotInterfereWithAdjacentPages() throws Exception
    {
        doFailedFault( 1, 42 );

        assertFalse( pageList.isLoaded( prevPageRef ) );
        assertFalse( pageList.isLoaded( nextPageRef ) );
        assertFalse( pageList.isBoundTo( prevPageRef, 1, 42 ) );
        assertFalse( pageList.isBoundTo( prevPageRef, 0, 0 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, 1, 42 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, 0, 0 ) );
    }

    @Test
    public void exclusiveLockMustStillBeHeldAfterFault() throws Exception
    {
        doFault( 1, 42 );
        pageList.unlockExclusive( pageRef ); // will throw if lock is not held
    }

    // xxx ---[ Page eviction tests ]---

    @Test
    public void tryEvictMustFailIfPageIsAlreadyExclusivelyLocked() throws Exception
    {
        doFault( 1, 42 ); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        assertFalse( pageList.tryEvict( pageRef, DUMMY_SWAPPER ) );
    }

    @Test
    public void tryEvictThatFailsOnExclusiveLockMustNotUndoSaidLock() throws Exception
    {
        doFault( 1, 42 ); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        pageList.tryEvict( pageRef, DUMMY_SWAPPER ); // This attempt will fail
        assertTrue( pageList.isExclusivelyLocked( pageRef ) ); // page should still have its lock
    }

    @Test
    public void tryEvictMustFailIfPageIsNotLoaded() throws Exception
    {
        assertFalse( pageList.tryEvict( pageRef, DUMMY_SWAPPER ) );
    }

    @Test
    public void tryEvictMustWhenPageIsNotLoadedMustNotLeavePageLocked() throws Exception
    {
        pageList.tryEvict( pageRef, DUMMY_SWAPPER ); // This attempt fails
        assertFalse( pageList.isExclusivelyLocked( pageRef ) ); // Page should not be left in locked state
    }

    @Test( expected = IllegalArgumentException.class )
    public void tryEvictMustThrowIfSwapperIsNull() throws Exception
    {
        pageList.tryEvict( pageRef, null );
    }

    @Test
    public void tryEvictMustLeavePageExclusivelyLockedOnSuccess() throws Exception
    {
        doFault( 1, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.tryEvict( pageRef, DUMMY_SWAPPER ) );
        pageList.unlockExclusive( pageRef ); // will throw if lock is not held
    }

    @Test
    public void pageMustNotBeLoadedAfterSuccessfulEviction() throws Exception
    {
        doFault( 1, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.isLoaded( pageRef ) );
        pageList.tryEvict( pageRef, DUMMY_SWAPPER );
        assertFalse( pageList.isLoaded( pageRef ) );
    }

    @Test
    public void pageMustNotBeBoundAfterSuccessfulEviction() throws Exception
    {
        doFault( 1, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.isBoundTo( pageRef, 1, 42 ) );
        assertTrue( pageList.isLoaded( pageRef ) );
        assertThat( pageList.getSwapperId( pageRef ), is( 1 ) );
        pageList.tryEvict( pageRef, DUMMY_SWAPPER );
        assertFalse( pageList.isBoundTo( pageRef, 1, 42 ) );
        assertFalse( pageList.isLoaded( pageRef ) );
        assertThat( pageList.getSwapperId( pageRef ), is( 0 ) );
    }

    @Test
    public void pageMustNotBeModifiedAfterSuccessfulEviction() throws Exception
    {
        doFault( 1, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, DUMMY_SWAPPER ) );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void tryEvictMustFlushPageIfModified() throws Exception
    {
        AtomicLong writtenFilePageId = new AtomicLong( -1 );
        AtomicLong writtenBufferAddress = new AtomicLong( -1 );
        AtomicInteger writtenBufferSize = new AtomicInteger( -1 );
        PageSwapper swapper = new DummyPageSwapper( "file" )
        {
            @Override
            public long write( long filePageId, long bufferAddress, int bufferSize ) throws IOException
            {
                assertTrue( writtenFilePageId.compareAndSet( -1, filePageId ) );
                assertTrue( writtenBufferAddress.compareAndSet( -1, bufferAddress ) );
                assertTrue( writtenBufferSize.compareAndSet( -1, bufferSize ) );
                return super.write( filePageId, bufferAddress, bufferSize );
            }
        };
        doFault( 1, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, swapper ) );
        assertThat( writtenFilePageId.get(), is( 42L ) );
        assertThat( writtenBufferAddress.get(), is( pageList.address( pageRef ) ) );
//        assertThat( writtenBufferSize.get(), is( /* ... */ ) ); // todo
    }
    // todo try evict must flush page if modified
    // todo try evict must not flush page if not modified
    // todo try evict must notify swapper on success
    // todo try evict must leave page unlocked if flush throws
    // todo try evict must leave page loaded if flush throws
    // todo try evict must leave page bound if flush throws
    // todo try evict must leave page modified if flush throws
    // todo try evict must report to eviction event
    // todo try evict that flushes must report to flush event
    // todo try evict that fails must not interfere with adjacent pages
    // todo try evict that succeeds must not interfere with adjacent pages

    // todo evict
    // todo flush
    // todo freelist? (entries chained via file page ids in a linked list? should work as free pages are always exclusively locked, and thus don't really need an isLoaded check)
}
