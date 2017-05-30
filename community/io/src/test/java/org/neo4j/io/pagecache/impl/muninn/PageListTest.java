/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.function.IntFunction;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.MemoryManager;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class PageListTest
{
    private static final long TIMEOUT = 5000;
    private static final int ALIGNMENT = 8;

    private static final int[] pageIds = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final DummyPageSwapper DUMMY_SWAPPER = new DummyPageSwapper( "", UnsafeUtil.pageSize() );

    @Parameterized.Parameters( name = "pageRef = {0}" )
    public static Iterable<Object[]> parameters()
    {
        IntFunction<Object[]> toArray = x -> new Object[]{x};
        return () -> Arrays.stream( pageIds ).mapToObj( toArray ).iterator();
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

    private final int pageId;
    private final int prevPageId;
    private final int nextPageId;
    private long pageRef;
    private long prevPageRef;
    private long nextPageRef;
    private final int pageSize;
    private SwapperSet swappers;
    private PageList pageList;

    public PageListTest( int pageId )
    {
        this.pageId = pageId;
        this.prevPageId = pageId == 0 ? pageIds.length - 1 : (pageId - 1) % pageIds.length;
        this.nextPageId = (pageId + 1) % pageIds.length;
        pageSize = UnsafeUtil.pageSize();
    }

    @Before
    public void setUp()
    {
        swappers = new SwapperSet();
        pageList = new PageList( pageIds.length, pageSize, mman, swappers, VictimPageReference.getVictimPage( pageSize ) );
        pageRef = pageList.deref( pageId );
        prevPageRef = pageList.deref( prevPageId );
        nextPageRef = pageList.deref( nextPageId );
    }

    @Test
    public void mustExposePageCount() throws Exception
    {
        int pageCount;
        long victimPage = VictimPageReference.getVictimPage( pageSize );

        pageCount = 3;
        assertThat( new PageList( pageCount, pageSize, mman, swappers, victimPage ).getPageCount(), is( pageCount ) );

        pageCount = 42;
        assertThat( new PageList( pageCount, pageSize, mman, swappers, victimPage ).getPageCount(), is( pageCount ) );
    }

    @Test
    public void mustBeAbleToReversePageRedToPageId() throws Exception
    {
        assertThat( pageList.toId( pageRef ), is( pageId ) );
    }

    // xxx ---[ Sequence lock tests ]---

    @Test
    public void pagesAreInitiallyExclusivelyLocked() throws Exception
    {
        assertTrue( pageList.isExclusivelyLocked( pageRef ) );
        pageList.unlockExclusive( pageRef );
    }

    @Test
    public void uncontendedOptimisticLockMustValidate() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedWriteLockMustBeAvailable() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test
    public void uncontendedOptimisticReadLockMustValidateAfterWriteLockRelease() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test( timeout = TIMEOUT )
    public void writeLocksMustNotBlockOtherWriteLocks() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void writeLocksMustNotBlockOtherWriteLocksInOtherThreads() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        //noinspection InfiniteLoopStatement
        for (; ; )
        {
            assertTrue( pageList.tryWriteLock( pageRef ) );
        }
    }

    @Test
    public void exclusiveLockMustInvalidateOptimisticLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void takingExclusiveLockMustInvalidateOptimisticLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryExclusiveLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotValidateUnderExclusiveLock() throws Exception
    {
        // exclusive lock implied by constructor
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void exclusiveLockReleaseMustInvalidateOptimisticReadLock() throws Exception
    {
        // exclusive lock implied by constructor
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedOptimisticReadLockMustValidateAfterExclusiveLockRelease() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void canTakeUncontendedExclusiveLocks() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void writeLocksMustFailExclusiveLocks() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void concurrentWriteLocksMustFailExclusiveLocks() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void exclusiveLockMustBeAvailableAfterWriteLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void cannotTakeExclusiveLockIfAlreadyTaken() throws Exception
    {
        // existing exclusive lock implied by constructor
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void exclusiveLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void exclusiveLockMustFailWriteLocks() throws Exception
    {
        // exclusive lock implied by constructor
        assertFalse( pageList.tryWriteLock( pageRef ) );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockExclusiveLockMustThrow() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( pageRef );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unmatchedUnlockWriteAfterTakingExclusiveLockMustThrow() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockWrite( pageRef );
    }

    @Test( timeout = TIMEOUT )
    public void writeLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @Test
    public void unlockExclusiveMustReturnStampForOptimisticReadLock() throws Exception
    {
        // exclusive lock implied by constructor
        long r = pageList.unlockExclusive( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustInvalidateOptimisticReadLocks() throws Exception
    {
        // exclusive lock implied by constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustPreventExclusiveLocks() throws Exception
    {
        // exclusive lock implied by constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void unlockExclusiveAndTakeWriteLockMustAllowConcurrentWriteLocks() throws Exception
    {
        // exclusive lock implied by constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test( timeout = TIMEOUT )
    public void unlockExclusiveAndTakeWriteLockMustBeAtomic() throws Exception
    {
        // exclusive lock implied by constructor
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
        // exclusive lock implied by constructor
        long r = pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailable() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void flushLockMustNotInvalidateOptimisticReadLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void flushLockMustNotFailWriteLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test
    public void flushLockMustFailExclusiveLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void cannotTakeFlushLockIfAlreadyTaken() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
        assertFalse( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void writeLockMustNotFailFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void exclusiveLockMustFailFlushLock() throws Exception
    {
        // exclusively locked from constructor
        assertFalse( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void unlockExclusiveAndTakeWriteLockMustNotFailFlushLock() throws Exception
    {
        // exclusively locked from constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void flushUnlockMustNotInvalidateOptimisticReadLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustValidateUnderFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void flushLockReleaseMustNotInvalidateOptimisticReadLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void uncontendedWriteLockMustBeAvailableAfterFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @Test
    public void uncontendedExclusiveLockMustBeAvailableAfterFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailableAfterWriteLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailableAfterExclusiveLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void uncontendedFlushLockMustBeAvailableAfterFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @Test
    public void stampFromUnlockExclusiveMustBeValidUnderFlushLock() throws Exception
    {
        // exclusively locked from constructor
        long r = pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @Test
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentWriteLocks() throws Exception
    {
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        long s = 0;
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        long ps = 0;
        long ns = 0;
        long s = 0;
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
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void exclusiveLockMustNotGetInterferenceFromAdjacentWriteLocks() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
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
        // exclusive locks on prevPageRef, nextPageRef and pageRef are implied from constructor
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        long ps = 0;
        long ns = 0;
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
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @Test
    public void turningExclusiveLockIntoWriteLockMustRaiseModifiedFlag() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void loweredModifiedFlagMustRemainLoweredAfterReleasingFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertFalse( pageList.isModified( pageRef ) );

        s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfUnsuccessful() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedTakingFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockWrite( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedReleasingFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
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
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.isModified( prevPageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        assertFalse( pageList.isModified( nextPageRef ) );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowUnlockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowReadLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryOptimisticReadLock( pageRef );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowFlushLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertThat( pageList.tryFlushLock( pageRef ), is( not( 0L ) ) );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test( expected = IllegalStateException.class )
    public void disallowWriteLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
    }

    @Test
    public void allowExclusiveLockedPageToExplicitlyLowerModifiedFlag() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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

    @Test
    public void unlockWriteAndTryTakeFlushLockMustTakeFlushLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long flushStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( flushStamp, is( not( 0L ) ) );
        assertThat( pageList.tryFlushLock( pageRef ), is( 0L ) );
        pageList.unlockFlush( pageRef, flushStamp, true );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unlockWriteAndTryTakeFlushLockMustThrowIfNotWriteLocked() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.unlockWriteAndTryTakeFlushLock( pageRef );
    }

    @Test( expected = IllegalMonitorStateException.class )
    public void unlockWriteAndTryTakeFlushLockMustThrowIfNotWriteLockedButExclusiveLocked() throws Exception
    {
        // exclusive lock implied by constructor
        pageList.unlockWriteAndTryTakeFlushLock( pageRef );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockMustFailIfFlushLockIsAlreadyTaken() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.tryFlushLock( pageRef );
        assertThat( stamp, is( not( 0L ) ) );
        long secondStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( secondStamp, is( 0L ) );
        pageList.unlockFlush( pageRef, stamp, true );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockMustReleaseWriteLockEvenIfFlushLockFails() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long flushStamp = pageList.tryFlushLock( pageRef );
        assertThat( flushStamp, is( not( 0L ) ) );
        assertThat( pageList.unlockWriteAndTryTakeFlushLock( pageRef ), is( 0L ) );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockMustReleaseWriteLockWhenFlushLockSucceeds() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertThat( pageList.unlockWriteAndTryTakeFlushLock( pageRef ), is( not( 0L ) ) );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
    }

    @Test
    public void unlockWriteAndTrueTakeFlushLockMustRaiseModifiedFlag() throws Exception
    {
        assertFalse( pageList.isModified( pageRef ) );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        assertThat( pageList.unlockWriteAndTryTakeFlushLock( pageRef ), is( not( 0L ) ) );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushMustLowerModifiedFlagIfSuccessful() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, true );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushMustNotLowerModifiedFlagIfFailed() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, false );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockWithOverlappingWriterAndThenUnlockFlushMustNotLowerModifiedFlag()
            throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) ); // two write locks, now
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef ); // one flush, one write lock
        assertThat( stamp, is( not( 0L ) ) );
        pageList.unlockWrite( pageRef ); // one flush, zero write locks
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, true ); // flush is successful, but had one overlapping writer
        assertTrue( pageList.isModified( pageRef ) ); // so it's still modified
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushWithOverlappingWriterMustNotLowerModifiedFlag()
            throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef ); // one flush lock
        assertThat( stamp, is( not( 0L ) ) );
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) ); // one flush and one write lock
        pageList.unlockFlush( pageRef, stamp, true ); // flush is successful, but have one overlapping writer
        pageList.unlockWrite( pageRef ); // no more locks, but a writer started within flush section ...
        assertTrue( pageList.isModified( pageRef ) ); // ... and overlapped unlockFlush, so it's still modified
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushWithContainedWriterMustNotLowerModifiedFlag()
            throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef ); // one flush lock
        assertThat( stamp, is( not( 0L ) ) );
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) ); // one flush and one write lock
        pageList.unlockWrite( pageRef ); // back to one flush lock
        pageList.unlockFlush( pageRef, stamp, true ); // flush is successful, but had one overlapping writer
        assertTrue( pageList.isModified( pageRef ) ); // so it's still modified
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockThatSucceedsMustPreventOverlappingExclusiveLock() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, true );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockThatFailsMustPreventOverlappingExclusiveLock() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, false );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockThatSucceedsMustPreventOverlappingFlushLock() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( pageList.tryFlushLock( pageRef ), is( 0L ) );
        pageList.unlockFlush( pageRef, stamp, true );
        assertThat( pageList.tryFlushLock( pageRef ), is( not( 0L ) ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockThatFailsMustPreventOverlappingFlushLock() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( pageList.tryFlushLock( pageRef ), is( 0L ) );
        pageList.unlockFlush( pageRef, stamp, false );
        assertThat( pageList.tryFlushLock( pageRef ), is( not( 0L ) ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockMustNotInvalidateReadersOverlappingWithFlushLock() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long flushStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
        pageList.unlockFlush( pageRef, flushStamp, true );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
    }

    @Test
    public void unlockWriteAndTryTakeFlushLockMustInvalidateReadersOverlappingWithWriteLock() throws Exception
    {
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        long flushStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, readStamp ) );
        pageList.unlockFlush( pageRef, flushStamp, true );
        assertFalse( pageList.validateReadLock( pageRef, readStamp ) );
    }

    // xxx ---[ Page state tests ]---

    @Test
    public void mustExposeCachePageSize() throws Exception
    {
        PageList list = new PageList( 0, 42, mman, swappers, VictimPageReference.getVictimPage( 42 ) );
        assertThat( list.getCachePageSize(), is( 42 ) );
    }

    @Test
    public void addressesMustBeZeroBeforeInitialisation() throws Exception
    {
        assertThat( pageList.getAddress( pageRef ), is( 0L ) );
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
        assertThat( pageList.getAddress( pageRef ), is( not( equalTo( 0L ) ) ) );
    }

    @Test
    public void pageListMustBeCopyableViaConstructor() throws Exception
    {
        assertThat( pageList.getAddress( pageRef ), is( equalTo( 0L ) ) );
        PageList pl = new PageList( pageList );
        assertThat( pl.getAddress( pageRef ), is( equalTo( 0L ) ) );

        pageList.initBuffer( pageRef );
        assertThat( pageList.getAddress( pageRef ), is( not( equalTo( 0L ) ) ) );
        assertThat( pl.getAddress( pageRef ), is( not( equalTo( 0L ) ) ) );
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
        for ( int id : pageIds )
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
        pageList.unlockExclusive( pageRef );
        pageList.initBuffer( pageRef );
        exception.expect( IllegalStateException.class );
        pageList.fault( pageRef, DUMMY_SWAPPER, 0, 0, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustThrowIfSwapperIsNull() throws Exception
    {
        // exclusive lock implied by the constructor
        pageList.initBuffer( pageRef );
        exception.expect( IllegalArgumentException.class );
        pageList.fault( pageRef, null, 0, 0, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustThrowIfFilePageIdIsUnbound() throws Exception
    {
        // exclusively locked from constructor
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
        PageSwapper swapper = new DummyPageSwapper( "some file", pageSize )
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
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, swapper, swapperId, filePageId, PageFaultEvent.NULL );

        long address = pageList.getAddress( pageRef );
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
        // exclusive lock implied by constructor
        int swapperId = 1;
        long filePageId = 42;
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
        // exclusive lock implied by constructor
        PageSwapper swapper = new DummyPageSwapper( "file", pageSize )
        {
            @Override
            public long read( long filePageId, long bufferAddress, int bufferSize ) throws IOException
            {
                throw new IOException( "boo" );
            }
        };
        int swapperId = 1;
        long filePageId = 42;
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
        // exclusive lock implied by constructor
        int swapperId = 1;
        long filePageId = 42;
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );

        exception.expect( IllegalStateException.class );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
    }

    @Test
    public void faultMustThrowIfPageIsLoadedButNotBound() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        DummyPageSwapper swapper = new DummyPageSwapper( "", pageSize )
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
        // exclusive lock implied by constructor
        int swapperId = 1;
        long filePageId = 42;
        pageList.initBuffer( pageRef );
        DummyPageSwapper swapper = new DummyPageSwapper( "", pageSize )
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
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        int swapperId = 2;
        doFault( swapperId, 42 );

        assertTrue( pageList.isBoundTo( pageRef, swapperId, 42 ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId + 1, 42 ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId - 1, 42 ) );
    }

    @Test
    public void faultMustNotInterfereWithAdjacentPages() throws Exception
    {
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
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
        pageList.unlockExclusive( pageRef );
        doFault( 1, 42 );
        pageList.unlockExclusive( pageRef ); // will throw if lock is not held
    }

    // xxx ---[ Page eviction tests ]---

    @Test
    public void tryEvictMustFailIfPageIsAlreadyExclusivelyLocked() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        assertFalse( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
    }

    @Test
    public void tryEvictThatFailsOnExclusiveLockMustNotUndoSaidLock() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL ); // This attempt will fail
        assertTrue( pageList.isExclusivelyLocked( pageRef ) ); // page should still have its lock
    }

    @Test
    public void tryEvictMustFailIfPageIsNotLoaded() throws Exception
    {
        assertFalse( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
    }

    @Test
    public void tryEvictMustWhenPageIsNotLoadedMustNotLeavePageLocked() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL ); // This attempt fails
        assertFalse( pageList.isExclusivelyLocked( pageRef ) ); // Page should not be left in locked state
    }

    @Test
    public void tryEvictMustLeavePageExclusivelyLockedOnSuccess() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        pageList.unlockExclusive( pageRef ); // will throw if lock is not held
    }

    @Test
    public void pageMustNotBeLoadedAfterSuccessfulEviction() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.isLoaded( pageRef ) );
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
        assertFalse( pageList.isLoaded( pageRef ) );
    }

    @Test
    public void pageMustNotBeBoundAfterSuccessfulEviction() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.isBoundTo( pageRef, 1, 42 ) );
        assertTrue( pageList.isLoaded( pageRef ) );
        assertThat( pageList.getSwapperId( pageRef ), is( 1 ) );
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
        assertFalse( pageList.isBoundTo( pageRef, 1, 42 ) );
        assertFalse( pageList.isLoaded( pageRef ) );
        assertThat( pageList.getSwapperId( pageRef ), is( 0 ) );
    }

    @Test
    public void pageMustNotBeModifiedAfterSuccessfulEviction() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void tryEvictMustFlushPageIfModified() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        AtomicLong writtenFilePageId = new AtomicLong( -1 );
        AtomicLong writtenBufferAddress = new AtomicLong( -1 );
        PageSwapper swapper = new DummyPageSwapper( "file", pageSize )
        {
            @Override
            public long write( long filePageId, long bufferAddress ) throws IOException
            {
                assertTrue( writtenFilePageId.compareAndSet( -1, filePageId ) );
                assertTrue( writtenBufferAddress.compareAndSet( -1, bufferAddress ) );
                return super.write( filePageId, bufferAddress );
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertThat( writtenFilePageId.get(), is( 42L ) );
        assertThat( writtenBufferAddress.get(), is( pageList.getAddress( pageRef ) ) );
    }

    @Test
    public void tryEvictMustNotFlushPageIfNotModified() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        AtomicInteger writes = new AtomicInteger();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public long write( long filePageId, long bufferAddress ) throws IOException
            {
                writes.getAndIncrement();
                return super.write( filePageId, bufferAddress );
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusive( pageRef ); // we take no write lock, so page is not modified
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertThat( writes.get(), is( 0 ) );
    }

    @Test
    public void tryEvictMustNotifySwapperOnSuccess() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public void evicted( long filePageId )
            {
                evictionNotified.set( true );
                assertThat( filePageId, is( 42L ) );
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertTrue( evictionNotified.get() );
    }

    @Test
    public void tryEvictMustNotifySwapperOnSuccessEvenWhenFlushing() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public void evicted( long filePageId )
            {
                evictionNotified.set( true );
                assertThat( filePageId, is( 42L ) );
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertTrue( evictionNotified.get() );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @Test
    public void tryEvictMustLeavePageUnlockedAndLoadedAndBoundAndModifiedIfFlushThrows() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public long write( long filePageId, long bufferAddress ) throws IOException
            {
                throw new IOException();
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        try
        {
            pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
            fail( "tryEvict should have thrown" );
        }
        catch ( IOException e )
        {
            // good
        }
        // there should be no lock preventing us from taking an exclusive lock
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        // page should still be loaded...
        assertTrue( pageList.isLoaded( pageRef ) );
        // ... and bound
        assertTrue( pageList.isBoundTo( pageRef, swapperId, 42 ) );
        // ... and modified
        assertTrue( pageList.isModified( pageRef ) );
    }

    @Test
    public void tryEvictMustNotNotifySwapperOfEvictionIfFlushThrows() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public long write( long filePageId, long bufferAddress ) throws IOException
            {
                throw new IOException();
            }

            @Override
            public void evicted( long filePageId )
            {
                evictionNotified.set( true );
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        try
        {
            pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
            fail( "tryEvict should have thrown" );
        }
        catch ( IOException e )
        {
            // good
        }
        // we should not have gotten any notification about eviction
        assertFalse( evictionNotified.get() );
    }

    private static class EvictionAndFlushRecorder implements EvictionEvent, FlushEventOpportunity, FlushEvent
    {
        private long filePageId;
        private PageSwapper swapper;
        private IOException evictionException;
        private long cachePageId;
        private boolean evictionClosed;
        private long bytesWritten;
        private boolean flushDone;
        private IOException flushException;
        private int pagesFlushed;

        // --- EvictionEvent:

        @Override
        public void close()
        {
            this.evictionClosed = true;
        }

        @Override
        public void setFilePageId( long filePageId )
        {
            this.filePageId = filePageId;
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
            this.swapper = swapper;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return this;
        }

        @Override
        public void threwException( IOException exception )
        {
            this.evictionException = exception;
        }

        @Override
        public void setCachePageId( long cachePageId )
        {
            this.cachePageId = cachePageId;
        }

        // --- FlushEventOpportunity:

        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper )
        {
            return this;
        }

        // --- FlushEvent:

        @Override
        public void addBytesWritten( long bytes )
        {
            this.bytesWritten += bytes;
        }

        @Override
        public void done()
        {
            this.flushDone = true;
        }

        @Override
        public void done( IOException exception )
        {
            this.flushDone = true;
            this.flushException = exception;

        }

        @Override
        public void addPagesFlushed( int pageCount )
        {
            this.pagesFlushed += pageCount;
        }
    }

    @Test
    public void tryEvictMustReportToEvictionEvent() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 );
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusive( pageRef );
        EvictionAndFlushRecorder recorder = new EvictionAndFlushRecorder();
        assertTrue( pageList.tryEvict( pageRef, () -> recorder ) );
        assertThat( recorder.evictionClosed, is( true ) );
        assertThat( recorder.filePageId, is( 42L ) ) ;
        assertThat( recorder.swapper, sameInstance( swapper ) );
        assertThat( recorder.evictionException, is( nullValue() ) );
        assertThat( recorder.cachePageId, is( pageRef ) );
        assertThat( recorder.bytesWritten, is( 0L ) );
        assertThat( recorder.flushDone, is( false ) );
        assertThat( recorder.flushException, is( nullValue() ) );
        assertThat( recorder.pagesFlushed, is( 0 ) );
    }

    @Test
    public void tryEvictThatFlushesMustReportToEvictionAndFlushEvents() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        int filePageSize = 313;
        PageSwapper swapper = new DummyPageSwapper( "a", filePageSize );
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        EvictionAndFlushRecorder recorder = new EvictionAndFlushRecorder();
        assertTrue( pageList.tryEvict( pageRef, () -> recorder ) );
        assertThat( recorder.evictionClosed, is( true ) );
        assertThat( recorder.filePageId, is( 42L ) ) ;
        assertThat( recorder.swapper, sameInstance( swapper ) );
        assertThat( recorder.evictionException, is( nullValue() ) );
        assertThat( recorder.cachePageId, is( pageRef ) );
        assertThat( recorder.bytesWritten, is( (long) filePageSize ) );
        assertThat( recorder.flushDone, is( true ) );
        assertThat( recorder.flushException, is( nullValue() ) );
        assertThat( recorder.pagesFlushed, is( 1 ) );
    }

    @Test
    public void tryEvictThatFailsMustReportExceptionsToEvictionAndFlushEvents() throws Exception
    {
        pageList.unlockExclusive( pageRef );
        IOException ioException = new IOException();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public long write( long filePageId, long bufferAddress ) throws IOException
            {
                throw ioException;
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        EvictionAndFlushRecorder recorder = new EvictionAndFlushRecorder();
        try
        {
            pageList.tryEvict( pageRef, () -> recorder );
            fail( "tryEvict should have thrown" );
        }
        catch ( IOException e )
        {
            // Ok
        }
        assertThat( recorder.evictionClosed, is( true ) );
        assertThat( recorder.filePageId, is( 42L ) ) ;
        assertThat( recorder.swapper, sameInstance( swapper ) );
        assertThat( recorder.evictionException, sameInstance( ioException ) );
        assertThat( recorder.cachePageId, is( pageRef ) );
        assertThat( recorder.bytesWritten, is( 0L ) );
        assertThat( recorder.flushDone, is( true ) );
        assertThat( recorder.flushException, sameInstance( ioException ) );
        assertThat( recorder.pagesFlushed, is( 0 ) );
    }

    @Test
    public void tryEvictThatSucceedsMustNotInterfereWithAdjacentPages() throws Exception
    {
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 );
        int swapperId = swappers.allocate( swapper );
        long prevStamp = pageList.tryOptimisticReadLock( prevPageRef );
        long nextStamp = pageList.tryOptimisticReadLock( nextPageRef );
        doFault( swapperId, 42 );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertTrue( pageList.validateReadLock( prevPageRef, prevStamp ) );
        assertTrue( pageList.validateReadLock( nextPageRef, nextStamp ) );
    }

    @Test
    public void tryEvictThatFlushesAndSucceedsMustNotInterfereWithAdjacentPages() throws Exception
    {
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 );
        int swapperId = swappers.allocate( swapper );
        long prevStamp = pageList.tryOptimisticReadLock( prevPageRef );
        long nextStamp = pageList.tryOptimisticReadLock( nextPageRef );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modifed
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertTrue( pageList.validateReadLock( prevPageRef, prevStamp ) );
        assertTrue( pageList.validateReadLock( nextPageRef, nextStamp ) );
    }

    @Test
    public void tryEvictThatFailsMustNotInterfereWithAdjacentPages() throws Exception
    {
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public long write( long filePageId, long bufferAddress ) throws IOException
            {
                throw new IOException();
            }
        };
        int swapperId = swappers.allocate( swapper );
        long prevStamp = pageList.tryOptimisticReadLock( prevPageRef );
        long nextStamp = pageList.tryOptimisticReadLock( nextPageRef );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modifed
        assertTrue( pageList.isModified( pageRef ) );
        try
        {
            pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
            fail( "tryEvict should have thrown" );
        }
        catch ( IOException e )
        {
            // ok
        }
        assertTrue( pageList.validateReadLock( prevPageRef, prevStamp ) );
        assertTrue( pageList.validateReadLock( nextPageRef, nextStamp ) );
    }

    // todo freelist? (entries chained via file page ids in a linked list? should work as free pages are always
    // todo exclusively locked, and thus don't really need an isLoaded check)
}
