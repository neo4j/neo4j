/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import java.util.stream.Stream;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.scheduler.DaemonThreadFactory;
import org.neo4j.util.concurrent.Futures;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.io.ByteUnit.MebiByte;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class PageListTest
{
    private static final int ALIGNMENT = 8;

    private static final int[] pageIds = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final DummyPageSwapper DUMMY_SWAPPER = new DummyPageSwapper( "", UnsafeUtil.pageSize() );

    private static Stream<Arguments> argumentsProvider()
    {
        IntFunction<Arguments> toArguments = Arguments::of;
        return Arrays.stream( pageIds ).mapToObj( toArguments );
    }

    private static ExecutorService executor;
    private static MemoryAllocator mman;

    @BeforeAll
    public static void setUpStatics()
    {
        executor = Executors.newCachedThreadPool( new DaemonThreadFactory() );
        mman = MemoryAllocator.createAllocator( MebiByte.toBytes( 1 ), EmptyMemoryTracker.INSTANCE );
    }

    @AfterAll
    public static void tearDownStatics()
    {
        mman.close();
        mman = null;
        executor.shutdown();
        executor = null;
    }

    private int prevPageId;
    private int nextPageId;
    private long pageRef;
    private long prevPageRef;
    private long nextPageRef;
    private int pageSize;
    private SwapperSet swappers;
    private PageList pageList;

    private void init( int pageId )
    {
        prevPageId = pageId == 0 ? pageIds.length - 1 : (pageId - 1) % pageIds.length;
        nextPageId = (pageId + 1) % pageIds.length;
        pageSize = UnsafeUtil.pageSize();

        swappers = new SwapperSet();
        long victimPage = VictimPageReference.getVictimPage( pageSize, INSTANCE );
        pageList = new PageList( pageIds.length, pageSize, mman, swappers, victimPage, ALIGNMENT );
        pageRef = pageList.deref( pageId );
        prevPageRef = pageList.deref( prevPageId );
        nextPageRef = pageList.deref( nextPageId );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void mustExposePageCount( int pageId )
    {
        init( pageId );

        int pageCount;
        long victimPage = VictimPageReference.getVictimPage( pageSize, INSTANCE );

        pageCount = 3;
        assertThat( new PageList( pageCount, pageSize, mman, swappers, victimPage, ALIGNMENT ).getPageCount() ).isEqualTo( pageCount );

        pageCount = 42;
        assertThat( new PageList( pageCount, pageSize, mman, swappers, victimPage, ALIGNMENT ).getPageCount() ).isEqualTo( pageCount );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void mustBeAbleToReversePageRedToPageId( int pageId )
    {
        init( pageId );

        assertThat( pageList.toId( pageRef ) ).isEqualTo( pageId );
    }

    // xxx ---[ Sequence lock tests ]---

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pagesAreInitiallyExclusivelyLocked( int pageId )
    {
        init( pageId );

        assertTrue( pageList.isExclusivelyLocked( pageRef ) );
        pageList.unlockExclusive( pageRef );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedOptimisticLockMustValidate( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long stamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, stamp ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void mustNotValidateRandomStamp( int pageId )
    {
        init( pageId );

        assertFalse( pageList.validateReadLock( pageRef, 4242 ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockMustInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void takingWriteLockMustInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryWriteLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void optimisticReadLockMustNotValidateUnderWriteLock( int pageId )
    {
        init( pageId );

        pageList.tryWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockReleaseMustInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedWriteLockMustBeAvailable( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedOptimisticReadLockMustValidateAfterWriteLockRelease( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLocksMustNotBlockOtherWriteLocks( int pageId )
    {
        init( pageId );

        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
            pageList.unlockExclusive( pageRef );
            assertTrue( pageList.tryWriteLock( pageRef ) );
            assertTrue( pageList.tryWriteLock( pageRef ) );
        });
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLocksMustNotBlockOtherWriteLocksInOtherThreads( int pageId )
    {
        init( pageId );

        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
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
            Futures.getAll( futures );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unmatchedUnlockWriteLockMustThrow( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
                pageList.unlockWrite( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockCountOverflowMustThrow( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
            assertTimeoutPreemptively( ofSeconds( 5 ), () ->
            {
                pageList.unlockExclusive( pageRef );
                //noinspection InfiniteLoopStatement
                for ( ; ; )
                {
                    assertTrue( pageList.tryWriteLock( pageRef ) );
                }
            } ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustInvalidateOptimisticLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void takingExclusiveLockMustInvalidateOptimisticLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.tryExclusiveLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void optimisticReadLockMustNotValidateUnderExclusiveLock( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockReleaseMustInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedOptimisticReadLockMustValidateAfterExclusiveLockRelease( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void canTakeUncontendedExclusiveLocks( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLocksMustFailExclusiveLocks( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void concurrentWriteLocksMustFailExclusiveLocks( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustBeAvailableAfterWriteLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void cannotTakeExclusiveLockIfAlreadyTaken( int pageId )
    {
        init( pageId );

        // existing exclusive lock implied by constructor
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustBeAvailableAfterExclusiveLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustFailWriteLocks( int pageId )
    {
        init( pageId );

        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
            // exclusive lock implied by constructor
            assertFalse( pageList.tryWriteLock( pageRef ) );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unmatchedUnlockExclusiveLockMustThrow( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            pageList.unlockExclusive( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unmatchedUnlockWriteAfterTakingExclusiveLockMustThrow( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            pageList.tryExclusiveLock( pageRef );
            pageList.unlockWrite( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockMustBeAvailableAfterExclusiveLock( int pageId )
    {
        init( pageId );

        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
            pageList.unlockExclusive( pageRef );
            pageList.tryExclusiveLock( pageRef );
            pageList.unlockExclusive( pageRef );
            assertTrue( pageList.tryWriteLock( pageRef ) );
            pageList.unlockWrite( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockExclusiveMustReturnStampForOptimisticReadLock( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        long r = pageList.unlockExclusive( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockExclusiveAndTakeWriteLockMustInvalidateOptimisticReadLocks( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockExclusiveAndTakeWriteLockMustPreventExclusiveLocks( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockExclusiveAndTakeWriteLockMustAllowConcurrentWriteLocks( int pageId )
    {
        init( pageId );

        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
        {
            // exclusive lock implied by constructor
            pageList.unlockExclusiveAndTakeWriteLock( pageRef );
            assertTrue( pageList.tryWriteLock( pageRef ) );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockExclusiveAndTakeWriteLockMustBeAtomic( int pageId )
    {
        init( pageId );

        assertTimeoutPreemptively( ofSeconds( 5 ), () ->
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
            Futures.getAll( futures );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void stampFromUnlockExclusiveMustNotBeValidIfThereAreWriteLocks( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        long r = pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertFalse( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedFlushLockMustBeAvailable( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushLockMustNotInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushLockMustNotFailWriteLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushLockMustFailExclusiveLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void cannotTakeFlushLockIfAlreadyTaken( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
        assertFalse( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockMustNotFailFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustFailFlushLock( int pageId )
    {
        init( pageId );

        // exclusively locked from constructor
        assertFalse( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockExclusiveAndTakeWriteLockMustNotFailFlushLock( int pageId )
    {
        init( pageId );

        // exclusively locked from constructor
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushUnlockMustNotInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void optimisticReadLockMustValidateUnderFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushLockReleaseMustNotInvalidateOptimisticReadLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        long r = pageList.tryOptimisticReadLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unmatchedUnlockFlushMustThrow( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
            pageList.unlockFlush( pageRef, pageList.tryOptimisticReadLock( pageRef ), true ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedOptimisticReadLockMustBeAvailableAfterFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        long r = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedWriteLockMustBeAvailableAfterFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryWriteLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedExclusiveLockMustBeAvailableAfterFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedFlushLockMustBeAvailableAfterWriteLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryWriteLock( pageRef );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedFlushLockMustBeAvailableAfterExclusiveLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryExclusiveLock( pageRef );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void uncontendedFlushLockMustBeAvailableAfterFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.tryFlushLock( pageRef ) != 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void stampFromUnlockExclusiveMustBeValidUnderFlushLock( int pageId )
    {
        init( pageId );

        // exclusively locked from constructor
        long r = pageList.unlockExclusive( pageRef );
        pageList.tryFlushLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, r ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentWriteLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentExclusiveLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void optimisticReadLockMustNotGetInterferenceFromAdjacentExclusiveAndWriteLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockMustNotGetInterferenceFromAdjacentExclusiveLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushLockMustNotGetInterferenceFromAdjacentExclusiveLocks( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        long s;
        assertTrue( pageList.tryExclusiveLock( prevPageRef ) );
        assertTrue( pageList.tryExclusiveLock( nextPageRef ) );
        assertTrue( (s = pageList.tryFlushLock( pageRef )) != 0 );
        pageList.unlockFlush( pageRef, s, true );
        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( nextPageRef );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void flushLockMustNotGetInterferenceFromAdjacentFlushLocks( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        long ps;
        long ns;
        long s;
        assertTrue( (ps = pageList.tryFlushLock( prevPageRef )) != 0 );
        assertTrue( (ns = pageList.tryFlushLock( nextPageRef )) != 0 );
        assertTrue( (s = pageList.tryFlushLock( pageRef )) != 0 );
        pageList.unlockFlush( pageRef, s, true );
        pageList.unlockFlush( prevPageRef, ps, true );
        pageList.unlockFlush( nextPageRef, ns, true );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustNotGetInterferenceFromAdjacentExclusiveLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustNotGetInterferenceFromAdjacentWriteLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustNotGetInterferenceFromAdjacentExclusiveAndWriteLocks( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustNotGetInterferenceFromAdjacentFlushLocks( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        long ps;
        long ns;
        assertTrue( (ps = pageList.tryFlushLock( prevPageRef )) != 0 );
        assertTrue( (ns = pageList.tryFlushLock( nextPageRef )) != 0);
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockExclusive( pageRef );
        pageList.unlockFlush( prevPageRef, ps, true );
        pageList.unlockFlush( nextPageRef, ns, true );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void takingWriteLockMustRaiseModifiedFlag( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void turningExclusiveLockIntoWriteLockMustRaiseModifiedFlag( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertFalse( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        assertFalse( pageList.isModified( pageRef ) );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustLowerModifiedFlagIfSuccessful( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void loweredModifiedFlagMustRemainLoweredAfterReleasingFlushLock( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustNotLowerModifiedFlagIfUnsuccessful( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, false );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockWasWithinFlushFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedTakingFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockWrite( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedReleasingFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long s = pageList.tryFlushLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustNotLowerModifiedFlagIfWriteLockOverlappedFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long s = pageList.tryFlushLock( pageRef );
        pageList.unlockFlush( pageRef, s, true );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void releasingFlushLockMustNotInterfereWithAdjacentModifiedFlags( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void writeLockMustNotInterfereWithAdjacentModifiedFlags( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        pageList.unlockWrite( pageRef );
        assertFalse( pageList.isModified( prevPageRef ) );
        assertTrue( pageList.isModified( pageRef ) );
        assertFalse( pageList.isModified( nextPageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void disallowUnlockedPageToExplicitlyLowerModifiedFlag( int pageId )
    {
        init( pageId );

        assertThrows( IllegalStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void disallowReadLockedPageToExplicitlyLowerModifiedFlag( int pageId )
    {
        init( pageId );

        assertThrows( IllegalStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            pageList.tryOptimisticReadLock( pageRef );
            pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void disallowFlushLockedPageToExplicitlyLowerModifiedFlag( int pageId )
    {
        init( pageId );

        assertThrows( IllegalStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            assertThat( pageList.tryFlushLock( pageRef ) ).isNotEqualTo( 0L );
            pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void disallowWriteLockedPageToExplicitlyLowerModifiedFlag( int pageId )
    {
        init( pageId );

        assertThrows( IllegalStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            assertTrue( pageList.tryWriteLock( pageRef ) );
            pageList.explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void allowExclusiveLockedPageToExplicitlyLowerModifiedFlag( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustTakeFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) );
        long flushStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( flushStamp ).isNotEqualTo( 0L );
        assertThat( pageList.tryFlushLock( pageRef ) ).isEqualTo( 0L );
        pageList.unlockFlush( pageRef, flushStamp, true );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustThrowIfNotWriteLocked( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        } );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustThrowIfNotWriteLockedButExclusiveLocked( int pageId )
    {
        init( pageId );

        assertThrows( IllegalMonitorStateException.class, () ->
            // exclusive lock implied by constructor
            pageList.unlockWriteAndTryTakeFlushLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustFailIfFlushLockIsAlreadyTaken( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.tryFlushLock( pageRef );
        assertThat( stamp ).isNotEqualTo( 0L );
        long secondStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( secondStamp ).isEqualTo( 0L );
        pageList.unlockFlush( pageRef, stamp, true );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustReleaseWriteLockEvenIfFlushLockFails( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long flushStamp = pageList.tryFlushLock( pageRef );
        assertThat( flushStamp ).isNotEqualTo( 0L );
        assertThat( pageList.unlockWriteAndTryTakeFlushLock( pageRef ) ).isEqualTo( 0L );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustReleaseWriteLockWhenFlushLockSucceeds( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertThat( pageList.unlockWriteAndTryTakeFlushLock( pageRef ) ).isNotEqualTo( 0L );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTrueTakeFlushLockMustRaiseModifiedFlag( int pageId )
    {
        init( pageId );

        assertFalse( pageList.isModified( pageRef ) );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        assertThat( pageList.unlockWriteAndTryTakeFlushLock( pageRef ) ).isNotEqualTo( 0L );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushMustLowerModifiedFlagIfSuccessful( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, true );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushMustNotLowerModifiedFlagIfFailed( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, false );
        assertTrue( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockWithOverlappingWriterAndThenUnlockFlushMustNotLowerModifiedFlag( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertTrue( pageList.tryWriteLock( pageRef ) ); // two write locks, now
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef ); // one flush, one write lock
        assertThat( stamp ).isNotEqualTo( 0L );
        pageList.unlockWrite( pageRef ); // one flush, zero write locks
        assertTrue( pageList.isModified( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, true ); // flush is successful, but had one overlapping writer
        assertTrue( pageList.isModified( pageRef ) ); // so it's still modified
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushWithOverlappingWriterMustNotLowerModifiedFlag( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef ); // one flush lock
        assertThat( stamp ).isNotEqualTo( 0L );
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) ); // one flush and one write lock
        pageList.unlockFlush( pageRef, stamp, true ); // flush is successful, but have one overlapping writer
        pageList.unlockWrite( pageRef ); // no more locks, but a writer started within flush section ...
        assertTrue( pageList.isModified( pageRef ) ); // ... and overlapped unlockFlush, so it's still modified
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockAndThenUnlockFlushWithContainedWriterMustNotLowerModifiedFlag( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef ); // one flush lock
        assertThat( stamp ).isNotEqualTo( 0L );
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryWriteLock( pageRef ) ); // one flush and one write lock
        pageList.unlockWrite( pageRef ); // back to one flush lock
        pageList.unlockFlush( pageRef, stamp, true ); // flush is successful, but had one overlapping writer
        assertTrue( pageList.isModified( pageRef ) ); // so it's still modified
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockThatSucceedsMustPreventOverlappingExclusiveLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, true );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockThatFailsMustPreventOverlappingExclusiveLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertFalse( pageList.tryExclusiveLock( pageRef ) );
        pageList.unlockFlush( pageRef, stamp, false );
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockThatSucceedsMustPreventOverlappingFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( pageList.tryFlushLock( pageRef ) ).isEqualTo( 0L );
        pageList.unlockFlush( pageRef, stamp, true );
        assertThat( pageList.tryFlushLock( pageRef ) ).isNotEqualTo( 0L );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockThatFailsMustPreventOverlappingFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long stamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertThat( pageList.tryFlushLock( pageRef ) ).isEqualTo( 0L );
        pageList.unlockFlush( pageRef, stamp, false );
        assertThat( pageList.tryFlushLock( pageRef ) ).isNotEqualTo( 0L );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustNotInvalidateReadersOverlappingWithFlushLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long flushStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
        pageList.unlockFlush( pageRef, flushStamp, true );
        assertTrue( pageList.validateReadLock( pageRef, readStamp ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unlockWriteAndTryTakeFlushLockMustInvalidateReadersOverlappingWithWriteLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        long readStamp = pageList.tryOptimisticReadLock( pageRef );
        long flushStamp = pageList.unlockWriteAndTryTakeFlushLock( pageRef );
        assertFalse( pageList.validateReadLock( pageRef, readStamp ) );
        pageList.unlockFlush( pageRef, flushStamp, true );
        assertFalse( pageList.validateReadLock( pageRef, readStamp ) );
    }

    // xxx ---[ Page state tests ]---

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void mustExposeCachePageSize( int pageId )
    {
        init( pageId );

        PageList list = new PageList( 0, 42, mman, swappers,
                VictimPageReference.getVictimPage( 42, INSTANCE ), ALIGNMENT );
        assertThat( list.getCachePageSize() ).isEqualTo( 42 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void addressesMustBeZeroBeforeInitialisation( int pageId )
    {
        init( pageId );

        assertThat( pageList.getAddress( pageRef ) ).isEqualTo( 0L );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void initialisingBufferMustConsumeMemoryFromMemoryManager( int pageId )
    {
        init( pageId );

        long initialUsedMemory = mman.usedMemory();
        pageList.initBuffer( pageRef );
        long resultingUsedMemory = mman.usedMemory();
        int allocatedMemory = (int) (resultingUsedMemory - initialUsedMemory);
        assertThat( allocatedMemory ).isGreaterThanOrEqualTo( pageSize );
        assertThat( allocatedMemory ).isLessThanOrEqualTo( pageSize + ALIGNMENT );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void addressMustNotBeZeroAfterInitialisation( int pageId )
    {
        init( pageId );

        pageList.initBuffer( pageRef );
        assertThat( pageList.getAddress( pageRef ) ).isNotEqualTo( 0L );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageListMustBeCopyableViaConstructor( int pageId )
    {
        init( pageId );

        assertThat( pageList.getAddress( pageRef ) ).isEqualTo( 0L );
        PageList pl = new PageList( pageList );
        assertThat( pl.getAddress( pageRef ) ).isEqualTo( 0L );

        pageList.initBuffer( pageRef );
        assertThat( pageList.getAddress( pageRef ) ).isNotEqualTo( 0L );
        assertThat( pl.getAddress( pageRef ) ).isNotEqualTo( 0L );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void usageCounterMustBeZeroByDefault( int pageId )
    {
        init( pageId );

        assertTrue( pageList.decrementUsage( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void usageCounterMustGoUpToFour( int pageId )
    {
        init( pageId );

        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
        assertTrue( pageList.decrementUsage( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void usageCounterMustTruncateAtFour( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void incrementingUsageCounterMustNotInterfereWithAdjacentUsageCounters( int pageId )
    {
        init( pageId );

        pageList.incrementUsage( pageRef );
        pageList.incrementUsage( pageRef );
        assertTrue( pageList.decrementUsage( prevPageRef ) );
        assertTrue( pageList.decrementUsage( nextPageRef ) );
        assertFalse( pageList.decrementUsage( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void decrementingUsageCounterMustNotInterfereWithAdjacentUsageCounters( int pageId )
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void filePageIdIsUnboundByDefault( int pageId )
    {
        init( pageId );

        assertThat( pageList.getFilePageId( pageRef ) ).isEqualTo( PageCursor.UNBOUND_PAGE_ID );
    }

    // xxx ---[ Page fault tests ]---

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustThrowWithoutExclusiveLock( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.initBuffer( pageRef );
        assertThrows( IllegalStateException.class, () ->
            pageList.fault( pageRef, DUMMY_SWAPPER, (short) 0, 0, PageFaultEvent.NULL ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustThrowIfSwapperIsNull( int pageId )
    {
        init( pageId );

        // exclusive lock implied by the constructor
        pageList.initBuffer( pageRef );
        assertThrows( IllegalArgumentException.class, () ->
            pageList.fault( pageRef, null, (short) 0, 0, PageFaultEvent.NULL ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustThrowIfFilePageIdIsUnbound( int pageId )
    {
        init( pageId );

        // exclusively locked from constructor
        pageList.initBuffer( pageRef );
        assertThrows( IllegalStateException.class, () ->
            pageList.fault( pageRef, DUMMY_SWAPPER, (short) 0, PageCursor.UNBOUND_PAGE_ID, PageFaultEvent.NULL ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustReadIntoPage( int pageId ) throws Exception
    {
        init( pageId );

        byte pageByteContents = (byte) 0xF7;
        short swapperId = 1;
        long filePageId = 2;
        PageSwapper swapper = new DummyPageSwapper( "some file", pageSize )
        {
            @Override
            public long read( long fpId, long bufferAddress ) throws IOException
            {
                if ( fpId == filePageId )
                {
                    UnsafeUtil.setMemory( bufferAddress, filePageSize, pageByteContents );
                    return filePageSize;
                }
                throw new IOException( "Did not expect this file page id = " + fpId );
            }
        };
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, swapper, swapperId, filePageId, PageFaultEvent.NULL );

        long address = pageList.getAddress( pageRef );
        assertThat( address ).isNotEqualTo( 0L );
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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageMustBeLoadedAndBoundAfterFault( int pageId ) throws Exception
    {
        init( pageId );

        // exclusive lock implied by constructor
        int swapperId = 1;
        long filePageId = 42;
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
        assertThat( pageList.getFilePageId( pageRef ) ).isEqualTo( filePageId );
        assertThat( pageList.getSwapperId( pageRef ) ).isEqualTo( swapperId );
        assertTrue( pageList.isLoaded( pageRef ) );
        assertTrue( pageList.isBoundTo( pageRef, swapperId, filePageId ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageWith5BytesFilePageIdMustBeLoadedAndBoundAfterFault( int pageId ) throws Exception
    {
        init( pageId );

        // exclusive lock implied by constructor
        int swapperId = 12;
        long filePageId = Integer.MAX_VALUE + 1L;
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
        assertThat( pageList.getFilePageId( pageRef ) ).isEqualTo( filePageId );
        assertThat( pageList.getSwapperId( pageRef ) ).isEqualTo( swapperId );
        assertTrue( pageList.isLoaded( pageRef ) );
        assertTrue( pageList.isBoundTo( pageRef, swapperId, filePageId ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageMustBeLoadedAndNotBoundIfFaultThrows( int pageId )
    {
        init( pageId );

        // exclusive lock implied by constructor
        PageSwapper swapper = new DummyPageSwapper( "file", pageSize )
        {
            @Override
            public long read( long filePageId, long bufferAddress ) throws IOException
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
            fail();
        }
        catch ( IOException e )
        {
            assertThat( e.getMessage() ).isEqualTo( "boo" );
        }
        assertThat( pageList.getFilePageId( pageRef ) ).isEqualTo( filePageId );
        assertThat( pageList.getSwapperId( pageRef ) ).isEqualTo( 0 ); // 0 means not bound
        assertTrue( pageList.isLoaded( pageRef ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId, filePageId ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustThrowIfPageIsAlreadyBound( int pageId ) throws Exception
    {
        init( pageId );

        // exclusive lock implied by constructor
        short swapperId = 1;
        long filePageId = 42;
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );

        assertThrows( IllegalStateException.class, () ->
            pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustThrowIfPageIsLoadedButNotBound( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        short swapperId = 1;
        long filePageId = 42;
        doFailedFault( swapperId, filePageId );

        // After the failed page fault, the page is loaded but not bound.
        // We still can't fault into a loaded page, though.
        assertThrows( IllegalStateException.class, () ->
            pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL ) );
    }

    private void doFailedFault( short swapperId, long filePageId )
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        DummyPageSwapper swapper = new DummyPageSwapper( "", pageSize )
        {
            @Override
            public long read( long filePageId, long bufferAddress ) throws IOException
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
            assertThat( e.getMessage() ).isEqualTo( "boom" );
        }
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustPopulatePageFaultEvent( int pageId ) throws Exception
    {
        init( pageId );

        // exclusive lock implied by constructor
        short swapperId = 1;
        long filePageId = 42;
        pageList.initBuffer( pageRef );
        DummyPageSwapper swapper = new DummyPageSwapper( "", pageSize )
        {
            @Override
            public long read( long filePageId, long bufferAddress )
            {
                return 333;
            }
        };
        StubPageFaultEvent event = new StubPageFaultEvent();
        pageList.fault( pageRef, swapper, swapperId, filePageId, event );
        assertThat( event.bytesRead ).isEqualTo( 333L );
        assertThat( event.cachePageId ).isEqualTo( pageId );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unboundPageMustNotBeLoaded( int pageId )
    {
        init( pageId );

        assertFalse( pageList.isLoaded( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void unboundPageMustNotBeBoundToAnything( int pageId )
    {
        init( pageId );

        assertFalse( pageList.isBoundTo( pageRef, (short) 0, 0 ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void boundPagesAreNotBoundToOtherPagesWithSameSwapper( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        long filePageId = 42;
        short swapperId = 2;
        doFault( swapperId, filePageId );

        assertTrue( pageList.isBoundTo( pageRef, swapperId, filePageId ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId, filePageId + 1 ) );
        assertFalse( pageList.isBoundTo( pageRef, swapperId, filePageId - 1 ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void boundPagesAreNotBoundToOtherPagesWithSameFilePageId( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        short swapperId = 2;
        doFault( swapperId, 42 );

        assertTrue( pageList.isBoundTo( pageRef, swapperId, 42 ) );
        assertFalse( pageList.isBoundTo( pageRef, (short) (swapperId + 1), 42 ) );
        assertFalse( pageList.isBoundTo( pageRef, (short) (swapperId - 1), 42 ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void faultMustNotInterfereWithAdjacentPages( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        doFault( (short) 1, 42 );

        assertFalse( pageList.isLoaded( prevPageRef ) );
        assertFalse( pageList.isLoaded( nextPageRef ) );
        assertFalse( pageList.isBoundTo( prevPageRef, (short) 1, 42 ) );
        assertFalse( pageList.isBoundTo( prevPageRef, (short) 0, 0 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, (short) 1, 42 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, (short) 0, 0 ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void failedFaultMustNotInterfereWithAdjacentPages( int pageId )
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        doFailedFault( (short) 1, 42 );

        assertFalse( pageList.isLoaded( prevPageRef ) );
        assertFalse( pageList.isLoaded( nextPageRef ) );
        assertFalse( pageList.isBoundTo( prevPageRef, (short) 1, 42 ) );
        assertFalse( pageList.isBoundTo( prevPageRef, (short) 0, 0 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, (short) 1, 42 ) );
        assertFalse( pageList.isBoundTo( nextPageRef, (short) 0, 0 ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void exclusiveLockMustStillBeHeldAfterFault( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        doFault( (short) 1, 42 );
        pageList.unlockExclusive( pageRef ); // will throw if lock is not held
    }

    // xxx ---[ Page eviction tests ]---

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustFailIfPageIsAlreadyExclusivelyLocked( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        assertFalse( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictThatFailsOnExclusiveLockMustNotUndoSaidLock( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page is now loaded
        // pages are delivered from the fault routine with the exclusive lock already held!
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL ); // This attempt will fail
        assertTrue( pageList.isExclusivelyLocked( pageRef ) ); // page should still have its lock
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustFailIfPageIsNotLoaded( int pageId ) throws Exception
    {
        init( pageId );

        assertFalse( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustWhenPageIsNotLoadedMustNotLeavePageLocked( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL ); // This attempt fails
        assertFalse( pageList.isExclusivelyLocked( pageRef ) ); // Page should not be left in locked state
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustLeavePageExclusivelyLockedOnSuccess( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        pageList.unlockExclusive( pageRef ); // will throw if lock is not held
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageMustNotBeLoadedAfterSuccessfulEviction( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.isLoaded( pageRef ) );
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
        assertFalse( pageList.isLoaded( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageMustNotBeBoundAfterSuccessfulEviction( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 ); // page now bound & exclusively locked
        pageList.unlockExclusive( pageRef ); // no longer exclusively locked; can now be evicted
        assertTrue( pageList.isBoundTo( pageRef, (short) 1, 42 ) );
        assertTrue( pageList.isLoaded( pageRef ) );
        assertThat( pageList.getSwapperId( pageRef ) ).isEqualTo( 1 );
        pageList.tryEvict( pageRef, EvictionRunEvent.NULL );
        assertFalse( pageList.isBoundTo( pageRef, (short) 1, 42 ) );
        assertFalse( pageList.isLoaded( pageRef ) );
        assertThat( pageList.getSwapperId( pageRef ) ).isEqualTo( 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void pageMustNotBeModifiedAfterSuccessfulEviction( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        int swapperId = swappers.allocate( DUMMY_SWAPPER );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertFalse( pageList.isModified( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustFlushPageIfModified( int pageId ) throws Exception
    {
        init( pageId );

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
        assertThat( writtenFilePageId.get() ).isEqualTo( 42L );
        assertThat( writtenBufferAddress.get() ).isEqualTo( pageList.getAddress( pageRef ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustNotFlushPageIfNotModified( int pageId ) throws Exception
    {
        init( pageId );

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
        assertThat( writes.get() ).isEqualTo( 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustNotifySwapperOnSuccess( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public void evicted( long filePageId )
            {
                evictionNotified.set( true );
                assertThat( filePageId ).isEqualTo( 42L );
            }
        };
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusive( pageRef );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertTrue( evictionNotified.get() );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustNotifySwapperOnSuccessEvenWhenFlushing( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        AtomicBoolean evictionNotified = new AtomicBoolean();
        PageSwapper swapper = new DummyPageSwapper( "a", 313 )
        {
            @Override
            public void evicted( long filePageId )
            {
                evictionNotified.set( true );
                assertThat( filePageId ).isEqualTo( 42L );
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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustLeavePageUnlockedAndLoadedAndBoundAndModifiedIfFlushThrows( int pageId ) throws Exception
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustNotNotifySwapperOfEvictionIfFlushThrows( int pageId ) throws Exception
    {
        init( pageId );

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
        private int pagesMerged;

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
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper, int pagesToFlush, int mergedPages )
        {
            return this;
        }

        @Override
        public void startFlush( int[][] translationTable )
        {

        }

        @Override
        public ChunkEvent startChunk( int[] chunk )
        {
            return ChunkEvent.NULL;
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

        @Override
        public void addPagesMerged( int pagesMerged )
        {
            this.pagesMerged += pagesMerged;
        }
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictMustReportToEvictionEvent( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( pageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 );
        int swapperId = swappers.allocate( swapper );
        doFault( swapperId, 42 );
        pageList.unlockExclusive( pageRef );
        EvictionAndFlushRecorder recorder = new EvictionAndFlushRecorder();
        assertTrue( pageList.tryEvict( pageRef, () -> recorder ) );
        assertThat( recorder.evictionClosed ).isEqualTo( true );
        assertThat( recorder.filePageId ).isEqualTo( 42L );
        assertThat( recorder.swapper ).isSameAs( swapper );
        assertThat( recorder.evictionException ).isNull();
        assertThat( recorder.cachePageId ).isEqualTo( pageRef );
        assertThat( recorder.bytesWritten ).isEqualTo( 0L );
        assertThat( recorder.flushDone ).isEqualTo( false );
        assertThat( recorder.flushException ).isNull();
        assertThat( recorder.pagesFlushed ).isEqualTo( 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictThatFlushesMustReportToEvictionAndFlushEvents( int pageId ) throws Exception
    {
        init( pageId );

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
        assertThat( recorder.evictionClosed ).isEqualTo( true );
        assertThat( recorder.filePageId ).isEqualTo( 42L );
        assertThat( recorder.swapper ).isSameAs( swapper );
        assertThat( recorder.evictionException ).isNull();
        assertThat( recorder.cachePageId ).isEqualTo( pageRef );
        assertThat( recorder.bytesWritten ).isEqualTo( filePageSize );
        assertThat( recorder.flushDone ).isEqualTo( true );
        assertThat( recorder.flushException ).isNull();
        assertThat( recorder.pagesFlushed ).isEqualTo( 1 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictThatFailsMustReportExceptionsToEvictionAndFlushEvents( int pageId ) throws Exception
    {
        init( pageId );

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
        assertThat( recorder.evictionClosed ).isEqualTo( true );
        assertThat( recorder.filePageId ).isEqualTo( 42L );
        assertThat( recorder.swapper ).isSameAs( swapper );
        assertThat( recorder.evictionException ).isSameAs( ioException );
        assertThat( recorder.cachePageId ).isEqualTo( pageRef );
        assertThat( recorder.bytesWritten ).isEqualTo( 0L );
        assertThat( recorder.flushDone ).isEqualTo( true );
        assertThat( recorder.flushException ).isSameAs( ioException );
        assertThat( recorder.pagesFlushed ).isEqualTo( 0 );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictThatSucceedsMustNotInterfereWithAdjacentPages( int pageId ) throws Exception
    {
        init( pageId );

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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictThatFlushesAndSucceedsMustNotInterfereWithAdjacentPages( int pageId ) throws Exception
    {
        init( pageId );

        pageList.unlockExclusive( prevPageRef );
        pageList.unlockExclusive( pageRef );
        pageList.unlockExclusive( nextPageRef );
        PageSwapper swapper = new DummyPageSwapper( "a", 313 );
        int swapperId = swappers.allocate( swapper );
        long prevStamp = pageList.tryOptimisticReadLock( prevPageRef );
        long nextStamp = pageList.tryOptimisticReadLock( nextPageRef );
        doFault( swapperId, 42 );
        pageList.unlockExclusiveAndTakeWriteLock( pageRef );
        pageList.unlockWrite( pageRef ); // page is now modified
        assertTrue( pageList.isModified( pageRef ) );
        assertTrue( pageList.tryEvict( pageRef, EvictionRunEvent.NULL ) );
        assertTrue( pageList.validateReadLock( prevPageRef, prevStamp ) );
        assertTrue( pageList.validateReadLock( nextPageRef, nextStamp ) );
    }

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void tryEvictThatFailsMustNotInterfereWithAdjacentPages( int pageId ) throws Exception
    {
        init( pageId );

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
        pageList.unlockWrite( pageRef ); // page is now modified
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

    @ParameterizedTest( name = "pageRef = {0}" )
    @MethodSource( "argumentsProvider" )
    public void failToSetHigherThenSupportedFilePageIdOnFault( int pageId )
    {
        init( pageId );

        assertThrows( IllegalArgumentException.class, () ->
        {
            pageList.unlockExclusive( pageRef );
            short swapperId = 2;
            doFault( swapperId, Long.MAX_VALUE );
        } );
    }

    private void doFault( int swapperId, long filePageId ) throws IOException
    {
        assertTrue( pageList.tryExclusiveLock( pageRef ) );
        pageList.initBuffer( pageRef );
        pageList.fault( pageRef, DUMMY_SWAPPER, swapperId, filePageId, PageFaultEvent.NULL );
    }

    // todo freelist? (entries chained via file page ids in a linked list? should work as free pages are always
    // todo exclusively locked, and thus don't really need an isLoaded check)
}
