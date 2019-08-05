/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.test.OtherThreadExecutor.command;

@PageCacheExtension
class FreeIdScannerTest
{
    private static final int IDS_PER_ENTRY = 256;

    @Inject
    PageCache pageCache;

    @Inject
    TestDirectory directory;

    private IdRangeLayout layout;
    private GBPTree<IdRangeKey, IdRange> tree;

    // instantiated in tests
    private AtomicBoolean atLeastOneFreeId;
    private ConcurrentLongQueue cache;
    private FoundIdMarker reuser;

    @BeforeEach
    void beforeEach()
    {
        this.layout = new IdRangeLayout( IDS_PER_ENTRY );
        this.tree = new GBPTreeBuilder<>( pageCache, directory.file( "file.id" ), layout ).build();
    }

    @AfterEach
    void afterEach() throws Exception
    {
        tree.close();
    }

    FreeIdScanner scanner( int idsPerEntry, int cacheSize, long generation )
    {
        return scanner( idsPerEntry, new SpmcLongQueue( cacheSize ), generation );
    }

    FreeIdScanner scanner( int idsPerEntry, ConcurrentLongQueue cache, long generation )
    {
        this.cache = cache;
        this.reuser = new FoundIdMarker();
        this.atLeastOneFreeId = new AtomicBoolean();
        return new FreeIdScanner( idsPerEntry, tree, cache, atLeastOneFreeId, reuser, generation, false );
    }

    @Test
    void shouldNotThinkItsWorthScanningIfNoFreedIdsAndNoOngoingScan()
    {
        // given
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 8, 1 );

        // then
        assertFalse( scanner.tryLoadFreeIdsIntoCache() );
    }

    @Test
    void shouldThinkItsWorthScanningIfAlreadyHasOngoingScan()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 256, generation );

        forEachId( generation, range( 0, 300 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();
        assertTrue( cache.size() > 0 );
        // take at least one so that scanner wants to load more from the ongoing scan
        assertEquals( 0, cache.takeOrDefault( -1 ) );

        // then
        assertTrue( scanner.tryLoadFreeIdsIntoCache() );
    }

    @Test
    void shouldFindMarkAndCacheOneIdFromAnEntry()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 8, generation );

        forEachId( generation, range( 0, 1 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 0, 1 ) );
    }

    @Test
    void shouldFindMarkAndCacheMultipleIdsFromAnEntry()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 8, generation );
        Range[] ranges = {range( 0, 2 ), range( 7, 8 )}; // 0, 1, 2, 7

        forEachId( generation, ranges ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( ranges );
    }

    @Test
    void shouldFindMarkAndCacheMultipleIdsFromMultipleEntries()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 16, generation );
        Range[] ranges = {range( 0, 2 ), range( 167, 175 )}; // 0, 1, 2 in one entry and 67,68,69,70,71,72,73,74 in another entry

        forEachId( generation, ranges ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( ranges );
    }

    @Test
    void shouldNotFindUsedIds()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 16, generation );

        forEachId( generation, range( 0, 5 ) ).accept( ( marker1, id1 ) ->
        {
            marker1.markDeleted( id1 );
            marker1.markFree( id1 );
        } );
        forEachId( generation, range( 1, 3 ) ).accept( ( marker, id ) ->
        {
            marker.markReserved( id );
            marker.markUsed( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 0, 1 ), range( 3, 5 ) );
    }

    @Test
    void shouldNotFindUnusedButNonReusableIds()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 16, generation );

        forEachId( generation, range( 0, 5 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );
        forEachId( generation, range( 1, 3 ) ).accept( IdRangeMarker::markReserved );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 0, 1 ), range( 3, 5 ) );
    }

    @Test
    void shouldOnlyScanUntilCacheIsFull()
    {
        // given
        int generation = 1;
        ConcurrentLongQueue cache = mock( ConcurrentLongQueue.class );
        when( cache.capacity() ).thenReturn( 8 );
        when( cache.size() ).thenReturn( 3 );
        when( cache.offer( anyLong() ) ).thenReturn( true );
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, cache, generation );

        forEachId( generation, range( 0, 8 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );
        // cache has capacity of 8 and there are 8 free ids, however cache isn't completely empty

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then verify that the cache only got offered 5 ids (capacity:8 - size:3)
        verify( cache, times( 5 ) ).offer( anyLong() );
    }

    @Test
    void shouldContinuePausedScan()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 8, generation );

        forEachId( generation, range( 0, 8 ), range( 64, 72 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 0, 8 ) );

        // and further when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 64, 72 ) );
    }

    @Test
    void shouldContinueFromAPausedEntryIfScanWasPausedInTheMiddleOfIt()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 8, generation );

        forEachId( generation, range( 0, 4 ), range( 64, 72 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 0, 4 ), range( 64, 68 ) );

        // and further when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 68, 72 ) );
    }

    @Test
    void shouldOnlyLetOneThreadAtATimePerformAScan() throws Exception
    {
        // given
        int generation = 1;
        Barrier.Control barrier = new Barrier.Control();
        ConcurrentLongQueue cache = mock( ConcurrentLongQueue.class );
        when( cache.capacity() ).thenReturn( 8 );
        when( cache.offer( anyLong() ) ).thenAnswer( invocationOnMock ->
        {
            barrier.reached();
            return true;
        } );
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, cache, generation );

        forEachId( generation, range( 0, 2 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> scanFuture = executorService.submit( scanner::tryLoadFreeIdsIntoCache );
        barrier.await();
        // now it's stuck in trying to offer to the cache

        // then a scan call from another thread should complete but not do anything
        verify( cache, times( 1 ) ).offer( anyLong() ); // <-- the 1 call is from the call which makes the other thread stuck above
        scanner.tryLoadFreeIdsIntoCache();
        verify( cache, times( 1 ) ).offer( anyLong() );

        // clean up
        barrier.release();
        scanFuture.get();
        executorService.shutdown();

        // and then
        verify( cache ).offer( 0 );
        verify( cache ).offer( 1 );
    }

    @Test
    void shouldDisregardReusabilityMarksOnEntriesWithOldGeneration()
    {
        // given
        int oldGeneration = 1;
        int currentGeneration = 2;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 32, currentGeneration );
        forEachId( oldGeneration, range( 0, 8 ), range( 64, 72 ) ).accept( IdRangeMarker::markDeleted );
        // explicitly set to true because the usage pattern in this test is not quite
        atLeastOneFreeId.set( true );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( 0, 8 ), range( 64, 72 ) );
    }

    @Test
    void shouldMarkFoundIdsAsNonReusable()
    {
        // given
        long generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 32, generation );

        forEachId( generation, range( 0, 5 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertArrayEquals( new long[]{0, 1, 2, 3, 4}, reuser.reservedIds.toArray() );
    }

    @Test
    void shouldClearCache()
    {
        // given
        long generation = 1;
        ConcurrentLongQueue cache = new SpmcLongQueue( 32 );
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, cache, generation );
        forEachId( generation, range( 0, 5 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );
        scanner.tryLoadFreeIdsIntoCache();

        // when
        long cacheSizeBeforeClear = cache.size();
        scanner.clearCache();

        // then
        assertEquals( 5, cacheSizeBeforeClear );
        assertEquals( 0, cache.size() );
        assertEquals( LongLists.immutable.of( 0, 1, 2, 3, 4 ), reuser.freedIds );
    }

    @Test
    void shouldNotScanWhenConcurrentClear() throws ExecutionException, InterruptedException
    {
        // given
        long generation = 1;
        ConcurrentLongQueue cache = new SpmcLongQueue( 32 );
        Barrier.Control barrier = new Barrier.Control();
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, new ControlledConcurrentLongQueue( cache, QueueMethodControl.TAKE, barrier ), generation );
        forEachId( generation, range( 0, 5 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        try ( OtherThreadExecutor<Void> clearThread = new OtherThreadExecutor<>( "clear", null ) )
        {
            // Wait for the clear call
            Future<Object> clear = clearThread.executeDontWait( command( scanner::clearCache ) );
            barrier.awaitUninterruptibly();

            // Attempt trigger a scan
            scanner.tryLoadFreeIdsIntoCache();

            // Let clear finish
            barrier.release();
            clear.get();
        }

        // then
        assertEquals( 0, cache.size() );
    }

    @Test
    void shouldLetClearCacheWaitForConcurrentScan() throws ExecutionException, InterruptedException, TimeoutException
    {
        // given
        long generation = 1;
        ConcurrentLongQueue cache = new SpmcLongQueue( 32 );
        Barrier.Control barrier = new Barrier.Control();
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, new ControlledConcurrentLongQueue( cache, QueueMethodControl.OFFER, barrier ), generation );
        forEachId( generation, range( 0, 1 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );

        // when
        try ( OtherThreadExecutor<Void> scanThread = new OtherThreadExecutor<>( "scan", null );
              OtherThreadExecutor<Void> clearThread = new OtherThreadExecutor<>( "clear", null ) )
        {
            // Wait for the offer call
            Future<Object> scan = scanThread.executeDontWait( command( scanner::tryLoadFreeIdsIntoCache ) );
            barrier.awaitUninterruptibly();

            // Make sure clear waits for the scan call
            Future<Object> clear = clearThread.executeDontWait( command( scanner::clearCache ) );
            clearThread.waitUntilWaiting();

            // Let the threads finish
            barrier.release();
            scan.get();
            clear.get();
        }

        // then
        assertEquals( 0, cache.size() );
    }

    @Test
    void shouldNotSkipRangeThatIsFoundButNoCacheSpaceLeft()
    {
        // given
        long generation = 1;
        int cacheSize = IDS_PER_ENTRY / 2;
        int halfCacheSize = cacheSize / 2;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, cacheSize, generation );
        forEachId( generation, range( 0, IDS_PER_ENTRY * 2 + 4 ) ).accept( ( marker, id ) ->
        {
            marker.markDeleted( id );
            marker.markFree( id );
        } );
        scanner.tryLoadFreeIdsIntoCache();
        assertCacheHasIdsNonExhaustive( range( 0, halfCacheSize ) );
        scanner.tryLoadFreeIdsIntoCache();
        assertCacheHasIdsNonExhaustive( range( halfCacheSize, cacheSize ) );

        // when
        scanner.tryLoadFreeIdsIntoCache();

        // then
        assertCacheHasIds( range( cacheSize, IDS_PER_ENTRY ) );
        scanner.tryLoadFreeIdsIntoCache();
        assertCacheHasIds( range( IDS_PER_ENTRY, IDS_PER_ENTRY + cacheSize ) );
        scanner.tryLoadFreeIdsIntoCache();
        assertCacheHasIds( range( IDS_PER_ENTRY + cacheSize, IDS_PER_ENTRY * 2 ) );
        scanner.tryLoadFreeIdsIntoCache();
        assertCacheHasIds( range( IDS_PER_ENTRY * 2, IDS_PER_ENTRY * 2 + 4 ) );
        assertEquals( -1, cache.takeOrDefault( -1 ) );
    }

    private void assertCacheHasIdsNonExhaustive( Range... ranges )
    {
        assertCacheHasIds( false, ranges );
    }

    private void assertCacheHasIds( Range... ranges )
    {
        assertCacheHasIds( true, ranges );
    }

    private void assertCacheHasIds( boolean exhaustive, Range... ranges )
    {
        for ( Range range : ranges )
        {
            for ( long id = range.fromId; id < range.toId; id++ )
            {
                assertEquals( id, cache.takeOrDefault( -1 ) );
            }
        }
        if ( exhaustive )
        {
            assertEquals( -1, cache.takeOrDefault( -1 ) );
        }
    }

    private Consumer<BiConsumer<IdRangeMarker, Long>> forEachId( long generation, Range... ranges )
    {
        return handler ->
        {
            try ( IdRangeMarker marker = new IdRangeMarker( IDS_PER_ENTRY, layout, tree.writer(), mock( Lock.class ), IdRangeMerger.DEFAULT,
                    true, atLeastOneFreeId, generation, new AtomicLong(), false ) )
            {
                for ( Range range : ranges )
                {
                    range.forEach( id -> handler.accept( marker, id ) );
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        };
    }

    private static class FoundIdMarker implements Supplier<ReuseMarker>, ReuseMarker
    {
        private final MutableLongList reservedIds = LongLists.mutable.empty();
        private final MutableLongList freedIds = LongLists.mutable.empty();

        @Override
        public void markReserved( long id )
        {
            reservedIds.add( id );
        }

        @Override
        public void markFree( long id )
        {
            freedIds.add( id );
        }

        @Override
        public void close()
        {
            // nothing to close
        }

        @Override
        public ReuseMarker get()
        {
            return this;
        }
    }

    private static Range range( long fromId, long toId )
    {
        return new Range( fromId, toId );
    }

    private static class Range
    {
        private final long fromId;
        private final long toId;

        Range( long fromId, long toId )
        {
            this.fromId = fromId;
            this.toId = toId;
        }

        void forEach( LongConsumer consumer )
        {
            for ( long id = fromId; id < toId; id++ )
            {
                consumer.accept( id );
            }
        }
    }

    private enum QueueMethodControl
    {
        TAKE,
        OFFER;
    }

    private static class ControlledConcurrentLongQueue implements ConcurrentLongQueue
    {
        private final ConcurrentLongQueue actual;
        private final QueueMethodControl method;
        private final Barrier.Control barrier;

        ControlledConcurrentLongQueue( ConcurrentLongQueue actual, QueueMethodControl method, Barrier.Control barrier )
        {
            this.actual = actual;
            this.method = method;
            this.barrier = barrier;
        }

        @Override
        public boolean offer( long v )
        {
            if ( method == QueueMethodControl.OFFER )
            {
                barrier.reached();
            }
            return actual.offer( v );
        }

        @Override
        public long takeOrDefault( long defaultValue )
        {
            if ( method == QueueMethodControl.TAKE )
            {
                barrier.reached();
            }
            return actual.takeOrDefault( defaultValue );
        }

        @Override
        public int capacity()
        {
            return actual.capacity();
        }

        @Override
        public int size()
        {
            return actual.size();
        }

        @Override
        public void clear()
        {
            actual.clear();
        }
    }
}
