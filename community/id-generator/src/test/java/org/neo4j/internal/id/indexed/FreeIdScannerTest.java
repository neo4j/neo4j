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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Barrier;
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
        assertFalse( scanner.scanMightFindFreeIds() );
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
        scanner.doSomeScanning();
        assertTrue( cache.size() > 0 );

        // then
        assertTrue( scanner.scanMightFindFreeIds() );
    }

    @Test
    void shouldThinkItsWorthScanningIfHasFreedIds()
    {
        // given
        int generation = 1;
        FreeIdScanner scanner = scanner( IDS_PER_ENTRY, 8, generation );

        // when
        atLeastOneFreeId.set( true );

        // then
        assertTrue( scanner.scanMightFindFreeIds() );
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
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

        // then
        assertCacheHasIds( range( 0, 8 ) );

        // and further when
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

        // then
        assertCacheHasIds( range( 0, 4 ), range( 64, 68 ) );

        // and further when
        scanner.doSomeScanning();

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
        Future<?> scanFuture = executorService.submit( scanner::doSomeScanning );
        barrier.await();
        // now it's stuck in trying to offer to the cache

        // then a scan call from another thread should complete but not do anything
        verify( cache, times( 1 ) ).offer( anyLong() ); // <-- the 1 call is from the call which makes the other thread stuck above
        scanner.doSomeScanning();
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

        // when
        scanner.doSomeScanning();

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
        scanner.doSomeScanning();

        // then
        assertArrayEquals( new long[]{0, 1, 2, 3, 4}, reuser.markedIds.toArray() );
    }

    private void assertCacheHasIds( Range... ranges )
    {
        for ( Range range : ranges )
        {
            for ( long id = range.fromId; id < range.toId; id++ )
            {
                assertEquals( id, cache.takeOrDefault( -1 ) );
            }
        }
        assertEquals( -1, cache.takeOrDefault( -1 ) );
    }

    private Consumer<BiConsumer<IdRangeMarker, Long>> forEachId( long generation, Range... ranges )
    {
        return handler ->
        {
            try ( IdRangeMarker marker = new IdRangeMarker( IDS_PER_ENTRY, layout, tree.writer(), mock( Lock.class ), IdRangeMerger.DEFAULT,
                    new AtomicBoolean(), generation ) )
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

    private static class FoundIdMarker implements ThrowingSupplier<ReuseMarker, IOException>, ReuseMarker
    {
        private final MutableLongList markedIds = LongLists.mutable.empty();

        @Override
        public void markReserved( long id )
        {
            markedIds.add( id );
        }

        @Override
        public void markFree( long id )
        {
            throw new UnsupportedOperationException( "Should not have been called" );
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
}
