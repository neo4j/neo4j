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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGenerator.Marker;
import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.annotations.documented.ReporterFactories.noopReporterFactory;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.FreeIds.NO_FREE_IDS;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.test.Race.throwing;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class IndexedIdGeneratorTest
{
    private static final long MAX_ID = 0x3_00000000L;

    @Inject
    private TestDirectory directory;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;

    private IndexedIdGenerator idGenerator;
    private File file;

    @BeforeEach
    void open()
    {
        file = directory.file( "file" );
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL );
    }

    @AfterEach
    void stop()
    {
        if ( idGenerator != null )
        {
            idGenerator.close();
        }
    }

    @Test
    void shouldAllocateFreedSingleIdSlot() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markDeleted( id );
        markReusable( id );

        // when
        long nextTimeId = idGenerator.nextId( NULL );

        // then
        assertEquals( id, nextTimeId );
    }

    @Test
    void shouldNotAllocateFreedIdUntilReused() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markDeleted( id );
        long otherId = idGenerator.nextId( NULL );
        assertNotEquals( id, otherId );

        // when
        markReusable( id );

        // then
        long reusedId = idGenerator.nextId( NULL );
        assertEquals( id, reusedId );
    }

    @Test
    void shouldStayConsistentAndNotLoseIdsInConcurrent_Allocate_Delete_Free() throws Throwable
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        Race race = new Race().withMaxDuration( 1, TimeUnit.SECONDS );
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        race.addContestants( 6, allocator( 500, allocations, expectedInUse ) );
        race.addContestants( 1, deleter( allocations ) );
        race.addContestants( 1, freer( allocations, expectedInUse ) );

        // when
        race.go();

        // then
        verifyReallocationDoesNotIncreaseHighId( allocations, expectedInUse );
    }

    @Test
    void shouldStayConsistentAndNotLoseIdsInConcurrentAllocate_Delete_Free_ClearCache() throws Throwable
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        Race race = new Race().withMaxDuration( 3, TimeUnit.SECONDS );
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        race.addContestants( 6, allocator( 500, allocations, expectedInUse ) );
        race.addContestants( 1, deleter( allocations ) );
        race.addContestants( 1, freer( allocations, expectedInUse ) );
        race.addContestant( throwing( () ->
        {
            Thread.sleep( 300 );
            idGenerator.clearCache( NULL );
        } ) );

        // when
        race.go();

        // then
        verifyReallocationDoesNotIncreaseHighId( allocations, expectedInUse );
    }

    @Test
    void shouldNotAllocateReservedMaxIntId() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.setHighId( IdValidator.INTEGER_MINUS_ONE );

        // when
        long id = idGenerator.nextId( NULL );

        // then
        assertEquals( IdValidator.INTEGER_MINUS_ONE + 1, id );
        assertFalse( IdValidator.isReservedId( id ) );
    }

    @Test
    void shouldNotGoBeyondMaxId() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.setHighId( MAX_ID - 1 );

        // when
        long oneBelowMaxId = idGenerator.nextId( NULL );
        assertEquals( MAX_ID - 1, oneBelowMaxId );
        long maxId = idGenerator.nextId( NULL );
        assertEquals( MAX_ID, maxId );

        // then
        assertThrows( IdCapacityExceededException.class, () -> idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldRebuildFromFreeIdsIfWasCreated() throws IOException
    {
        // given that it was created in this test right now, we know that

        // when
        idGenerator.start( freeIds( 10, 20, 30 ), NULL );

        // then
        assertEquals( 10L, idGenerator.nextId( NULL ) );
        assertEquals( 20L, idGenerator.nextId( NULL ) );
        assertEquals( 30L, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldRebuildFromFreeIdsIfWasCreatedAndSomeUpdatesWereMadeDuringRecovery() throws IOException
    {
        // given that it was created in this test right now, we know that
        // and given some updates before calling start (coming from recovery)
        markUsed( 5 );
        markUsed( 100 );

        // when
        idGenerator.start( freeIds( 10, 20, 30 ), NULL );

        // then
        assertEquals( 10L, idGenerator.nextId( NULL ) );
        assertEquals( 20L, idGenerator.nextId( NULL ) );
        assertEquals( 30L, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldRebuildFromFreeIdsIfExistedButAtStartingGeneration() throws IOException
    {
        // given that it was created in this test right now, we know that
        idGenerator.close();
        open();

        // when
        idGenerator.start( freeIds( 10, 20, 30 ), NULL );

        // then
        assertEquals( 10L, idGenerator.nextId( NULL ) );
        assertEquals( 20L, idGenerator.nextId( NULL ) );
        assertEquals( 30L, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldCheckpointAfterRebuild() throws IOException
    {
        // given that it was created in this test right now, we know that

        // when
        idGenerator.start( freeIds( 10, 20, 30 ), NULL );
        idGenerator.close();
        open();

        // then
        assertEquals( 10L, idGenerator.nextId( NULL ) );
        assertEquals( 20L, idGenerator.nextId( NULL ) );
        assertEquals( 30L, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldNotRebuildInConsecutiveSessions() throws IOException
    {
        // given that it was created in this test right now, we know that
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.close();
        open();

        // when
        idGenerator.start( visitor ->
        {
            throw new RuntimeException( "Failing because it should not be called" );
        }, NULL );

        // then
        assertEquals( 0L, idGenerator.nextId( NULL ) );
        assertEquals( 1L, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldHandle_Used_Deleted_Used() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markUsed( id );
        markDeleted( id );

        // when
        markUsed( id );
        restart();

        // then
        assertNotEquals( id, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldHandle_Used_Deleted_Free_Used() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markUsed( id );
        markDeleted( id );
        markFree( id );

        // when
        markUsed( id );
        restart();

        // then
        assertNotEquals( id, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldHandle_Used_Deleted_Free_Reserved_Used() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markUsed( id );
        markDeleted( id );
        markFree( id );
        try ( IdRangeMarker marker = idGenerator.lockAndInstantiateMarker( true, NULL ) )
        {
            marker.markReserved( id );
        }

        // when
        markUsed( id );
        restart();

        // then
        assertNotEquals( id, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldMarkDroppedIdsAsDeletedAndFree() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        long droppedId = idGenerator.nextId( NULL );
        long id2 = idGenerator.nextId( NULL );

        // when
        try ( Marker commitMarker = idGenerator.marker( NULL ) )
        {
            commitMarker.markUsed( id );
            commitMarker.markUsed( id2 );
        }
        restart();

        // then
        assertEquals( droppedId, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldConcurrentlyAllocateAllIdsAroundReservedIds() throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        long startingId = IdValidator.INTEGER_MINUS_ONE - 100;
        idGenerator.setHighId( startingId );
        idGenerator.markHighestWrittenAtHighId();

        // when
        Race race = new Race();
        int threads = 8;
        int allocationsPerThread = 32;
        LongList[] allocatedIds = new LongList[threads];
        for ( int i = 0; i < 8; i++ )
        {
            LongArrayList list = new LongArrayList( 32 );
            allocatedIds[i] = list;
            race.addContestant( () ->
            {
                for ( int j = 0; j < allocationsPerThread; j++ )
                {
                    list.add( idGenerator.nextId( NULL ) );
                }
            }, 1 );
        }
        race.goUnchecked();

        // then
        MutableLongList allIds = new LongArrayList( allocationsPerThread * threads );
        Stream.of( allocatedIds ).forEach( allIds::addAll );
        allIds = allIds.sortThis();
        assertEquals( allocationsPerThread * threads, allIds.size() );
        MutableLongIterator allIdsIterator = allIds.longIterator();
        long nextExpected = startingId;
        while ( allIdsIterator.hasNext() )
        {
            assertEquals( nextExpected, allIdsIterator.next() );
            do
            {
                nextExpected++;
            }
            while ( IdValidator.isReservedId( nextExpected ) );
        }
    }

    @Test
    void shouldUseHighIdSupplierOnCreatingNewFile()
    {
        // given
        stop();
        assertTrue( file.delete() );

        // when
        long highId = 101L;
        LongSupplier highIdSupplier = mock( LongSupplier.class );
        when( highIdSupplier.getAsLong() ).thenReturn( highId );
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, highIdSupplier, MAX_ID, false, NULL );

        // then
        verify( highIdSupplier ).getAsLong();
        assertEquals( highId, idGenerator.getHighId() );
    }

    @Test
    void shouldNotUseHighIdSupplierOnOpeningNewFile() throws IOException
    {
        // given
        long highId = idGenerator.getHighId();
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.checkpoint( UNLIMITED, NULL );
        stop();

        // when
        LongSupplier highIdSupplier = mock( LongSupplier.class );
        when( highIdSupplier.getAsLong() ).thenReturn( 101L );
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, highIdSupplier, MAX_ID, false, NULL );

        // then
        verifyNoMoreInteractions( highIdSupplier );
        assertEquals( highId, idGenerator.getHighId() );
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly()
    {
        File file = directory.file( "non-existing" );
        final IllegalStateException e = assertThrows( IllegalStateException.class,
                () -> new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true, NULL ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof NoSuchFileException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof IllegalStateException ) );
    }

    @Test
    void shouldNotRebuildIfReadOnly()
    {
        File file = directory.file( "existing" );
        new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL ).close();
        // Never start id generator means it will need rebuild on next start

        // Start in readOnly mode
        try ( IndexedIdGenerator readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true,
                NULL ) )
        {
            final UnsupportedOperationException e = assertThrows( UnsupportedOperationException.class, () -> readOnlyGenerator.start( NO_FREE_IDS, NULL ) );
            assertEquals( "Can not write to id generator while in read only mode.", e.getMessage() );
        }
    }

    @Test
    void shouldStartInReadOnlyModeIfEmpty() throws IOException
    {
        File file = directory.file( "existing" );
        var indexedIdGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL );
        indexedIdGenerator.start( NO_FREE_IDS, NULL );
        indexedIdGenerator.close();
        // Never start id generator means it will need rebuild on next start

        // Start in readOnly mode should not throw
        try ( var readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true, NULL ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS, NULL );
        }
    }

    @Test
    void shouldNotNextIdIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> () -> idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldNotMarkerIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> () -> idGenerator.marker( NULL ) );
    }

    @Test
    void shouldNotSetHighIdIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> () -> idGenerator.setHighId( 1 ) );
    }

    @Test
    void shouldNotMarkHighestWrittenAtHighIdIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> idGenerator::markHighestWrittenAtHighId );
    }

    @Test
    void shouldInvokeMonitorOnCorrectCalls() throws IOException
    {
        stop();
        IndexedIdGenerator.Monitor monitor = mock( IndexedIdGenerator.Monitor.class );
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL, monitor,
                immutable.empty() );
        verify( monitor ).opened( -1, 0 );
        idGenerator.start( NO_FREE_IDS, NULL );

        long allocatedHighId = idGenerator.nextId( NULL );
        verify( monitor ).allocatedFromHigh( allocatedHighId );

        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( allocatedHighId );
            verify( monitor ).markedAsUsed( allocatedHighId );
            marker.markDeleted( allocatedHighId );
            verify( monitor ).markedAsDeleted( allocatedHighId );
            marker.markFree( allocatedHighId );
            verify( monitor ).markedAsFree( allocatedHighId );
        }

        long reusedId = idGenerator.nextId( NULL );
        verify( monitor ).allocatedFromReused( reusedId );
        idGenerator.checkpoint( UNLIMITED, NULL );
        // two times, one in start and one now in checkpoint
        verify( monitor, times( 2 ) ).checkpoint( anyLong(), anyLong() );
        idGenerator.clearCache( NULL );
        verify( monitor ).clearingCache();
        verify( monitor ).clearedCache();

        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( allocatedHighId + 3 );
            verify( monitor ).bridged( allocatedHighId + 1 );
            verify( monitor ).bridged( allocatedHighId + 2 );
        }

        idGenerator.close();
        verify( monitor ).close();

        // Also test normalization (which requires a restart)
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL, monitor,
                immutable.empty() );
        idGenerator.start( NO_FREE_IDS, NULL );
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( allocatedHighId + 1 );
        }
        verify( monitor ).normalized( 0 );

        idGenerator.close();
        idGenerator = null;
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnConsistencyCheck" ) )
        {
            idGenerator.consistencyCheck( noopReporterFactory(), cursorTracer );

            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
        }
    }

    @Test
    void noPageCacheActivityWithNoMaintenanceOnOnNextId()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "noPageCacheActivityWithNoMaintenanceOnOnNextId" ) )
        {
            idGenerator.nextId( cursorTracer );

            assertThat( cursorTracer.hits() ).isZero();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
        }
    }

    @Test
    void tracePageCacheActivityOnOnNextId()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "noPageCacheActivityWithNoMaintenanceOnOnNextId" ) )
        {
            idGenerator.marker( NULL ).markDeleted( 1 );
            idGenerator.clearCache( NULL );
            idGenerator.nextId( cursorTracer );

            assertThat( cursorTracer.hits() ).isOne();
            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
        }
    }

    @Test
    void tracePageCacheActivityWhenMark() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheActivityWhenMark" ) )
        {
            idGenerator.start( NO_FREE_IDS, NULL );
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            try ( var marker = idGenerator.marker( cursorTracer ) )
            {
                assertThat( cursorTracer.pins() ).isOne();
                assertThat( cursorTracer.hits() ).isOne();

                marker.markDeleted( 1 );

                assertThat( cursorTracer.pins() ).isGreaterThan( 1 );
                assertThat( cursorTracer.unpins() ).isGreaterThan( 1 );
                assertThat( cursorTracer.hits() ).isGreaterThan( 1 );
            }
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorCacheClear()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorCacheClear" ) )
        {
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.marker( NULL ).markDeleted( 1 );
            idGenerator.clearCache( cursorTracer );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorMaintenance()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorMaintenance" ) )
        {
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.maintenance( cursorTracer );

            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.marker( NULL ).markDeleted( 1 );
            idGenerator.clearCache( NULL );
            idGenerator.maintenance( cursorTracer );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorCheckpoint()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorCheckpoint" ) )
        {
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.checkpoint( UNLIMITED, cursorTracer );

            // 2 state pages involved into checkpoint (twice)
            assertThat( cursorTracer.pins() ).isEqualTo( 4 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
            assertThat( cursorTracer.hits() ).isEqualTo( 4 );
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorStartWithRebuild() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorStartWithRebuild" ) )
        {
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.start( NO_FREE_IDS, cursorTracer );

            // 2 state pages involved into checkpoint (twice) + one more pin/hit/unpin on maintenance + range marker writer
            assertThat( cursorTracer.pins() ).isEqualTo( 6 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 6 );
            assertThat( cursorTracer.hits() ).isEqualTo( 6 );
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorStartWithoutRebuild() throws IOException
    {
        try ( var prepareIndexWithoutRebuild = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL ) )
        {
            prepareIndexWithoutRebuild.checkpoint( UNLIMITED, NULL );
        }
        try ( var idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL ) )
        {
            var pageCacheTracer = new DefaultPageCacheTracer();
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorStartWithoutRebuild" ) )
            {
                assertThat( cursorTracer.pins() ).isZero();
                assertThat( cursorTracer.unpins() ).isZero();
                assertThat( cursorTracer.hits() ).isZero();

                idGenerator.start( NO_FREE_IDS, cursorTracer );

                // pin/hit/unpin on maintenance
                assertThat( cursorTracer.pins() ).isOne();
                assertThat( cursorTracer.unpins() ).isOne();
                assertThat( cursorTracer.hits() ).isOne();
            }
        }
    }

    @Test
    void shouldAllocateConsecutiveIdBatches()
    {
        // given
        AtomicInteger numAllocations = new AtomicInteger();
        Race race = new Race().withEndCondition( () -> numAllocations.get() >= 10_000 );
        Collection<IdRange> allocations = ConcurrentHashMap.newKeySet();
        race.addContestants( 4, () ->
        {
            int size = ThreadLocalRandom.current().nextInt( 10, 1_000 );
            IdRange idRange = idGenerator.nextIdBatch( size, true, NULL );
            assertEquals( 0, idRange.getDefragIds().length );
            assertEquals( size, idRange.getRangeLength() );
            allocations.add( idRange );
            numAllocations.incrementAndGet();
        } );

        // when
        race.goUnchecked();

        // then
        IdRange[] sortedAllocations = allocations.toArray( new IdRange[allocations.size()] );
        Arrays.sort( sortedAllocations, Comparator.comparingLong( IdRange::getRangeStart ) );
        long prevEndExclusive = 0;
        for ( IdRange allocation : sortedAllocations )
        {
            assertEquals( prevEndExclusive, allocation.getRangeStart() );
            prevEndExclusive = allocation.getRangeStart() + allocation.getRangeLength();
        }
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldNotAllocateReservedIdsInBatchedAllocation( boolean consecutive ) throws IOException
    {
        // given
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.setHighId( IdValidator.INTEGER_MINUS_ONE - 100 );

        // when
        IdRange batch = idGenerator.nextIdBatch( 200, consecutive, NULL );

        // then
        assertFalse( IdValidator.hasReservedIdInRange( batch.getRangeStart(), batch.getRangeStart() + batch.getRangeLength() ) );
        for ( long defragId : batch.getDefragIds() )
        {
            assertFalse( IdValidator.isReservedId( defragId ) );
        }
    }

    private void assertOperationThrowInReadOnlyMode( Function<IndexedIdGenerator,Executable> operation ) throws IOException
    {
        File file = directory.file( "existing" );
        var indexedIdGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, NULL );
        indexedIdGenerator.start( NO_FREE_IDS, NULL );
        indexedIdGenerator.close();

        // Start in readOnly mode
        try ( var readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true, NULL ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS, NULL );
            final UnsupportedOperationException e = assertThrows( UnsupportedOperationException.class, operation.apply( readOnlyGenerator ) );
            assertEquals( "Can not write to id generator while in read only mode.", e.getMessage() );
        }
    }

    private void verifyReallocationDoesNotIncreaseHighId( ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse )
    {
        // then after all remaining allocations have been freed, allocating that many ids again should not need to increase highId,
        // i.e. all such allocations should be allocated from the free-list
        deleteAndFree( allocations, expectedInUse );
        long highIdBeforeReallocation = idGenerator.getHighId();
        long numberOfIdsOutThere = highIdBeforeReallocation;
        ConcurrentSparseLongBitSet reallocationIds = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        while ( numberOfIdsOutThere > 0 )
        {
            long id = idGenerator.nextId( NULL );
            Allocation allocation = new Allocation( id );
            numberOfIdsOutThere -= 1;
            reallocationIds.set( allocation.id, 1, true );
        }
        assertThat( idGenerator.getHighId() - highIdBeforeReallocation ).isEqualTo( 0L );
    }

    private void restart() throws IOException
    {
        idGenerator.checkpoint( UNLIMITED, NULL );
        stop();
        open();
        idGenerator.start( NO_FREE_IDS, NULL );
    }

    private static FreeIds freeIds( long... freeIds )
    {
        return visitor ->
        {
            for ( long freeId : freeIds )
            {
                visitor.accept( freeId );
            }
            return freeIds[freeIds.length - 1];
        };
    }

    private Runnable freer( ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse )
    {
        return new Runnable()
        {
            private Random r = new Random( random.nextLong() );

            @Override
            public void run()
            {
                // Mark ids as eligible for reuse
                int size = allocations.size();
                if ( size > 0 )
                {
                    int slot = r.nextInt( size );
                    Iterator<Allocation> iterator = allocations.iterator();
                    Allocation allocation = null;
                    for ( int i = 0; i < slot && iterator.hasNext(); i++ )
                    {
                        allocation = iterator.next();
                    }
                    if ( allocation != null )
                    {
                        if ( allocation.free( expectedInUse ) )
                        {
                            iterator.remove();
                        }
                        // else someone else got there before us
                    }
                }
            }
        };
    }

    private Runnable deleter( ConcurrentLinkedQueue<Allocation> allocations )
    {
        return new Runnable()
        {
            private Random r = new Random( random.nextLong() );

            @Override
            public void run()
            {
                // Delete ids
                int size = allocations.size();
                if ( size > 0 )
                {
                    int slot = r.nextInt( size );
                    Iterator<Allocation> iterator = allocations.iterator();
                    Allocation allocation = null;
                    for ( int i = 0; i < slot && iterator.hasNext(); i++ )
                    {
                        allocation = iterator.next();
                    }
                    if ( allocation != null )
                    {
                        // Won't delete if it has already been deleted, but that's fine
                        allocation.delete();
                    }
                }
            }
        };
    }

    private Runnable allocator( int maxAllocationsAhead, ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse )
    {
        return () ->
        {
            // Allocate ids
            if ( allocations.size() < maxAllocationsAhead )
            {
                long id = idGenerator.nextId( NULL );
                Allocation allocation = new Allocation( id );
                allocation.markAsInUse( expectedInUse );
                allocations.add( allocation );
            }
        };
    }

    private void deleteAndFree( ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse )
    {
        for ( Allocation allocation : allocations )
        {
            allocation.delete();
            allocation.free( expectedInUse );
        }
    }

    private void markUsed( long id )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( id );
        }
    }

    private void markDeleted( long id )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markDeleted( id );
        }
    }

    private void markReusable( long id )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markFree( id );
        }
    }

    private void markFree( long id )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markFree( id );
        }
    }

    private class Allocation
    {
        private final long id;
        private final AtomicBoolean deleting = new AtomicBoolean();
        private volatile boolean deleted;
        private final AtomicBoolean freeing = new AtomicBoolean();

        Allocation( long id )
        {
            this.id = id;
        }

        void delete()
        {
            if ( deleting.compareAndSet( false, true ) )
            {
                markDeleted( id );
                deleted = true;
            }
        }

        boolean free( ConcurrentSparseLongBitSet expectedInUse )
        {
            if ( !deleted )
            {
                return false;
            }

            if ( freeing.compareAndSet( false, true ) )
            {
                expectedInUse.set( id, 1, false );
                markReusable( id );
                return true;
            }
            return false;
        }

        void markAsInUse( ConcurrentSparseLongBitSet expectedInUse )
        {
            expectedInUse.set( id, 1, true );
            // Simulate that actual commit comes very close after allocation, in reality they are slightly more apart
            // Also this test marks all ids, regardless if they come from highId or the free-list. This to simulate more real-world
            // scenario and to exercise the idempotent clearing feature.
            markUsed( id );
        }

        @Override
        public String toString()
        {
            return String.valueOf( id );
        }
    }
}
