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

import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGenerator.Marker;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.FreeIds.NO_FREE_IDS;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;
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

    private IndexedIdGenerator freelist;
    private File file;

    @BeforeEach
    void open()
    {
        file = directory.file( "file" );
        freelist = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false );
    }

    @AfterEach
    void stop()
    {
        if ( freelist != null )
        {
            freelist.close();
        }
    }

    @Test
    void shouldAllocateFreedSingleIdSlot() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long id = freelist.nextId();
        markDeleted( id );
        markReusable( id );

        // when
        long nextTimeId = freelist.nextId();

        // then
        assertEquals( id, nextTimeId );
    }

    @Test
    void shouldNotAllocateFreedIdUntilReused() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long id = freelist.nextId();
        markDeleted( id );
        long otherId = freelist.nextId();
        assertNotEquals( id, otherId );

        // when
        markReusable( id );

        // then
        long reusedId = freelist.nextId();
        assertEquals( id, reusedId );
    }

    @Test
    void shouldStayConsistentAndNotLoseIdsInConcurrent_Allocate_Delete_Free() throws Throwable
    {
        // given
        freelist.start( NO_FREE_IDS );
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
        freelist.start( NO_FREE_IDS );
        Race race = new Race().withMaxDuration( 3, TimeUnit.SECONDS );
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        race.addContestants( 6, allocator( 500, allocations, expectedInUse ) );
        race.addContestants( 1, deleter( allocations ) );
        race.addContestants( 1, freer( allocations, expectedInUse ) );
        race.addContestant( throwing( () ->
        {
            Thread.sleep( 300 );
            freelist.clearCache();
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
        freelist.start( NO_FREE_IDS );
        freelist.setHighId( IdValidator.INTEGER_MINUS_ONE );

        // when
        long id = freelist.nextId();

        // then
        assertEquals( IdValidator.INTEGER_MINUS_ONE + 1, id );
        assertFalse( IdValidator.isReservedId( id ) );
    }

    @Test
    void shouldNotGoBeyondMaxId() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        freelist.setHighId( MAX_ID - 1 );

        // when
        long oneBelowMaxId = freelist.nextId();
        assertEquals( MAX_ID - 1, oneBelowMaxId );
        long maxId = freelist.nextId();
        assertEquals( MAX_ID, maxId );

        // then
        try
        {
            freelist.nextId();
            fail( "Should have fail to go beyond max id" );
        }
        catch ( IdCapacityExceededException e )
        {
            // good
        }
    }

    @Test
    void shouldRebuildFromFreeIdsIfWasCreated() throws IOException
    {
        // given that it was created in this test right now, we know that

        // when
        freelist.start( freeIds( 10, 20, 30 ) );

        // then
        assertEquals( 10L, freelist.nextId() );
        assertEquals( 20L, freelist.nextId() );
        assertEquals( 30L, freelist.nextId() );
    }

    @Test
    void shouldRebuildFromFreeIdsIfWasCreatedAndSomeUpdatesWereMadeDuringRecovery() throws IOException
    {
        // given that it was created in this test right now, we know that
        // and given some updates before calling start (coming from recovery)
        markUsed( 5 );
        markUsed( 100 );

        // when
        freelist.start( freeIds( 10, 20, 30 ) );

        // then
        assertEquals( 10L, freelist.nextId() );
        assertEquals( 20L, freelist.nextId() );
        assertEquals( 30L, freelist.nextId() );
    }

    @Test
    void shouldRebuildFromFreeIdsIfExistedButAtStartingGeneration() throws IOException
    {
        // given that it was created in this test right now, we know that
        freelist.close();
        open();

        // when
        freelist.start( freeIds( 10, 20, 30 ) );

        // then
        assertEquals( 10L, freelist.nextId() );
        assertEquals( 20L, freelist.nextId() );
        assertEquals( 30L, freelist.nextId() );
    }

    @Test
    void shouldCheckpointAfterRebuild() throws IOException
    {
        // given that it was created in this test right now, we know that

        // when
        freelist.start( freeIds( 10, 20, 30 ) );
        freelist.close();
        open();

        // then
        assertEquals( 10L, freelist.nextId() );
        assertEquals( 20L, freelist.nextId() );
        assertEquals( 30L, freelist.nextId() );
    }

    @Test
    void shouldNotRebuildInConsecutiveSessions() throws IOException
    {
        // given that it was created in this test right now, we know that
        freelist.start( NO_FREE_IDS );
        freelist.close();
        open();

        // when
        freelist.start( visitor ->
        {
            throw new RuntimeException( "Failing because it should not be called" );
        } );

        // then
        assertEquals( 0L, freelist.nextId() );
        assertEquals( 1L, freelist.nextId() );
    }

    @Test
    void shouldHandle_Used_Deleted_Used() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long id = freelist.nextId();
        markUsed( id );
        markDeleted( id );

        // when
        markUsed( id );
        restart();

        // then
        assertNotEquals( id, freelist.nextId() );
    }

    @Test
    void shouldHandle_Used_Deleted_Free_Used() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long id = freelist.nextId();
        markUsed( id );
        markDeleted( id );
        markFree( id );

        // when
        markUsed( id );
        restart();

        // then
        assertNotEquals( id, freelist.nextId() );
    }

    @Test
    void shouldHandle_Used_Deleted_Free_Reserved_Used() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long id = freelist.nextId();
        markUsed( id );
        markDeleted( id );
        markFree( id );
        try ( IdRangeMarker marker = freelist.lockAndInstantiateMarker( true ) )
        {
            marker.markReserved( id );
        }

        // when
        markUsed( id );
        restart();

        // then
        assertNotEquals( id, freelist.nextId() );
    }

    @Test
    void shouldMarkDroppedIdsAsDeletedAndFree() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long id = freelist.nextId();
        long droppedId = freelist.nextId();
        long id2 = freelist.nextId();

        // when
        try ( Marker commitMarker = freelist.marker() )
        {
            commitMarker.markUsed( id );
            commitMarker.markUsed( id2 );
        }
        restart();

        // then
        assertEquals( droppedId, freelist.nextId() );
    }

    @Test
    void shouldConcurrentlyAllocateAllIdsAroundReservedIds() throws IOException
    {
        // given
        freelist.start( NO_FREE_IDS );
        long startingId = IdValidator.INTEGER_MINUS_ONE - 100;
        freelist.setHighId( startingId );
        freelist.markHighestWrittenAtHighId();

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
                    list.add( freelist.nextId() );
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
        freelist = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, highIdSupplier, MAX_ID, false );

        // then
        verify( highIdSupplier ).getAsLong();
        assertEquals( highId, freelist.getHighId() );
    }

    @Test
    void shouldNotUseHighIdSupplierOnOpeningNewFile() throws IOException
    {
        // given
        long highId = freelist.getHighId();
        freelist.start( NO_FREE_IDS );
        freelist.checkpoint( IOLimiter.UNLIMITED );
        stop();

        // when
        LongSupplier highIdSupplier = mock( LongSupplier.class );
        when( highIdSupplier.getAsLong() ).thenReturn( 101L );
        freelist = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, highIdSupplier, MAX_ID, false );

        // then
        verifyNoMoreInteractions( highIdSupplier );
        assertEquals( highId, freelist.getHighId() );
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly()
    {
        File file = directory.file( "non-existing" );
        final IllegalStateException e = assertThrows( IllegalStateException.class,
                () -> new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof NoSuchFileException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof IllegalStateException ) );
    }

    @Test
    void shouldNotRebuildIfReadOnly()
    {
        File file = directory.file( "existing" );
        new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false ).close();
        // Never start id generator means it will need rebuild on next start

        // Start in readOnly mode
        try ( IndexedIdGenerator readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true ) )
        {
            final UnsupportedOperationException e = assertThrows( UnsupportedOperationException.class, () -> readOnlyGenerator.start( NO_FREE_IDS ) );
            assertEquals( "Can not write to id generator while in read only mode.", e.getMessage() );
        }
    }

    @Test
    void shouldStartInReadOnlyModeIfEmpty() throws IOException
    {
        File file = directory.file( "existing" );
        final IndexedIdGenerator indexedIdGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false );
        indexedIdGenerator.start( NO_FREE_IDS );
        indexedIdGenerator.close();
        // Never start id generator means it will need rebuild on next start

        // Start in readOnly mode should not throw
        try ( IndexedIdGenerator readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS );
        }
    }

    @Test
    void shouldNotNextIdIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> idGenerator::nextId );
    }

    @Test
    void shouldNotMarkerIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> idGenerator::marker );
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
        freelist = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, monitor );
        verify( monitor ).opened( -1, 0 );
        freelist.start( NO_FREE_IDS );

        long allocatedHighId = freelist.nextId();
        verify( monitor ).allocatedFromHigh( allocatedHighId );

        try ( Marker marker = freelist.marker() )
        {
            marker.markUsed( allocatedHighId );
            verify( monitor ).markedAsUsed( allocatedHighId );
            marker.markDeleted( allocatedHighId );
            verify( monitor ).markedAsDeleted( allocatedHighId );
            marker.markFree( allocatedHighId );
            verify( monitor ).markedAsFree( allocatedHighId );
        }

        long reusedId = freelist.nextId();
        verify( monitor ).allocatedFromReused( reusedId );
        freelist.checkpoint( IOLimiter.UNLIMITED );
        // two times, one in start and one now in checkpoint
        verify( monitor, times( 2 ) ).checkpoint( anyLong(), anyLong() );
        freelist.clearCache();
        verify( monitor ).clearingCache();
        verify( monitor ).clearedCache();

        try ( Marker marker = freelist.marker() )
        {
            marker.markUsed( allocatedHighId + 3 );
            verify( monitor ).bridged( allocatedHighId + 1 );
            verify( monitor ).bridged( allocatedHighId + 2 );
        }

        freelist.close();
        verify( monitor ).close();

        // Also test normalization (which requires a restart)
        freelist = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false, monitor );
        freelist.start( NO_FREE_IDS );
        try ( Marker marker = freelist.marker() )
        {
            marker.markUsed( allocatedHighId + 1 );
        }
        verify( monitor ).normalized( 0 );

        freelist.close();
        freelist = null;
    }

    private void assertOperationThrowInReadOnlyMode( Function<IndexedIdGenerator,Executable> operation ) throws IOException
    {
        File file = directory.file( "existing" );
        final IndexedIdGenerator indexedIdGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, false );
        indexedIdGenerator.start( NO_FREE_IDS );
        indexedIdGenerator.close();

        // Start in readOnly mode
        try ( IndexedIdGenerator readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), IdType.LABEL_TOKEN, false, () -> 0, MAX_ID, true ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS );
            final UnsupportedOperationException e = assertThrows( UnsupportedOperationException.class, operation.apply( readOnlyGenerator ) );
            assertEquals( "Can not write to id generator while in read only mode.", e.getMessage() );
        }
    }

    private void verifyReallocationDoesNotIncreaseHighId( ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse )
    {
        // then after all remaining allocations have been freed, allocating that many ids again should not need to increase highId,
        // i.e. all such allocations should be allocated from the free-list
        deleteAndFree( allocations, expectedInUse );
        long highIdBeforeReallocation = freelist.getHighId();
        long numberOfIdsOutThere = highIdBeforeReallocation;
        ConcurrentSparseLongBitSet reallocationIds = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        while ( numberOfIdsOutThere > 0 )
        {
            long id = freelist.nextId();
            Allocation allocation = new Allocation( id );
            numberOfIdsOutThere -= 1;
            reallocationIds.set( allocation.id, 1, true );
        }
        assertThat( freelist.getHighId() - highIdBeforeReallocation ).isEqualTo( 0L );
    }

    private void restart() throws IOException
    {
        freelist.checkpoint( IOLimiter.UNLIMITED );
        stop();
        open();
        freelist.start( NO_FREE_IDS );
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
                long id = freelist.nextId();
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
        try ( Marker marker = freelist.marker() )
        {
            marker.markUsed( id );
        }
    }

    private void markDeleted( long id )
    {
        try ( Marker marker = freelist.marker() )
        {
            marker.markDeleted( id );
        }
    }

    private void markReusable( long id )
    {
        try ( Marker marker = freelist.marker() )
        {
            marker.markFree( id );
        }
    }

    private void markFree( long id )
    {
        try ( Marker marker = freelist.marker() )
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

        boolean delete()
        {
            if ( deleting.compareAndSet( false, true ) )
            {
                markDeleted( id );
                deleted = true;
                return true;
            }
            return false;
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
