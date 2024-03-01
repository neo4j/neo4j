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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.factory.Sets;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGenerator.Marker;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.id.TestIdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.exceptions.WriteOnReadOnlyAccessDbException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparingLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import static org.neo4j.configuration.GraphDatabaseInternalSettings.strictly_prioritize_id_freelist;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.FreeIds.NO_FREE_IDS;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.internal.id.IdSlotDistribution.diminishingSlotDistribution;
import static org.neo4j.internal.id.IdSlotDistribution.evenSlotDistribution;
import static org.neo4j.internal.id.IdSlotDistribution.powerTwoSlotSizesDownwards;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.test.Race.throwing;

@PageCacheExtension
@ExtendWith( { RandomExtension.class, LifeExtension.class } )
class IndexedIdGeneratorTest
{
    private static final long MAX_ID = 0x3_00000000L;

    @Inject
    private TestDirectory directory;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomSupport random;
    @Inject
    private LifeSupport lifeSupport;

    private IndexedIdGenerator idGenerator;
    private Path file;

    @BeforeEach
    void getFile()
    {
        file = directory.file( "file" );
    }

    void open()
    {
        open( Config.defaults(), IndexedIdGenerator.NO_MONITOR, writable(), SINGLE_IDS );
    }

    void open( Config config, IndexedIdGenerator.Monitor monitor, DatabaseReadOnlyChecker readOnlyChecker, IdSlotDistribution slotDistribution )
    {
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readOnlyChecker, config,
                DEFAULT_DATABASE_NAME, NULL, monitor, immutable.empty(), slotDistribution );
    }

    @AfterEach
    void stop()
    {
        if ( idGenerator != null )
        {
            idGenerator.close();
            idGenerator = null;
        }
    }

    @Test
    void idGeneratorWithChangesStillPreserveState() throws IOException
    {
        open();
        int generatedIds = 10;
        var config = Config.defaults();
        var readOnlyDatabases = new ReadOnlyDatabases( new ConfigBasedLookupFactory( config, mock( DatabaseIdRepository.class ) ) );
        var configLister = new ConfigReadOnlyDatabaseListener( readOnlyDatabases, config );
        lifeSupport.add( configLister );
        var defaultDatabaseId = DatabaseIdFactory.from( DEFAULT_DATABASE_NAME, UUID.randomUUID() ); //UUID required, but ignored by config lookup
        var readableChecker = readOnlyDatabases.forDatabase( defaultDatabaseId );

        try ( var customGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readableChecker,
                config, DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            customGenerator.start( NO_FREE_IDS, NULL );
            for ( int i = 0; i < generatedIds; i++ )
            {
                customGenerator.nextId( NULL );
            }
            config.set( GraphDatabaseSettings.read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );

            assertDoesNotThrow( () -> customGenerator.nextId( NULL ) );

            customGenerator.checkpoint( NULL );
        }

        try ( var reopenedGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readableChecker,
                config, DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            reopenedGenerator.start( NO_FREE_IDS, NULL );
            assertDoesNotThrow( () -> reopenedGenerator.nextId( NULL ) );

            config.set( GraphDatabaseSettings.read_only_databases, emptySet() );

            assertNotEquals( generatedIds, reopenedGenerator.nextId( NULL ) );
        }
    }

    @Test
    void shouldAllocateFreedSingleIdSlot() throws IOException
    {
        // given
        open();
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markDeleted( id );
        markFree( id );

        // when
        idGenerator.maintenance( NULL );
        long nextTimeId = idGenerator.nextId( NULL );

        // then
        assertEquals( id, nextTimeId );
    }

    @Test
    void shouldNotAllocateFreedIdUntilReused() throws IOException
    {
        // given
        open();
        idGenerator.start( NO_FREE_IDS, NULL );
        long id = idGenerator.nextId( NULL );
        markDeleted( id );
        long otherId = idGenerator.nextId( NULL );
        assertNotEquals( id, otherId );

        // when
        markFree( id );

        // then
        idGenerator.maintenance( NULL );
        long reusedId = idGenerator.nextId( NULL );
        assertEquals( id, reusedId );
    }

    @Test
    void shouldHandleSlotsLargerThanOne() throws IOException
    {
        // given
        int[] slotSizes = {1, 2, 4};
        open( Config.defaults(), NO_MONITOR, writable(), diminishingSlotDistribution( slotSizes ) );
        idGenerator.start( NO_FREE_IDS, NULL );

        // when
        int firstSize = 2;
        int secondSize = 4;
        long firstId = idGenerator.nextConsecutiveIdRange( firstSize, true, NULL );
        assertThat( firstId ).isEqualTo( 0 );
        long secondId = idGenerator.nextConsecutiveIdRange( secondSize, true, NULL );
        assertThat( secondId ).isEqualTo( firstId + firstSize );
        markUsed( firstId, firstSize );
        markUsed( secondId, secondSize );
        markDeleted( firstId, firstSize );
        markDeleted( secondId, secondSize );
        markFree( firstId, firstSize );
        markFree( secondId, secondSize );
        idGenerator.maintenance( NULL );

        // then
        assertThat( idGenerator.nextConsecutiveIdRange( 4, true, NULL ) ).isEqualTo( 0 );
        assertThat( idGenerator.nextConsecutiveIdRange( 4, true, NULL ) ).isEqualTo( 6 );
        assertThat( idGenerator.nextConsecutiveIdRange( 2, true, NULL ) ).isEqualTo( 4 );
    }

    @Test
    void shouldStayConsistentAndNotLoseIdsInConcurrent_Allocate_Delete_Free() throws Throwable
    {
        // given
        int maxSlotSize = 4;
        open( Config.defaults( strictly_prioritize_id_freelist, true ), NO_MONITOR, writable(),
                evenSlotDistribution( powerTwoSlotSizesDownwards( maxSlotSize ) ) );
        idGenerator.start( NO_FREE_IDS, NULL );

        Race race = new Race().withMaxDuration( 1, TimeUnit.SECONDS );
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        race.addContestants( 6, allocator( 500, allocations, expectedInUse, maxSlotSize ) );
        race.addContestants( 2, deleter( allocations ) );
        race.addContestants( 2, freer( allocations, expectedInUse ) );

        // when
        race.go();

        // then
        if ( maxSlotSize == 1 )
        {
            verifyReallocationDoesNotIncreaseHighId( allocations, expectedInUse );
        }
    }

    @ParameterizedTest
    @ValueSource( ints = {1, 2, 4, 8, 16} )
    void shouldStayConsistentAndNotLoseIdsInConcurrentAllocateDeleteFreeClearCache( int maxSlotSize ) throws Throwable
    {
        // given
        open( Config.defaults( strictly_prioritize_id_freelist, true ), NO_MONITOR, writable(),
                diminishingSlotDistribution( powerTwoSlotSizesDownwards( maxSlotSize ) ) );
        idGenerator.start( NO_FREE_IDS, NULL );

        Race race = new Race().withMaxDuration( 3, TimeUnit.SECONDS );
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet( IDS_PER_ENTRY );
        race.addContestants( 6, allocator( 500, allocations, expectedInUse, maxSlotSize ) );
        race.addContestants( 2, deleter( allocations ) );
        race.addContestants( 2, freer( allocations, expectedInUse ) );
        race.addContestant( throwing( () ->
        {
            Thread.sleep( 300 );
            idGenerator.clearCache( NULL );
        } ) );

        // when
        race.go();

        // then
        if ( maxSlotSize == 1 )
        {
            verifyReallocationDoesNotIncreaseHighId( allocations, expectedInUse );
        }
    }

    @Test
    void shouldNotAllocateReservedMaxIntId() throws IOException
    {
        // given
        open();
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
        open();
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
        // given
        open();

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
        open();
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
        // given
        open();
        stop();
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
        // given
        open();

        // when
        idGenerator.start( freeIds( 10, 20, 30 ), NULL );
        stop();
        open();
        idGenerator.start( NO_FREE_IDS, NULL );

        // then
        assertEquals( 10L, idGenerator.nextId( NULL ) );
        assertEquals( 20L, idGenerator.nextId( NULL ) );
        assertEquals( 30L, idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldNotRebuildInConsecutiveSessions() throws IOException
    {
        // given
        open();
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
        open();
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
        open();
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
        open();
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
        open();
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
        open();
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
        // when
        long highId = 101L;
        LongSupplier highIdSupplier = mock( LongSupplier.class );
        when( highIdSupplier.getAsLong() ).thenReturn( highId );
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, highIdSupplier, MAX_ID, writable(), Config.defaults(),
                DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS );

        // then
        verify( highIdSupplier ).getAsLong();
        assertEquals( highId, idGenerator.getHighId() );
    }

    @Test
    void shouldNotUseHighIdSupplierOnOpeningNewFile() throws IOException
    {
        // given
        open();
        long highId = idGenerator.getHighId();
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.checkpoint( NULL );
        stop();

        // when
        LongSupplier highIdSupplier = mock( LongSupplier.class );
        when( highIdSupplier.getAsLong() ).thenReturn( 101L );
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, highIdSupplier, MAX_ID, writable(), Config.defaults(),
                DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS );

        // then
        verifyNoMoreInteractions( highIdSupplier );
        assertEquals( highId, idGenerator.getHighId() );
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly()
    {
        open();
        Path file = directory.file( "non-existing" );
        final IllegalStateException e = assertThrows( IllegalStateException.class,
                () -> new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readOnly(), Config.defaults(),
                        DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof IllegalStateException ) );
    }

    @Test
    void shouldNotRebuildIfReadOnly()
    {
        Path file = directory.file( "existing" );
        new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, writable(), Config.defaults(), DEFAULT_DATABASE_NAME,
                NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ).close();
        // Never start id generator means it will need rebuild on next start

        // Start in readOnly mode
        try ( IndexedIdGenerator readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID,
                readOnly(), Config.defaults(), DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            var e = assertThrows( Exception.class, () -> readOnlyGenerator.start( NO_FREE_IDS, NULL ) );
            assertThat( e ).hasCauseInstanceOf( WriteOnReadOnlyAccessDbException.class );
        }
    }

    @Test
    void shouldStartInReadOnlyModeIfEmpty() throws IOException
    {
        Path file = directory.file( "existing" );
        var indexedIdGenerator =
                new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, writable(), Config.defaults(),
                        DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS );
        indexedIdGenerator.start( NO_FREE_IDS, NULL );
        indexedIdGenerator.close();
        // Never start id generator means it will need rebuild on next start

        // Start in readOnly mode should not throw
        try ( var readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readOnly(),
                Config.defaults(), DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS, NULL );
        }
    }

    @Test
    void shouldNotNextIdIfReadOnly() throws IOException
    {
        assertOperationPermittedInReadOnlyMode( idGenerator -> () -> idGenerator.nextId( NULL ) );
    }

    @Test
    void shouldNotMarkerIfReadOnly() throws IOException
    {
        assertOperationPermittedInReadOnlyMode( idGenerator -> () -> idGenerator.marker( NULL ) );
    }

    @Test
    void shouldNotSetHighIdIfReadOnly() throws IOException
    {
        assertOperationPermittedInReadOnlyMode( idGenerator -> () -> idGenerator.setHighId( 1 ) );
    }

    @Test
    void shouldNotMarkHighestWrittenAtHighIdIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( idGenerator -> idGenerator::markHighestWrittenAtHighId );
    }

    @Test
    void shouldInvokeMonitorOnCorrectCalls() throws IOException
    {
        IndexedIdGenerator.Monitor monitor = mock( IndexedIdGenerator.Monitor.class );
        open( Config.defaults(), monitor, writable(), SINGLE_IDS );
        verify( monitor ).opened( -1, 0 );
        idGenerator.start( NO_FREE_IDS, NULL );

        long allocatedHighId = idGenerator.nextId( NULL );
        verify( monitor ).allocatedFromHigh( allocatedHighId, 1 );

        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( allocatedHighId );
            verify( monitor ).markedAsUsed( allocatedHighId, 1 );
            marker.markDeleted( allocatedHighId );
            verify( monitor ).markedAsDeleted( allocatedHighId, 1 );
            marker.markFree( allocatedHighId );
            verify( monitor ).markedAsFree( allocatedHighId, 1 );
        }

        idGenerator.maintenance( NULL );
        long reusedId = idGenerator.nextId( NULL );
        verify( monitor ).allocatedFromReused( reusedId, 1 );
        idGenerator.checkpoint( NULL );
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

        stop();
        verify( monitor ).close();

        // Also test normalization (which requires a restart)
        open( Config.defaults(), monitor, writable(), SINGLE_IDS );
        idGenerator.start( NO_FREE_IDS, NULL );
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( allocatedHighId + 1 );
        }
        verify( monitor ).normalized( 0 );
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck()
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnConsistencyCheck" ) ) )
        {
            idGenerator.consistencyCheck( noopReporterFactory(), cursorContext );

            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
        }
    }

    @Test
    void noPageCacheActivityWithNoMaintenanceOnOnNextId()
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "noPageCacheActivityWithNoMaintenanceOnOnNextId" ) ) )
        {
            idGenerator.nextId( cursorContext );

            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.hits() ).isZero();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
        }
    }

    @Test
    void tracePageCacheActivityOnOnNextId()
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "noPageCacheActivityWithNoMaintenanceOnOnNextId" ) ) )
        {
            idGenerator.marker( NULL ).markDeleted( 1 );
            idGenerator.clearCache( NULL );
            idGenerator.maintenance( cursorContext );

            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.hits() ).isOne();
            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
        }
    }

    @Test
    void tracePageCacheActivityWhenMark() throws IOException
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheActivityWhenMark" ) ) )
        {
            idGenerator.start( NO_FREE_IDS, NULL );
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            try ( var marker = idGenerator.marker( cursorContext ) )
            {
                assertThat( cursorTracer.pins() ).isOne();

                marker.markDeleted( 1 );

                assertThat( cursorTracer.pins() ).isGreaterThan( 1 );
                assertThat( cursorTracer.unpins() ).isGreaterThan( 1 );
            }
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorCacheClear()
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorCacheClear" ) ) )
        {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.marker( NULL ).markDeleted( 1 );
            idGenerator.clearCache( cursorContext );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorMaintenance()
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorMaintenance" ) ) )
        {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.maintenance( cursorContext );

            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.marker( NULL ).markDeleted( 1 );
            idGenerator.clearCache( NULL );
            idGenerator.maintenance( cursorContext );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorCheckpoint()
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorCheckpoint" ) ) )
        {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.checkpoint( cursorContext );

            // 2 state pages involved into checkpoint (twice)
            assertThat( cursorTracer.pins() ).isEqualTo( 4 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
            assertThat( cursorTracer.hits() ).isEqualTo( 4 );
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorStartWithRebuild() throws IOException
    {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorStartWithRebuild" ) ) )
        {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isZero();
            assertThat( cursorTracer.unpins() ).isZero();
            assertThat( cursorTracer.hits() ).isZero();

            idGenerator.start( NO_FREE_IDS, cursorContext );

            // 2 state pages involved into checkpoint (twice) + one more pin/hit/unpin on maintenance + range marker writer
            assertThat( cursorTracer.pins() ).isEqualTo( 6 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 6 );
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorStartWithoutRebuild() throws IOException
    {
        try ( var prepareIndexWithoutRebuild = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, writable(),
                Config.defaults(), DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            prepareIndexWithoutRebuild.checkpoint( NULL );
        }
        try ( var idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, writable(),
                Config.defaults(), DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            var pageCacheTracer = new DefaultPageCacheTracer();
            try ( var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "tracePageCacheOnIdGeneratorStartWithoutRebuild" ) ) )
            {
                var cursorTracer = cursorContext.getCursorTracer();
                assertThat( cursorTracer.pins() ).isZero();
                assertThat( cursorTracer.unpins() ).isZero();
                assertThat( cursorTracer.hits() ).isZero();

                idGenerator.start( NO_FREE_IDS, cursorContext );

                // pin/hit/unpin on maintenance
                assertThat( cursorTracer.pins() ).isOne();
                assertThat( cursorTracer.unpins() ).isOne();
            }
        }
    }

    @Test
    void shouldAllocateConsecutiveIdBatches()
    {
        // given
        open();
        AtomicInteger numAllocations = new AtomicInteger();
        Race race = new Race().withEndCondition( () -> numAllocations.get() >= 10_000 );
        Collection<long[]> allocations = ConcurrentHashMap.newKeySet();
        race.addContestants( 4, () ->
        {
            int size = ThreadLocalRandom.current().nextInt( 10, 1_000 );
            long batchStartId = idGenerator.nextConsecutiveIdRange( size, false, NULL );
            allocations.add( new long[]{batchStartId, size} );
            numAllocations.incrementAndGet();
        } );

        // when
        race.goUnchecked();

        // then
        long[][] sortedAllocations = allocations.toArray( new long[allocations.size()][] );
        Arrays.sort( sortedAllocations, comparingLong( a -> a[0] ) );
        long prevEndExclusive = 0;
        for ( long[] allocation : sortedAllocations )
        {
            assertEquals( prevEndExclusive, allocation[0] );
            prevEndExclusive = allocation[0] + allocation[1];
        }
    }

    @Test
    void shouldNotAllocateReservedIdsInBatchedAllocation() throws IOException
    {
        // given
        open();
        idGenerator.start( NO_FREE_IDS, NULL );
        idGenerator.setHighId( IdValidator.INTEGER_MINUS_ONE - 100 );

        // when
        int numberOfIds = 200;
        long batchStartId = idGenerator.nextConsecutiveIdRange( numberOfIds, false, NULL );

        // then
        assertFalse( IdValidator.hasReservedIdInRange( batchStartId, batchStartId + numberOfIds ) );
    }

    @Test
    void shouldAwaitConcurrentOngoingMaintenanceIfToldTo() throws Exception
    {
        // given
        Barrier.Control barrier = new Barrier.Control();
        IndexedIdGenerator.Monitor monitor = new IndexedIdGenerator.Monitor.Adapter()
        {
            private boolean first = true;

            @Override
            public void cached( long cachedId, int numberOfIds )
            {
                if ( first )
                {
                    barrier.reached();
                    first = false;
                }
                super.cached( cachedId, numberOfIds );
            }
        };
        open( Config.defaults(), monitor, writable(), SINGLE_IDS );
        idGenerator.start( NO_FREE_IDS, NULL );
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            for ( int i = 0; i < 5; i++ )
            {
                marker.markDeleted( i );
                marker.markFree( i );
            }
        }

        // when
        try ( OtherThreadExecutor t2 = new OtherThreadExecutor( "T2" );
              OtherThreadExecutor t3 = new OtherThreadExecutor( "T3" ) )
        {
            Future<Object> t2Future = t2.executeDontWait( () ->
            {
                idGenerator.maintenance( NULL );
                return null;
            } );
            barrier.await();

            // check that a maintenance call blocks
            Future<Object> t3Future = t3.executeDontWait( () ->
            {
                idGenerator.maintenance( NULL );
                return null;
            } );
            t3.waitUntilWaiting( details -> details.isAt( FreeIdScanner.class, "tryLoadFreeIdsIntoCache" ) );
            barrier.release();
            t2Future.get();
            t3Future.get();
        }
    }

    @Test
    void shouldPrioritizeFreelistOnConcurrentAllocation() throws Exception
    {
        // given
        Barrier.Control barrier = new Barrier.Control();
        AtomicInteger numReserved = new AtomicInteger();
        AtomicInteger numCached = new AtomicInteger();
        AtomicBoolean enabled = new AtomicBoolean( true );
        IndexedIdGenerator.Monitor monitor = new IndexedIdGenerator.Monitor.Adapter()
        {
            @Override
            public void markedAsReserved( long markedId, int numberOfIds )
            {
                numReserved.incrementAndGet();
            }

            @Override
            public void cached( long cachedId, int numberOfIds )
            {
                int cached = numCached.incrementAndGet();
                if ( cached == numReserved.get() && enabled.get() )
                {
                    enabled.set( false );
                    barrier.reached();
                }
            }

            @Override
            public void allocatedFromHigh( long allocatedId, int numberOfIds )
            {
                fail( "Should not allocate from high ID" );
            }
        };
        open( Config.defaults(), monitor, writable(), SINGLE_IDS );
        idGenerator.start( NO_FREE_IDS, NULL );

        // delete and free more than cache-size IDs
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            for ( int i = 0; i < IndexedIdGenerator.SMALL_CACHE_CAPACITY + 10; i++ )
            {
                marker.markDeleted( i );
                marker.markFree( i );
            }
        }

        // when
        // let one thread call nextId() and block when it has filled the cache (the above monitor will see to that it happens)
        try ( OtherThreadExecutor t2 = new OtherThreadExecutor( "T2" ) )
        {
            Future<Void> nextIdFuture = t2.executeDontWait( () ->
            {
                long id = idGenerator.nextId( NULL );
                assertEquals( IndexedIdGenerator.SMALL_CACHE_CAPACITY, id );
                return null;
            } );

            // and let another thread allocate all those IDs before the T2 thread had a chance to get one of them
            barrier.await();
            for ( int i = 0; i < numCached.get(); i++ )
            {
                idGenerator.nextId( NULL );
            }

            // then let first thread continue and it should not allocate off of high id
            barrier.release();
            nextIdFuture.get();
        }
    }

    @ValueSource( booleans = {true, false} )
    @ParameterizedTest
    void shouldAllocateRangesFromHighIdConcurrently( boolean favorSamePage ) throws IOException
    {
        // given
        int[] slotSizes = {1, 2, 4, 8};
        open( Config.defaults(), NO_MONITOR, writable(), diminishingSlotDistribution( slotSizes ) );
        idGenerator.start( NO_FREE_IDS, NULL );
        int numThreads = 4;
        BitSet[] allocatedIds = new BitSet[numThreads];
        for ( int i = 0; i < allocatedIds.length; i++ )
        {
            allocatedIds[i] = new BitSet();
        }

        // when
        Race race = new Race().withEndCondition( () -> false );
        race.addContestants( 4, t -> () ->
        {
            int size = ThreadLocalRandom.current().nextInt( 1, 8 );
            long startId = idGenerator.nextConsecutiveIdRange( size, favorSamePage, NULL );
            long endId = startId + size - 1;
            if ( favorSamePage )
            {
                assertThat( startId / IDS_PER_ENTRY ).isEqualTo( endId / IDS_PER_ENTRY );
            }
            for ( long id = startId; id <= endId; id++ )
            {
                allocatedIds[t].set( (int) id );
            }
        }, 1_000 );
        race.goUnchecked();

        // then
        int totalCount = stream( allocatedIds ).mapToInt( BitSet::cardinality ).sum();
        BitSet merged = new BitSet();
        for ( BitSet ids : allocatedIds )
        {
            merged.or( ids );
        }
        int mergedCount = merged.cardinality();
        // I.e. no overlapping ids
        assertThat( mergedCount ).isEqualTo( totalCount );
    }

    @Test
    void shouldSkipLastIdsOfRangeIfAllocatingFromHighIdAcrossPageBoundary() throws IOException
    {
        // given
        open( Config.defaults(), NO_MONITOR, writable(), diminishingSlotDistribution( powerTwoSlotSizesDownwards( 64 ) ) );
        idGenerator.start( NO_FREE_IDS, NULL );
        long preId1 = idGenerator.nextConsecutiveIdRange( 64, true, NULL );
        long preId2 = idGenerator.nextConsecutiveIdRange( 32, true, NULL );
        long preId3 = idGenerator.nextConsecutiveIdRange( 16, true, NULL );
        assertThat( preId1 ).isEqualTo( 0 );
        assertThat( preId2 ).isEqualTo( 64 );
        assertThat( preId3 ).isEqualTo( 64 + 32 );

        // when
        long id = idGenerator.nextConsecutiveIdRange( 32, true, NULL );

        // then
        long postId = idGenerator.nextConsecutiveIdRange( 8, true, NULL );
        assertThat( id ).isEqualTo( 128 );
        assertThat( postId ).isEqualTo( 128 + 32 );
    }

    private void assertOperationPermittedInReadOnlyMode( Function<IndexedIdGenerator,Executable> operation ) throws IOException
    {
        Path file = directory.file( "existing" );
        var indexedIdGenerator =
                new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, writable(), Config.defaults(),
                        DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS );
        indexedIdGenerator.start( NO_FREE_IDS, NULL );
        indexedIdGenerator.close();

        // Start in readOnly mode
        try ( var readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readOnly(),
                Config.defaults(), DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS, NULL );
            assertDoesNotThrow( () -> operation.apply( readOnlyGenerator ) );
        }
    }

    @Test
    void shouldAllocateFromHighIdOnContentionAndNonStrict() throws Exception
    {
        // given
        stop();
        var barrier = new Barrier.Control();
        var monitor = new IndexedIdGenerator.Monitor.Adapter()
        {
            @Override
            public void markedAsReserved( long markedId, int numberOfIds )
            {
                barrier.reached();
            }
        };
        idGenerator = new IndexedIdGenerator( pageCache, file, immediate(), SchemaIdType.LABEL_TOKEN, false, () -> 0, MAX_ID, writable(),
                Config.defaults( strictly_prioritize_id_freelist, false ), "db", NULL, monitor, Sets.immutable.empty(), SINGLE_IDS );
        idGenerator.start( NO_FREE_IDS, NULL );
        var id = idGenerator.nextId( NULL );
        markUsed( id );
        markDeleted( id );
        markFree( id );

        // when
        try ( var t2 = new OtherThreadExecutor( "T2" ) )
        {
            var nextIdFuture = t2.executeDontWait( () -> idGenerator.nextId( NULL ) );
            barrier.awaitUninterruptibly();
            var id2 = idGenerator.nextId( NULL );
            assertThat( id2 ).isGreaterThan( id );
            barrier.release();
            assertThat( nextIdFuture.get() ).isEqualTo( id );
        }
    }

    private void assertOperationThrowInReadOnlyMode( Function<IndexedIdGenerator,Executable> operation )
            throws IOException
    {
        Path file = directory.file( "existing" );
        var indexedIdGenerator =
                new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, writable(), Config.defaults(),
                        DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS );
        indexedIdGenerator.start( NO_FREE_IDS, NULL );
        indexedIdGenerator.close();

        // Start in readOnly mode
        try ( var readOnlyGenerator = new IndexedIdGenerator( pageCache, file, immediate(), TestIdType.TEST, false, () -> 0, MAX_ID, readOnly(),
                Config.defaults(), DEFAULT_DATABASE_NAME, NULL, NO_MONITOR, immutable.empty(), SINGLE_IDS ) )
        {
            readOnlyGenerator.start( NO_FREE_IDS, NULL );
            var e = assertThrows( Exception.class, operation.apply( readOnlyGenerator ) );
            assertThat( e ).hasCauseInstanceOf( WriteOnReadOnlyAccessDbException.class );
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
            Allocation allocation = new Allocation( id, 1 );
            numberOfIdsOutThere -= 1;
            reallocationIds.set( allocation.id, 1, true );
        }
        assertThat( idGenerator.getHighId() - highIdBeforeReallocation ).isEqualTo( 0L );
    }

    private void restart() throws IOException
    {
        idGenerator.checkpoint( NULL );
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
            private final Random rng = new Random( random.nextLong() );

            @Override
            public void run()
            {
                // Mark ids as eligible for reuse
                int size = allocations.size();
                if ( size > 0 )
                {
                    int slot = rng.nextInt( size );
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
            private final Random rng = new Random( random.nextLong() );

            @Override
            public void run()
            {
                // Delete ids
                int size = allocations.size();
                if ( size > 0 )
                {
                    int slot = rng.nextInt( size );
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

    private Runnable allocator( int maxAllocationsAhead, ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse,
            int maxSlotSize )
    {
        return new Runnable()
        {
            private final Random rng = new Random( random.nextLong() );

            @Override
            public void run()
            {
                // Allocate ids
                if ( allocations.size() < maxAllocationsAhead )
                {
                    int size = rng.nextInt( maxSlotSize ) + 1;
                    long id = idGenerator.nextConsecutiveIdRange( size, true, NULL );
                    Allocation allocation = new Allocation( id, size );
                    allocation.markAsInUse( expectedInUse );
                    allocations.add( allocation );
                }
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
        markUsed( id, 1 );
    }

    private void markUsed( long id, int size )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markUsed( id, size );
        }
    }

    private void markDeleted( long id )
    {
        markDeleted( id, 1 );
    }

    private void markDeleted( long id, int size )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markDeleted( id, size );
        }
    }

    private void markFree( long id )
    {
        markFree( id, 1 );
    }

    private void markFree( long id, int size )
    {
        try ( Marker marker = idGenerator.marker( NULL ) )
        {
            marker.markFree( id, size );
        }
    }

    private class Allocation
    {
        private final long id;
        private final int size;
        private final AtomicBoolean deleting = new AtomicBoolean();
        private volatile boolean deleted;
        private final AtomicBoolean freeing = new AtomicBoolean();

        Allocation( long id, int size )
        {
            this.id = id;
            this.size = size;
        }

        void delete()
        {
            if ( deleting.compareAndSet( false, true ) )
            {
                markDeleted( id, size );
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
                expectedInUse.set( id, size, false );
                markFree( id, size );
                return true;
            }
            return false;
        }

        void markAsInUse( ConcurrentSparseLongBitSet expectedInUse )
        {
            expectedInUse.set( id, size, true );
            // Simulate that actual commit comes very close after allocation, in reality they are slightly more apart
            // Also this test marks all ids, regardless if they come from highId or the free-list. This to simulate more real-world
            // scenario and to exercise the idempotent clearing feature.
            markUsed( id, size );
        }

        @Override
        public String toString()
        {
            return format( "{id:%d, slots:%d}", id, size );
        }
    }
}
