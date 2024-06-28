/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.id.indexed;

import static java.lang.String.format;
import static java.util.Arrays.stream;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.annotations.documented.ReporterFactories.noopReporterFactory;
import static org.neo4j.collection.PrimitiveLongCollections.count;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.strictly_prioritize_id_freelist;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.IndexedIdGeneratorUnsafe.changeHeaderDataLength;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.FreeIds.NO_FREE_IDS;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.internal.id.IdSlotDistribution.diminishingSlotDistribution;
import static org.neo4j.internal.id.IdSlotDistribution.evenSlotDistribution;
import static org.neo4j.internal.id.IdSlotDistribution.powerTwoSlotSizesDownwards;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.SMALL_CACHE_CAPACITY;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.id.TestIdType;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
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
import org.neo4j.time.Clocks;

@PageCacheExtension
@ExtendWith({RandomExtension.class, LifeExtension.class})
class IndexedIdGeneratorTest {
    private static final long MAX_ID = 0x3_00000000L;
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    @Inject
    private LifeSupport lifeSupport;

    private IndexedIdGenerator idGenerator;
    private Path file;

    @BeforeEach
    void getFile() {
        file = directory.file("file");
    }

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return immutable.empty();
    }

    private void open() {
        open(customization());
    }

    private void open(Customization customization) {
        idGenerator = openIdGenerator(customization);
    }

    private IndexedIdGenerator openIdGenerator(Customization customization) {
        return new IndexedIdGenerator(
                pageCache,
                fileSystem,
                customization.file,
                immediate(),
                customization.idType,
                false,
                () -> 0,
                MAX_ID,
                customization.readOnly,
                customization.config,
                DEFAULT_DATABASE_NAME,
                CONTEXT_FACTORY,
                customization.monitor,
                getOpenOptions(),
                customization.slotDistribution,
                PageCacheTracer.NULL,
                true,
                true);
    }

    @AfterEach
    void stop() {
        if (idGenerator != null) {
            idGenerator.close();
            idGenerator = null;
        }
    }

    @Test
    void shouldAllocateFreedSingleIdSlot() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long id = idGenerator.nextId(NULL_CONTEXT);
        markDeleted(id);
        markFree(id);

        // when
        idGenerator.maintenance(NULL_CONTEXT);
        long nextTimeId = idGenerator.nextId(NULL_CONTEXT);

        // then
        assertEquals(id, nextTimeId);
    }

    @Test
    void shouldNotAllocateFreedIdUntilReused() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long id = idGenerator.nextId(NULL_CONTEXT);
        markDeleted(id);
        long otherId = idGenerator.nextId(NULL_CONTEXT);
        assertNotEquals(id, otherId);

        // when
        markFree(id);

        // then
        idGenerator.maintenance(NULL_CONTEXT);
        long reusedId = idGenerator.nextId(NULL_CONTEXT);
        assertEquals(id, reusedId);
    }

    @Test
    void shouldHandleSlotsLargerThanOne() throws IOException {
        // given
        int[] slotSizes = {1, 2, 4};
        open(customization().with(diminishingSlotDistribution(slotSizes)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        // when
        int firstSize = 2;
        int secondSize = 4;
        long firstId = idGenerator.nextConsecutiveIdRange(firstSize, true, NULL_CONTEXT);
        assertThat(firstId).isEqualTo(0);
        long secondId = idGenerator.nextConsecutiveIdRange(secondSize, true, NULL_CONTEXT);
        assertThat(secondId).isEqualTo(firstId + firstSize);
        markUsed(firstId, firstSize);
        markUsed(secondId, secondSize);
        markDeleted(firstId, firstSize);
        markDeleted(secondId, secondSize);
        markFree(firstId, firstSize);
        markFree(secondId, secondSize);
        idGenerator.maintenance(NULL_CONTEXT);

        // then
        assertThat(idGenerator.nextConsecutiveIdRange(4, true, NULL_CONTEXT)).isEqualTo(2);
        assertThat(idGenerator.nextConsecutiveIdRange(4, true, NULL_CONTEXT)).isEqualTo(6);
        assertThat(idGenerator.nextConsecutiveIdRange(2, true, NULL_CONTEXT)).isEqualTo(0);
    }

    @Test
    void shouldStayConsistentAndNotLoseIdsInConcurrent_Allocate_Delete_Free() throws Throwable {
        // given
        int maxSlotSize = 4;
        open(customization()
                .with(evenSlotDistribution(powerTwoSlotSizesDownwards(maxSlotSize)))
                .with(Config.defaults(strictly_prioritize_id_freelist, true)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        Race race = new Race().withMaxDuration(1, TimeUnit.SECONDS);
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet(IDS_PER_ENTRY);
        race.addContestants(6, allocator(500, allocations, expectedInUse, maxSlotSize, idGenerator, () -> 0));
        race.addContestants(2, deleter(allocations));
        race.addContestants(2, freer(allocations, expectedInUse));

        // when
        race.go();

        // then
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(count(idGenerator.notUsedIdsIterator()));
        if (maxSlotSize == 1) {
            verifyReallocationDoesNotIncreaseHighId(allocations, expectedInUse);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8, 16})
    void shouldStayConsistentAndNotLoseIdsInConcurrentAllocateDeleteFreeClearCache(int maxSlotSize) throws Throwable {
        // given
        open(customization()
                .with(Config.defaults(strictly_prioritize_id_freelist, true))
                .with(diminishingSlotDistribution(powerTwoSlotSizesDownwards(maxSlotSize))));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        Race race = new Race().withMaxDuration(3, TimeUnit.SECONDS);
        ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
        ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet(IDS_PER_ENTRY);
        race.addContestants(6, allocator(500, allocations, expectedInUse, maxSlotSize, idGenerator, () -> 0));
        race.addContestants(2, deleter(allocations));
        race.addContestants(2, freer(allocations, expectedInUse));
        race.addContestant(throwing(() -> {
            Thread.sleep(300);
            idGenerator.clearCache(true, NULL_CONTEXT);
        }));

        // when
        race.go();

        // then
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(count(idGenerator.notUsedIdsIterator()));
        if (maxSlotSize == 1) {
            verifyReallocationDoesNotIncreaseHighId(allocations, expectedInUse);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleAllocateReservedMaxIntId(boolean caresAboutReservedId) throws IOException {
        // given
        open(customization().with(idType(caresAboutReservedId)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        idGenerator.setHighId(IdValidator.INTEGER_MINUS_ONE);

        // when
        long id = idGenerator.nextId(NULL_CONTEXT);

        // then
        if (caresAboutReservedId) {
            assertThat(id).isEqualTo(IdValidator.INTEGER_MINUS_ONE + 1);
        } else {
            assertThat(id).isEqualTo(IdValidator.INTEGER_MINUS_ONE);
        }
        assertThat(IdValidator.isReservedId(id)).isEqualTo(!caresAboutReservedId);
    }

    @Test
    void shouldNotGoBeyondMaxId() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        idGenerator.setHighId(MAX_ID - 1);

        // when
        long oneBelowMaxId = idGenerator.nextId(NULL_CONTEXT);
        assertEquals(MAX_ID - 1, oneBelowMaxId);
        long maxId = idGenerator.nextId(NULL_CONTEXT);
        assertEquals(MAX_ID, maxId);

        // then
        assertThrows(IdCapacityExceededException.class, () -> idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldIterateOverFreeIds() throws IOException {
        // given
        open();
        idGenerator.start(freeIds(10, 20, 30, IDS_PER_ENTRY + 10, 10 * IDS_PER_ENTRY + 10), NULL_CONTEXT);
        // when/then
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator()) {
            assertEquals(10L, freeIds.next());
            assertEquals(20L, freeIds.next());
            assertEquals(30L, freeIds.next());
            assertEquals(IDS_PER_ENTRY + 10L, freeIds.next());
            assertEquals(10 * IDS_PER_ENTRY + 10L, freeIds.next());
            assertFalse(freeIds.hasNext());
        }
    }

    @Test
    void shouldHandleNoFreeIdsInIterator() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        // when/then
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator()) {
            assertFalse(freeIds.hasNext());
        }
    }

    @Test
    void shouldIterateOverFreeIdsWithLimits() throws IOException {
        // given
        open();
        idGenerator.start(freeIds(10, 20, 30), NULL_CONTEXT);
        // when/then
        // simple cases
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator(5, 15)) {
            assertEquals(10L, freeIds.next());
            assertFalse(freeIds.hasNext());
        }
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator(15, 35)) {
            assertEquals(20L, freeIds.next());
            assertEquals(30L, freeIds.next());
            assertFalse(freeIds.hasNext());
        }
        // edge cases inclusiveFrom exclusiveTo
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator(0, 10)) {
            assertFalse(freeIds.hasNext());
        }
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator(10, 20)) {
            assertEquals(10L, freeIds.next());
            assertFalse(freeIds.hasNext());
        }
        // looking for only one id
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator(10, 10)) {
            assertTrue(freeIds.hasNext());
            assertEquals(10L, freeIds.next());
            assertFalse(freeIds.hasNext());
        }
        try (PrimitiveLongResourceIterator freeIds = idGenerator.notUsedIdsIterator(15, 15)) {
            assertFalse(freeIds.hasNext());
        }
    }

    @Test
    void shouldIterateOverFreeIdsWithLimitsSpecial() throws IOException {
        // given
        open();
        var id = IDS_PER_ENTRY + 10;
        idGenerator.start(freeIds(id), NULL_CONTEXT);

        // when
        try (var freeIds = idGenerator.notUsedIdsIterator(20, id + 10)) {
            assertThat(freeIds.hasNext()).isTrue();
            assertThat(freeIds.next()).isEqualTo(id);
            assertThat(freeIds.hasNext()).isFalse();
        }
    }

    @Test
    void shouldRebuildFromFreeIdsIfWasCreated() throws IOException {
        // given
        open();

        // when
        idGenerator.start(freeIds(10, 20, 30), NULL_CONTEXT);

        // then
        assertEquals(10L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(20L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(30L, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldRebuildFromFreeIdsIfWasCreatedAndSomeUpdatesWereMadeDuringRecovery() throws IOException {
        // given that it was created in this test right now, we know that
        // and given some updates before calling start (coming from recovery)
        open();
        markUsed(5);
        markUsed(100);

        // when
        idGenerator.start(freeIds(10, 20, 30), NULL_CONTEXT);

        // then
        assertEquals(10L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(20L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(30L, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldRebuildFromFreeIdsIfExistedButAtStartingGeneration() throws IOException {
        // given
        open();
        stop();
        open();

        // when
        idGenerator.start(freeIds(10, 20, 30), NULL_CONTEXT);

        // then
        assertEquals(10L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(20L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(30L, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldNotCheckpointAfterRebuild() throws IOException {
        // given
        open();

        // when
        var freeIdsFirstCall = freeIds(10, 20, 30);
        idGenerator.start(freeIdsFirstCall, NULL_CONTEXT);
        assertThat(freeIdsFirstCall.wasCalled).isTrue();
        stop();
        open();
        var freeIdsSecondCall = freeIds(11, 21, 31);
        idGenerator.start(freeIdsSecondCall, NULL_CONTEXT);
        assertThat(freeIdsSecondCall.wasCalled).isTrue();

        // then
        assertEquals(11L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(21L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(31L, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldNotRebuildInConsecutiveSessions() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        idGenerator.close();
        open();

        // when
        idGenerator.start(
                visitor -> {
                    throw new RuntimeException("Failing because it should not be called");
                },
                NULL_CONTEXT);

        // then
        assertEquals(0L, idGenerator.nextId(NULL_CONTEXT));
        assertEquals(1L, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldRebuildWithVariantThatProvidesUsedIds() throws IOException {
        // given
        open();
        var numberOfUsedIds = random.nextInt(100, 300);
        var usedIds = new long[numberOfUsedIds];
        long nextId = random.nextInt(5);
        for (var i = 0; i < usedIds.length; i++) {
            usedIds[i] = nextId;
            nextId += random.nextInt(1, 4);
        }

        // when
        idGenerator.start(usedIds(usedIds), NULL_CONTEXT);

        // then
        var actualFreeIds = asList(idGenerator.notUsedIdsIterator());
        var expectedFreeIds = asFreeIds(usedIds);
        assertThat(actualFreeIds).isEqualTo(expectedFreeIds);
    }

    @Test
    void shouldHandle_Used_Deleted_Used() throws IOException {
        // given
        var customization = customization();
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long id = idGenerator.nextId(NULL_CONTEXT);
        markUsed(id);
        markDeleted(id);

        // when
        markUsed(id);
        restart(customization);

        // then
        assertNotEquals(id, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldHandle_Used_Deleted_Free_Used() throws IOException {
        // given
        var customization = customization();
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long id = idGenerator.nextId(NULL_CONTEXT);
        markUsed(id);
        markDeleted(id);
        markFree(id);

        // when
        markUsed(id);
        restart(customization);

        // then
        assertNotEquals(id, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldHandle_Used_Deleted_Free_Reserved_Used() throws IOException {
        // given
        var customization = customization();
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long id = idGenerator.nextId(NULL_CONTEXT);
        markUsed(id);
        markDeleted(id);
        markFree(id);
        try (IdRangeMarker marker = idGenerator.lockAndInstantiateMarker(true, false, NULL_CONTEXT)) {
            marker.markReserved(id);
        }

        // when
        markUsed(id);
        restart(customization);

        // then
        assertNotEquals(id, idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldMarkDroppedIdsAsDeletedAndFree() throws IOException {
        // given
        var customization = customization();
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long id = idGenerator.nextId(NULL_CONTEXT);
        long droppedId = idGenerator.nextId(NULL_CONTEXT);
        long id2 = idGenerator.nextId(NULL_CONTEXT);

        // when
        try (var commitMarker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            commitMarker.markUsed(id);
            commitMarker.markUsed(id2);
        }
        restart(customization);

        // then
        assertEquals(droppedId, idGenerator.nextId(NULL_CONTEXT));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldConcurrentlyAllocateAllIdsAroundReservedIds(boolean caresAboutReservedId) throws IOException {
        // given
        open(customization().with(idType(caresAboutReservedId)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long startingId = IdValidator.INTEGER_MINUS_ONE - 100;
        idGenerator.setHighId(startingId);
        idGenerator.markHighestWrittenAtHighId();

        // when
        Race race = new Race();
        int threads = 8;
        int allocationsPerThread = 32;
        LongList[] allocatedIds = new LongList[threads];
        for (int i = 0; i < 8; i++) {
            LongArrayList list = new LongArrayList(32);
            allocatedIds[i] = list;
            race.addContestant(
                    () -> {
                        for (int j = 0; j < allocationsPerThread; j++) {
                            list.add(idGenerator.nextId(NULL_CONTEXT));
                        }
                    },
                    1);
        }
        race.goUnchecked();

        // then
        MutableLongList allIds = new LongArrayList(allocationsPerThread * threads);
        Stream.of(allocatedIds).forEach(allIds::addAll);
        allIds = allIds.sortThis();
        assertEquals(allocationsPerThread * threads, allIds.size());
        MutableLongIterator allIdsIterator = allIds.longIterator();
        long nextExpected = startingId;
        while (allIdsIterator.hasNext()) {
            assertEquals(nextExpected, allIdsIterator.next());
            do {
                nextExpected++;
            } while (caresAboutReservedId && IdValidator.isReservedId(nextExpected));
        }
    }

    @Test
    void shouldUseHighIdSupplierOnCreatingNewFile() {
        // when
        long highId = 101L;
        LongSupplier highIdSupplier = mock(LongSupplier.class);
        when(highIdSupplier.getAsLong()).thenReturn(highId);
        idGenerator = new IndexedIdGenerator(
                pageCache,
                fileSystem,
                file,
                immediate(),
                TestIdType.TEST,
                false,
                highIdSupplier,
                MAX_ID,
                false,
                Config.defaults(),
                DEFAULT_DATABASE_NAME,
                CONTEXT_FACTORY,
                NO_MONITOR,
                getOpenOptions(),
                SINGLE_IDS,
                PageCacheTracer.NULL,
                true,
                true);

        // then
        verify(highIdSupplier).getAsLong();
        assertEquals(highId, idGenerator.getHighId());
    }

    @Test
    void shouldNotUseHighIdSupplierOnOpeningNewFile() throws IOException {
        // given
        open();
        long highId = idGenerator.getHighId();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();

        // when
        LongSupplier highIdSupplier = mock(LongSupplier.class);
        when(highIdSupplier.getAsLong()).thenReturn(101L);
        idGenerator = new IndexedIdGenerator(
                pageCache,
                fileSystem,
                file,
                immediate(),
                TestIdType.TEST,
                false,
                highIdSupplier,
                MAX_ID,
                false,
                Config.defaults(),
                DEFAULT_DATABASE_NAME,
                CONTEXT_FACTORY,
                NO_MONITOR,
                getOpenOptions(),
                SINGLE_IDS,
                PageCacheTracer.NULL,
                true,
                true);

        // then
        verifyNoMoreInteractions(highIdSupplier);
        assertEquals(highId, idGenerator.getHighId());
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly() {
        open();
        Path file = directory.file("non-existing");
        final IllegalStateException e = assertThrows(
                IllegalStateException.class,
                () -> openIdGenerator(customization().with(file).readOnly()));
        assertTrue(Exceptions.contains(e, t -> t instanceof TreeFileNotFoundException));
    }

    @Test
    void shouldStartInReadOnlyModeIfEmpty() throws IOException {
        Path file = directory.file("existing");
        try (var indexedIdGenerator = openIdGenerator(customization().with(file))) {
            indexedIdGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            indexedIdGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        // Start in readOnly mode should not throw
        try (var readOnlyGenerator = openIdGenerator(customization().with(file).readOnly())) {
            readOnlyGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        }
    }

    @Test
    void shouldNotNextIdIfReadOnly() throws IOException {
        assertOperationPermittedInReadOnlyMode(idGenerator -> () -> idGenerator.nextId(NULL_CONTEXT));
    }

    @Test
    void shouldNotMarkerIfReadOnly() throws IOException {
        assertOperationPermittedInReadOnlyMode(idGenerator -> () -> idGenerator.transactionalMarker(NULL_CONTEXT));
    }

    @Test
    void shouldNotSetHighIdIfReadOnly() throws IOException {
        assertOperationPermittedInReadOnlyMode(idGenerator -> () -> idGenerator.setHighId(1));
    }

    @Test
    void shouldNotMarkHighestWrittenAtHighIdIfReadOnly() throws IOException {
        assertOperationThrowInReadOnlyMode(idGenerator -> idGenerator::markHighestWrittenAtHighId);
    }

    @Test
    void shouldInvokeMonitorOnCorrectCalls() throws IOException {
        IndexedIdGenerator.Monitor monitor = mock(IndexedIdGenerator.Monitor.class);
        open(customization().with(monitor));
        verify(monitor).opened(-1, 0, 0);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        long allocatedHighId = idGenerator.nextId(NULL_CONTEXT);
        verify(monitor).allocatedFromHigh(allocatedHighId, 1);

        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markUsed(allocatedHighId);
            verify(monitor).markedAsUsed(allocatedHighId, 1);
            marker.markDeleted(allocatedHighId);
            verify(monitor).markedAsDeleted(allocatedHighId, 1);
        }
        try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
            marker.markFree(allocatedHighId);
            // IDs marked as free can go directly to cache w/o getting marked as free
            verify(monitor, never()).markedAsFree(allocatedHighId, 1);
        }

        idGenerator.maintenance(NULL_CONTEXT);
        long reusedId = idGenerator.nextId(NULL_CONTEXT);
        verify(monitor).allocatedFromReused(reusedId, 1);
        idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        // two times, one in start and one now in checkpoint
        verify(monitor, times(1)).checkpoint(anyLong(), anyLong());
        idGenerator.clearCache(true, NULL_CONTEXT);
        verify(monitor).clearingCache();
        verify(monitor).clearedCache();

        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markUsed(allocatedHighId + 3);
            verify(monitor).bridged(allocatedHighId + 1, 2);
        }

        stop();
        verify(monitor).close();

        // Also test normalization (which requires a restart)
        open(customization().with(monitor));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markUsed(allocatedHighId + 1);
        }
        verify(monitor).normalized(0);
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck() {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        idGenerator.consistencyCheck(
                noopReporterFactory(),
                cursorContextFactory,
                Runtime.getRuntime().availableProcessors());

        assertThat(pageCacheTracer.hits()).isEqualTo(2);
        assertThat(pageCacheTracer.pins()).isEqualTo(2);
        assertThat(pageCacheTracer.unpins()).isEqualTo(2);
    }

    @Test
    void noPageCacheActivityWithNoMaintenanceOnOnNextId() {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("noPageCacheActivityWithNoMaintenanceOnOnNextId"))) {
            idGenerator.nextId(cursorContext);

            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.hits()).isZero();
            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
        }
    }

    @Test
    void tracePageCacheActivityOnOnNextId() throws IOException {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("noPageCacheActivityWithNoMaintenanceOnOnNextId"))) {
            idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            markDeleted(1);
            idGenerator.clearCache(true, NULL_CONTEXT);
            idGenerator.maintenance(cursorContext);

            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.hits()).isOne();
            assertThat(cursorTracer.pins()).isOne();
            assertThat(cursorTracer.unpins()).isOne();
        }
    }

    @Test
    void tracePageCacheActivityWhenMark() throws IOException {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext =
                CONTEXT_FACTORY.create(pageCacheTracer.createPageCursorTracer("tracePageCacheActivityWhenMark"))) {
            idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
            assertThat(cursorTracer.hits()).isZero();

            try (var marker = idGenerator.transactionalMarker(cursorContext)) {
                marker.markDeleted(1);
            }
            assertThat(cursorTracer.pins()).isGreaterThanOrEqualTo(1);
            assertThat(cursorTracer.unpins()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorCacheClear() throws IOException {
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("tracePageCacheOnIdGeneratorCacheClear"))) {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
            assertThat(cursorTracer.hits()).isZero();
            markUsed(1);

            markDeleted(1);
            markFree(1);
            idGenerator.maintenance(NULL_CONTEXT);
            idGenerator.clearCache(true, cursorContext);

            assertThat(cursorTracer.pins()).isEqualTo(2);
            assertThat(cursorTracer.unpins()).isEqualTo(2);
            assertThat(cursorTracer.hits()).isEqualTo(2);
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorMaintenance() throws IOException {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("tracePageCacheOnIdGeneratorMaintenance"))) {
            idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
            assertThat(cursorTracer.hits()).isZero();

            idGenerator.maintenance(cursorContext);

            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
            assertThat(cursorTracer.hits()).isZero();

            markDeleted(1);
            idGenerator.clearCache(true, NULL_CONTEXT);
            idGenerator.maintenance(cursorContext);

            assertThat(cursorTracer.pins()).isOne();
            assertThat(cursorTracer.unpins()).isOne();
            assertThat(cursorTracer.hits()).isOne();
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorCheckpoint() {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("tracePageCacheOnIdGeneratorCheckpoint"))) {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
            assertThat(cursorTracer.hits()).isZero();

            idGenerator.checkpoint(FileFlushEvent.NULL, cursorContext);

            // 2 state pages involved into checkpoint (twice)
            assertThat(cursorTracer.pins()).isEqualTo(4);
            assertThat(cursorTracer.unpins()).isEqualTo(4);
            assertThat(cursorTracer.hits()).isEqualTo(2);
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorStartWithRebuild() throws IOException {
        open();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try (var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("tracePageCacheOnIdGeneratorStartWithRebuild"))) {
            var cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isZero();
            assertThat(cursorTracer.unpins()).isZero();
            assertThat(cursorTracer.hits()).isZero();

            idGenerator.start(freeIds(1), cursorContext);

            // 2 state pages involved into checkpoint (twice) + one more pin/hit/unpin on maintenance + range marker
            // writer
            assertThat(cursorTracer.pins()).isEqualTo(4);
            assertThat(cursorTracer.unpins()).isEqualTo(4);
        }
    }

    @Test
    void tracePageCacheOnIdGeneratorStartWithoutRebuild() throws IOException {
        try (var prepareIndexWithoutRebuild = openIdGenerator(customization())) {
            prepareIndexWithoutRebuild.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }
        try (var idGenerator = openIdGenerator(customization())) {
            var pageCacheTracer = new DefaultPageCacheTracer();
            try (var cursorContext = CONTEXT_FACTORY.create(
                    pageCacheTracer.createPageCursorTracer("tracePageCacheOnIdGeneratorStartWithoutRebuild"))) {
                var cursorTracer = cursorContext.getCursorTracer();
                assertThat(cursorTracer.pins()).isZero();
                assertThat(cursorTracer.unpins()).isZero();
                assertThat(cursorTracer.hits()).isZero();

                idGenerator.start(NO_FREE_IDS, cursorContext);

                // pin/hit/unpin on maintenance
                assertThat(cursorTracer.pins()).isOne();
                assertThat(cursorTracer.unpins()).isOne();
            }
        }
    }

    @Test
    void shouldAllocateConsecutiveIdBatches() {
        // given
        open();
        AtomicInteger numAllocations = new AtomicInteger();
        Race race = new Race().withEndCondition(() -> numAllocations.get() >= 10_000);
        Collection<long[]> allocations = ConcurrentHashMap.newKeySet();
        race.addContestants(4, () -> {
            int size = ThreadLocalRandom.current().nextInt(10, 1_000);
            long batchStartId = idGenerator.nextConsecutiveIdRange(size, false, NULL_CONTEXT);
            allocations.add(new long[] {batchStartId, size});
            numAllocations.incrementAndGet();
        });

        // when
        race.goUnchecked();

        // then
        long[][] sortedAllocations = allocations.toArray(new long[allocations.size()][]);
        Arrays.sort(sortedAllocations, comparingLong(a -> a[0]));
        long prevEndExclusive = 0;
        for (long[] allocation : sortedAllocations) {
            assertEquals(prevEndExclusive, allocation[0]);
            prevEndExclusive = allocation[0] + allocation[1];
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleAllocateReservedIdsInBatchedAllocation(boolean caresAboutReservedId) throws IOException {
        // given
        open(customization().with(idType(caresAboutReservedId)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        idGenerator.setHighId(IdValidator.INTEGER_MINUS_ONE - 100);

        // when
        int numberOfIds = 200;
        long batchStartId = idGenerator.nextConsecutiveIdRange(numberOfIds, false, NULL_CONTEXT);

        // then
        assertThat(IdValidator.hasReservedIdInRange(batchStartId, batchStartId + numberOfIds))
                .isEqualTo(!caresAboutReservedId);
    }

    @Test
    void shouldAwaitConcurrentOngoingMaintenanceIfToldTo() throws Exception {
        // given
        Barrier.Control barrier = new Barrier.Control();
        AtomicBoolean enabled = new AtomicBoolean(false);
        IndexedIdGenerator.Monitor monitor = new IndexedIdGenerator.Monitor.Adapter() {
            @Override
            public void cached(long cachedId, int numberOfIds) {
                if (enabled.compareAndSet(true, false)) {
                    barrier.reached();
                }
                super.cached(cachedId, numberOfIds);
            }
        };
        open(customization().with(monitor));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            for (int i = 0; i < SMALL_CACHE_CAPACITY + 10; i++) {
                marker.markDeletedAndFree(i);
            }
        }
        for (int i = 0; i < 12; i++) {
            idGenerator.nextId(NULL_CONTEXT);
        }
        // Now the cache shouldn't be full and there should be some IDs that maintenance could load

        // when
        enabled.set(true);
        try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2");
                OtherThreadExecutor t3 = new OtherThreadExecutor("T3")) {
            Future<Object> t2Future = t2.executeDontWait(() -> {
                idGenerator.maintenance(NULL_CONTEXT);
                return null;
            });
            barrier.await();

            // check that a maintenance call blocks
            Future<Object> t3Future = t3.executeDontWait(() -> {
                idGenerator.maintenance(NULL_CONTEXT);
                return null;
            });
            t3.waitUntilWaiting(details -> details.isAt(FreeIdScanner.class, "tryLoadFreeIdsIntoCache"));
            barrier.release();
            t2Future.get();
            t3Future.get();
        }
    }

    @Test
    void shouldPrioritizeFreelistOnConcurrentAllocation() throws Exception {
        // given
        Barrier.Control barrier = new Barrier.Control();
        AtomicInteger numCached = new AtomicInteger();
        AtomicBoolean enabled = new AtomicBoolean(false);
        IndexedIdGenerator.Monitor monitor = new IndexedIdGenerator.Monitor.Adapter() {
            @Override
            public void cached(long cachedId, int numberOfIds) {
                if (enabled.get()) {
                    enabled.set(false);
                    barrier.reached();
                }
            }

            @Override
            public void allocatedFromHigh(long allocatedId, int numberOfIds) {
                fail("Should not allocate from high ID");
            }
        };
        open(customization().with(monitor));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        // delete and free more than cache-size IDs
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            for (int i = 0; i < SMALL_CACHE_CAPACITY * 2 + 10; i++) {
                marker.markDeletedAndFree(i);
            }
        }

        // when
        // let one thread call nextId() and block when it has filled the cache (the above monitor will see to that it
        // happens)
        enabled.set(true);
        try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2")) {
            Future<Void> nextIdFuture = t2.executeDontWait(() -> {
                for (int i = 0; i < SMALL_CACHE_CAPACITY + 10; i++) {
                    idGenerator.nextId(NULL_CONTEXT);
                }
                return null;
            });

            // and let another thread allocate all those IDs before the T2 thread had a chance to get one of them
            barrier.await();
            for (int i = 0; i < numCached.get(); i++) {
                idGenerator.nextId(NULL_CONTEXT);
            }

            // then let first thread continue and it should not allocate off of high id
            barrier.release();
            nextIdFuture.get();
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldAllocateRangesFromHighIdConcurrently(boolean favorSamePage) throws IOException {
        // given
        int[] slotSizes = {1, 2, 4, 8};
        open(customization().with(diminishingSlotDistribution(slotSizes)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        int numThreads = 4;
        BitSet[] allocatedIds = new BitSet[numThreads];
        for (int i = 0; i < allocatedIds.length; i++) {
            allocatedIds[i] = new BitSet();
        }

        // when
        Race race = new Race().withEndCondition(() -> false);
        race.addContestants(
                4,
                t -> () -> {
                    int size = ThreadLocalRandom.current().nextInt(1, 8);
                    long startId = idGenerator.nextConsecutiveIdRange(size, favorSamePage, NULL_CONTEXT);
                    long endId = startId + size - 1;
                    if (favorSamePage) {
                        assertThat(startId / IDS_PER_ENTRY).isEqualTo(endId / IDS_PER_ENTRY);
                    }
                    for (long id = startId; id <= endId; id++) {
                        allocatedIds[t].set((int) id);
                    }
                },
                1_000);
        race.goUnchecked();

        // then
        int totalCount = stream(allocatedIds).mapToInt(BitSet::cardinality).sum();
        BitSet merged = new BitSet();
        for (BitSet ids : allocatedIds) {
            merged.or(ids);
        }
        int mergedCount = merged.cardinality();
        // I.e. no overlapping ids
        assertThat(mergedCount).isEqualTo(totalCount);
    }

    @Test
    void shouldSkipLastIdsOfRangeIfAllocatingFromHighIdAcrossPageBoundary() throws IOException {
        // given
        open(customization().with(diminishingSlotDistribution(powerTwoSlotSizesDownwards(64))));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long preId1 = idGenerator.nextConsecutiveIdRange(64, true, NULL_CONTEXT);
        long preId2 = idGenerator.nextConsecutiveIdRange(32, true, NULL_CONTEXT);
        long preId3 = idGenerator.nextConsecutiveIdRange(16, true, NULL_CONTEXT);
        assertThat(preId1).isEqualTo(0);
        assertThat(preId2).isEqualTo(64);
        assertThat(preId3).isEqualTo(64 + 32);

        // when
        long id = idGenerator.nextConsecutiveIdRange(32, true, NULL_CONTEXT);

        // then
        long postId = idGenerator.nextConsecutiveIdRange(8, true, NULL_CONTEXT);
        assertThat(id).isEqualTo(128);
        // the skipped ID ends up in the cache and will therefore be handed out here
        assertThat(postId).isEqualTo(64 + 32 + 16);
    }

    @Test
    void shouldHandleClusterLikeStressfullLoad() throws IOException {
        // given
        int maxSlotSize = 16;
        try (var clusteredIdGenerator =
                new ClusteredIdGenerator(3, evenSlotDistribution(powerTwoSlotSizesDownwards(maxSlotSize)))) {
            clusteredIdGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

            Race race = new Race().withMaxDuration(5, TimeUnit.SECONDS);
            ConcurrentLinkedQueue<Allocation> allocations = new ConcurrentLinkedQueue<>();
            ConcurrentSparseLongBitSet expectedInUse = new ConcurrentSparseLongBitSet(IDS_PER_ENTRY);
            race.addContestants(
                    4,
                    allocator(
                            500,
                            allocations,
                            expectedInUse,
                            maxSlotSize,
                            clusteredIdGenerator,
                            clusteredIdGenerator::currentLeaseId));
            race.addContestants(2, deleter(allocations));
            race.addContestants(2, freer(allocations, expectedInUse));
            race.addContestant(throwing(() -> {
                Thread.sleep(ThreadLocalRandom.current().nextInt(500));
                clusteredIdGenerator.maintenance(NULL_CONTEXT);
            }));
            race.addContestant(throwing(() -> {
                Thread.sleep(ThreadLocalRandom.current().nextInt(500));
                clusteredIdGenerator.switchLeader();
            }));
            race.goUnchecked();
        }
    }

    /**
     * A contrived view of how a cluster interacts with an ID generator.
     * There's some notion of lease and leader switches. This should make it possible to trigger
     * cases that other single-instance tests could not.
     */
    private class ClusteredIdGenerator implements IdGenerator {
        private final IdGenerator[] members;
        private final AtomicInteger leaderIndex = new AtomicInteger();
        private final AtomicInteger leaseId = new AtomicInteger();
        private final ReadWriteLock leaderSwitchLock = new ReentrantReadWriteLock();

        ClusteredIdGenerator(int size, IdSlotDistribution slotDistribution) {
            var config = Config.defaults();
            members = new IdGenerator[size];
            for (int i = 0; i < size; i++) {
                var file = directory.file("id-generator-" + i);
                var monitor = new LoggingIndexedIdGeneratorMonitor(
                        fileSystem,
                        file.resolveSibling(file.getFileName().toString() + ".log"),
                        Clocks.nanoClock(),
                        100,
                        ByteUnit.MebiByte,
                        1,
                        TimeUnit.HOURS);
                members[i] = openIdGenerator(customization()
                        .with(config)
                        .with(monitor)
                        .with(slotDistribution)
                        .with(file));
            }
        }

        private IdGenerator leader() {
            return members[leaderIndex.get()];
        }

        private <T, E extends Exception> T withReadLock(ThrowingSupplier<T, E> action) throws E {
            leaderSwitchLock.readLock().lock();
            try {
                return action.get();
            } finally {
                leaderSwitchLock.readLock().unlock();
            }
        }

        private <E extends Exception> void withReadLockNoResult(ThrowingAction<E> action) throws E {
            withReadLock(() -> {
                action.apply();
                return null;
            });
        }

        @Override
        public long nextId(CursorContext cursorContext) {
            return withReadLock(() -> leader().nextId(cursorContext));
        }

        @Override
        public long nextConsecutiveIdRange(int numberOfIds, boolean favorSamePage, CursorContext cursorContext) {
            return withReadLock(() -> leader().nextConsecutiveIdRange(numberOfIds, favorSamePage, cursorContext));
        }

        @Override
        public PageIdRange nextPageRange(CursorContext cursorContext, int idsPerPage) {
            return withReadLock(() -> leader().nextPageRange(cursorContext, idsPerPage));
        }

        @Override
        public void releasePageRange(PageIdRange range, CursorContext cursorContext) {
            withReadLockNoResult(() -> leader().releasePageRange(range, cursorContext));
        }

        @Override
        public void setHighId(long id) {
            withReadLockNoResult(() -> leader().setHighId(id));
        }

        @Override
        public void markHighestWrittenAtHighId() {
            withReadLockNoResult(() -> leader().markHighestWrittenAtHighId());
        }

        @Override
        public long getHighestWritten() {
            return withReadLock(() -> leader().getHighestWritten());
        }

        @Override
        public long getHighId() {
            return withReadLock(() -> leader().getHighId());
        }

        @Override
        public long getHighestPossibleIdInUse() {
            return withReadLock(() -> leader().getHighestPossibleIdInUse());
        }

        @Override
        public long getUnusedIdCount() {
            return withReadLock(() -> leader().getUnusedIdCount());
        }

        @Override
        public TransactionalMarker transactionalMarker(CursorContext cursorContext) {
            leaderSwitchLock.readLock().lock();
            var markers = new TransactionalMarker[members.length];
            for (int i = 0; i < markers.length; i++) {
                markers[i] = members[i].transactionalMarker(cursorContext);
            }
            return new TransactionalMarker() {
                private long highestUsedId = -1;

                @Override
                public void markUsed(long id, int numberOfIds) {
                    for (var marker : markers) {
                        marker.markUsed(id, numberOfIds);
                    }
                    highestUsedId = Math.max(highestUsedId, id + numberOfIds);
                }

                @Override
                public void markDeleted(long id, int numberOfIds) {
                    for (var marker : markers) {
                        marker.markDeleted(id, numberOfIds);
                    }
                }

                @Override
                public void markDeletedAndFree(long id, int numberOfIds) {
                    for (var marker : markers) {
                        marker.markDeletedAndFree(id, numberOfIds);
                    }
                }

                @Override
                public void markUnallocated(long id, int numberOfIds) {
                    for (var marker : markers) {
                        marker.markUnallocated(id, numberOfIds);
                    }
                }

                @Override
                public void close() {
                    try {
                        IOUtils.closeAllUnchecked(markers);
                        // Update high ID (this is what followers do when replicating transactions from leader)
                        if (highestUsedId != -1) {
                            for (int i = 0; i < members.length; i++) {
                                if (i != leaderIndex.get()) {
                                    members[i].setHighestPossibleIdInUse(highestUsedId);
                                }
                            }
                        }
                    } finally {
                        leaderSwitchLock.readLock().unlock();
                    }
                }
            };
        }

        @Override
        public ContextualMarker contextualMarker(CursorContext cursorContext) {
            leaderSwitchLock.readLock().lock();
            var marker = leader().contextualMarker(cursorContext);
            return new ContextualMarker() {
                @Override
                public void markFree(long id, int numberOfIds) {
                    marker.markFree(id, numberOfIds);
                }

                @Override
                public void markReserved(long id, int numberOfIds) {
                    marker.markReserved(id, numberOfIds);
                }

                @Override
                public void markUnreserved(long id, int numberOfIds) {
                    marker.markUnreserved(id, numberOfIds);
                }

                @Override
                public void markUncached(long id, int numberOfIds) {
                    marker.markUncached(id, numberOfIds);
                }

                @Override
                public void flush() {
                    marker.flush();
                }

                @Override
                public void close() {
                    try {
                        marker.close();
                    } finally {
                        leaderSwitchLock.readLock().unlock();
                    }
                }
            };
        }

        @Override
        public void close() {
            IOUtils.closeAllUnchecked(members);
        }

        @Override
        public void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext) {
            for (var member : members) {
                member.checkpoint(flushEvent, cursorContext);
            }
        }

        @Override
        public void maintenance(CursorContext cursorContext) {
            for (var member : members) {
                member.maintenance(cursorContext);
            }
        }

        @Override
        public void start(FreeIds freeIdsForRebuild, CursorContext cursorContext) throws IOException {
            for (var member : members) {
                member.start(freeIdsForRebuild, cursorContext);
            }
        }

        @Override
        public void clearCache(boolean allocationEnabled, CursorContext cursorContext) {
            throw new UnsupportedOperationException(
                    "Don't call this directly for this class, instead call switchLeader");
        }

        int currentLeaseId() {
            return leaseId.get();
        }

        void switchLeader() {
            leaderSwitchLock.writeLock().lock();
            try {
                int newLeaderIndex;
                do {
                    newLeaderIndex = random.nextInt(members.length);
                } while (newLeaderIndex == leaderIndex.get());

                for (int i = 0; i < members.length; i++) {
                    boolean allocationEnabled = i == newLeaderIndex;
                    members[i].clearCache(allocationEnabled, NULL_CONTEXT);
                }
                leaderIndex.set(newLeaderIndex);
                leaseId.incrementAndGet();
            } finally {
                leaderSwitchLock.writeLock().unlock();
            }
        }

        @Override
        public boolean allocationEnabled() {
            return leader().allocationEnabled();
        }

        @Override
        public IdType idType() {
            return leader().idType();
        }

        @Override
        public boolean hasOnlySingleIds() {
            return leader().hasOnlySingleIds();
        }

        @Override
        public boolean consistencyCheck(
                ReporterFactory reporterFactory,
                CursorContextFactory contextFactory,
                int numThreads,
                ProgressMonitorFactory progressMonitorFactory) {
            boolean consistent = true;
            for (var member : members) {
                consistent &=
                        member.consistencyCheck(reporterFactory, contextFactory, numThreads, progressMonitorFactory);
            }
            return consistent;
        }
    }

    private void assertOperationPermittedInReadOnlyMode(Function<IndexedIdGenerator, Executable> operation)
            throws IOException {
        Path file = directory.file("existing");
        try (var indexedIdGenerator = openIdGenerator(customization().with(file))) {
            indexedIdGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            indexedIdGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        // Start in readOnly mode
        try (var readOnlyGenerator = openIdGenerator(customization().with(file).readOnly())) {
            readOnlyGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            assertDoesNotThrow(() -> operation.apply(readOnlyGenerator));
        }
    }

    @Test
    void shouldReuseRolledbackIdAllocatedFromHighId() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        var id = idGenerator.nextId(NULL_CONTEXT);
        markUnallocated(id);
        idGenerator.maintenance(NULL_CONTEXT);

        // when
        var idAfterUnallocated = idGenerator.nextId(NULL_CONTEXT);

        // then
        assertThat(idAfterUnallocated).isEqualTo(id);
    }

    @Test
    void shouldReuseRolledbackIdAllocatedFromHighIdAfterHigherCommittedIds() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        var id = idGenerator.nextId(NULL_CONTEXT);
        var otherId = idGenerator.nextId(NULL_CONTEXT);
        markUsed(otherId);
        markUnallocated(id);
        idGenerator.maintenance(NULL_CONTEXT);

        // when
        var idAfterUnallocated = idGenerator.nextId(NULL_CONTEXT);

        // then
        assertThat(idAfterUnallocated).isEqualTo(id);
    }

    @Test
    void shouldReuseRolledbackIdAllocatedFromReusedId() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        var id1 = idGenerator.nextId(NULL_CONTEXT);
        var id2 = idGenerator.nextId(NULL_CONTEXT);
        var id3 = idGenerator.nextId(NULL_CONTEXT);
        markUsed(id1);
        markUsed(id2);
        markUsed(id3);
        markDeleted(id2);
        markFree(id2);
        idGenerator.maintenance(NULL_CONTEXT);
        assertThat(idGenerator.nextId(NULL_CONTEXT)).isEqualTo(id2);
        markUnallocated(id2);
        idGenerator.maintenance(NULL_CONTEXT);

        // when
        var idAfterUnallocated = idGenerator.nextId(NULL_CONTEXT);

        // then
        assertThat(idAfterUnallocated).isEqualTo(id2);
    }

    @Test
    void shouldAllocateFromHighIdOnContentionAndNonStrict() throws Exception {
        // given
        var barrier = new Barrier.Control();
        var monitor = new IndexedIdGenerator.Monitor.Adapter() {
            @Override
            public void markedAsReserved(long markedId, int numberOfIds) {
                barrier.reached();
            }
        };
        open(customization().with(monitor).with(Config.defaults(strictly_prioritize_id_freelist, false)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        var id = idGenerator.nextId(NULL_CONTEXT);
        markUsed(id);
        markDeleted(id);
        markFree(id);
        idGenerator.clearCache(true, NULL_CONTEXT);

        // when
        try (var t2 = new OtherThreadExecutor("T2")) {
            var nextIdFuture = t2.executeDontWait(() -> idGenerator.nextId(NULL_CONTEXT));
            barrier.awaitUninterruptibly();
            var id2 = idGenerator.nextId(NULL_CONTEXT);
            assertThat(id2).isGreaterThan(id);
            barrier.release();
            assertThat(nextIdFuture.get()).isEqualTo(id);
        }
    }

    @Test
    void shouldCountUnusedIds() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        var id1 = idGenerator.nextId(NULL_CONTEXT);
        var id2 = idGenerator.nextId(NULL_CONTEXT);
        var id3 = idGenerator.nextId(NULL_CONTEXT);
        markUsed(id1);
        markUsed(id2);
        markUsed(id3);

        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(0);

        markDeleted(id2);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(1);

        markFree(id2);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(1);

        markDeleted(id3);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(2);

        idGenerator.maintenance(NULL_CONTEXT);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(2);
        assertThat(idGenerator.nextId(NULL_CONTEXT)).isEqualTo(id2);

        markUnallocated(id2);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(2);
        idGenerator.maintenance(NULL_CONTEXT);

        var idAfterUnallocated = idGenerator.nextId(NULL_CONTEXT);
        assertThat(idAfterUnallocated).isEqualTo(id2);

        markUsed(idAfterUnallocated);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(1);
    }

    @Test
    void shouldPlaceFreedIdDirectlyInCacheWhenFreedFitsInCache() throws IOException {
        // given
        var monitor = mock(IndexedIdGenerator.Monitor.class);
        var slotSizes = new int[] {1, 2, 4, 8};
        open(customization().with(monitor).with(evenSlotDistribution(IDS_PER_ENTRY, slotSizes)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        for (var size : slotSizes) {
            var id = idGenerator.nextConsecutiveIdRange(size, true, NULL_CONTEXT);
            markUsed(id, size);
            markDeleted(id, size);

            // when
            markFree(id, size);

            // then
            verify(monitor, never()).markedAsFree(anyLong(), anyInt());
            var cachedId = idGenerator.nextConsecutiveIdRange(size, true, NULL_CONTEXT);
            assertThat(cachedId).isEqualTo(id);
        }
    }

    @Test
    void shouldMarkAsFreeWhenCannotFitInCache() throws IOException {
        // given
        var monitor = mock(IndexedIdGenerator.Monitor.class);
        var slotSizes = new int[] {1, 2, 4, 8};
        open(customization().with(monitor).with(evenSlotDistribution(IDS_PER_ENTRY, slotSizes)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long[] initialCacheFillingIds = new long[10_000];
        for (int i = 0; i < initialCacheFillingIds.length; i++) {
            initialCacheFillingIds[i] = idGenerator.nextId(NULL_CONTEXT);
        }
        long[] testableIds = new long[slotSizes.length];
        for (int i = 0; i < testableIds.length; i++) {
            testableIds[i] = idGenerator.nextConsecutiveIdRange(slotSizes[i], true, NULL_CONTEXT);
        }
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            for (var id : initialCacheFillingIds) {
                marker.markUsed(id);
                marker.markDeleted(id);
            }
        }
        try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
            for (var id : initialCacheFillingIds) {
                marker.markFree(id);
            }
        }
        idGenerator.maintenance(NULL_CONTEXT);

        reset(monitor);
        for (var i = 0; i < slotSizes.length; i++) {
            var size = slotSizes[i];
            var id = testableIds[i];
            markUsed(id, size);
            markDeleted(id, size);

            // when
            markFree(id, size);

            // then
            verify(monitor).markedAsFree(id, size);
        }
    }

    @Test
    void shouldCompleteNextIdOnAllocationDisabled() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        // NOTE: this test relies on clearCache setting "at least one free ID available" to true
        idGenerator.clearCache(false, NULL_CONTEXT);

        // when
        long id = idGenerator.nextId(NULL_CONTEXT);

        // then
        assertThat(id).isGreaterThanOrEqualTo(0);
        // although the real point of this test is that it doesn't hang in nextId
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleAllocateReservedId(boolean caresAboutReservedId) throws IOException {
        var customization = customization().with(idType(caresAboutReservedId));
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        // Set highest written slightly below reserved ID
        idGenerator.setHighId(IdValidator.INTEGER_MINUS_ONE - 10);
        idGenerator.markHighestWrittenAtHighId();
        // Mark some ID as used above high ID
        long[] ids = new long[20];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = idGenerator.nextId(NULL_CONTEXT);
        }
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            for (long id : ids) {
                marker.markUsed(id);
            }
            for (long id : ids) {
                marker.markDeleted(id);
            }
        }
        restart(customization);

        // See which IDs comes out of the ID generator
        boolean foundReservedId = false;
        for (int i = 0; i < 100; i++) {
            long id = idGenerator.nextId(NULL_CONTEXT);
            foundReservedId |= IdValidator.isReservedId(id);
        }
        assertThat(foundReservedId).isEqualTo(!caresAboutReservedId);
    }

    private void assertOperationThrowInReadOnlyMode(Function<IndexedIdGenerator, Executable> operation)
            throws IOException {
        Path file = directory.file("existing");
        try (var indexedIdGenerator = openIdGenerator(customization().with(file))) {
            indexedIdGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            indexedIdGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        // Start in readOnly mode
        try (var readOnlyGenerator = openIdGenerator(customization().with(file).readOnly())) {
            readOnlyGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            var e = assertThrows(Exception.class, operation.apply(readOnlyGenerator));
            assertThat(e).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void shouldNotLetWastedIdsSurviveClearCache() throws IOException {
        // ID generator w/ slots [4,2,1]
        int[] slotSizes = {1, 2, 4};
        open(customization().with(evenSlotDistribution(slotSizes)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        // Allocate lots of IDs and delete them (enough to fill the cache completely)
        for (int slotSize : slotSizes) {
            long[] ids = new long[150];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = idGenerator.nextConsecutiveIdRange(slotSize, true, NULL_CONTEXT);
            }
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                for (long id : ids) {
                    marker.markUsed(id, slotSize);
                }
                for (long id : ids) {
                    marker.markDeleted(id, slotSize);
                }
            }
            try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
                for (long id : ids) {
                    marker.markFree(id, slotSize);
                }
            }
        }

        // Allocate an ID X of size 3, its "waste" Y will not fit in cache so will be registered in the "waste" list
        long x = idGenerator.nextConsecutiveIdRange(3, true, NULL_CONTEXT);

        // Pretend that another member gets leader -> Call clearCache(false)
        idGenerator.clearCache(false, NULL_CONTEXT);

        // Pretend that the new leader now allocates Y -> this results in Y marked as inUse
        long y = x + 3;
        markUsed(y);

        // Pretend that this member gets leader again -> Call clearCache(true)
        idGenerator.clearCache(true, NULL_CONTEXT);

        // Allocate lots of IDs; none of them should be Y
        for (int i = 0; i < 1_000; i++) {
            long id = idGenerator.nextConsecutiveIdRange(1, true, NULL_CONTEXT);
            assertThat(id).isNotEqualTo(y);
        }
    }

    @Test
    void shouldNotLetSkippedHighIdsSurviveClearCache() throws IOException {
        // ID generator w/ slots [4,2,1]
        int[] slotSizes = {1, 2, 4};
        open(customization().with(evenSlotDistribution(slotSizes)));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

        // Allocate lots of IDs and delete them (enough to fill the cache completely)
        long lastAllocatedId = -1;
        for (int slotSize : slotSizes) {
            long[] ids = new long[150];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = idGenerator.nextConsecutiveIdRange(slotSize, true, NULL_CONTEXT);
            }
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                for (long id : ids) {
                    marker.markUsed(id, slotSize);
                }
                for (long id : ids) {
                    marker.markDeleted(id, slotSize);
                }
            }
            try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
                for (long id : ids) {
                    marker.markFree(id, slotSize);
                }
            }
            lastAllocatedId = ids[ids.length - 1] + slotSize;
        }

        // Allocate a large ID which should result in at least some skipped IDs,
        // they will be registed in the "skipped high IDs" list.
        long x = idGenerator.nextConsecutiveIdRange(IDS_PER_ENTRY, true, NULL_CONTEXT);

        // Verify that there have been some skipped high IDs as part of this allocation
        assertThat(x).isGreaterThan(lastAllocatedId + 1);
        long yFirst = lastAllocatedId + 1;
        long yLast = x - 1;

        // Pretend that another member gets leader -> Call clearCache(false)
        idGenerator.clearCache(false, NULL_CONTEXT);

        // Pretend that the new leader now allocates Y -> this results in Y marked as inUse
        markUsed(yFirst, (int) (yLast - yFirst + 1));

        // Pretend that this member gets leader again -> Call clearCache(true)
        idGenerator.clearCache(true, NULL_CONTEXT);

        // Allocate lots of IDs; none of them should be within Y
        for (int i = 0; i < 1_000; i++) {
            long id = idGenerator.nextConsecutiveIdRange(1, true, NULL_CONTEXT);
            assertThat(id).satisfiesAnyOf(_id -> assertThat(_id).isLessThan(yFirst), _id -> assertThat(_id)
                    .isGreaterThan(yLast));
        }
    }

    @Test
    void shouldCatchUpOnNumUnusedIdsOnStartupIfMissingFromHeader() throws IOException {
        // given
        var readNumUnusedIds = new MutableLong();
        var monitor = new IndexedIdGenerator.Monitor.Adapter() {
            @Override
            public void opened(long highestWrittenId, long highId, long numUnusedIds) {
                readNumUnusedIds.setValue(numUnusedIds);
            }
        };
        open(customization().with(monitor));
        assertThat(readNumUnusedIds.longValue()).isEqualTo(0);
        idGenerator.start(freeIds(1, 10, 15), NULL_CONTEXT);
        idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();
        // and opening it up now should see the correct numUnusedIds in the header
        open(customization().with(monitor));
        assertThat(readNumUnusedIds.longValue()).isEqualTo(3);
        stop();

        // when rewriting the header to have it look like it has no numUnusedIds
        changeHeaderDataLength(pageCache, file, new IdRangeLayout(IDS_PER_ENTRY), getOpenOptions(), -Long.BYTES);

        // then starting it up again should see the uninitialized numUnusedIds and then
        // bring that count up to date
        open(customization().with(monitor));
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        assertThat(readNumUnusedIds.longValue()).isEqualTo(HeaderReader.UNINITIALIZED);
        assertThat(idGenerator.getUnusedIdCount()).isEqualTo(3);
        idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();
        // and opening it up now should see the correct numUnusedIds in the header
        open(customization().with(monitor));
        assertThat(readNumUnusedIds.longValue()).isEqualTo(3);
        stop();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8})
    void shouldBridgeAcrossReservedId(int idSize) throws IOException {
        // given
        var customization = customization();
        if (idSize > 1) {
            customization.with(evenSlotDistribution(powerTwoSlotSizesDownwards(idSize)));
        }
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long lowId = IdValidator.INTEGER_MINUS_ONE - 10;
        idGenerator.setHighId(lowId);
        idGenerator.markHighestWrittenAtHighId();
        long[] ids = new long[20];
        var expectedIds = LongLists.mutable.empty();
        for (int i = 0; i < ids.length; i++) {
            ids[i] = idGenerator.nextConsecutiveIdRange(idSize, true, NULL_CONTEXT);
            if (i > 0 && ids[i] > ids[i - 1] + idSize) {
                for (long gapId = ids[i - 1] + idSize; gapId < ids[i]; gapId++) {
                    if (!IdValidator.isReservedId(gapId)) {
                        expectedIds.add(gapId);
                    }
                }
            }
        }
        long deletedIdBelowLowId = lowId - 3 - idSize;
        markDeleted(deletedIdBelowLowId, idSize);

        // when
        markUsed(ids[ids.length - 1], idSize);

        // then
        var unusedIds = asList(idGenerator.notUsedIdsIterator());
        expectedIds.addAll(LongStream.range(deletedIdBelowLowId, deletedIdBelowLowId + idSize)
                .toArray());
        for (int i = 0; i < ids.length - 1; i++) {
            expectedIds.addAll(LongStream.range(ids[i], ids[i] + idSize).toArray());
        }
        expectedIds.sortThis();
        assertThat(unusedIds).isEqualTo(expectedIds);
    }

    @Test
    void shouldMarkConcurrentlyWithBridgingAcrossReservedId() throws IOException {
        // given
        open();
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
        long lowId = IdValidator.INTEGER_MINUS_ONE - 100;
        idGenerator.setHighId(lowId);
        idGenerator.markHighestWrittenAtHighId();

        // when
        var race = new Race();
        race.addContestants(4, throwing(() -> {
            for (int t = 0; t < 100; t++) {
                int batchSize = ThreadLocalRandom.current().nextInt(1, 5);
                long[] ids = new long[batchSize];
                for (int i = 0; i < batchSize; i++) {
                    ids[i] = idGenerator.nextId(NULL_CONTEXT);
                }
                try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                    for (long id : ids) {
                        marker.markUsed(id);
                    }
                }
            }
        }));
        race.goUnchecked();

        // then
        assertThat(idGenerator.getHighId()).isGreaterThan(IdValidator.INTEGER_MINUS_ONE);
        assertThat(asList(idGenerator.notUsedIdsIterator()).isEmpty()).isTrue();
    }

    private void verifyReallocationDoesNotIncreaseHighId(
            ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse) {
        // then after all remaining allocations have been freed, allocating that many ids again should not need to
        // increase highId,
        // i.e. all such allocations should be allocated from the free-list
        deleteAndFree(allocations, expectedInUse);
        long highIdBeforeReallocation = idGenerator.getHighId();
        long numberOfIdsOutThere = highIdBeforeReallocation;
        ConcurrentSparseLongBitSet reallocationIds = new ConcurrentSparseLongBitSet(IDS_PER_ENTRY);
        while (numberOfIdsOutThere > 0) {
            long id = idGenerator.nextId(NULL_CONTEXT);
            Allocation allocation = new Allocation(idGenerator, id, 1, 0, () -> 0);
            numberOfIdsOutThere -= 1;
            reallocationIds.set(allocation.id, 1, true);
        }
        assertThat(idGenerator.getHighId()).isEqualTo(highIdBeforeReallocation);
    }

    private void restart(Customization customization) throws IOException {
        idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();
        open(customization);
        idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
    }

    private static RecordingFreeIds freeIds(long... freeIds) {
        return new RecordingFreeIds(true, freeIds);
    }

    private static RecordingFreeIds usedIds(long... usedIds) {
        return new RecordingFreeIds(false, usedIds);
    }

    private Runnable freer(ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse) {
        return new Runnable() {
            private final Random rng = new Random(random.nextLong());

            @Override
            public void run() {
                // Mark ids as eligible for reuse
                int size = allocations.size();
                if (size > 0) {
                    int slot = rng.nextInt(size);
                    Iterator<Allocation> iterator = allocations.iterator();
                    Allocation allocation = null;
                    for (int i = 0; i < slot && iterator.hasNext(); i++) {
                        allocation = iterator.next();
                    }
                    if (allocation != null) {
                        if (allocation.free(expectedInUse)) {
                            iterator.remove();
                        }
                        // else someone else got there before us
                    }
                }
            }
        };
    }

    private Runnable deleter(ConcurrentLinkedQueue<Allocation> allocations) {
        return new Runnable() {
            private final Random rng = new Random(random.nextLong());

            @Override
            public void run() {
                // Delete ids
                int size = allocations.size();
                if (size > 0) {
                    int slot = rng.nextInt(size);
                    Iterator<Allocation> iterator = allocations.iterator();
                    Allocation allocation = null;
                    for (int i = 0; i < slot && iterator.hasNext(); i++) {
                        allocation = iterator.next();
                    }
                    if (allocation != null) {
                        // Won't delete if it has already been deleted, but that's fine
                        allocation.delete();
                    }
                }
            }
        };
    }

    private Runnable allocator(
            int maxAllocationsAhead,
            ConcurrentLinkedQueue<Allocation> allocations,
            ConcurrentSparseLongBitSet expectedInUse,
            int maxSlotSize,
            IdGenerator idGenerator,
            IntSupplier leaseIdSupplier) {
        return new Runnable() {
            private final Random rng = new Random(random.nextLong());

            @Override
            public void run() {
                // Allocate ids
                if (allocations.size() < maxAllocationsAhead) {
                    int size = rng.nextInt(maxSlotSize) + 1;
                    int leaseId = leaseIdSupplier.getAsInt();
                    long id = idGenerator.nextConsecutiveIdRange(size, true, NULL_CONTEXT);
                    Allocation allocation = new Allocation(idGenerator, id, size, leaseId, leaseIdSupplier);
                    allocation.markAsInUse(expectedInUse);
                    allocations.add(allocation);
                }
            }
        };
    }

    private void deleteAndFree(
            ConcurrentLinkedQueue<Allocation> allocations, ConcurrentSparseLongBitSet expectedInUse) {
        for (Allocation allocation : allocations) {
            allocation.delete();
            allocation.free(expectedInUse);
        }
    }

    private void markUsed(long id) {
        markUsed(id, 1);
    }

    private void markUsed(long id, int size) {
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markUsed(id, size);
        }
    }

    private void markDeleted(long id) {
        markDeleted(id, 1);
    }

    private void markDeleted(long id, int size) {
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markDeleted(id, size);
        }
    }

    private void markFree(long id) {
        markFree(id, 1);
    }

    private void markFree(long id, int size) {
        try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
            marker.markFree(id, size);
        }
    }

    private void markUnallocated(long id) {
        markUnallocated(id, 1);
    }

    private void markUnallocated(long id, int size) {
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markUnallocated(id, size);
        }
    }

    private LongList asList(PrimitiveLongResourceIterator iterator) {
        try (iterator) {
            var list = LongLists.mutable.empty();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return list;
        }
    }

    private LongList asFreeIds(long[] usedIds) {
        var list = LongLists.mutable.empty();
        var prevUsedId = -1L;
        for (int i = 0; i < usedIds.length; i++) {
            assert i == 0 || usedIds[i] > usedIds[i - 1];
            long usedId = usedIds[i];
            while (++prevUsedId < usedId) {
                list.add(prevUsedId);
            }
        }
        return list;
    }

    private static TestIdType idType(boolean caresAboutReservedId) {
        return caresAboutReservedId ? TestIdType.TEST : TestIdType.TEST_USING_RESERVED;
    }

    private static class Allocation {
        private final IdGenerator idGenerator;
        private final long id;
        private final int size;
        private final AtomicBoolean deleting = new AtomicBoolean();
        private volatile boolean deleted;
        private final AtomicBoolean freeing = new AtomicBoolean();

        // State for being able to mimic cluster load and interaction
        private final int leaseId;
        private final IntSupplier leaseIdSupplier;
        private volatile int deletedInLeaseId;

        Allocation(IdGenerator idGenerator, long id, int size, int leaseId, IntSupplier leaseIdSupplier) {
            this.idGenerator = idGenerator;
            this.id = id;
            this.size = size;
            this.leaseId = leaseId;
            this.leaseIdSupplier = leaseIdSupplier;
        }

        void delete() {
            if (deleting.compareAndSet(false, true)) {
                try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                    marker.markDeleted(id, size);
                    deletedInLeaseId = leaseIdSupplier.getAsInt();
                }
                deleted = true;
            }
        }

        boolean free(ConcurrentSparseLongBitSet expectedInUse) {
            if (!deleted) {
                return false;
            }

            if (freeing.compareAndSet(false, true)) {
                expectedInUse.set(id, size, false);
                try (var marker = idGenerator.contextualMarker(NULL_CONTEXT)) {
                    if (leaseIdSupplier.getAsInt() == deletedInLeaseId) {
                        marker.markFree(id, size);
                    }
                    // else there has been a leader switch and as such this ID is already free,
                    // and freeing it here is breaking this contract and could cause this ID to
                    // be placed into cache multiple times. I.e. we're adhering to rules of interaction
                    // between clustering and ID generators.
                }
                return true;
            }
            return false;
        }

        void markAsInUse(ConcurrentSparseLongBitSet expectedInUse) {
            // Simulate that actual commit comes very close after allocation, in reality they are slightly more apart
            // Also this test marks all ids, regardless if they come from highId or the free-list. This to simulate more
            // real-world scenario and to exercise the idempotent clearing feature.
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                if (leaseIdSupplier.getAsInt() == leaseId) {
                    expectedInUse.set(id, size, true);
                    marker.markUsed(id, size);
                } else {
                    // there has been a leader switch since this ID was allocated, which makes this
                    // allocation void. I.e. we're adhering to rules of interaction between clustering and ID
                    // generators.
                    deleting.set(true);
                    deleted = true;
                    freeing.set(true);
                }
            }
        }

        @Override
        public String toString() {
            return format("{id:%d, slots:%d}", id, size);
        }
    }

    private static class RecordingFreeIds implements FreeIds {
        private boolean wasCalled;
        private final boolean visitsDeletedIds;
        private final long[] freeIds;

        RecordingFreeIds(boolean visitsDeletedIds, long... freeIds) {
            this.visitsDeletedIds = visitsDeletedIds;
            this.freeIds = freeIds;
        }

        @Override
        public boolean visitsDeletedIds() {
            return visitsDeletedIds;
        }

        @Override
        public long accept(IdVisitor visitor) throws IOException {
            wasCalled = true;
            for (long freeId : freeIds) {
                visitor.accept(freeId);
            }
            return freeIds[freeIds.length - 1];
        }
    }

    private Customization customization() {
        return new Customization(file);
    }

    private static class Customization {
        private Path file;
        private IdType idType = TestIdType.TEST;
        private Config config = Config.defaults();
        private IndexedIdGenerator.Monitor monitor = NO_MONITOR;
        private boolean readOnly;
        private IdSlotDistribution slotDistribution = SINGLE_IDS;

        Customization(Path file) {
            this.file = file;
        }

        Customization with(Path file) {
            this.file = file;
            return this;
        }

        Customization with(IdType idType) {
            this.idType = idType;
            return this;
        }

        Customization with(Config config) {
            this.config = config;
            return this;
        }

        Customization with(IndexedIdGenerator.Monitor monitor) {
            this.monitor = monitor;
            return this;
        }

        Customization readOnly() {
            this.readOnly = true;
            return this;
        }

        Customization with(IdSlotDistribution slotDistribution) {
            this.slotDistribution = slotDistribution;
            return this;
        }
    }
}
