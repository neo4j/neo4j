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
package org.neo4j.internal.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.ScopedMemoryPool;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
@ExtendWith(LifeExtension.class)
class BufferingIdGeneratorFactoryTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private LifeSupport life;

    private MockedIdGeneratorFactory actual;
    private ControllableSnapshotSupplier boundaries;
    private BufferingIdGeneratorFactory bufferingIdGeneratorFactory;
    private IdGenerator idGenerator;
    private ScopedMemoryPool dbMemoryPool;

    private void setup(boolean offHeap) throws IOException {
        actual = new MockedIdGeneratorFactory();
        boundaries = new ControllableSnapshotSupplier();
        GlobalMemoryGroupTracker globalMemoryGroupTracker = new GlobalMemoryGroupTracker(
                new MemoryPools(), MemoryGroup.OTHER, ByteUnit.mebiBytes(1), true, true, null);
        dbMemoryPool = globalMemoryGroupTracker.newDatabasePool("test", ByteUnit.mebiBytes(1), null);
        bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory(actual);
        Config config = Config.defaults(GraphDatabaseInternalSettings.buffered_ids_offload, offHeap);
        bufferingIdGeneratorFactory.initialize(
                fs, directory.file("tmp-ids"), config, boundaries, boundaries, dbMemoryPool.getPoolMemoryTracker());
        idGenerator = bufferingIdGeneratorFactory.open(
                pageCache,
                Path.of("doesnt-matter"),
                TestIdType.TEST,
                () -> 0L,
                Integer.MAX_VALUE,
                false,
                config,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                immutable.empty(),
                SINGLE_IDS);
        life.add(bufferingIdGeneratorFactory);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldDelayFreeingOfDeletedIds(boolean offHeap) throws IOException {
        setup(offHeap);

        // WHEN
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markDeleted(7, 2);
        }
        actual.markers.get(TestIdType.TEST).verifyDeleted(7, 2);
        actual.markers.get(TestIdType.TEST).verifyClosed();
        actual.markers.get(TestIdType.TEST).verifyNoMoreMarks();

        // after some maintenance and transaction still not closed
        bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);
        actual.markers.get(TestIdType.TEST).verifyNoMoreMarks();

        // although after transactions have all closed
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);

        // THEN
        actual.markers.get(TestIdType.TEST).verifyFreed(7, 2);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldHandleDeletingAndFreeingConcurrently(boolean offHeap) throws IOException {
        // given
        setup(offHeap);
        AtomicLong nextId = new AtomicLong();
        AtomicInteger numMaintenanceCalls = new AtomicInteger();
        Race race = new Race().withEndCondition(() -> numMaintenanceCalls.get() >= 10 || nextId.get() >= 1_000);
        race.addContestants(4, () -> {
            int numIds = ThreadLocalRandom.current().nextInt(1, 5);
            try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
                for (int i = 0; i < numIds; i++) {
                    marker.markDeleted(nextId.getAndIncrement(), 1);
                }
            }
        });
        Deque<IdController.TransactionSnapshot> conditions = new ConcurrentLinkedDeque<>();
        race.addContestant(throwing(() -> {
            bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);
            if (boundaries.mostRecentlyReturned == null) {
                return;
            }

            IdController.TransactionSnapshot condition = boundaries.mostRecentlyReturned;
            boundaries.mostRecentlyReturned = null;
            if (ThreadLocalRandom.current().nextBoolean()) {
                // Chance to let this condition be true immediately
                boundaries.enable(condition);
            }
            if (ThreadLocalRandom.current().nextBoolean()) {
                // Chance to enable a previously disabled condition
                for (IdController.TransactionSnapshot olderCondition : conditions) {
                    if (!boundaries.eligibleForFreeing(olderCondition)) {
                        boundaries.enable(olderCondition);
                        break;
                    }
                }
            }
            conditions.add(condition);
            numMaintenanceCalls.incrementAndGet();
        }));

        // when
        race.goUnchecked();
        for (IdController.TransactionSnapshot condition : conditions) {
            boundaries.enable(condition);
        }
        boundaries.automaticallyEnableConditions = true;
        bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);
        // the second maintenance call is because the first call will guarantee that the queued buffers will be freed,
        // making room to queue the last deleted IDs from the ID generator in the second call.
        bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);
        for (long id = 0; id < nextId.get(); id++) {
            actual.markers.get(TestIdType.TEST).verifyFreed(id, 1);
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldMemoryTrackBufferedIDs(boolean offHeap) throws IOException {
        // given
        setup(offHeap);
        long heapSizeBeforeDeleting = dbMemoryPool.usedHeap();

        // when deleting some IDs
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            for (int i = 0; i < 100; i++) {
                marker.markDeleted(i, 1);
            }
        }
        assertThat(dbMemoryPool.usedHeap()).isGreaterThan(heapSizeBeforeDeleting);
        // maintenance where transactions are still open. Here the buffered IDs should have been written to the page
        // cache and the heap usage freed
        bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);
        if (offHeap) {
            assertThat(dbMemoryPool.usedHeap()).isEqualTo(heapSizeBeforeDeleting);
        } else {
            assertThat(dbMemoryPool.usedHeap()).isGreaterThan(heapSizeBeforeDeleting);
        }
        // maintenance where transactions are closed and i.e. the buffered IDs gets released
        boundaries.setMostRecentlyReturnedSnapshotToAllClosed();
        bufferingIdGeneratorFactory.maintenance(NULL_CONTEXT);

        // then heap usage should go down again
        assertThat(dbMemoryPool.usedHeap()).isEqualTo(heapSizeBeforeDeleting);
    }

    private static class ControllableSnapshotSupplier
            implements Supplier<IdController.TransactionSnapshot>, IdController.IdFreeCondition {
        boolean automaticallyEnableConditions;
        volatile IdController.TransactionSnapshot mostRecentlyReturned;
        private final Set<IdController.TransactionSnapshot> enabledSnapshots = new HashSet<>();

        @Override
        public IdController.TransactionSnapshot get() {
            mostRecentlyReturned = new IdController.TransactionSnapshot(10, 0, 0);
            if (automaticallyEnableConditions) {
                enabledSnapshots.add(mostRecentlyReturned);
            }
            return mostRecentlyReturned;
        }

        @Override
        public boolean eligibleForFreeing(IdController.TransactionSnapshot snapshot) {
            return enabledSnapshots.contains(snapshot);
        }

        void enable(IdController.TransactionSnapshot snapshot) {
            enabledSnapshots.add(snapshot);
        }

        void setMostRecentlyReturnedSnapshotToAllClosed() {
            enabledSnapshots.add(mostRecentlyReturned);
        }
    }

    private static class MockedIdGeneratorFactory implements IdGeneratorFactory {
        private final Map<IdType, IdGenerator> generators = new HashMap<>();
        private final Map<IdType, MockedMarker> markers = new HashMap<>();

        @Override
        public IdGenerator open(
                PageCache pageCache,
                Path filename,
                IdType idType,
                LongSupplier highIdScanner,
                long maxId,
                boolean readOnly,
                Config config,
                CursorContextFactory contextFactory,
                ImmutableSet<OpenOption> openOptions,
                IdSlotDistribution slotDistribution) {
            IdGenerator idGenerator = mock(IdGenerator.class);
            MockedMarker marker = new MockedMarker();
            generators.put(idType, idGenerator);
            markers.put(idType, marker);
            when(idGenerator.contextualMarker(any())).thenReturn(marker);
            when(idGenerator.transactionalMarker(any())).thenReturn(marker);
            when(idGenerator.allocationEnabled()).thenReturn(true);
            return idGenerator;
        }

        @Override
        public IdGenerator create(
                PageCache pageCache,
                Path filename,
                IdType idType,
                long highId,
                boolean throwIfFileExists,
                long maxId,
                boolean readOnly,
                Config config,
                CursorContextFactory contextFactory,
                ImmutableSet<OpenOption> openOptions,
                IdSlotDistribution slotDistribution) {
            return open(
                    pageCache,
                    filename,
                    idType,
                    () -> highId,
                    maxId,
                    readOnly,
                    config,
                    contextFactory,
                    openOptions,
                    SINGLE_IDS);
        }

        @Override
        public IdGenerator get(IdType idType) {
            return generators.get(idType);
        }

        @Override
        public void visit(Consumer<IdGenerator> visitor) {
            generators.values().forEach(visitor);
        }

        @Override
        public void clearCache(boolean allocationEnabled, CursorContext cursorContext) {
            // no-op
        }

        @Override
        public Collection<Path> listIdFiles() {
            return Collections.emptyList();
        }
    }

    private static class MockedMarker implements IdGenerator.TransactionalMarker, IdGenerator.ContextualMarker {
        private final Set<Pair<Long, Integer>> used = ConcurrentHashMap.newKeySet();
        private final Set<Pair<Long, Integer>> deleted = ConcurrentHashMap.newKeySet();
        private final Set<Pair<Long, Integer>> freed = ConcurrentHashMap.newKeySet();
        private boolean closed;

        @Override
        public void markUsed(long id, int numberOfIds) {
            used.add(Pair.of(id, numberOfIds));
        }

        @Override
        public void markDeleted(long id, int numberOfIds) {
            deleted.add(Pair.of(id, numberOfIds));
        }

        @Override
        public void markFree(long id, int numberOfIds) {
            freed.add(Pair.of(id, numberOfIds));
        }

        @Override
        public void markDeletedAndFree(long id, int numberOfIds) {
            markDeleted(id, numberOfIds);
            markFree(id, numberOfIds);
        }

        @Override
        public void markUnallocated(long id, int numberOfIds) {}

        @Override
        public void markReserved(long id, int numberOfIds) {}

        @Override
        public void markUnreserved(long id, int numberOfIds) {}

        @Override
        public void markUncached(long id, int numberOfIds) {}

        void verifyDeleted(long id, int numberOfIds) {
            assertThat(deleted.remove(Pair.of(id, numberOfIds))).isTrue();
        }

        void verifyFreed(long id, int numberOfIds) {
            assertThat(freed.remove(Pair.of(id, numberOfIds))).isTrue();
        }

        @Override
        public void flush() {}

        @Override
        public void close() {
            closed = true;
        }

        void verifyClosed() {
            assertThat(closed).isTrue();
        }

        void verifyNoMoreMarks() {
            assertThat(used).isEmpty();
            assertThat(deleted).isEmpty();
            assertThat(freed).isEmpty();
        }
    }
}
