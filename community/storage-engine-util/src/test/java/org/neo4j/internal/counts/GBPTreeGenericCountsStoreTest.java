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
package org.neo4j.internal.counts;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.internal.counts.GBPTreeCountsStore.nodeKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.relationshipKey;
import static org.neo4j.internal.counts.GBPTreeGenericCountsStore.EMPTY_REBUILD;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.test.OtherThreadExecutor.command;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.util.concurrent.OutOfOrderSequence.EMPTY_META;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.counts.InvalidCountException;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree;
import org.neo4j.internal.counts.GBPTreeGenericCountsStore.Rebuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.util.concurrent.OutOfOrderSequence;

@PageCacheExtension
@RandomSupportExtension
class GBPTreeGenericCountsStoreTest {
    private static final int HIGH_TOKEN_ID = 30;
    private static final int LABEL_ID_1 = 1;
    private static final int LABEL_ID_2 = 2;
    private static final int RELATIONSHIP_TYPE_ID_1 = 1;
    private static final int RELATIONSHIP_TYPE_ID_2 = 2;
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RandomSupport random;

    private GBPTreeGenericCountsStore countsStore;

    @BeforeEach
    void openCountsStore() throws Exception {
        openCountsStore(EMPTY_REBUILD);
    }

    @AfterEach
    void closeCountsStore() {
        countsStore.close();
    }

    @Test
    void tracePageCacheAccessOnCountStoreOpen() throws IOException {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var file = directory.file("another.file");

        assertZeroGlobalTracer(pageCacheTracer);

        CursorContextFactory cursorContextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        try (var counts = new GBPTreeCountsStore(
                pageCache,
                new StoreFile(file),
                directory.getFileSystem(),
                immediate(),
                CountsBuilder.EMPTY,
                false,
                NO_MONITOR,
                DEFAULT_DATABASE_NAME,
                randomMaxCacheSize(),
                NullLogProvider.getInstance(),
                cursorContextFactory,
                pageCacheTracer,
                getOpenOptions())) {
            assertThat(pageCacheTracer.pins()).isEqualTo(4);
            assertThat(pageCacheTracer.unpins()).isEqualTo(4);
            assertThat(pageCacheTracer.hits()).isOne();
            assertThat(pageCacheTracer.faults()).isEqualTo(3);
        }

        assertThat(pageCacheTracer.pins()).isEqualTo(4);
        assertThat(pageCacheTracer.unpins()).isEqualTo(4);
        assertThat(pageCacheTracer.hits()).isOne();
        assertThat(pageCacheTracer.faults()).isEqualTo(3);
    }

    @Test
    void tracePageCacheAccessOnNodeCount() {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContext =
                CONTEXT_FACTORY.create(pageCacheTracer.createPageCursorTracer("tracePageCacheAccessOnNodeCount"));
        assertZeroTracer(cursorContext);
        assertThat(countsStore.read(nodeKey(0), cursorContext)).isZero();

        assertThat(cursorContext.getCursorTracer().pins()).isOne();
        assertThat(cursorContext.getCursorTracer().unpins()).isOne();
        assertThat(cursorContext.getCursorTracer().hits()).isOne();
    }

    @Test
    void tracePageCacheAccessOnRelationshipCount() {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContext = CONTEXT_FACTORY.create(
                pageCacheTracer.createPageCursorTracer("tracePageCacheAccessOnRelationshipCount"));
        assertZeroTracer(cursorContext);
        assertThat(countsStore.read(relationshipKey(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL), cursorContext))
                .isZero();

        assertThat(cursorContext.getCursorTracer().pins()).isOne();
        assertThat(cursorContext.getCursorTracer().unpins()).isOne();
        assertThat(cursorContext.getCursorTracer().hits()).isOne();
    }

    @Test
    void tracePageCacheAccessOnApply() {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContext =
                CONTEXT_FACTORY.create(pageCacheTracer.createPageCursorTracer("tracePageCacheAccessOnApply"));
        assertZeroTracer(cursorContext);

        try (CountUpdater updater = countsStore.updaterImpl(1 + BASE_TX_ID, true, cursorContext)) {
            updater.increment(nodeKey(LABEL_ID_1), 10);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), 3);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), 7);
        }

        assertThat(cursorContext.getCursorTracer().pins()).isEqualTo(3);
        assertThat(cursorContext.getCursorTracer().unpins()).isEqualTo(3);
        assertThat(cursorContext.getCursorTracer().hits()).isEqualTo(3);
    }

    @Test
    void failToApplySameTransactionTwice() {
        long txId = BASE_TX_ID + 1;

        try (var updater = countsStore.updaterImpl(txId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), 10);
        }
        assertThatThrownBy(() -> {
                    try (var updater = countsStore.updaterImpl(txId, true, NULL_CONTEXT)) {
                        updater.increment(nodeKey(LABEL_ID_1), 10);
                    }
                })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("but highest gap-free is");
    }

    @Test
    void applySeveralChunksOfSameTransaction() {
        long txId = BASE_TX_ID + 1;

        assertThatCode(() -> {
                    for (int i = 0; i < 100; i++) {
                        try (var updater = countsStore.updaterImpl(txId, false, NULL_CONTEXT)) {
                            updater.increment(nodeKey(LABEL_ID_1), 10);
                        }
                    }

                    try (var updater = countsStore.updaterImpl(txId, true, NULL_CONTEXT)) {
                        updater.increment(nodeKey(LABEL_ID_1), 10);
                    }
                })
                .doesNotThrowAnyException();
    }

    @Test
    void shouldUpdateAndReadSomeCounts() throws IOException {
        // given
        long txId = BASE_TX_ID;
        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), 10);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), 3);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), 7);
        }
        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), 5); // now at 15
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), 2); // now at 5
        }

        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);

        // when/then
        assertThat(countsStore.read(nodeKey(LABEL_ID_1), NULL_CONTEXT)).isEqualTo(15);
        assertThat(countsStore.read(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), NULL_CONTEXT))
                .isEqualTo(5);
        assertThat(countsStore.read(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), NULL_CONTEXT))
                .isEqualTo(7);

        // and when
        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), -7);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), -5);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), -2);
        }

        // then
        assertThat(countsStore.read(nodeKey(LABEL_ID_1), NULL_CONTEXT)).isEqualTo(8);
        assertThat(countsStore.read(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), NULL_CONTEXT))
                .isZero();
        assertThat(countsStore.read(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), NULL_CONTEXT))
                .isEqualTo(5);
    }

    @Test
    void shouldReturnTrueWhenGoingToAndFromZero() {
        long txId = BASE_TX_ID;
        CountsKey key = nodeKey(LABEL_ID_1);
        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            assertThat(updater.increment(key, 1)).isTrue(); // 0->1 true
            assertThat(updater.increment(key, 1)).isFalse(); // 1->2 false
        }
        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            assertThat(updater.increment(key, -1)).isFalse(); // 2->1 false
            assertThat(updater.increment(key, -1)).isTrue(); // 1->0 true
        }
    }

    @Test
    void shouldCheckpointAndRecoverConsistentlyUnderStressfulLoad() throws Throwable {
        // given
        int threads = 50; // it's good if it's way more than number of cores so that it creates some scheduling issues
        int numberOfRounds = 5;
        int roundTimeMillis = 300;
        ConcurrentMap<CountsKey, AtomicLong> expected = new ConcurrentHashMap<>();
        AtomicLong nextTxId = new AtomicLong(BASE_TX_ID);
        AtomicLong lastCheckPointedTxId = new AtomicLong(nextTxId.longValue());
        long lastRoundClosedAt = BASE_TX_ID;

        // Start at some number > 0 so that we can do negative deltas now and then
        long baseCount = 10_000;
        try (CountUpdater initialApplier = countsStore.updaterImpl(nextTxId.incrementAndGet(), true, NULL_CONTEXT)) {
            for (int s = -1; s < HIGH_TOKEN_ID; s++) {
                initialApplier.increment(nodeKey(s), baseCount);
                for (int t = -1; t < HIGH_TOKEN_ID; t++) {
                    for (int e = -1; e < HIGH_TOKEN_ID; e++) {
                        initialApplier.increment(relationshipKey(s, t, e), baseCount);
                    }
                }
            }
        }
        OutOfOrderSequence lastClosedTxId = new ArrayQueueOutOfOrderSequence(nextTxId.get(), 200, EMPTY_META);

        // when
        for (int r = 0; r < numberOfRounds; r++) {
            // Let loose updaters and a check-pointer
            Race race = new Race().withMaxDuration(roundTimeMillis, TimeUnit.MILLISECONDS);
            race.addContestants(threads, throwing(() -> {
                long txId = nextTxId.incrementAndGet();
                // Sleep a random time after getting the txId, this creates bigger temporary gaps in the txId sequence
                Thread.sleep(ThreadLocalRandom.current().nextInt(5));
                generateAndApplyTransaction(expected, txId);
                lastClosedTxId.offer(txId, EMPTY_META);
            }));
            race.addContestant(throwing(() -> {
                long checkpointTxId = lastClosedTxId.getHighestGapFreeNumber();
                countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
                lastCheckPointedTxId.set(checkpointTxId);
                Thread.sleep(ThreadLocalRandom.current().nextInt(roundTimeMillis / 5));
            }));
            race.go();

            // Crash here, well not really crash but close the counts store knowing that there's any number of
            // transactions since the last checkpoint
            // and we know the last committed tx id as well as the (pessimistic) last check-pointed tx id.
            crashAndRestartCountsStore();
            recover(lastCheckPointedTxId.get(), nextTxId.get());
            assertThat(nextTxId.get()).isGreaterThan(lastRoundClosedAt);
            lastRoundClosedAt = nextTxId.get();

            // then
            assertCountsMatchesExpected(expected, baseCount);
        }
    }

    @Test
    void shouldNotReapplyAlreadyAppliedTransactionBelowHighestGapFree() throws Exception {
        // given
        int labelId = 5;
        long expectedCount = 0;
        int delta = 3;
        for (long txId = BASE_TX_ID + 1; txId < 10; txId++) {
            incrementNodeCount(txId, labelId, delta);
            expectedCount += delta;
        }
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(expectedCount);

        // when reapplying after a restart
        checkpointAndRestartCountsStore();

        for (long txId = BASE_TX_ID + 1; txId < 10; txId++) {
            incrementNodeCount(txId, labelId, delta);
        }
        // then it should not change the delta
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(expectedCount);
    }

    @Test
    void shouldNotReapplyAlreadyAppliedTransactionAmongStrayTxIds() throws Exception {
        // given
        int labelId = 20;
        incrementNodeCount(BASE_TX_ID + 1, labelId, 5);
        // intentionally skip BASE_TX_ID + 2
        incrementNodeCount(BASE_TX_ID + 3, labelId, 7);

        // when
        checkpointAndRestartCountsStore();
        incrementNodeCount(BASE_TX_ID + 3, labelId, 7);
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(5 + 7);
        incrementNodeCount(BASE_TX_ID + 2, labelId, 3);

        // then
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(5 + 7 + 3);
    }

    @Test
    void shouldUseCountsBuilderOnCreation() throws Exception {
        // given
        long rebuiltAtTransactionId = 5;
        int labelId = 3;
        int labelId2 = 6;
        int relationshipTypeId = 7;
        closeCountsStore();
        deleteCountsStore();

        // when
        TestableCountsBuilder builder = new TestableCountsBuilder(rebuiltAtTransactionId) {
            @Override
            public void rebuild(CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                super.rebuild(updater, cursorContext, memoryTracker);
                updater.increment(nodeKey(labelId), 10);
                updater.increment(relationshipKey(labelId, relationshipTypeId, labelId2), 14);
            }
        };
        openCountsStore(builder);
        assertThat(builder.lastCommittedTxIdCalled).isTrue();
        assertThat(builder.rebuildCalled).isTrue();
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(10);
        assertThat(countsStore.read(nodeKey(labelId2), NULL_CONTEXT)).isZero();
        assertThat(countsStore.read(relationshipKey(labelId, relationshipTypeId, labelId2), NULL_CONTEXT))
                .isEqualTo(14);

        // and when
        checkpointAndRestartCountsStore();
        // Re-applying a txId below or equal to the "rebuild transaction id" should not apply it
        incrementNodeCount(rebuiltAtTransactionId - 1, labelId, 100);
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(10);
        incrementNodeCount(rebuiltAtTransactionId, labelId, 100);
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(10);

        // then
        incrementNodeCount(rebuiltAtTransactionId + 1, labelId, 100);
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(110);
    }

    @Test
    void shouldNotApplyTransactionOnCreatedCountsStoreDuringRecovery() throws IOException {
        // given
        int labelId = 123;
        incrementNodeCount(BASE_TX_ID + 1, labelId, 4);
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
        incrementNodeCount(BASE_TX_ID + 2, labelId, -2);
        closeCountsStore();
        deleteCountsStore();
        GBPTreeGenericCountsStore.Monitor monitor = mock(GBPTreeGenericCountsStore.Monitor.class);
        // instantiate, but don't start
        instantiateCountsStore(
                new Rebuilder() {
                    @Override
                    public void rebuild(
                            CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                        updater.increment(nodeKey(labelId), 2);
                    }

                    @Override
                    public long lastCommittedTxId() {
                        return BASE_TX_ID + 2;
                    }
                },
                false,
                monitor);

        // when doing recovery of the last transaction (since this is on an empty counts store then making the count
        // negative, i.e. 0 - 2)
        // applying this negative delta would have failed in the updater.
        incrementNodeCount(BASE_TX_ID + 2, labelId, -2);
        verify(monitor).ignoredTransaction(BASE_TX_ID + 2);
        countsStore.start(NULL_CONTEXT, INSTANCE);

        // then
        assertThat(countsStore.read(nodeKey(labelId), NULL_CONTEXT)).isEqualTo(2);
    }

    @Test
    void checkpointShouldWaitForApplyingTransactionsToClose() throws Exception {
        // given
        CountUpdater updater1 = countsStore.updaterImpl(BASE_TX_ID + 1, true, NULL_CONTEXT);
        CountUpdater updater2 = countsStore.updaterImpl(BASE_TX_ID + 2, true, NULL_CONTEXT);

        try (OtherThreadExecutor checkpointer = new OtherThreadExecutor("Checkpointer", 1, MINUTES)) {
            // when
            Future<Object> checkpoint = checkpointer.executeDontWait(command(
                    () -> countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT)));
            checkpointer.waitUntilWaiting();

            // and when closing one of the updaters it should still wait
            updater1.close();
            checkpointer.waitUntilWaiting();
            assertThat(checkpoint.isDone()).isFalse();

            // then when closing the other one it should be able to complete
            updater2.close();
            checkpoint.get();
        }
    }

    @Test
    void checkpointShouldBlockApplyingNewTransactions() throws Exception {
        // given
        CountUpdater updaterBeforeCheckpoint = countsStore.updaterImpl(BASE_TX_ID + 1, true, NULL_CONTEXT);

        final AtomicReference<CountUpdater> updater = new AtomicReference<>();
        try (OtherThreadExecutor checkpointer = new OtherThreadExecutor("Checkpointer", 1, MINUTES);
                OtherThreadExecutor applier = new OtherThreadExecutor("Applier", 1, MINUTES)) {
            // when
            Future<Object> checkpoint = checkpointer.executeDontWait(command(
                    () -> countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT)));
            checkpointer.waitUntilWaiting();

            // and when trying to open another applier it must wait
            Future<Void> applierAfterCheckpoint = applier.executeDontWait(() -> {
                updater.set(countsStore.updaterImpl(BASE_TX_ID + 2, true, NULL_CONTEXT));
                return null;
            });
            applier.waitUntilWaiting();
            assertThat(checkpoint.isDone()).isFalse();
            assertThat(applierAfterCheckpoint.isDone()).isFalse();

            // then when closing first updater the checkpoint should be able to complete
            updaterBeforeCheckpoint.close();
            checkpoint.get();

            // and then also the applier after the checkpoint has released the critical section should be able to
            // continue
            applierAfterCheckpoint.get();
            applier.execute(() -> {
                updater.get().close();
                return null;
            });
        }
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly() {
        final Path file = directory.file("non-existing");
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> new GBPTreeCountsStore(
                        pageCache,
                        new StoreFile(file),
                        fs,
                        immediate(),
                        CountsBuilder.EMPTY,
                        true,
                        NO_MONITOR,
                        DEFAULT_DATABASE_NAME,
                        randomMaxCacheSize(),
                        NullLogProvider.getInstance(),
                        CONTEXT_FACTORY,
                        PageCacheTracer.NULL,
                        getOpenOptions()));
    }

    @Test
    void shouldAllowToCreateUpdatedEvenInReadOnlyMode() throws IOException {
        // given
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
        closeCountsStore();
        instantiateCountsStore(EMPTY_REBUILD, true, NO_MONITOR);
        countsStore.start(NULL_CONTEXT, INSTANCE);

        // then
        assertThatCode(() -> countsStore.updaterImpl(BASE_TX_ID + 1, true, NULL_CONTEXT))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldNotCheckpointInReadOnlyMode() throws IOException {
        // given
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
        closeCountsStore();
        instantiateCountsStore(EMPTY_REBUILD, true, NO_MONITOR);
        countsStore.start(NULL_CONTEXT, INSTANCE);

        // then it's fine to call checkpoint, because no changes can actually be made on a read-only counts store anyway
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
    }

    @Test
    void shouldNotSeeOutdatedCountsOnCheckpoint() throws Throwable {
        // given
        try (CountUpdater updater = countsStore.updaterImpl(BASE_TX_ID + 1, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), 10);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), 3);
            updater.increment(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), 7);
        }

        // when
        Race race = new Race();
        race.addContestant(
                throwing(() -> countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT)),
                1);
        race.addContestants(
                10,
                throwing(() -> {
                    assertThat(countsStore.read(nodeKey(LABEL_ID_1), NULL_CONTEXT))
                            .isEqualTo(10);
                    assertThat(countsStore.read(
                                    relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2), NULL_CONTEXT))
                            .isEqualTo(3);
                    assertThat(countsStore.read(
                                    relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2), NULL_CONTEXT))
                            .isEqualTo(7);
                }),
                1);

        // then
        race.go();
    }

    @Test
    void shouldNotCreateFileOnDumpingNonExistentCountsStore() {
        // given
        Path file = directory.file("abcd");

        // when
        assertThatExceptionOfType(NoSuchFileException.class)
                .isThrownBy(() -> GBPTreeCountsStore.dump(
                        pageCache,
                        fs,
                        new StoreFile(file),
                        System.out,
                        CONTEXT_FACTORY,
                        PageCacheTracer.NULL,
                        immutable.empty()));

        // then
        assertThat(fs.fileExists(file)).isFalse();
    }

    @Test
    void shouldDeleteAndMarkForRebuildOnCorruptStore() throws Exception {
        // given
        try (CountUpdater updater = countsStore.updaterImpl(BASE_TX_ID + 1, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), 9);
        }
        closeCountsStore();
        try (StoreChannel channel = fs.open(countsStoreFile(), Set.of(StandardOpenOption.WRITE))) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[8192]);
            for (int i = 0; buffer.hasRemaining(); i++) {
                buffer.put((byte) i);
            }
            buffer.flip();
            channel.writeAll(buffer, 0);
        }

        // when
        Rebuilder countsBuilder = mock(Rebuilder.class);
        when(countsBuilder.lastCommittedTxId()).thenReturn(BASE_TX_ID);
        doAnswer(invocationOnMock -> {
                    CountUpdater updater = invocationOnMock.getArgument(0, CountUpdater.class);
                    updater.increment(nodeKey(LABEL_ID_1), 3);
                    return null;
                })
                .when(countsBuilder)
                .rebuild(any(), any(), any());
        openCountsStore(countsBuilder);

        // then rebuild store instead of throwing exception
        verify(countsBuilder).rebuild(any(), any(), any());
        assertThat(countsStore.read(nodeKey(LABEL_ID_1), NULL_CONTEXT)).isEqualTo(3);
    }

    @Test
    void shouldWriteAbsoluteCountsWithDirectUpdater() throws IOException {
        // given
        Map<CountsKey, Long> expected = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            expected.put(randomKey(), random.nextLong(1, Long.MAX_VALUE));
        }

        // when
        try (CountUpdater countUpdater = countsStore.createDirectUpdater(false, NULL_CONTEXT)) {
            expected.forEach(countUpdater::increment);
        }

        // then
        expected.forEach(
                (key, count) -> assertThat(countsStore.read(key, NULL_CONTEXT)).isEqualTo(count));
    }

    @Test
    void shouldWriteDeltaCountsWithDirectUpdater() throws IOException {
        // given
        Map<CountsKey, Long> expected = new HashMap<>();
        for (int i = 0; i < 1_000; i++) {
            expected.put(randomKey(), random.nextLong(1, Integer.MAX_VALUE));
        }

        // when
        for (int i = 0; i < 2; i++) {
            try (CountUpdater countUpdater = countsStore.createDirectUpdater(true, NULL_CONTEXT)) {
                expected.forEach(countUpdater::increment);
            }
        }

        // then
        expected.forEach(
                (key, count) -> assertThat(countsStore.read(key, NULL_CONTEXT)).isEqualTo(count * 2));
    }

    @Test
    void shouldHandleInvalidCountValues() throws IOException {
        // given
        long txId = BASE_TX_ID;
        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), -5);
            updater.increment(nodeKey(LABEL_ID_2), 10);
        }

        // write the illegal value to the tree
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);

        try (CountUpdater updater = countsStore.updaterImpl(++txId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(LABEL_ID_1), 10); // this will be just ignored
            updater.increment(nodeKey(LABEL_ID_2), 5); // now at 15
        }

        // Check that the problematic count is invalid before checkpoint ...
        InvalidCountException e1 = assertThatExceptionOfType(InvalidCountException.class)
                .isThrownBy(() -> countsStore.read(nodeKey(LABEL_ID_1), NULL_CONTEXT))
                .actual();
        assertThat(e1)
                .hasMessageContaining("The count value for key 'CountsKey[type:1, first:1, second:0]' is invalid. "
                        + "This is a serious error which is typically caused by a store corruption");
        // and other counts still work
        assertThat(countsStore.read(nodeKey(LABEL_ID_2), NULL_CONTEXT)).isEqualTo(15);

        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);

        // ... and after checkpoint, too
        InvalidCountException e2 = assertThatExceptionOfType(InvalidCountException.class)
                .isThrownBy(() -> countsStore.read(nodeKey(LABEL_ID_1), NULL_CONTEXT))
                .actual();
        assertThat(e2)
                .hasMessageContaining("The count value for key 'CountsKey[type:1, first:1, second:0]' is invalid.");
        assertThat(countsStore.read(nodeKey(LABEL_ID_2), NULL_CONTEXT)).isEqualTo(15);
    }

    @Test
    void shouldRebuildOnMismatchingLastCommittedTxId() throws IOException {
        // given some pre-state
        long countsStoreTxId = BASE_TX_ID + 1;
        try (CountUpdater updater = countsStore.updaterImpl(countsStoreTxId, true, NULL_CONTEXT)) {
            updater.increment(nodeKey(1), 1);
        }

        // when
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
        closeCountsStore();
        MutableBoolean rebuildTriggered = new MutableBoolean();
        openCountsStore(new Rebuilder() {
            @Override
            public long lastCommittedTxId() {
                return countsStoreTxId + 1;
            }

            @Override
            public void rebuild(CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                rebuildTriggered.setTrue();
            }
        });

        // then
        assertThat(rebuildTriggered.booleanValue()).isTrue();
    }

    @Test
    void shouldNotRebuildOnMismatchingLastCommittedTxIdButMatchingAfterRecovery() throws IOException {
        // given some pre-state
        long countsStoreTxId = BASE_TX_ID + 1;
        CountsKey key = nodeKey(1);
        try (CountUpdater updater = countsStore.updaterImpl(countsStoreTxId, true, NULL_CONTEXT)) {
            updater.increment(key, 1);
        }
        // leaving a gap intentionally
        try (CountUpdater updater = countsStore.updaterImpl(countsStoreTxId + 2, true, NULL_CONTEXT)) {
            updater.increment(key, 3);
        }
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);

        // when
        closeCountsStore();
        MutableBoolean rebuildTriggered = new MutableBoolean();
        instantiateCountsStore(
                new Rebuilder() {
                    @Override
                    public long lastCommittedTxId() {
                        return countsStoreTxId + 2;
                    }

                    @Override
                    public void rebuild(
                            CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                        rebuildTriggered.setTrue();
                    }
                },
                false,
                NO_MONITOR);
        // and do recovery
        try (CountUpdater updater = countsStore.updaterImpl(countsStoreTxId + 1, true, NULL_CONTEXT)) {
            updater.increment(key, 7);
        }
        assertThat(countsStore.updaterImpl(countsStoreTxId + 2, true, NULL_CONTEXT))
                .isNull(); // already applied
        countsStore.start(NULL_CONTEXT, INSTANCE);

        // then
        assertThat(rebuildTriggered.booleanValue()).isFalse();
        assertThat(countsStore.read(key, NULL_CONTEXT)).isEqualTo(11);
    }

    @Test
    void checkpointShouldAllowCacheSwitchesWhileFlushingTheTree() throws Exception {
        AtomicLong txId = new AtomicLong(BASE_TX_ID);
        // One update so there is something for checkpoint to flush
        try (CountUpdater updaterBeforeCheckpoint =
                countsStore.updaterImpl(txId.incrementAndGet(), true, NULL_CONTEXT)) {
            updaterBeforeCheckpoint.increment(nodeKey(251), 1);
        }

        try (OtherThreadExecutor checkpointer = new OtherThreadExecutor("Checkpointer", 1, MINUTES);
                OtherThreadExecutor applier = new OtherThreadExecutor("Applier", 1, MINUTES)) {

            // Do countstore checkpoint all the way until the actual tree checkpoint
            FileFlushEvent fileFlushEvent = spy(FileFlushEvent.NULL);
            BinaryLatch latch = new BinaryLatch();
            doAnswer((invocationOnMock) -> {
                        latch.await();
                        return 0;
                    })
                    .when(fileFlushEvent)
                    .startFlush(any());
            Future<Object> checkpoint = checkpointer.executeDontWait(
                    command(() -> countsStore.checkpoint(fileFlushEvent, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT)));
            checkpointer.waitUntilWaiting(location -> location.isAt(MultiRootGBPTree.class, "checkpoint"));

            // While checkpoint is in checkpoint we should still be able to fill up the cache and do the switching
            for (int i = 0; i < 250; i++) {
                final long id = txId.incrementAndGet();
                try (CountUpdater countUpdater = countsStore.updaterImpl(id, true, NULL_CONTEXT)) {
                    countUpdater.increment(nodeKey((int) id), 1);
                }
            }

            latch.release();
            checkpoint.get();
        }
    }

    private CountsKey randomKey() {
        CountsKey key = new CountsKey();
        key.initialize((byte) random.nextInt(1, 5), random.nextLong(), random.nextInt());
        return key;
    }

    private void incrementNodeCount(long txId, int labelId, int delta) {
        try (CountUpdater updater = countsStore.updaterImpl(txId, true, NULL_CONTEXT)) {
            if (updater != null) {
                updater.increment(nodeKey(labelId), delta);
            }
        }
    }

    private void assertCountsMatchesExpected(ConcurrentMap<CountsKey, AtomicLong> source, long baseCount) {
        ConcurrentMap<CountsKey, AtomicLong> expected = new ConcurrentHashMap<>();
        source.entrySet().stream()
                .filter(entry -> entry.getValue().get() != 0) // counts store won't have entries w/ 0 count
                .forEach(entry -> expected.put(
                        entry.getKey(), entry.getValue())); // copy them over to the one we're going to verify
        countsStore.visitAllCounts(
                (key, count) -> {
                    AtomicLong expectedCount = expected.remove(key);
                    if (expectedCount == null) {
                        assertThat(count)
                                .as(() -> format("Counts store has wrong count for (absent) %s", key))
                                .isEqualTo(baseCount);
                    } else {
                        assertThat(count)
                                .as(() -> format("Counts store has wrong count for %s", key))
                                .isEqualTo(baseCount + expectedCount.get());
                    }
                },
                NULL_CONTEXT);
        assertThat(expected).as(expected::toString).isEmpty();
    }

    private void recover(long lastCheckPointedTxId, long lastCommittedTxId) {
        ConcurrentMap<CountsKey, AtomicLong> throwAwayMap = new ConcurrentHashMap<>();
        for (long txId = lastCheckPointedTxId + 1; txId <= lastCommittedTxId; txId++) {
            generateAndApplyTransaction(throwAwayMap, txId);
        }
    }

    /**
     * Generates a transaction, i.e. a counts change set. The data is random, but uses a seed which is the seed of the {@link RandomSupport} in this test
     * as well as the the supplied txId. Calling this method in any given test multiple times with any specific txId will generate the same data.
     *
     * @param expected map of counts to update with the generated changes.
     * @param txId transaction id to generate transaction data for and ultimately apply to the counts store (and the expected map).
     */
    private void generateAndApplyTransaction(ConcurrentMap<CountsKey, AtomicLong> expected, long txId) {
        Random rng = new Random(random.seed() + txId);
        try (CountUpdater updater = countsStore.updaterImpl(txId, true, NULL_CONTEXT)) {
            if (updater != null) {
                int numberOfKeys = rng.nextInt(10);
                for (int j = 0; j < numberOfKeys; j++) {
                    long delta = rng.nextInt(11) - 1; // chance to get -1
                    CountsKey expectedKey;
                    if (rng.nextBoolean()) { // Node
                        int labelId = randomTokenId(rng);
                        updater.increment(nodeKey(labelId), delta);
                        expectedKey = nodeKey(labelId);
                    } else { // Relationship
                        int startLabelId = randomTokenId(rng);
                        int type = randomTokenId(rng);
                        int endLabelId = randomTokenId(rng);
                        updater.increment(relationshipKey(startLabelId, type, endLabelId), delta);
                        expectedKey = relationshipKey(startLabelId, type, endLabelId);
                    }
                    expected.computeIfAbsent(expectedKey, k -> new AtomicLong()).addAndGet(delta);
                }
            }
        }
    }

    private static int randomTokenId(Random rng) {
        // i.e. also include chance for -1 which is the "any" token
        return rng.nextInt(HIGH_TOKEN_ID + 1) - 1;
    }

    private void checkpointAndRestartCountsStore() throws Exception {
        countsStore.checkpoint(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, NULL_CONTEXT);
        closeCountsStore();
        openCountsStore();
    }

    private void crashAndRestartCountsStore() throws Exception {
        closeCountsStore();
        openCountsStore();
    }

    private void deleteCountsStore() throws IOException {
        directory.getFileSystem().deleteFile(countsStoreFile());
    }

    private Path countsStoreFile() {
        return directory.file("counts.db");
    }

    private void openCountsStore(Rebuilder builder) throws IOException {
        instantiateCountsStore(builder, false, NO_MONITOR);
        countsStore.start(NULL_CONTEXT, INSTANCE);
    }

    private void instantiateCountsStore(Rebuilder builder, boolean readOnly, GBPTreeGenericCountsStore.Monitor monitor)
            throws IOException {
        countsStore = new GBPTreeGenericCountsStore(
                pageCache,
                new StoreFile(countsStoreFile()),
                fs,
                immediate(),
                builder,
                readOnly,
                "test",
                monitor,
                DEFAULT_DATABASE_NAME,
                randomMaxCacheSize(),
                NullLogProvider.getInstance(),
                CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                getOpenOptions());
    }

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    private static void assertZeroGlobalTracer(PageCacheTracer pageCacheTracer) {
        assertThat(pageCacheTracer.faults()).isZero();
        assertThat(pageCacheTracer.pins()).isZero();
        assertThat(pageCacheTracer.unpins()).isZero();
        assertThat(pageCacheTracer.hits()).isZero();
    }

    private static void assertZeroTracer(CursorContext cursorContext) {
        var cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.faults()).isZero();
        assertThat(cursorTracer.pins()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
        assertThat(cursorTracer.hits()).isZero();
    }

    private static class TestableCountsBuilder implements Rebuilder {
        private final long rebuiltAtTransactionId;
        boolean lastCommittedTxIdCalled;
        boolean rebuildCalled;

        TestableCountsBuilder(long rebuiltAtTransactionId) {
            this.rebuiltAtTransactionId = rebuiltAtTransactionId;
        }

        @Override
        public void rebuild(CountUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
            rebuildCalled = true;
        }

        @Override
        public long lastCommittedTxId() {
            lastCommittedTxIdCalled = true;
            return rebuiltAtTransactionId;
        }
    }

    private int randomMaxCacheSize() {
        return random.nextInt(10, 100);
    }
}
