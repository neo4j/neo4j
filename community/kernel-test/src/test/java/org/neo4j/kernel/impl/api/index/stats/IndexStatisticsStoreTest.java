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
package org.neo4j.kernel.impl.api.index.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.annotations.documented.ReporterFactories.noopReporterFactory;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
@ExtendWith(RandomExtension.class)
class IndexStatisticsStoreTest {
    private LifeSupport lifeSupport = new LifeSupport();

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private IndexStatisticsStore store;
    private final PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
    private CursorContextFactory contextFactory;

    @BeforeEach
    void setUp() throws IOException {
        contextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        store = openStore("stats");
        lifeSupport.start();
    }

    @AfterEach
    void stop() {
        lifeSupport.shutdown();
    }

    private IndexStatisticsStore openStore(String fileName) throws IOException {
        var statisticsStore = new IndexStatisticsStore(
                pageCache,
                fileSystem,
                testDirectory.file(fileName),
                immediate(),
                false,
                DEFAULT_DATABASE_NAME,
                contextFactory,
                pageCacheTracer,
                getOpenOptions());
        return lifeSupport.add(statisticsStore);
    }

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck() throws IOException {
        var store = openStore("consistencyCheck");
        for (int i = 0; i < 100; i++) {
            store.setSampleStats(i, new IndexSample());
        }
        store.checkpoint(FileFlushEvent.NULL, CursorContext.NULL_CONTEXT);

        var checkPageCacheTracer = new DefaultPageCacheTracer();
        var checkContextFactory = new CursorContextFactory(checkPageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        store.consistencyCheck(
                noopReporterFactory(), checkContextFactory, Runtime.getRuntime().availableProcessors());

        assertThat(checkPageCacheTracer.pins()).isEqualTo(9);
        assertThat(checkPageCacheTracer.unpins()).isEqualTo(9);
        assertThat(checkPageCacheTracer.hits()).isEqualTo(9);
    }

    @Test
    void tracePageCacheAccessOnStatisticStoreInitialisation() throws IOException {
        long initialPins = pageCacheTracer.pins();
        long initialUnpins = pageCacheTracer.unpins();
        long initialHits = pageCacheTracer.hits();
        long initialFaults = pageCacheTracer.faults();

        openStore("tracedStats");

        assertThat(pageCacheTracer.faults() - initialFaults).isEqualTo(3);
        assertThat(pageCacheTracer.pins() - initialPins).isEqualTo(4);
        assertThat(pageCacheTracer.unpins() - initialUnpins).isEqualTo(4);
        assertThat(pageCacheTracer.hits() - initialHits).isEqualTo(1);
    }

    @Test
    void tracePageCacheAccessOnCheckpoint() throws IOException {
        var store = openStore("checkpoint");

        try (var cursorContext = contextFactory.create("tracePageCacheAccessOnCheckpoint")) {
            for (int i = 0; i < 100; i++) {
                store.setSampleStats(i, new IndexSample());
            }

            store.checkpoint(FileFlushEvent.NULL, cursorContext);
            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat(cursorTracer.pins()).isEqualTo(30);
            assertThat(cursorTracer.unpins()).isEqualTo(30);
            assertThat(cursorTracer.hits()).isEqualTo(21);
            assertThat(cursorTracer.faults()).isEqualTo(9);
        }
    }

    @Test
    void shouldReplaceIndexSample() {
        // given
        long indexId = 4;

        // when/then
        replaceAndVerifySample(indexId, new IndexSample(456, 123, 456, 3));
        replaceAndVerifySample(indexId, new IndexSample(555, 444, 550, 0));
    }

    @Test
    void shouldIncrementIndexUpdates() {
        // given
        long indexId = 4;
        IndexSample initialSample = new IndexSample(456, 5, 200, 123);
        store.setSampleStats(indexId, initialSample);

        // when
        int addedUpdates = 5;
        store.incrementIndexUpdates(indexId, addedUpdates);

        // then
        assertEquals(
                new IndexSample(
                        initialSample.indexSize(),
                        initialSample.uniqueValues(),
                        initialSample.sampleSize(),
                        initialSample.updates() + addedUpdates),
                store.indexSample(indexId));
    }

    @Test
    void shouldStoreDataOnCheckpoint() throws IOException {
        // given
        long indexId1 = 1;
        long indexId2 = 2;
        IndexSample sample1 = new IndexSample(500, 100, 200, 25);
        IndexSample sample2 = new IndexSample(501, 101, 201, 26);
        store.setSampleStats(indexId1, sample1);
        store.setSampleStats(indexId2, sample2);

        // when
        restartStore();

        // then
        assertEquals(sample1, store.indexSample(indexId1));
        assertEquals(sample2, store.indexSample(indexId2));
    }

    private void restartStore() throws IOException {
        store.checkpoint(FileFlushEvent.NULL, CursorContext.NULL_CONTEXT);
        lifeSupport.shutdown();
        lifeSupport = new LifeSupport();
        store = openStore("stats");
        lifeSupport.start();
    }

    @Test
    void shouldAllowMultipleThreadsIncrementIndexUpdates() throws Throwable {
        // given
        long indexId = 5;
        Race race = new Race();
        int contestants = 20;
        int delta = 3;
        store.setSampleStats(indexId, new IndexSample(0, 0, 0));
        race.addContestants(contestants, () -> store.incrementIndexUpdates(indexId, delta), 1);

        // when
        race.go();

        // then
        assertEquals(new IndexSample(0, 0, 0, contestants * delta), store.indexSample(indexId));
    }

    @Test
    void shouldHandleConcurrentUpdatesWithCheckpointing() throws Throwable {
        // given
        Race race = new Race();
        AtomicBoolean checkpointDone = new AtomicBoolean();
        int contestantsPerIndex = 5;
        int indexes = 3;
        int delta = 5;
        AtomicIntegerArray expected = new AtomicIntegerArray(indexes);
        race.addContestant(throwing(() -> {
            for (int i = 0; i < 20; i++) {
                Thread.sleep(5);
                store.checkpoint(FileFlushEvent.NULL, CursorContext.NULL_CONTEXT);
            }
            checkpointDone.set(true);
        }));
        for (int i = 0; i < indexes; i++) {
            int indexId = i;
            store.setSampleStats(indexId, new IndexSample(0, 0, 0));
            race.addContestants(contestantsPerIndex, () -> {
                while (!checkpointDone.get()) {
                    store.incrementIndexUpdates(indexId, delta);
                    expected.addAndGet(indexId, delta);
                }
            });
        }

        // when
        race.go();

        // then
        for (int i = 0; i < indexes; i++) {
            assertEquals(new IndexSample(0, 0, 0, expected.get(i)), store.indexSample(i));
        }
        restartStore();
        for (int i = 0; i < indexes; i++) {
            assertEquals(new IndexSample(0, 0, 0, expected.get(i)), store.indexSample(i));
        }
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly() {
        final Exception e = assertThrows(
                Exception.class,
                () -> new IndexStatisticsStore(
                        pageCache,
                        fileSystem,
                        testDirectory.file("non-existing"),
                        immediate(),
                        true,
                        DEFAULT_DATABASE_NAME,
                        contextFactory,
                        pageCacheTracer,
                        getOpenOptions()));
        assertTrue(Exceptions.contains(e, t -> t instanceof TreeFileNotFoundException));
    }

    @Test
    void shouldCacheUsageStatistics() {
        // given
        var indexId = 2L;
        var usage = new IndexUsageStats(System.currentTimeMillis(), 123, System.currentTimeMillis() - 1000);
        store.addUsageStats(indexId, usage);

        // when
        var retrievedUsage = store.usageStats(indexId);

        // then
        assertThat(retrievedUsage).isEqualTo(usage);
        assertThat(retrievedUsage).isNotSameAs(usage);
    }

    @Test
    void shouldStoreSampleAndUsageStatistics() throws IOException {
        // given
        var indexId = 12345L;
        var usage = new IndexUsageStats(System.currentTimeMillis(), 987654, System.currentTimeMillis() - 1000);
        var sample = new IndexSample(10, 20, 30, 40);
        store.setSampleStats(indexId, sample);
        store.addUsageStats(indexId, usage);
        assertThat(store.indexSample(indexId)).isEqualTo(sample);
        assertThat(store.usageStats(indexId)).isEqualTo(usage);

        // when
        restartStore();

        // then
        assertThat(store.indexSample(indexId)).isEqualTo(sample);
        assertThat(store.usageStats(indexId)).isEqualTo(usage);
    }

    @Test
    void shouldSetFirstTrackedTimeOnFirstUsageStatisticsUpdate() throws IOException {
        // given
        var indexId = 998L;
        var firstUsage = new IndexUsageStats(System.currentTimeMillis(), 10, 1234567);

        // when
        store.addUsageStats(indexId, firstUsage);

        // then
        assertThat(store.usageStats(indexId)).isEqualTo(firstUsage);

        // and when
        var secondUsage = new IndexUsageStats(System.currentTimeMillis(), 5, 9999999);
        store.addUsageStats(indexId, secondUsage);

        // then
        assertThat(store.usageStats(indexId).trackedSince()).isEqualTo(firstUsage.trackedSince());
        restartStore();
        assertThat(store.usageStats(indexId).trackedSince()).isEqualTo(firstUsage.trackedSince());
    }

    @Test
    void shouldAddUsageStatisticsQueryCountToCache() {
        // given
        var indexId = 12345L;
        var lastUsedTime = System.currentTimeMillis();
        var trackedSinceTime = System.currentTimeMillis() - 1000;
        var firstUsage = new IndexUsageStats(lastUsedTime, 987654, trackedSinceTime);
        var secondUsage = new IndexUsageStats(lastUsedTime + 2000, 100, trackedSinceTime + 1000);
        var expectedUsage = new IndexUsageStats(
                secondUsage.lastRead(), firstUsage.readCount() + secondUsage.readCount(), firstUsage.trackedSince());

        // When
        store.addUsageStats(indexId, firstUsage);
        store.addUsageStats(indexId, secondUsage);

        // Then
        assertThat(store.usageStats(indexId)).isEqualTo(expectedUsage);
    }

    @Test
    void shouldAddUsageStatisticsQueryCountToStore() throws IOException {
        // given
        var indexId = 12345L;
        var lastUsedTime = System.currentTimeMillis();
        var trackedSinceTime = System.currentTimeMillis() - 1000;
        var firstUsage = new IndexUsageStats(lastUsedTime, 987654, trackedSinceTime);
        store.addUsageStats(indexId, firstUsage);

        // When
        restartStore();
        var secondUsage = new IndexUsageStats(lastUsedTime + 2000, 100, trackedSinceTime + 1000);
        var expectedUsage = new IndexUsageStats(
                secondUsage.lastRead(), firstUsage.readCount() + secondUsage.readCount(), firstUsage.trackedSince());
        store.addUsageStats(indexId, secondUsage);

        // Then
        assertThat(store.usageStats(indexId)).isEqualTo(expectedUsage);
    }

    @Test
    void shouldAddUsageStatisticsFromConcurrentThreads() {
        // given
        var indexId = 132435L;
        var race = new Race();
        var sessionsPerThread = 10;
        var queriesPerSession = 10;
        var numThreads = 4;
        var expectedMinTimeMillis = new AtomicLong(Long.MAX_VALUE);
        var expectedMaxTimeMillis = new AtomicLong();
        race.addContestants(
                numThreads,
                throwing(() -> {
                    var rng = ThreadLocalRandom.current();
                    var myClockMillis = new MutableLong();
                    for (var i = 0; i < sessionsPerThread; i++) {
                        var time = myClockMillis.addAndGet(rng.nextInt(100));
                        if (i == 0) {
                            expectedMinTimeMillis.updateAndGet(operand -> Long.min(time, operand));
                        }
                        store.addUsageStats(indexId, new IndexUsageStats(time, queriesPerSession, time));
                    }
                    expectedMaxTimeMillis.updateAndGet(operand -> Long.max(myClockMillis.longValue(), operand));
                }),
                1);

        // when
        race.goUnchecked();

        // then
        var usageStats = store.usageStats(indexId);
        assertThat(usageStats.lastRead()).isEqualTo(expectedMaxTimeMillis.get());
        assertThat(usageStats.readCount()).isEqualTo(sessionsPerThread * queriesPerSession * numThreads);
        assertThat(usageStats.trackedSince()).isLessThan(usageStats.lastRead());
        assertThat(usageStats.trackedSince()).isEqualTo(expectedMinTimeMillis.get());
    }

    private void replaceAndVerifySample(long indexId, IndexSample indexSample) {
        store.setSampleStats(indexId, indexSample);
        assertEquals(indexSample, store.indexSample(indexId));
    }
}
