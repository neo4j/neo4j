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
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.index.internal.gbptree.RootLayerConfiguration.multipleRoots;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.data.Percentage;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.EmptyDependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

@ExtendWith(RandomExtension.class)
@EphemeralPageCacheExtension
class MultiRootGBPTreeTest {
    private static final SimpleByteArrayLayout rootKeyLayout = new SimpleByteArrayLayout();
    private static final SimpleByteArrayLayout layout = new SimpleByteArrayLayout();

    @Inject
    private RandomSupport random;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    private MultiRootGBPTree<RawBytes, RawBytes, RawBytes> tree;
    private long highestUsableSeed;

    @BeforeEach
    void start() {
        PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        var path = directory.file("tree");
        tree = new MultiRootGBPTree<>(
                pageCache,
                fileSystem,
                path,
                layout,
                NO_MONITOR,
                NO_HEADER_READER,
                immediate(),
                false,
                getOpenOptions(),
                "db",
                "test multi-root tree",
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                multipleRoots(rootKeyLayout, (int) kibiBytes(1)),
                pageCacheTracer,
                EmptyDependencyResolver.EMPTY_RESOLVER,
                TreeNodeLayoutFactory.getInstance(),
                LoggingStructureWriteLog.forGBPTree(fileSystem, path));
        highestUsableSeed = layout.highestUsableSeed();
    }

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @AfterEach
    void stop() throws IOException {
        if (tree != null) {
            try {
                assertThat(consistencyCheckStrict(tree)).isTrue();
            } finally {
                tree.close();
                tree = null;
            }
        }
    }

    @Test
    void multiRootMinimalHashcodeCacheIndex() throws IOException {
        tree.close();
        tree = null;

        PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
        var layoutWithBadHashes = new MinimalHashCodeEntriesLayout();
        var path = directory.file("tree");
        try (var badHashesTree = new MultiRootGBPTree<>(
                pageCache,
                fileSystem,
                path,
                layoutWithBadHashes,
                NO_MONITOR,
                NO_HEADER_READER,
                immediate(),
                false,
                getOpenOptions(),
                "db",
                "test multi-root tree",
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                multipleRoots(layoutWithBadHashes, (int) kibiBytes(1)),
                pageCacheTracer,
                EmptyDependencyResolver.EMPTY_RESOLVER,
                TreeNodeLayoutFactory.getInstance(),
                LoggingStructureWriteLog.forGBPTree(fileSystem, path))) {

            var externalId1 = 101;
            badHashesTree.create(layoutWithBadHashes.key(externalId1), NULL_CONTEXT);
            insertData(badHashesTree, externalId1, 1, 100);
            assertSeek(badHashesTree, externalId1, 1, 100);

            assertThat(consistencyCheckStrict(badHashesTree)).isTrue();
        }
    }

    @Test
    void shouldThrowOnAccessNonExistentExternalRoot() {
        assertThatThrownBy(() ->
                        tree.access(rootKeyLayout.key(99)).writer(NULL_CONTEXT).close())
                .isInstanceOf(DataTreeNotFoundException.class);
    }

    @Test
    void shouldCreateAndAccessExternalRoots() throws IOException {
        // given/when
        var externalId1 = 101;
        var externalId2 = 979;
        tree.create(rootKeyLayout.key(externalId1), NULL_CONTEXT);
        tree.create(rootKeyLayout.key(externalId2), NULL_CONTEXT);
        insertData(externalId1, 1, 100);
        insertData(externalId2, 1_000, 100);

        // then
        assertSeek(externalId1, 1, 100);
        assertSeek(externalId2, 1_000, 100);
    }

    @Test
    void shouldFailCreatingExistingRoot() throws IOException {
        // given
        tree.create(rootKeyLayout.key(123), NULL_CONTEXT);

        // then
        assertThatThrownBy(() -> tree.create(rootKeyLayout.key(123), NULL_CONTEXT))
                .isInstanceOf(DataTreeAlreadyExistsException.class);
    }

    @Test
    void shouldCreateAndDeleteMultipleRootsInParallel() {
        // given
        var maxNumRoots = random.nextInt(1_000, 2_000);
        var numThreads = random.nextInt(2, 8);
        var firstSeed = random.nextLong(0, Integer.MAX_VALUE);
        var highRootId = new AtomicLong();
        var deletedRootIds = LongSets.mutable.empty();
        var sequence = new ArrayQueueOutOfOrderSequence(-1, 50, OutOfOrderSequence.EMPTY_META);

        // when
        var race = new Race().withEndCondition(() -> highRootId.get() >= maxNumRoots);
        race.addContestants(numThreads, throwing(() -> {
            var create = ThreadLocalRandom.current().nextFloat() < 0.7 || sequence.getHighestGapFreeNumber() <= 0;
            if (!create) {
                // check if there's one to delete
                long rootIdToDelete;
                synchronized (deletedRootIds) {
                    var attempts = 0;
                    do {
                        rootIdToDelete = ThreadLocalRandom.current().nextLong(sequence.getHighestGapFreeNumber());
                        if (attempts++ > 100) {
                            rootIdToDelete = -1;
                            break;
                        }
                    } while (!deletedRootIds.add(rootIdToDelete));
                }
                if (rootIdToDelete != -1) {
                    deleteRootContents(rootIdToDelete);
                    tree.delete(rootKeyLayout.key(rootIdToDelete), NULL_CONTEXT);
                }
            } else {
                var rootId = highRootId.getAndIncrement();
                tree.create(rootKeyLayout.key(rootId), NULL_CONTEXT);
                try (var writer = tree.access(rootKeyLayout.key(rootId)).writer(NULL_CONTEXT)) {
                    writer.put(layout.key(firstSeed + rootId), layout.value(firstSeed + rootId));
                }
                sequence.offer(rootId, OutOfOrderSequence.EMPTY_META);
            }
        }));
        race.goUnchecked();
    }

    @Test
    void shouldWriteToMultipleRootsInParallel() throws Exception {
        // given
        var numRoots = random.nextInt(2, 50);
        var numThreads = random.nextInt(2, 16);
        var externalIds = randomExternalIds(numRoots);
        var executor = Executors.newFixedThreadPool(numThreads);
        var numWritten = new AtomicInteger[numRoots];
        for (var i = 0; i < numRoots; i++) {
            numWritten[i] = new AtomicInteger();
        }

        try {
            // create the roots in parallel
            var creationTasks = new ArrayList<Callable<Void>>();
            for (var i = 0; i < numRoots; i++) {
                var rootIndex = i;
                creationTasks.add(() -> {
                    tree.create(rootKeyLayout.key(externalIds[rootIndex]), NULL_CONTEXT);
                    return null;
                });
            }
            getAllResults(executor.invokeAll(creationTasks));

            // when writing in parallel
            var numTasks = random.nextInt(numThreads * 100, numThreads * 10_000);
            var writeTasks = new ArrayList<Callable<Void>>();
            for (var i = 0; i < numTasks; i++) {
                if (random.nextInt(100) == 0) {
                    // Checkpoint
                    writeTasks.add(() -> {
                        tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
                        return null;
                    });
                } else {
                    // Write one oe more entries into a selected root
                    var rootIndex = random.nextInt(numRoots);
                    var numEntries = random.nextInt(1, 5);
                    writeTasks.add(() -> {
                        try (var writer = tree.access(rootKeyLayout.key(externalIds[rootIndex]))
                                .writer(NULL_CONTEXT)) {
                            for (var e = 0; e < numEntries; e++) {
                                var entrySeed = externalIds[rootIndex] + numWritten[rootIndex].getAndIncrement();
                                writer.put(layout.key(entrySeed), layout.value(entrySeed));
                            }
                        }
                        return null;
                    });
                }
            }
            getAllResults(executor.invokeAll(writeTasks));
        } finally {
            executor.shutdown();
        }

        // then all mappings and data should be there
        for (var i = 0; i < externalIds.length; i++) {
            assertSeek(externalIds[i], externalIds[i], numWritten[i].get());
        }
    }

    @Test
    void shouldCreateDeleteAndAccessRoots() throws IOException {
        // given
        var numWriterThreads = random.nextInt(2, 4);
        var numReaderThreads = random.nextInt(2, 4);
        Map<Long, RootContents> roots = new ConcurrentHashMap<>();

        // when
        var numCreatedRoots = new AtomicInteger();
        var numDeletedRoots = new AtomicInteger();
        var numWrites = new AtomicInteger();
        var race = new Race()
                .withEndCondition(() ->
                        numCreatedRoots.get() >= 1_000 && numDeletedRoots.get() >= 1_000 && numWrites.get() >= 5_000);
        race.addContestants(numWriterThreads, throwing(() -> {
            var random = ThreadLocalRandom.current();
            var rng = random.nextFloat();
            if (rng < 0.1) {
                // create root
                createRoot(roots, random);
                numCreatedRoots.incrementAndGet();
            } else if (rng < 0.15) {
                // delete root
                var root = findRoot(roots, random, rootContents -> rootContents.exists.compareAndSet(true, false));
                if (root != null) {
                    while (true) {
                        try {
                            deleteRoot(roots, root);
                            numDeletedRoots.incrementAndGet();
                            break;
                        } catch (DataTreeNotEmptyException e) {
                            // This is OK since this test doesn't actively prevent writers from inserting into this root
                            // while deleting it
                            // The thing is that if it fails here then this test just attempted to remove all its
                            // contents, but apparently
                            // there were other writers inserting stuff at the same time so some contents are left, and
                            // the actual state
                            // no longer aligns with what RootContent says (low/high), so let's just continue to try and
                            // deleted it until
                            // it succeeds.
                        }
                    }
                }
            } else {
                // write to root
                var root = findRoot(roots, random, rootContents -> rootContents.exists.get());
                if (root != null) {
                    try {
                        writeToRoot(random, root);
                        numWrites.incrementAndGet();
                    } catch (DataTreeNotFoundException e) {
                        // This is OK since this test doesn't actively prevent roots from being deleted while writing to
                        // them
                    }
                }
            }
        }));
        race.addContestants(numReaderThreads, throwing(() -> {
            // read from root
            var random = ThreadLocalRandom.current();
            var root = findRoot(roots, random, rootContents -> rootContents.exists.get());
            if (root != null) {
                try (var seek = allSeek(root.key)) {
                    for (int i = 0; i < 10 && seek.next(); i++) {
                        // do nothing, just advance the seeker a bit
                    }
                } catch (DataTreeNotFoundException e) {
                    // This is OK since this test doesn't actively prevent roots from being deleted while reading from
                    // them
                }
            }
        }));
        race.addContestant(throwing(() -> {
            Thread.sleep(200);
            tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }));
        race.goUnchecked();

        // then verify expected contents
        List<String> problems = new ArrayList<>();
        for (var root : roots.entrySet()) {
            assertThat(root.getValue().exists.get()).isTrue();
            try (var seek = allSeek(root.getKey())) {
                for (long nextExpectedSeed = root.getValue().low(),
                                max = root.getValue().high();
                        nextExpectedSeed < max;
                        nextExpectedSeed++) {
                    assertThat(seek.next()).isTrue();
                    var buffer = ByteBuffer.wrap(seek.value().bytes);
                    var valueSeed = buffer.getLong();
                    var valueKey = buffer.getLong();
                    var keySeed = layout.keySeed(seek.key());
                    if (keySeed != nextExpectedSeed
                            || valueSeed != nextExpectedSeed
                            || valueKey != root.getValue().key) {
                        problems.add(format(
                                "Unexpected entry: nextExpectedSeed:%d keySeed:%d valueSeed:%d valueKey:%s root:%s",
                                nextExpectedSeed, keySeed, valueSeed, valueKey, root));
                    }
                }
                while (seek.next()) {
                    var buffer = ByteBuffer.wrap(seek.value().bytes);
                    var valueSeed = buffer.getLong();
                    var valueKey = buffer.getLong();
                    var keySeed = layout.keySeed(seek.key());
                    problems.add(format(
                            "Unexpected high entry: keySeed:%d valueSeed:%d valueKey:%s for root:%s",
                            keySeed, valueSeed, valueKey, root));
                }
            }
        }
        if (!problems.isEmpty()) {
            fail(format("Unexpected results:%n%s", StringUtils.join(problems, format("%n"))));
        }
    }

    @Test
    void dontHangParallelCheckingOfMultiRootTrees() throws IOException {
        // A simple test that would deadlock in consistency checker before the corresponding fix.
        // One thread was waiting on everything started for the MultiRootLayer.consistencyCheck and the other one was
        // hanging waiting for all of its child tasks to finish in GBPTreeConsistencyChecker.checkSubtree.
        // The fix removed the inner waiting for children to not block one thread for each subtree on just waiting.

        Map<Long, RootContents> roots = new ConcurrentHashMap<>();
        var numWrites = new AtomicInteger();

        var random = ThreadLocalRandom.current();
        createRoot(roots, random);
        createRoot(roots, random);
        while (numWrites.get() <= 15000) {
            var root = findRoot(roots, random, rootContents -> rootContents.exists.get());
            if (root != null) {
                try {
                    writeToRoot(random, root);
                    numWrites.incrementAndGet();
                } catch (DataTreeNotFoundException e) {
                    // This is OK since this test doesn't actively prevent roots from being deleted while writing to
                    // them
                }
            }
        }

        assertThat(GBPTreeTestUtil.consistencyCheck(tree, new GBPTreeConsistencyCheckVisitor.Adaptor(), 2))
                .isTrue();
    }

    @Test
    void shouldCreateDeleteAndUpdateRootsConcurrently() throws IOException {
        // when
        var ops = new AtomicInteger();
        var numCheckpoints = new AtomicInteger();
        var race = new Race().withEndCondition(() -> ops.get() > 10000 && numCheckpoints.get() >= 20);
        List<RawBytes> keys = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            keys.add(rootKeyLayout.key(i));
        }
        race.addContestants(3, throwing(() -> {
            try {
                RawBytes key = random.among(keys);
                tree.create(key, NULL_CONTEXT);
                updateKey(key, false);
            } catch (DataTreeAlreadyExistsException ignored) {
            }
            ops.incrementAndGet();
        }));
        race.addContestants(3, throwing(() -> {
            try {
                RawBytes key = random.among(keys);
                updateKey(key, true);
                tree.delete(key, NULL_CONTEXT);
            } catch (DataTreeNotFoundException | DataTreeNotEmptyException ignored) {
            }
            ops.incrementAndGet();
        }));

        race.addContestants(3, throwing(() -> {
            updateKey(random.among(keys), false);
            ops.incrementAndGet();
        }));

        race.addContestant(throwing(() -> {
            Thread.sleep(50);
            tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
            numCheckpoints.incrementAndGet();
        }));
        race.goUnchecked();
    }

    private void updateKey(RawBytes key, boolean delete) throws IOException {
        try {
            var access = tree.access(key);
            try (var writer = access.writer(NULL_CONTEXT)) {
                if (delete) {
                    writer.remove(layout.key(0));
                } else {
                    writer.put(layout.key(0), valueWithKey(random.nextInt(), random.nextInt()));
                }
            }
        } catch (DataTreeNotFoundException ignored) {
        }
    }

    @Test
    void shouldDeleteRoot() throws IOException {
        // given
        long externalId1 = 123456;
        long externalId2 = 654321;
        tree.create(rootKeyLayout.key(externalId1), NULL_CONTEXT);
        tree.create(rootKeyLayout.key(externalId2), NULL_CONTEXT);
        assertThat(allExternalRoots()).isEqualTo(LongSets.immutable.of(externalId1, externalId2));

        // when
        tree.delete(rootKeyLayout.key(externalId1), NULL_CONTEXT);

        // then
        assertThatThrownBy(() -> tree.delete(rootKeyLayout.key(externalId1), NULL_CONTEXT))
                .isInstanceOf(DataTreeNotFoundException.class);
        assertThatThrownBy(() -> tree.access(rootKeyLayout.key(externalId1))
                        .writer(NULL_CONTEXT)
                        .close())
                .isInstanceOf(DataTreeNotFoundException.class);
        assertThat(allExternalRoots()).isEqualTo(LongSets.immutable.of(externalId2));
    }

    @Test
    void shouldReopenMultiRootGBPTree() throws IOException {
        // given
        long externalId1 = 111;
        long externalId2 = 222;
        tree.create(rootKeyLayout.key(externalId1), NULL_CONTEXT);
        tree.create(rootKeyLayout.key(externalId2), NULL_CONTEXT);
        long seed1 = random.seed();
        long seed2 = random.seed() + 1;
        int count1 = random.nextInt(100) + 50;
        int count2 = random.nextInt(100) + 50;
        insertData(externalId1, seed1, count1);
        insertData(externalId2, seed2, count2);
        assertSeek(externalId1, seed1, count1);
        assertSeek(externalId2, seed2, count2);

        // when
        tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();
        start();

        // then
        assertSeek(externalId1, seed1, count1);
        assertSeek(externalId2, seed2, count2);
    }

    @Test
    void shouldIdentifyWrongRootLayoutOnOpen() throws IOException {
        // given
        tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();

        // when/then
        var wrongRootLayout = new SimpleLongLayout.Builder().build();
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        assertThatThrownBy(() -> new MultiRootGBPTree<>(
                        pageCache,
                        fileSystem,
                        directory.file("tree"),
                        layout,
                        NO_MONITOR,
                        NO_HEADER_READER,
                        immediate(),
                        false,
                        getOpenOptions(),
                        "db",
                        "test multi-root tree",
                        new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        multipleRoots(wrongRootLayout, (int) kibiBytes(1)),
                        cacheTracer,
                        EmptyDependencyResolver.EMPTY_RESOLVER,
                        TreeNodeLayoutFactory.getInstance(),
                        StructureWriteLog.EMPTY))
                .isInstanceOf(MetadataMismatchException.class);
    }

    @Test
    void shouldIdentifyWrongDataLayoutOnOpen() throws IOException {
        // given
        tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        stop();

        // when/then
        var wrongDataLayout = new SimpleLongLayout.Builder().build();
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        assertThatThrownBy(() -> new MultiRootGBPTree<>(
                        pageCache,
                        fileSystem,
                        directory.file("tree"),
                        wrongDataLayout,
                        NO_MONITOR,
                        NO_HEADER_READER,
                        immediate(),
                        false,
                        getOpenOptions(),
                        "db",
                        "test multi-root tree",
                        new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        multipleRoots(rootKeyLayout, (int) kibiBytes(1)),
                        cacheTracer,
                        EmptyDependencyResolver.EMPTY_RESOLVER,
                        TreeNodeLayoutFactory.getInstance(),
                        StructureWriteLog.EMPTY))
                .isInstanceOf(MetadataMismatchException.class);
    }

    @Test
    void shouldReopenWhenRootAndDataLayoutHasDifferentFormatIdentifiers() throws IOException {
        // given
        var file = directory.file("other-tree");
        var dataLayout = new SimpleLongLayout.Builder().withFixedSize(true).build();

        // when/then
        for (int i = 0; i < 2; i++) {
            PageCacheTracer cacheTracer = PageCacheTracer.NULL;
            new MultiRootGBPTree<>(
                            pageCache,
                            fileSystem,
                            file,
                            dataLayout,
                            NO_MONITOR,
                            NO_HEADER_READER,
                            immediate(),
                            false,
                            getOpenOptions(),
                            "db",
                            "test multi-root tree",
                            new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                            multipleRoots(rootKeyLayout, (int) kibiBytes(1)),
                            cacheTracer,
                            EmptyDependencyResolver.EMPTY_RESOLVER,
                            TreeNodeLayoutFactory.getInstance(),
                            StructureWriteLog.EMPTY)
                    .close();
        }
    }

    @Test
    void shouldEstimateNumberOfEntriesInTree() throws IOException {
        // given
        long[] rootIds = {1, 2, 3};
        var race = new Race().withEndCondition(() -> false);
        for (var i = 0; i < rootIds.length; i++) {
            var index = i;
            race.addContestant(
                    throwing(() -> {
                        var key = rootKeyLayout.key(rootIds[index]);
                        tree.create(key, NULL_CONTEXT);
                        int numberOfEntries = 3_000 * (index + 1);
                        try (var writer = tree.access(key).writer(NULL_CONTEXT)) {
                            for (int e = 0; e < numberOfEntries; e++) {
                                writer.put(layout.key(e), layout.value(e));
                            }
                        }
                    }),
                    1);
        }
        race.goUnchecked();

        // when
        for (int i = 0; i < rootIds.length; i++) {
            var rootId = rootIds[i];
            var estimate = tree.access(rootKeyLayout.key(rootId)).estimateNumberOfEntriesInTree(NULL_CONTEXT);
            assertThat(estimate).isCloseTo(3_000 * (i + 1), Percentage.withPercentage(5));
        }
    }

    @Test
    void shouldAbortDuringVisitAllDataTreeRoots() throws IOException {
        // given
        final var externalId1 = 123456;
        final var externalId2 = 654321;
        tree.create(rootKeyLayout.key(externalId1), NULL_CONTEXT);
        tree.create(rootKeyLayout.key(externalId2), NULL_CONTEXT);

        // when
        final var roots = LongSets.mutable.empty();
        tree.visitAllRoots(NULL_CONTEXT, key -> {
            roots.add(rootKeyLayout.keySeed(key));
            return true;
        });

        // then
        assertThat(roots.toImmutable()).isEqualTo(LongSets.immutable.of(externalId1));
    }

    @Test
    void shouldCreateRootsThroughCheckpoints() throws IOException {
        // given
        var nextRootKey = new AtomicLong();
        var numCheckpoints = new AtomicInteger();
        var race = new Race().withEndCondition(() -> nextRootKey.get() > 1_000 && numCheckpoints.get() > 20);
        race.addContestant(throwing(() -> tree.create(rootKeyLayout.key(nextRootKey.getAndIncrement()), NULL_CONTEXT)));
        race.addContestant(throwing(() -> {
            tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
            numCheckpoints.incrementAndGet();
        }));

        // when
        race.goUnchecked();

        // then
        var nextExpectedRootKey = new AtomicLong();
        tree.visitAllRoots(NULL_CONTEXT, root -> {
            assertThat(rootKeyLayout.keySeed(root)).isEqualTo(nextExpectedRootKey.getAndIncrement());
            assertThat(rootKeyLayout.keySeed(root)).isLessThan(nextRootKey.get());
            var low = layout.newKey();
            var high = layout.newKey();
            layout.initializeAsLowest(low);
            layout.initializeAsHighest(high);
            try (var seek = tree.access(root).seek(low, high, NULL_CONTEXT)) {
                var count = 0;
                while (seek.next()) {
                    count++;
                }
                assertThat(count).isZero();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return false;
        });
    }

    @Test
    void shouldDeleteRootConcurrently() throws IOException {
        // when
        var rootKey = rootKeyLayout.key(0);
        tree.create(rootKey, NULL_CONTEXT);
        var race = new Race();
        race.addContestants(
                8,
                throwing(() -> {
                    try {
                        tree.delete(rootKey, NULL_CONTEXT);
                    } catch (DataTreeNotFoundException ignored) {
                    }
                }),
                1);
        race.goUnchecked();

        // then
        assertThatThrownBy(() -> tree.access(rootKey).writer(NULL_CONTEXT))
                .isInstanceOf(DataTreeNotFoundException.class);
    }

    private void deleteRootContents(long key) throws IOException {
        List<RawBytes> keys = new ArrayList<>();
        try (var seek = allSeek(key)) {
            while (seek.next()) {
                keys.add(layout.copyKey(seek.key()));
            }
        }
        try (var writer = tree.access(rootKeyLayout.key(key)).writer(NULL_CONTEXT)) {
            keys.forEach(writer::remove);
        }
    }

    private void writeToRoot(ThreadLocalRandom random, RootContents root) throws IOException {
        var access = tree.access(rootKeyLayout.key(root.key));
        try (var writer = access.writer(NULL_CONTEXT)) {
            var writeCount = random.nextInt(1, 100);
            for (var i = 0; i < writeCount; i++) {
                if (random.nextFloat() < 0.1) {
                    // remove
                    var seed = root.nextLow();
                    if (seed != -1) {
                        var removedValue = writer.remove(layout.key(seed));
                        if (removedValue == null) {
                            // Suspicion has it that this root is right now being deleted, or rather that the one doing
                            // it (above in the test)
                            // first removes all its data. Let's verify that it's the case.
                            assertThat(root.exists.get()).isFalse();
                        } else {
                            var buffer = ByteBuffer.wrap(removedValue.bytes);
                            var valueSeed = buffer.getLong();
                            var valueKey = buffer.getLong();
                            if (valueSeed != seed) {
                                fail("Value contains a different seed:" + valueSeed + " than expected:" + seed);
                            }
                            if (valueKey != root.key) {
                                fail("Value contains a different key:" + valueKey + " than expected:" + root.key);
                            }
                        }
                    }
                } else {
                    var seed = root.nextHigh();
                    writer.put(layout.key(seed), valueWithKey(seed, root.key));
                }
            }
        }
    }

    private RawBytes valueWithKey(long seed, long key) {
        return new RawBytes(
                ByteBuffer.allocate(Long.BYTES * 2).putLong(seed).putLong(key).array());
    }

    private void deleteRoot(Map<Long, RootContents> roots, RootContents root) throws IOException {
        deleteRootContents(root.key);
        tree.delete(rootKeyLayout.key(root.key), NULL_CONTEXT);
        roots.remove(root.key);
    }

    private void createRoot(Map<Long, RootContents> roots, ThreadLocalRandom random) throws IOException {
        long key;
        RootContents rootContents;
        do {
            key = random.nextLong(highestUsableSeed);
            rootContents = new RootContents(key, new AtomicBoolean(), new AtomicLong());
        } while (roots.putIfAbsent(key, rootContents) != null);
        tree.create(rootKeyLayout.key(key), NULL_CONTEXT);
        rootContents.exists.set(true);
    }

    private RootContents findRoot(Map<Long, RootContents> roots, Random random, Predicate<RootContents> condition) {
        var keys = roots.keySet().toArray(new Long[0]);
        var key = keys.length > 0 ? keys[random.nextInt(keys.length)] : null;
        if (key == null) {
            return null;
        }
        var rootContents = roots.get(key);
        if (rootContents == null || !condition.test(rootContents)) {
            return null;
        }
        return rootContents;
    }

    private LongSet allExternalRoots() throws IOException {
        MutableLongSet set = LongSets.mutable.empty();
        tree.visitAllRoots(NULL_CONTEXT, key -> {
            set.add(rootKeyLayout.keySeed(key));
            return false;
        });
        return set;
    }

    private long[] randomExternalIds(int count) {
        var ids = LongSets.mutable.empty();
        var externalIds = new long[count];
        for (var i = 0; i < count; i++) {
            long id;
            do {
                id = random.nextLong(0, highestUsableSeed - 10_000);
            } while (!ids.add(id));
            externalIds[i] = id;
        }
        return externalIds;
    }

    private void assertSeek(
            MultiRootGBPTree<RawBytes, RawBytes, RawBytes> tree, long externalId, long startSeed, int count)
            throws IOException {
        var low = layout.newKey();
        var high = layout.newKey();
        layout.initializeAsLowest(low);
        layout.initializeAsHighest(high);
        try (var seek = tree.access(rootKeyLayout.key(externalId)).seek(low, high, NULL_CONTEXT)) {
            for (var i = 0; i < count; i++) {
                assertThat(seek.next()).isTrue();
                assertThat(seek.key().bytes).isEqualTo(layout.key(startSeed + i).bytes);
                assertThat(seek.value().bytes).isEqualTo(layout.value(startSeed + i).bytes);
            }
            assertThat(seek.next()).isFalse();
        }
    }

    private void assertSeek(long externalId, long startSeed, int count) throws IOException {
        assertSeek(tree, externalId, startSeed, count);
    }

    private void insertData(long externalId, long startSeed, int count) throws IOException {
        insertData(tree, externalId, startSeed, count);
    }

    private void insertData(
            MultiRootGBPTree<RawBytes, RawBytes, RawBytes> tree, long externalId, long startSeed, int count)
            throws IOException {
        try (var writer = tree.access(rootKeyLayout.key(externalId)).writer(NULL_CONTEXT)) {
            for (var i = 0; i < count; i++) {
                writer.put(layout.key(startSeed + i), layout.value(startSeed + i));
            }
        }
    }

    private Seeker<RawBytes, RawBytes> allSeek(long key) throws IOException {
        var low = layout.newKey();
        var high = layout.newKey();
        layout.initializeAsLowest(low);
        layout.initializeAsHighest(high);
        var access = tree.access(rootKeyLayout.key(key));
        return access.seek(low, high, NULL_CONTEXT);
    }

    record RootContents(long key, AtomicBoolean exists, AtomicLong position) {
        long nextLow() {
            long from;
            long to;
            long low;
            do {
                from = position.get();
                low = low(from);
                long high = high(from);
                if (low <= high) {
                    // Can't remove something that isn't there yet
                    return -1;
                }
                to = (low + 1) | (high << 32);
            } while (!position.compareAndSet(from, to));
            return low;
        }

        long nextHigh() {
            long from;
            long to;
            long high;
            do {
                from = position.get();
                long low = low(from);
                high = high(from);
                to = low | ((high + 1) << 32);
            } while (!position.compareAndSet(from, to));
            return high;
        }

        long low() {
            return low(position.get());
        }

        long high() {
            return high(position.get());
        }

        private long high(long from) {
            return (from >>> 32) & 0xFFFFFFFFL;
        }

        private long low(long from) {
            return from & 0xFFFFFFFFL;
        }

        @Override
        public String toString() {
            long pos = position.get();
            return "RootContents{" + "key=" + key + ", exists=" + exists + ", low=" + low(pos) + ", high=" + high(pos)
                    + '}';
        }
    }

    private static class MinimalHashCodeMyRawBytes extends RawBytes {

        public MinimalHashCodeMyRawBytes() {}

        public MinimalHashCodeMyRawBytes(byte[] byteArray) {
            super(byteArray);
        }

        @Override
        public int hashCode() {
            return Integer.MIN_VALUE;
        }
    }

    private static class MinimalHashCodeEntriesLayout extends SimpleByteArrayLayout {
        @Override
        public RawBytes newKey() {
            return new MinimalHashCodeMyRawBytes();
        }

        @Override
        public RawBytes copyKey(RawBytes rawBytes, RawBytes into) {
            return new MinimalHashCodeMyRawBytes(Arrays.copyOf(rawBytes.bytes, rawBytes.bytes.length));
        }
    }
}
