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

import static java.lang.Math.abs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.PageCacheConfig;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith({RandomExtension.class, DefaultFileSystemExtension.class, TestDirectorySupportExtension.class})
class PartitionedSeekTest {
    private static final int PAGE_SIZE = 512;

    @RegisterExtension
    static PageCacheSupportExtension pageCacheSupportExtension =
            new PageCacheSupportExtension(PageCacheConfig.config().withPageSize(PAGE_SIZE));

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    @Inject
    private PageCache pageCache;

    private SimpleLongLayout layout;
    private Path treeFile;

    @BeforeEach
    void setup() {
        // Make keys larger with padding so they fill up tree faster, but not beyond entry limit.
        layout = SimpleLongLayout.longLayout().build();
        treeFile = testDirectory.file("tree");
    }

    @ParameterizedTest
    @MethodSource("assertEntries")
    void shouldPartitionTreeWithLeafRoot(String name, AssertEntries assertEntries) throws IOException {
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            // given
            int to = insertEntries(tree, 0, 5, 1);
            DepthAndRootVisitor visit = visit(tree);
            assertThat(visit.numberOfLevels).as("depth of tree").isEqualTo(1);

            // when
            List<MutableLong> partitionEdges = tree.partitionedSeek(layout.key(0), layout.key(to), 4, NULL_CONTEXT);

            // then
            assertThat(partitionEdges.size() - 1).as("number of partitions").isEqualTo(1);
            assertEntries.of(partitionEdges, 0, 5, tree);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldPartitionTreeWithFewerNumberOfRootKeys(String name, AssertEntries assertEntries) throws IOException {
        shouldPartitionTree(2, 3, 4, 3, assertEntries);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldPartitionTreeWithPreciseNumberOfRootKeys(String name, AssertEntries assertEntries) throws IOException {
        shouldPartitionTree(2, 5, 5, 5, assertEntries);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldPartitionTreeWithMoreNumberOfRootKeys(String name, AssertEntries assertEntries) throws IOException {
        shouldPartitionTree(2, 12, 6, 6, assertEntries);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldPartitionTreeOnLevel1(String name, AssertEntries assertEntries) throws IOException {
        shouldPartitionTree(3, 3, 4, 4, assertEntries);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldPartitionTreeWithRandomKeysAndFindAll(String name, AssertEntries assertEntries) throws IOException {
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            // given
            int numberOfRootChildren = random.nextInt(1, 10);
            int numberOfDesiredLevels = numberOfRootChildren == 0 ? 1 : random.nextInt(2, 4);
            int numberOfDesiredPartitions = random.nextInt(1, 10);
            int high = insertEntriesUntil(tree, numberOfDesiredLevels, numberOfRootChildren);
            long from = random.nextLong(0, high);
            long to = random.nextLong(from, high);

            // when
            List<MutableLong> partitionEdges =
                    tree.partitionedSeek(layout.key(from), layout.key(to), numberOfDesiredPartitions, NULL_CONTEXT);

            // then
            IntList entryCountPerPartition = assertEntries.of(partitionEdges, from, to, tree);
            verifyEntryCountPerPartition(entryCountPerPartition);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldCreateReasonablePartitionsWhenFromInclusiveMatchKeyInRoot(String name, AssertEntries assertEntries)
            throws IOException {
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            // given
            int numberOfRootChildren = random.nextInt(1, 10);
            int numberOfDesiredLevels = numberOfRootChildren == 0 ? 1 : random.nextInt(2, 4);
            int high = insertEntriesUntil(tree, numberOfDesiredLevels, numberOfRootChildren);

            List<MutableLong> rootKeys = getKeysOnLevel(tree, 0);
            int numberOfDesiredPartitions = random.nextInt(1, rootKeys.size() + 1);
            long from = layout.keySeed(rootKeys.get(0));
            long to = random.nextLong(from, high);

            // when
            List<MutableLong> partitionEdges =
                    tree.partitionedSeek(layout.key(from), layout.key(to), numberOfDesiredPartitions, NULL_CONTEXT);

            // then
            IntList entryCountPerPartition = assertEntries.of(partitionEdges, from, to, tree);
            verifyEntryCountPerPartition(entryCountPerPartition);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("assertEntries")
    void shouldCreateReasonablePartitionsWhenToExclusiveMatchKeyInRoot(String name, AssertEntries assertEntries)
            throws IOException {
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            // given
            int numberOfRootChildren = random.nextInt(1, 10);
            int numberOfDesiredLevels = numberOfRootChildren == 0 ? 1 : random.nextInt(2, 4);
            insertEntriesUntil(tree, numberOfDesiredLevels, numberOfRootChildren);

            List<MutableLong> rootKeys = getKeysOnLevel(tree, 0);
            int numberOfDesiredPartitions = random.nextInt(1, rootKeys.size() + 1);
            long to = layout.keySeed(rootKeys.get(rootKeys.size() - 1));
            long from = random.nextLong(0, to);

            // when
            List<MutableLong> partitionEdges =
                    tree.partitionedSeek(layout.key(from), layout.key(to), numberOfDesiredPartitions, NULL_CONTEXT);

            // then
            IntList entryCountPerSeeker = assertEntries.of(partitionEdges, from, to, tree);
            verifyEntryCountPerPartition(entryCountPerSeeker);
        }
    }

    @Test
    void shouldPartitionSeekersDuringTreeModifications() throws IOException {
        int internalMaxKeyCount = new InternalNodeFixedSize<>(pageCache.pageSize(), layout).maxKeyCount();
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            // given a tree with half filled root
            int stride = 15;
            int high = insertEntriesUntil(tree, 2, internalMaxKeyCount / 2, stride /*holes between each key*/);
            int count = high / stride;

            // when calling partitionedSeek while concurrently inserting
            // modifications go something like this:
            // - initial state is a tree with keys like 0, 15 (stride), 30, 45, 60... a.s.o.
            // - each round creates all keys+1 and while doing so calling partitioned seek
            // there will be racing between changing the root, even splitting the root, and calling partitionedSeek
            MutableLong min = layout.key(0);
            MutableLong max = layout.key(Long.MAX_VALUE);
            for (int i = 0; i < stride - 1; i++) {
                int offset = i + 1;
                AtomicReference<List<MutableLong>> atomicPartitionEdges = new AtomicReference<>();
                Race race = new Race();
                race.addContestant(throwing(() -> insertEntries(tree, offset, count, stride)));
                race.addContestant(throwing(() ->
                        atomicPartitionEdges.set(tree.partitionedSeek(min, max, random.nextInt(2, 20), NULL_CONTEXT))));
                race.goUnchecked();

                // then
                long nextExpected = 0;
                List<MutableLong> partitionEdges = atomicPartitionEdges.get();
                for (int j = 0; j < partitionEdges.size() - 1; j++) {
                    Seeker<MutableLong, MutableLong> seeker =
                            tree.seek(partitionEdges.get(j), partitionEdges.get(j + 1), NULL_CONTEXT);
                    while (seeker.next()) {
                        assertThat(nextExpected++)
                                .as("current key is next in the expected sequence")
                                .isEqualTo(seeker.key().longValue());
                        if (nextExpected % stride > offset) {
                            nextExpected += stride - nextExpected % stride;
                        }
                    }
                }
                assertThat(nextExpected).as("expected end of seek range").isEqualTo(high);
            }

            // when calling partitionedSeek while concurrently removing
            // removing go something like this:
            // - initial state is a tree with keys like 0, 1, 2... high
            // - each round removes all keys on given offset modulo stride, starting from the high end, starting by
            // removing 14, 29, 44,...
            //   and while doing so calling partitioned seek
            // there will be racing between changing the root, merging the leaves and calling partitionedSeek
            for (int i = stride - 2; i >= 0; i--) {
                int offset = i + 1;
                AtomicReference<List<MutableLong>> atomicPartitionEdges = new AtomicReference<>();
                Race race = new Race();
                race.addContestant(throwing(() -> removeEntries(tree, offset, count, stride)));
                race.addContestant(throwing(() ->
                        atomicPartitionEdges.set(tree.partitionedSeek(min, max, random.nextInt(2, 20), NULL_CONTEXT))));
                race.goUnchecked();

                // then
                long nextExpected = 0;
                List<MutableLong> partitionEdges = atomicPartitionEdges.get();
                for (int j = 0; j < partitionEdges.size() - 1; j++) {
                    Seeker<MutableLong, MutableLong> seeker =
                            tree.seek(partitionEdges.get(j), partitionEdges.get(j + 1), NULL_CONTEXT);
                    while (seeker.next()) {
                        assertThat(nextExpected++)
                                .as("current key is next in the expected sequence")
                                .isEqualTo(seeker.key().longValue());
                        if (nextExpected % stride >= offset) {
                            nextExpected += stride - nextExpected % stride;
                        }
                    }
                }
                assertThat(nextExpected).as("expected end of seek range").isEqualTo(high);
            }
        }
    }

    @Test
    void shouldThrowOnAttemptBackwardPartitionedSeek() throws IOException {
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            assertThatThrownBy(
                            () -> tree.partitionedSeek(layout.key(10), layout.key(0), 5, NULL_CONTEXT),
                            "should only seek forward")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Partitioned seek only supports forward seeking for the time being");
        }
    }

    private GBPTree<MutableLong, MutableLong> instantiateTree() {
        return new GBPTreeBuilder<>(pageCache, fileSystem, treeFile, layout).build();
    }

    private void shouldPartitionTree(
            int numberOfDesiredLevels,
            int numberOfDesiredRootChildren,
            int numberOfDesiredPartitions,
            int expectedNumberOfPartitions,
            AssertEntries assertEntries)
            throws IOException {
        try (GBPTree<MutableLong, MutableLong> tree = instantiateTree()) {
            // given
            int to = insertEntriesUntil(tree, numberOfDesiredLevels, numberOfDesiredRootChildren);

            // when
            List<MutableLong> partitionEdges =
                    tree.partitionedSeek(layout.key(0), layout.key(to), numberOfDesiredPartitions, NULL_CONTEXT);

            // then
            assertThat(partitionEdges.size() - 1).as("number of partitions").isEqualTo(expectedNumberOfPartitions);
            assertEntries.of(partitionEdges, 0, to, tree);
        }
    }

    private static IntList assertEntriesSingleThreaded(
            List<MutableLong> partitionEdges, long from, long to, Seeker.Factory<MutableLong, MutableLong> factory) {
        List<LongList> collectedEntryKeysPerPartition = new ArrayList<>();
        for (int i = 0; i < partitionEdges.size() - 1; i++) {
            MutableLong partitionFrom = partitionEdges.get(i);
            MutableLong partitionTo = partitionEdges.get(i + 1);
            LongList entryKeysInPartition =
                    collectAndCheckEntryKeysInPartition(partitionFrom, partitionTo, from, to, factory);
            collectedEntryKeysPerPartition.add(entryKeysInPartition);
        }

        long closedTo = from == to ? to : (to - 1);
        assertAllExpectedKeysInOrderWithinAClosedRange(
                collectedEntryKeysPerPartition.stream().flatMapToLong(LongList::primitiveStream), from, closedTo);
        return getEntryCountsPerPartition(collectedEntryKeysPerPartition.stream());
    }

    private static IntList assertEntriesMultiThreaded(
            List<MutableLong> partitionEdges, long from, long to, Seeker.Factory<MutableLong, MutableLong> factory) {
        LongList[] collectedEntryKeysPerPartition = new LongList[partitionEdges.size() - 1];
        Race race = new Race();
        for (int i = 0; i < collectedEntryKeysPerPartition.length; i++) {
            int index = i;
            MutableLong partitionFrom = partitionEdges.get(i);
            MutableLong partitionTo = partitionEdges.get(i + 1);
            race.addContestant(() -> collectedEntryKeysPerPartition[index] =
                    collectAndCheckEntryKeysInPartition(partitionFrom, partitionTo, from, to, factory));
        }
        race.goUnchecked();
        long closedTo = from == to ? to : (to - 1);
        assertAllExpectedKeysInOrderWithinAClosedRange(
                Arrays.stream(collectedEntryKeysPerPartition).flatMapToLong(LongList::primitiveStream), from, closedTo);
        return getEntryCountsPerPartition(Arrays.stream(collectedEntryKeysPerPartition));
    }

    private static LongList collectAndCheckEntryKeysInPartition(
            MutableLong partitionFrom,
            MutableLong partitionTo,
            long from,
            long to,
            Seeker.Factory<MutableLong, MutableLong> factory) {
        try (Seeker<MutableLong, MutableLong> partition = factory.seek(partitionFrom, partitionTo, NULL_CONTEXT)) {
            LongList keys = collectEntryKeysInPartition(partition);
            assertAllExpectedKeysInOrderWithinAClosedRange(keys.primitiveStream(), keys.getFirst(), keys.getLast());
            if (from == to) {
                assertThat(keys.size()).as("exact match has singular key").isEqualTo(1);
            } else {
                assertThat(keys.getFirst())
                        .as("first key of partition is not before start of range")
                        .isGreaterThanOrEqualTo(from);
                assertThat(keys.getLast())
                        .as("last key of partition is before the excluded end of the range")
                        .isLessThan(to);
            }
            return keys;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static LongList collectEntryKeysInPartition(Seeker<MutableLong, MutableLong> partition) throws IOException {
        MutableLongList keys = LongLists.mutable.empty();
        while (partition.next()) {
            long key = partition.key().longValue();
            keys.add(key);
        }
        return keys;
    }

    private static void assertAllExpectedKeysInOrderWithinAClosedRange(LongStream keys, long from, long to) {
        List<Long> seenKeys = keys.boxed().toList();
        List<Long> expectedKeys = LongStream.rangeClosed(from, to).boxed().toList();
        assertThat(seenKeys)
                .as("keys seen are exactly the range [%d,%d]", from, to)
                .containsExactlyElementsOf(expectedKeys);
    }

    private static IntList getEntryCountsPerPartition(Stream<LongList> collectedEntryKeysPerPartition) {
        return new IntArrayList(
                collectedEntryKeysPerPartition.mapToInt(LongList::size).toArray());
    }

    private static void verifyEntryCountPerPartition(IntList entryCountPerSeeker) {
        // verify that partitions have some sort of fair distribution
        // First and last partition may have varying number of entries, but the middle ones should be (at least in this
        // test case)
        // max a factor two from each other, entry-count wise
        if (entryCountPerSeeker.size() > 1) {
            int reference = entryCountPerSeeker.get(1);
            for (int i = 2; i < entryCountPerSeeker.size() - 1; i++) {
                int difference = abs(reference - entryCountPerSeeker.get(i));
                assertThat(difference)
                        .as("absolute difference between middle partition sizes")
                        .isLessThanOrEqualTo(reference);
            }
        }
    }

    private int insertEntriesUntil(
            GBPTree<MutableLong, MutableLong> tree, int numberOfDesiredLevels, int numberOfDesiredRootKeys)
            throws IOException {
        return insertEntriesUntil(tree, numberOfDesiredLevels, numberOfDesiredRootKeys, 1);
    }

    private int insertEntriesUntil(
            GBPTree<MutableLong, MutableLong> tree, int numberOfDesiredLevels, int numberOfDesiredRootKeys, int stride)
            throws IOException {
        int id = 0;
        DepthAndRootVisitor result;
        while ((result = visit(tree)) != null
                && (result.numberOfLevels < numberOfDesiredLevels || result.rootChildCount < numberOfDesiredRootKeys)) {
            id = insertEntries(tree, id, 10, stride);
        }
        return id;
    }

    private int insertEntries(GBPTree<MutableLong, MutableLong> tree, int startId, int count, int stride)
            throws IOException {
        int id = startId;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            MutableLong value = layout.value(0);
            for (int i = 0; i < count; i++, id += stride) {
                MutableLong key = layout.key(id);
                writer.put(key, value);
            }
        }
        return id;
    }

    private void removeEntries(GBPTree<MutableLong, MutableLong> tree, int startId, int count, int stride)
            throws IOException {
        int id = startId;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            for (int i = 0; i < count; i++, id += stride) {
                MutableLong key = layout.key(id);
                writer.remove(key);
            }
        }
    }

    private List<MutableLong> getKeysOnLevel(GBPTree<MutableLong, MutableLong> tree, int level) throws IOException {
        List<MutableLong> keysOnLevel = new ArrayList<>();
        GBPTreeVisitor.Adaptor<SingleRoot, MutableLong, MutableLong> visitor = new GBPTreeVisitor.Adaptor<>() {
            private int currentLevel;

            @Override
            public void beginLevel(int level) {
                currentLevel = level;
            }

            @Override
            public void key(MutableLong key, boolean isLeaf, long offloadId) {
                if (currentLevel == level) {
                    MutableLong into = layout.newKey();
                    layout.copyKey(key, into);
                    keysOnLevel.add(into);
                }
            }
        };
        tree.visit(visitor, NULL_CONTEXT);
        return keysOnLevel;
    }

    private static DepthAndRootVisitor visit(GBPTree<MutableLong, MutableLong> tree) throws IOException {
        DepthAndRootVisitor visitor = new DepthAndRootVisitor();
        tree.visit(visitor, NULL_CONTEXT);
        return visitor;
    }

    private static class DepthAndRootVisitor extends GBPTreeVisitor.Adaptor<SingleRoot, MutableLong, MutableLong> {
        private int numberOfLevels;
        private int currentLevel;
        private int rootChildCount;

        @Override
        public void beginLevel(int level) {
            currentLevel = level;
            numberOfLevels++;
        }

        @Override
        public void beginNode(long pageId, boolean isLeaf, long generation, int keyCount) {
            if (currentLevel == 0 && !isLeaf) {
                rootChildCount = keyCount + 1;
            }
        }
    }

    private static Stream<Arguments> assertEntries() {
        return Stream.of(
                Arguments.of("single-threaded", (AssertEntries) PartitionedSeekTest::assertEntriesSingleThreaded),
                Arguments.of("multi-threaded", (AssertEntries) PartitionedSeekTest::assertEntriesMultiThreaded));
    }

    @FunctionalInterface
    private interface AssertEntries {
        IntList of(
                List<MutableLong> partitionEdges, long from, long to, Seeker.Factory<MutableLong, MutableLong> factory)
                throws IOException;
    }
}
