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

import static java.lang.Integer.max;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.DataTree.W_SPLIT_KEEP_ALL_LEFT;
import static org.neo4j.index.internal.gbptree.DataTree.W_SPLIT_KEEP_ALL_RIGHT;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.OpenOption;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
@ExtendWith(RandomExtension.class)
abstract class GBPTreeITBase<KEY, VALUE> {
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private RandomSupport random;

    private int flags;
    protected TestLayout<KEY, VALUE> layout;
    private ValueAggregator<VALUE> addingAggregator;
    private GBPTree<KEY, VALUE> index;
    private PageCache pageCache;

    @BeforeEach
    void setUp() {
        flags = random.nextBoolean() ? 0 : random.nextBoolean() ? W_SPLIT_KEEP_ALL_LEFT : W_SPLIT_KEEP_ALL_RIGHT;
        int pageSize = 512;
        pageCache = PageCacheSupportExtension.getPageCache(
                fileSystem, config().withPageSize(pageSize).withAccessChecks(true));
        var openOptions = getOpenOptions();
        layout = getLayout(random, GBPTreeTestUtil.calculatePayloadSize(pageCache, openOptions));
        addingAggregator = getAddingAggregator();
        index = new GBPTreeBuilder<>(pageCache, fileSystem, testDirectory.file("index"), layout)
                .with(openOptions)
                .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        index.close();
        pageCache.close();
    }

    private Writer<KEY, VALUE> createWriter(GBPTree<KEY, VALUE> index, WriterFactory factory) throws IOException {
        return factory.create(index, flags);
    }

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    abstract TestLayout<KEY, VALUE> getLayout(RandomSupport random, int pageSize);

    abstract Class<KEY> getKeyClass();

    protected abstract ValueAggregator<VALUE> getAddingAggregator();

    protected abstract VALUE sumValues(VALUE value1, VALUE value2);

    @EnumSource(WriterFactory.class)
    @ParameterizedTest
    void shouldStayCorrectAfterRandomModifications(WriterFactory writerFactory) throws Exception {
        // GIVEN
        Comparator<KEY> keyComparator = layout;
        TreeMap<KEY, VALUE> data = new TreeMap<>(keyComparator);
        int count = 100;
        int totalNumberOfRounds = 10;
        for (int i = 0; i < count; i++) {
            data.put(randomKey(random.random()), randomValue(random.random()));
        }

        // WHEN
        try (Writer<KEY, VALUE> writer = createWriter(index, writerFactory)) {
            for (var entry : data.entrySet()) {
                writer.put(entry.getKey(), entry.getValue());
            }
        }

        for (int round = 0; round < totalNumberOfRounds; round++) {
            // THEN
            for (int i = 0; i < count; i++) {
                KEY first = randomKey(random.random());
                KEY second = randomKey(random.random());
                KEY from;
                KEY to;
                if (layout.keySeed(first) < layout.keySeed(second)) {
                    from = first;
                    to = second;
                } else {
                    from = second;
                    to = first;
                }
                Map<KEY, VALUE> expectedHits = expectedHits(data, from, to, keyComparator);
                try (Seeker<KEY, VALUE> result = index.seek(from, to, NULL_CONTEXT)) {
                    while (result.next()) {
                        KEY key = result.key();
                        if (expectedHits.remove(key) == null) {
                            fail("Unexpected hit " + key + " when searching for " + from + " - " + to);
                        }

                        assertTrue(keyComparator.compare(key, from) >= 0);
                        if (keyComparator.compare(from, to) != 0) {
                            assertTrue(keyComparator.compare(key, to) < 0);
                        }
                    }
                    if (!expectedHits.isEmpty()) {
                        fail("There were results which were expected to be returned, but weren't:" + expectedHits
                                + " when searching range " + from + " - " + to);
                    }
                }
            }

            index.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
            randomlyModifyIndex(index, data, random.random(), (double) round / totalNumberOfRounds, writerFactory);
        }

        // and finally
        consistencyCheckStrict(index);
    }

    @EnumSource(WriterFactory.class)
    @ParameterizedTest
    void shouldHandleRemoveEntireTree(WriterFactory writerFactory) throws Exception {
        // given
        int numberOfEntries = 50_000;
        try (Writer<KEY, VALUE> writer = createWriter(index, writerFactory)) {
            for (int i = 0; i < numberOfEntries; i++) {
                writer.put(key(i), value(i));
            }
        }

        // when
        BitSet removed = new BitSet();
        try (Writer<KEY, VALUE> writer = createWriter(index, writerFactory)) {
            for (int i = 0; i < numberOfEntries - numberOfEntries / 10; i++) {
                int candidate;
                do {
                    candidate = random.nextInt(max(1, random.nextInt(numberOfEntries)));
                } while (removed.get(candidate));
                removed.set(candidate);

                writer.remove(key(candidate));
            }
        }

        int next = 0;
        try (Writer<KEY, VALUE> writer = createWriter(index, writerFactory)) {
            for (int i = 0; i < numberOfEntries / 10; i++) {
                next = removed.nextClearBit(next);
                removed.set(next);
                writer.remove(key(next));
            }
        }

        // then
        try (Seeker<KEY, VALUE> seek = index.seek(key(0), key(numberOfEntries), NULL_CONTEXT)) {
            assertFalse(seek.next());
        }

        // and finally
        consistencyCheckStrict(index);
    }

    @EnumSource(WriterFactory.class)
    @ParameterizedTest
    void shouldHandleDescendingWithEmptyRange(WriterFactory writerFactory) throws IOException {
        // Write
        try (Writer<KEY, VALUE> writer = createWriter(index, writerFactory)) {
            long[] seeds = new long[] {0, 1, 4};
            for (long seed : seeds) {
                KEY key = layout.key(seed);
                VALUE value = layout.value(0);
                writer.put(key, value);
            }
        }

        KEY from = layout.key(3);
        KEY to = layout.key(1);
        try (Seeker<KEY, VALUE> seek = index.seek(from, to, NULL_CONTEXT)) {
            assertFalse(seek.next());
        }
        index.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
    }

    private void randomlyModifyIndex(
            GBPTree<KEY, VALUE> index,
            TreeMap<KEY, VALUE> data,
            Random random,
            double removeProbability,
            WriterFactory writerFactory)
            throws IOException {
        int changeCount = random.nextInt(10) + 10;
        try (Writer<KEY, VALUE> writer = createWriter(index, writerFactory)) {
            for (int i = 0; i < changeCount; i++) {
                if (!data.isEmpty() && random.nextDouble() < removeProbability) {
                    if (random.nextBoolean()) {
                        // remove
                        KEY key = randomKey(data, random);
                        VALUE value = data.remove(key);
                        VALUE removedValue = writer.remove(key);
                        assertEqualsValue(value, removedValue);
                    } else {
                        // aggregate, removes one value
                        KEY key = randomKey(data, random);
                        var entry0Value = data.get(key);
                        var entry1 = data.ceilingEntry(key);
                        if (entry1 != null) {
                            var keyTo = data.ceilingKey(entry1.getKey());
                            if (keyTo == null) {
                                keyTo = layout.key(Long.MAX_VALUE);
                            }
                            var modified = writer.aggregate(key, keyTo, addingAggregator);
                            if (modified == 2) {
                                data.remove(key);
                                data.put(entry1.getKey(), sumValues(entry0Value, entry1.getValue()));
                            }
                            assertThat(modified).isIn(0, 2);
                        }
                    }
                } else { // put
                    KEY key = randomKey(random);
                    VALUE value = randomValue(random);
                    writer.put(key, value);
                    data.put(key, value);
                }
            }
        }
    }

    private Map<KEY, VALUE> expectedHits(Map<KEY, VALUE> data, KEY from, KEY to, Comparator<KEY> comparator) {
        Map<KEY, VALUE> hits = new TreeMap<>(comparator);
        for (Map.Entry<KEY, VALUE> candidate : data.entrySet()) {
            if (comparator.compare(from, to) == 0 && comparator.compare(candidate.getKey(), from) == 0) {
                hits.put(candidate.getKey(), candidate.getValue());
            } else if (comparator.compare(candidate.getKey(), from) >= 0
                    && comparator.compare(candidate.getKey(), to) < 0) {
                hits.put(candidate.getKey(), candidate.getValue());
            }
        }
        return hits;
    }

    private KEY randomKey(Map<KEY, VALUE> data, Random random) {
        //noinspection unchecked
        KEY[] keys = data.keySet().toArray((KEY[]) Array.newInstance(getKeyClass(), data.size()));
        return keys[random.nextInt(keys.length)];
    }

    private KEY randomKey(Random random) {
        return key(random.nextInt(1_000));
    }

    private VALUE randomValue(Random random) {
        return value(random.nextInt(1_000));
    }

    private VALUE value(long seed) {
        return layout.value(seed);
    }

    private KEY key(long seed) {
        return layout.key(seed);
    }

    private void assertEqualsValue(VALUE expected, VALUE actual) {
        assertEquals(
                0,
                layout.compareValue(expected, actual),
                format("expected equal, expected=%s, actual=%s", expected, actual));
    }

    // KEEP even if unused
    @SuppressWarnings("unused")
    private void printTree() throws IOException {
        index.printTree(PrintConfig.defaults(), NULL_CONTEXT);
    }

    @SuppressWarnings("unused")
    private void printNode(@SuppressWarnings("SameParameterValue") int id) throws IOException {
        index.printNode(id, NULL_CONTEXT);
    }
}
