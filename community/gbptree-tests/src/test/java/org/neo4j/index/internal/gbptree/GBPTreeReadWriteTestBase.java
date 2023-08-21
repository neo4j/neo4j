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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
abstract class GBPTreeReadWriteTestBase<KEY, VALUE> {
    private static final int PAGE_SIZE_8K = (int) ByteUnit.kibiBytes(8);
    private static final int PAGE_SIZE_16K = (int) ByteUnit.kibiBytes(16);
    private static final int PAGE_SIZE_32K = (int) ByteUnit.kibiBytes(32);

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private RandomSupport random;

    private PageCache pageCache;
    private TestLayout<KEY, VALUE> layout;
    private Path indexFile;

    @AfterEach
    void tearDown() {
        if (pageCache != null) {
            pageCache.close();
            pageCache = null;
        }
    }

    abstract TestLayout<KEY, VALUE> getLayout(RandomSupport random, int pageSize);

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @ParameterizedTest
    @MethodSource("pageSizes")
    void shouldSeeSimpleInsertions(int pageSize) throws Exception {
        setupTest(pageSize);
        try (GBPTree<KEY, VALUE> index = index()) {
            int count = 1000;
            try (Writer<KEY, VALUE> writer = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (int i = 0; i < count; i++) {
                    writer.put(key(i), value(i));
                }
            }

            try (Seeker<KEY, VALUE> cursor = index.seek(key(0), key(Long.MAX_VALUE), NULL_CONTEXT)) {
                for (int i = 0; i < count; i++) {
                    assertTrue(cursor.next());
                    assertEqualsKey(key(i), cursor.key());
                }
                assertFalse(cursor.next());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("pageSizes")
    void shouldSeeSimpleInsertionsWithExactMatch(int pageSize) throws Exception {
        setupTest(pageSize);
        try (GBPTree<KEY, VALUE> index = index()) {
            int count = 1000;
            try (Writer<KEY, VALUE> writer = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (int i = 0; i < count; i++) {
                    writer.put(key(i), value(i));
                }
            }

            for (int i = 0; i < count; i++) {
                try (Seeker<KEY, VALUE> cursor = index.seek(key(i), key(i), NULL_CONTEXT)) {
                    assertTrue(cursor.next());
                    assertEqualsKey(key(i), cursor.key());
                    assertFalse(cursor.next());
                }
            }
        }
    }

    /* Randomized tests */

    @ParameterizedTest
    @MethodSource("pageSizes")
    void shouldSplitCorrectly(int pageSize) throws Exception {
        setupTest(pageSize);
        // GIVEN
        try (GBPTree<KEY, VALUE> index = index()) {
            // WHEN
            int count = 1_000;
            List<KEY> seen = new ArrayList<>(count);
            try (Writer<KEY, VALUE> writer = index.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (int i = 0; i < count; i++) {
                    KEY key;
                    do {
                        key = key(random.nextInt(100_000));
                    } while (listContains(seen, key));
                    VALUE value = value(i);
                    writer.put(key, value);
                    seen.add(key);
                }
            }

            // THEN
            try (Seeker<KEY, VALUE> cursor = index.seek(key(0), key(Long.MAX_VALUE), NULL_CONTEXT)) {
                long prev = -1;
                while (cursor.next()) {
                    KEY hit = cursor.key();
                    long hitSeed = layout.keySeed(hit);
                    if (hitSeed < prev) {
                        fail(hit + " smaller than prev " + prev);
                    }
                    prev = hitSeed;
                    assertTrue(removeFromList(seen, hit));
                }

                if (!seen.isEmpty()) {
                    fail("expected hits " + seen);
                }
            }
        }
    }

    private static Stream<Integer> pageSizes() {
        return Stream.of(PAGE_SIZE_8K, PAGE_SIZE_16K, PAGE_SIZE_32K);
    }

    private void setupTest(int pageSize) {
        indexFile = testDirectory.file("index");
        pageCache = StandalonePageCacheFactory.createPageCache(fs, new ThreadPoolJobScheduler(), pageSize);
        var payloadSize = GBPTreeTestUtil.calculatePayloadSize(pageCache, getOpenOptions());
        layout = getLayout(random, payloadSize);
    }

    private GBPTree<KEY, VALUE> index() {
        return new GBPTreeBuilder<>(pageCache, fs, indexFile, layout)
                .with(getOpenOptions())
                .build();
    }

    private boolean removeFromList(List<KEY> list, KEY item) {
        for (int i = 0; i < list.size(); i++) {
            if (layout.compare(list.get(i), item) == 0) {
                list.remove(i);
                return true;
            }
        }
        return false;
    }

    private boolean listContains(List<KEY> list, KEY item) {
        for (KEY key : list) {
            if (layout.compare(key, item) == 0) {
                return true;
            }
        }
        return false;
    }

    private VALUE value(long seed) {
        return layout.value(seed);
    }

    private KEY key(long seed) {
        return layout.key(seed);
    }

    private void assertEqualsKey(KEY expected, KEY actual) {
        assertEquals(
                0,
                layout.compare(expected, actual),
                format("expected equal, expected=%s, actual=%s", expected, actual));
    }
}
