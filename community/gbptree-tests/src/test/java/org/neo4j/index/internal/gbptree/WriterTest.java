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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
class WriterTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private GBPTree<MutableLong, MutableLong> tree;

    @BeforeEach
    void setupTree() {
        tree = new GBPTreeBuilder<>(
                        pageCache,
                        fileSystem,
                        directory.file("tree"),
                        longLayout().withFixedSize(true).build())
                .build();
    }

    @AfterEach
    void closeTree() throws IOException {
        tree.close();
    }

    @Test
    void shouldPutEntry() throws IOException {
        // when
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertEquals(key, cursor.key().longValue());
            assertEquals(value, cursor.value().longValue());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldMergeNonExistentEntry() throws IOException {
        // when
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.merge(new MutableLong(key), new MutableLong(value), ValueMergers.overwrite());
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertEquals(key, cursor.key().longValue());
            assertEquals(value, cursor.value().longValue());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotChangeEntryOnMergeExistentEntryWithUnchangingMerger() throws IOException {
        // given
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.merge(new MutableLong(key), new MutableLong(value + 1), ValueMergers.keepExisting());
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertEquals(key, cursor.key().longValue());
            assertEquals(value, cursor.value().longValue());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldChangeEntryOnMergeExistentEntryWithChangingMerger() throws IOException {
        // given
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.merge(new MutableLong(key), new MutableLong(value + 1), ValueMergers.overwrite());
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertEquals(key, cursor.key().longValue());
            assertEquals(value + 1, cursor.value().longValue());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotCreateEntryOnMergeIfExistsForNonExistentEntry() throws IOException {
        // when
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.mergeIfExists(new MutableLong(key), new MutableLong(value + 1), ValueMergers.overwrite());
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotChangeEntryOnMergeIfExistsForExistentEntryWithUnchangingMerger() throws IOException {
        // given
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.mergeIfExists(new MutableLong(key), new MutableLong(value + 1), ValueMergers.keepExisting());
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertEquals(key, cursor.key().longValue());
            assertEquals(value, cursor.value().longValue());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldChangeEntryOnMergeIfExistsForExistentEntryWithChangingMerger() throws IOException {
        // given
        long key = 0;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.mergeIfExists(new MutableLong(key), new MutableLong(value + 1), ValueMergers.overwrite());
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(key), new MutableLong(key), NULL_CONTEXT)) {
            assertTrue(cursor.next());
            assertEquals(key, cursor.key().longValue());
            assertEquals(value + 1, cursor.value().longValue());
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldReturnNullRemovedValueIfNotFound() throws IOException {
        // given
        long key = 999;
        long value = 888;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            MutableLong removedValue = writer.remove(new MutableLong(key + 1));
            // then
            assertNull(removedValue);
        }
    }

    @Test
    void shouldReturnRemovedValueIfFound() throws IOException {
        // given
        long key = 999;
        long value = 888;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.put(new MutableLong(key), new MutableLong(value));
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            MutableLong removedValue = writer.remove(new MutableLong(key));
            // then
            assertEquals(new MutableLong(value), removedValue);
        }
    }

    @Test
    void shouldAggregate() throws IOException {
        // given
        long keys = 10;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            for (int i = 0; i < keys; i++) {
                writer.put(new MutableLong(i), new MutableLong(value));
            }
        }

        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.aggregate(new MutableLong(0), new MutableLong(keys), (v, a) -> a.add(v));
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(0), new MutableLong(keys), NULL_CONTEXT)) {
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.key().longValue()).isEqualTo(keys - 1);
            assertThat(cursor.value().longValue()).isEqualTo(keys * value);
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldUpdateCelingValue() throws IOException {
        // given
        long keys = 10;
        long value = 10;
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            for (int i = 0; i < keys; i++) {
                writer.put(new MutableLong(i + 100), new MutableLong(value));
            }
        }

        long update = 3215621;
        // when
        try (Writer<MutableLong, MutableLong> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
            writer.updateCeilingValue(new MutableLong(0), new MutableLong(Long.MAX_VALUE), v -> {
                v.setValue(update);
                return v;
            });
        }

        // then
        try (Seeker<MutableLong, MutableLong> cursor =
                tree.seek(new MutableLong(0), new MutableLong(Long.MAX_VALUE), NULL_CONTEXT)) {
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.key().longValue()).isEqualTo(100);
            assertThat(cursor.value().longValue()).isEqualTo(update);
        }
    }
}
