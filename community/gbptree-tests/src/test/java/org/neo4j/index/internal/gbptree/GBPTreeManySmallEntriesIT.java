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
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableShort;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
@ExtendWith(RandomExtension.class)
class GBPTreeManySmallEntriesIT {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleManySmallEntries(boolean fixedSizeLayout) throws IOException {
        Layout<MutableShort, Void> layout = new TinyLayout(fixedSizeLayout);
        List<MutableShort> expected = new ArrayList<>();
        try (GBPTree<MutableShort, Void> tree =
                new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout).build()) {
            // given
            int count = 10_000;
            try (Writer<MutableShort, Void> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (int i = 0; i < count; i++) {
                    // Do some deletions too so that the dynamic layout needs some defrag now and then
                    if (random.nextInt(10) == 0 && !expected.isEmpty()) {
                        MutableShort key = expected.remove(random.nextInt(expected.size()));
                        writer.remove(key);
                    } else {
                        MutableShort key = new MutableShort(i);
                        writer.put(key, null);
                        expected.add(key);
                    }
                }
            }

            // when/then
            try (Seeker<MutableShort, Void> seeker =
                    tree.seek(new MutableShort(Short.MIN_VALUE), new MutableShort(Short.MAX_VALUE), NULL_CONTEXT)) {
                expected.sort(Comparator.naturalOrder());
                for (MutableShort key : expected) {
                    assertThat(seeker.next()).isTrue();
                    assertThat(seeker.key()).isEqualTo(key);
                }
                assertThat(seeker.next()).isFalse();
            }
        }
    }

    private static class TinyLayout extends Layout.Adapter<MutableShort, Void> {
        TinyLayout(boolean fixedSize) {
            super(fixedSize, 12121212, 0, 0);
        }

        @Override
        public MutableShort newKey() {
            return new MutableShort();
        }

        @Override
        public MutableShort copyKey(MutableShort key, MutableShort into) {
            into.setValue(key.shortValue());
            return into;
        }

        @Override
        public Void newValue() {
            return null;
        }

        @Override
        public int keySize(MutableShort key) {
            return 2;
        }

        @Override
        public int valueSize(Void unused) {
            return 0;
        }

        @Override
        public void writeKey(PageCursor cursor, MutableShort key) {
            cursor.putShort(key.shortValue());
        }

        @Override
        public void writeValue(PageCursor cursor, Void unused) {}

        @Override
        public void readKey(PageCursor cursor, MutableShort into, int keySize) {
            into.setValue(cursor.getShort());
        }

        @Override
        public void readValue(PageCursor cursor, Void into, int valueSize) {}

        @Override
        public void initializeAsLowest(MutableShort key) {
            key.setValue(Short.MIN_VALUE);
        }

        @Override
        public void initializeAsHighest(MutableShort key) {
            key.setValue(Short.MAX_VALUE);
        }

        @Override
        public int compare(MutableShort o1, MutableShort o2) {
            return Short.compare(o1.shortValue(), o2.shortValue());
        }
    }
}
