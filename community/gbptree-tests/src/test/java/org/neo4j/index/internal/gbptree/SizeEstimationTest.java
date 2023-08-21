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
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(RandomExtension.class)
@PageCacheExtension
class SizeEstimationTest {
    private static final double EXPECTED_MARGIN_OF_ERROR = 0.1;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @Test
    void shouldEstimateSizeOnFixedSizeKeys() throws IOException {
        SimpleLongLayout layout = SimpleLongLayout.longLayout().build();
        assertEstimateSizeCorrectly(layout);
    }

    @Test
    void shouldEstimateSizeOnDynamicSizeKeys() throws IOException {
        int pageSize = GBPTreeTestUtil.calculatePayloadSize(pageCache, getOpenOptions());
        int largeEntriesSize = DynamicSizeUtil.keyValueSizeCapFromPageSize(pageSize) / 2;
        int largeEntryModulo = random.nextInt(0, 10); // 0 = no large keys
        SimpleByteArrayLayout layout = new SimpleByteArrayLayout(largeEntriesSize, largeEntryModulo);
        assertEstimateSizeCorrectly(layout);
    }

    private <KEY, VALUE> void assertEstimateSizeCorrectly(TestLayout<KEY, VALUE> layout) throws IOException {
        try (GBPTree<KEY, VALUE> tree = new GBPTreeBuilder<>(pageCache, fileSystem, testDirectory.file("tree"), layout)
                .with(getOpenOptions())
                .build()) {
            // given
            int count = random.nextInt(500, 2_500);
            try (Writer<KEY, VALUE> writer = tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT)) {
                for (int i = 0; i < count; i++) {
                    writer.put(layout.key(i), layout.value(i));
                }
            }

            // when/then
            assertEstimateWithinMargin(tree, count);
        }
    }

    private static void assertEstimateWithinMargin(GBPTree<?, ?> tree, int actualCount) throws IOException {
        // when
        int estimate = toIntExact(tree.estimateNumberOfEntriesInTree(NULL_CONTEXT));

        // then
        int diff = abs(actualCount - estimate);
        double diffRatio = (double) diff / actualCount;
        assertThat(diffRatio).isLessThan(EXPECTED_MARGIN_OF_ERROR);
    }
}
