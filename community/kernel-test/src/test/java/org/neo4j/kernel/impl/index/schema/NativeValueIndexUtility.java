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
package org.neo4j.kernel.impl.index.schema;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.ValueGroup;

public class NativeValueIndexUtility<KEY extends NativeIndexKey<KEY>> {
    private final ValueCreatorUtil<KEY> valueCreatorUtil;
    private final Layout<KEY, NullValue> layout;

    public NativeValueIndexUtility(ValueCreatorUtil<KEY> valueCreatorUtil, Layout<KEY, NullValue> layout) {
        this.valueCreatorUtil = valueCreatorUtil;
        this.layout = layout;
    }

    void verifyUpdates(ValueIndexEntryUpdate<IndexDescriptor>[] updates, Supplier<GBPTree<KEY, NullValue>> treeProvider)
            throws IOException {
        List<KEY> expectedHits = convertToHits(updates, layout);
        List<KEY> actualHits = new ArrayList<>();
        try (GBPTree<KEY, NullValue> tree = treeProvider.get();
                Seeker<KEY, NullValue> scan = scan(tree)) {
            while (scan.next()) {
                actualHits.add(deepCopy(scan));
            }
        }

        Comparator<KEY> hitComparator = (h1, h2) -> {
            int keyCompare = layout.compare(h1, h2);
            if (keyCompare == 0) {
                return valueCreatorUtil.compareIndexedPropertyValue(h1, h2);
            } else {
                return keyCompare;
            }
        };
        assertSameHits(expectedHits, actualHits, hitComparator);
    }

    private Seeker<KEY, NullValue> scan(GBPTree<KEY, NullValue> tree) throws IOException {
        KEY lowest = layout.newKey();
        lowest.initialize(Long.MIN_VALUE);
        lowest.initValueAsLowest(0, ValueGroup.UNKNOWN);
        KEY highest = layout.newKey();
        highest.initialize(Long.MAX_VALUE);
        highest.initValueAsHighest(0, ValueGroup.UNKNOWN);
        return tree.seek(lowest, highest, NULL_CONTEXT);
    }

    private void assertSameHits(List<KEY> expectedHits, List<KEY> actualHits, Comparator<KEY> comparator) {
        expectedHits.sort(comparator);
        actualHits.sort(comparator);
        assertEquals(
                expectedHits.size(),
                actualHits.size(),
                format("Array length differ%nExpected:%d, Actual:%d", expectedHits.size(), actualHits.size()));

        for (int i = 0; i < expectedHits.size(); i++) {
            KEY expected = expectedHits.get(i);
            KEY actual = actualHits.get(i);
            assertEquals(
                    0,
                    comparator.compare(expected, actual),
                    "Hits differ on item number " + i + ". Expected " + expected + " but was " + actual);
        }
    }

    private KEY deepCopy(Seeker<KEY, NullValue> from) {
        KEY intoKey = layout.newKey();
        layout.copyKey(from.key(), intoKey);
        return intoKey;
    }

    private List<KEY> convertToHits(ValueIndexEntryUpdate<IndexDescriptor>[] updates, Layout<KEY, NullValue> layout) {
        List<KEY> hits = new ArrayList<>(updates.length);
        for (ValueIndexEntryUpdate<IndexDescriptor> u : updates) {
            KEY key = layout.newKey();
            key.initialize(u.getEntityId());
            for (int i = 0; i < u.values().length; i++) {
                key.initFromValue(i, u.values()[i], NEUTRAL);
            }
            hits.add(key);
        }
        return hits;
    }
}
