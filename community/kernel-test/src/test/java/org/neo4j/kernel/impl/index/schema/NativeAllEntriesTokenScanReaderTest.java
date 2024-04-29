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

import static java.lang.Long.max;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.common.EntityType.NODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.IntFunction;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class NativeAllEntriesTokenScanReaderTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldSeeNonOverlappingRanges() throws Exception {
        var idLayout = new DefaultTokenIndexIdLayout();
        // new ranges at: 0, 4, 8, 12 ...
        shouldIterateCorrectlyOver(
                idLayout,
                labels(0, idLayout, 0, 1, 2, 3),
                labels(1, idLayout, 4, 6),
                labels(2, idLayout, 12),
                labels(3, idLayout, 17, 18));
    }

    @Test
    void shouldSeeOverlappingRanges() throws Exception {
        var idLayout = new DefaultTokenIndexIdLayout();
        // new ranges at: 0, 4, 8, 12 ...
        shouldIterateCorrectlyOver(
                idLayout,
                labels(0, idLayout, 0, 1, 3, 55),
                labels(3, idLayout, 1, 2, 5, 6, 43),
                labels(5, idLayout, 8, 9, 15, 42),
                labels(6, idLayout, 4, 8, 12));
    }

    @Test
    void shouldSeeRangesFromRandomData() throws Exception {
        var idLayout = new DefaultTokenIndexIdLayout();
        List<Labels> labels = randomData(random, idLayout);

        shouldIterateCorrectlyOver(idLayout, labels.toArray(Labels[]::new));
    }

    private static void shouldIterateCorrectlyOver(DefaultTokenIndexIdLayout idLayout, Labels... data)
            throws Exception {
        // GIVEN
        try (AllEntriesTokenScanReader reader =
                new NativeAllEntriesTokenScanReader(store(data), highestLabelId(data), NODE, idLayout)) {
            // WHEN/THEN
            assertRanges(reader, data, idLayout);
        }
    }

    static List<Labels> randomData(RandomSupport random, DefaultTokenIndexIdLayout idLayout) {
        List<Labels> labels = new ArrayList<>();
        int labelCount = random.intBetween(30, 100);
        int labelId = 0;
        for (int i = 0; i < labelCount; i++) {
            labelId += random.intBetween(1, 20);
            int nodeCount = random.intBetween(20, 100);
            long[] nodeIds = new long[nodeCount];
            long nodeId = 0;
            for (int j = 0; j < nodeCount; j++) {
                nodeId += random.intBetween(1, 100);
                nodeIds[j] = nodeId;
            }
            labels.add(labels(labelId, idLayout, nodeIds));
        }
        return labels;
    }

    private static int highestLabelId(Labels[] data) {
        int highest = 0;
        for (Labels labels : data) {
            highest = Integer.max(highest, labels.labelId);
        }
        return highest;
    }

    private static void assertRanges(
            AllEntriesTokenScanReader reader, Labels[] data, DefaultTokenIndexIdLayout idLayout) {
        Iterator<EntityTokenRange> iterator = reader.iterator();
        long highestRangeId = highestRangeId(data);
        for (long rangeId = 0; rangeId <= highestRangeId; rangeId++) {
            SortedMap<Long /*nodeId*/, List<Integer> /*labelIds*/> expected = rangeOf(data, rangeId, idLayout);
            if (expected != null) {
                Assertions.assertTrue(iterator.hasNext(), "Was expecting range " + expected);
                EntityTokenRange range = iterator.next();

                Assertions.assertEquals(rangeId, range.id());
                for (Map.Entry<Long, List<Integer>> expectedEntry : expected.entrySet()) {
                    int[] labels = range.tokens(expectedEntry.getKey());
                    assertArrayEquals(
                            expectedEntry.getValue().stream()
                                    .mapToInt(Integer::intValue)
                                    .toArray(),
                            labels);
                }
            }
            // else there was nothing in this range
        }
        Assertions.assertFalse(iterator.hasNext());
    }

    private static SortedMap<Long, List<Integer>> rangeOf(
            Labels[] data, long rangeId, DefaultTokenIndexIdLayout idLayout) {
        SortedMap<Long, List<Integer>> result = new TreeMap<>();
        for (Labels label : data) {
            for (Pair<TokenScanKey, TokenScanValue> entry : label.entries) {
                if (entry.first().idRange == rangeId) {
                    long baseNodeId = idLayout.firstIdOfRange(entry.first().idRange);
                    long bits = entry.other().bits;
                    while (bits != 0) {
                        long nodeId = baseNodeId + Long.numberOfTrailingZeros(bits);
                        result.computeIfAbsent(nodeId, id -> new ArrayList<>()).add(label.labelId);
                        bits &= bits - 1;
                    }
                }
            }
        }
        return result.isEmpty() ? null : result;
    }

    private static long highestRangeId(Labels[] data) {
        long highest = 0;
        for (Labels labels : data) {
            Pair<TokenScanKey, TokenScanValue> highestEntry = labels.entries.get(labels.entries.size() - 1);
            highest = max(highest, highestEntry.first().idRange);
        }
        return highest;
    }

    private static IntFunction<Seeker<TokenScanKey, TokenScanValue>> store(Labels... labels) {
        final MutableIntObjectMap<Labels> labelsMap = new IntObjectHashMap<>(labels.length);
        for (Labels item : labels) {
            labelsMap.put(item.labelId, item);
        }

        return labelId -> {
            Labels item = labelsMap.get(labelId);
            return item != null ? item.cursor() : EMPTY_CURSOR;
        };
    }

    static Labels labels(int labelId, DefaultTokenIndexIdLayout idLayout, long... nodeIds) {
        List<Pair<TokenScanKey, TokenScanValue>> entries = new ArrayList<>();
        long currentRange = 0;
        TokenScanValue value = new TokenScanValue();
        for (long nodeId : nodeIds) {
            long range = idLayout.rangeOf(nodeId);
            if (range != currentRange) {
                if (value.bits != 0) {
                    entries.add(Pair.of(new TokenScanKey().set(labelId, currentRange), value));
                    value = new TokenScanValue();
                }
            }
            value.set(idLayout.idWithinRange(nodeId));
            currentRange = range;
        }

        if (value.bits != 0) {
            entries.add(Pair.of(new TokenScanKey().set(labelId, currentRange), value));
        }

        return new Labels(labelId, entries, idLayout, nodeIds);
    }

    static class Labels {
        private final int labelId;
        private final List<Pair<TokenScanKey, TokenScanValue>> entries;
        private final DefaultTokenIndexIdLayout idLayout;
        private final long[] nodeIds;

        Labels(
                int labelId,
                List<Pair<TokenScanKey, TokenScanValue>> entries,
                DefaultTokenIndexIdLayout idLayout,
                long... nodeIds) {
            this.labelId = labelId;
            this.entries = entries;
            this.idLayout = idLayout;
            this.nodeIds = nodeIds;
        }

        Seeker<TokenScanKey, TokenScanValue> cursor() {
            return new LabelsSeeker(entries, true);
        }

        Seeker<TokenScanKey, TokenScanValue> descendingCursor() {
            return new LabelsSeeker(entries, false);
        }

        public long[] getNodeIds() {
            return nodeIds;
        }

        public int getId() {
            return labelId;
        }

        public long getMaxNodeId() {
            return Arrays.stream(nodeIds).max().orElse(-1);
        }

        public long getMinNodeId() {
            return Arrays.stream(nodeIds).min().orElse(-1);
        }

        public String toString() {
            return "Label: " + labelId;
        }

        public boolean hasNodeId(long nodeId) {
            return Arrays.binarySearch(getNodeIds(), nodeId) >= 0;
        }
    }

    static final Seeker<TokenScanKey, TokenScanValue> EMPTY_CURSOR = new Seeker<>() {
        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void close() { // Nothing to close
        }

        @Override
        public TokenScanKey key() {
            throw new IllegalStateException();
        }

        @Override
        public TokenScanValue value() {
            throw new IllegalStateException();
        }
    };
}
