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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.ToIntFunction;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RawBytes;
import org.neo4j.test.RandomSupport;

class BlockEntryMergerTestUtils {
    static <KEY, VALUE> void assertMergedPartStream(
            List<BlockEntry<KEY, VALUE>> expectedData, BlockEntryCursor<KEY, VALUE> actual) throws IOException {
        Iterator<BlockEntry<KEY, VALUE>> expected = expectedData.iterator();
        while (actual.next()) {
            assertThat(expected).hasNext();
            BlockEntry<KEY, VALUE> expectedEntry = expected.next();
            assertThat(actual.key()).isEqualTo(expectedEntry.key());
            assertThat(actual.value()).isEqualTo(expectedEntry.value());
        }
        assertThat(expected.hasNext()).isFalse();
    }

    static List<BlockEntryCursor<RawBytes, RawBytes>> buildParts(
            RandomSupport random, Layout<RawBytes, RawBytes> layout, List<BlockEntry<RawBytes, RawBytes>> allData) {
        return buildParts(random, layout, allData, random.nextInt(1, 12), rng -> rng.nextInt(1, 1_000));
    }

    static List<BlockEntryCursor<RawBytes, RawBytes>> buildParts(
            RandomSupport random,
            Layout<RawBytes, RawBytes> layout,
            List<BlockEntry<RawBytes, RawBytes>> allData,
            int numParts,
            ToIntFunction<RandomSupport> partSize) {
        List<BlockEntryCursor<RawBytes, RawBytes>> parts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            List<BlockEntry<RawBytes, RawBytes>> partData = buildPart(random, layout, partSize.applyAsInt(random));
            allData.addAll(partData);
            parts.add(new ListBasedBlockEntryCursor<>(partData));
        }
        sort(allData, layout);
        return parts;
    }

    static List<BlockEntry<RawBytes, RawBytes>> buildPart(
            RandomSupport random, Layout<RawBytes, RawBytes> layout, int count) {
        List<BlockEntry<RawBytes, RawBytes>> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            entries.add(new BlockEntry<>(randomBytesInstance(random), randomBytesInstance(random)));
        }
        sort(entries, layout);
        return entries;
    }

    private static RawBytes randomBytesInstance(RandomSupport random) {
        return new RawBytes(random.nextBytes(new byte[Long.BYTES]));
    }

    private static void sort(List<BlockEntry<RawBytes, RawBytes>> entries, Layout<RawBytes, RawBytes> layout) {
        entries.sort((b1, b2) -> layout.compare(b1.key(), b2.key()));
    }
}
