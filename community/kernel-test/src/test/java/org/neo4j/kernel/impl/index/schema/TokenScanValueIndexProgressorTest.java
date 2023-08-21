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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.kernel.impl.index.schema.NativeAllEntriesTokenScanReaderTest.EMPTY_CURSOR;
import static org.neo4j.kernel.impl.index.schema.NativeAllEntriesTokenScanReaderTest.randomData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.EntityRange;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@SuppressWarnings("StatementWithEmptyBody")
@ExtendWith(RandomExtension.class)
public class TokenScanValueIndexProgressorTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldNotProgressOnEmptyCursor() {
        MyClient client = new MyClient();
        TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                EMPTY_CURSOR, client, IndexOrder.ASCENDING, EntityRange.FULL, new DefaultTokenIndexIdLayout());
        assertFalse(progressor.next());
        assertThat(client.observedIds).isEmpty();
    }

    @Test
    void shouldProgressAscendingThroughBitSet() {
        var idLayout = new DefaultTokenIndexIdLayout();
        List<NativeAllEntriesTokenScanReaderTest.Labels> labels = randomData(random, idLayout);

        for (NativeAllEntriesTokenScanReaderTest.Labels label : labels) {
            long[] nodeIds = label.getNodeIds();
            MyClient client = new MyClient();
            TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                    label.cursor(), client, IndexOrder.ASCENDING, EntityRange.FULL, idLayout);
            while (progressor.next()) {}

            assertThat(client.observedIds)
                    .containsExactly(LongStream.of(nodeIds).boxed().toArray(Long[]::new));
        }
    }

    @Test
    void shouldProgressDescendingThroughBitSet() {
        var idLayout = new DefaultTokenIndexIdLayout();
        List<NativeAllEntriesTokenScanReaderTest.Labels> labels = randomData(random, idLayout);

        for (NativeAllEntriesTokenScanReaderTest.Labels label : labels) {
            long[] nodeIds = label.getNodeIds();
            MyClient client = new MyClient();
            TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                    label.descendingCursor(), client, IndexOrder.DESCENDING, EntityRange.FULL, idLayout);
            while (progressor.next()) {}

            assertThat(client.observedIds)
                    .containsExactly(LongStream.of(nodeIds)
                            .boxed()
                            .sorted(Collections.reverseOrder())
                            .toArray(Long[]::new));
        }
    }

    @Test
    void shouldRespectRequestedRange() {
        var idLayout = new DefaultTokenIndexIdLayout();
        NativeAllEntriesTokenScanReaderTest.Labels label =
                NativeAllEntriesTokenScanReaderTest.labels(1, idLayout, 20, 39, 40, 41, 60, 80, 99, 100, 101, 120);
        MyClient client = new MyClient();
        TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                label.cursor(), client, IndexOrder.ASCENDING, new EntityRange(40, 100), idLayout);
        while (progressor.next()) {}

        assertThat(client.observedIds).containsExactlyInAnyOrder(40L, 41L, 60L, 80L, 99L);
    }

    static class MyClient implements IndexProgressor.EntityTokenClient {
        final List<Long> observedIds = new ArrayList<>();

        @Override
        public void initialize(IndexProgressor progressor, int token, IndexOrder order) {}

        @Override
        public void initialize(
                IndexProgressor progressor, int token, LongIterator added, LongSet removed, AccessMode accessMode) {}

        @Override
        public boolean acceptEntity(long reference, int tokenId) {
            observedIds.add(reference);
            return true;
        }
    }
}
