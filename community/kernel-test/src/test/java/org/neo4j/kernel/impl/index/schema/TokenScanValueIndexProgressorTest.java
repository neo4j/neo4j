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
import static org.neo4j.kernel.impl.index.schema.NativeAllEntriesTokenScanReaderTest.Labels;
import static org.neo4j.kernel.impl.index.schema.NativeAllEntriesTokenScanReaderTest.labels;
import static org.neo4j.kernel.impl.index.schema.NativeAllEntriesTokenScanReaderTest.randomData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.LongStream;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
                EMPTY_CURSOR, client, IndexOrder.ASCENDING, EntityRange.FULL, new DefaultTokenIndexIdLayout(), 0);
        assertFalse(progressor.next());
        assertThat(client.observedIds).isEmpty();
    }

    @Test
    void shouldProgressAscendingThroughBitSet() {
        var idLayout = new DefaultTokenIndexIdLayout();
        List<Labels> labels = randomData(random, idLayout);

        for (Labels label : labels) {
            long[] nodeIds = label.getNodeIds();
            MyClient client = new MyClient();
            TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                    label.cursor(), client, IndexOrder.ASCENDING, EntityRange.FULL, idLayout, label.getId());
            while (progressor.next()) {}

            assertThat(client.observedIds)
                    .containsExactlyElementsOf(LongStream.of(nodeIds).boxed().toList());
        }
    }

    @Test
    void shouldProgressDescendingThroughBitSet() {
        var idLayout = new DefaultTokenIndexIdLayout();
        List<Labels> labels = randomData(random, idLayout);

        for (Labels label : labels) {
            long[] nodeIds = label.getNodeIds();
            MyClient client = new MyClient();
            TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                    label.descendingCursor(), client, IndexOrder.DESCENDING, EntityRange.FULL, idLayout, label.getId());
            while (progressor.next()) {}

            assertThat(client.observedIds)
                    .containsExactlyElementsOf(LongStream.of(nodeIds)
                            .boxed()
                            .sorted(Collections.reverseOrder())
                            .toList());
        }
    }

    @Test
    void shouldRespectRequestedRange() {
        var idLayout = new DefaultTokenIndexIdLayout();
        Labels label = labels(1, idLayout, 20, 39, 40, 41, 60, 80, 99, 100, 101, 120);
        MyClient client = new MyClient();
        TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                label.cursor(), client, IndexOrder.ASCENDING, new EntityRange(40, 100), idLayout, label.getId());
        while (progressor.next()) {}

        assertThat(client.observedIds).containsExactlyInAnyOrder(40L, 41L, 60L, 80L, 99L);
    }

    static class MyClient implements IndexProgressor.EntityTokenClient {
        final List<Long> observedIds = new ArrayList<>();

        @Override
        public void initialize(IndexProgressor progressor, int token, IndexOrder order) {}

        @Override
        public void initialize(IndexProgressor progressor, int token, LongIterator added, LongSet removed) {}

        @Override
        public boolean acceptEntity(long reference, int tokenId) {
            observedIds.add(reference);
            return true;
        }
    }

    @Test
    void shouldRespectRequestedRangeSeekDescending() {
        runSeekTest(
                IndexOrder.DESCENDING,
                label -> {
                    long maxNodeId = label.getMaxNodeId();
                    long minNodeId = label.getMinNodeId();
                    long lower = random.nextLong(minNodeId, maxNodeId + 1);
                    long upper = random.nextLong(lower, maxNodeId + 1);
                    return new EntityRange(lower, upper);
                },
                (label, client, progressor, range) -> {
                    List<Long> outsideLower = Lists.mutable.empty();
                    List<Long> insideRange = Lists.mutable.empty();
                    List<Long> outsideUpper = Lists.mutable.empty();

                    LongStream.of(label.getNodeIds())
                            .boxed()
                            .sorted(Collections.reverseOrder())
                            .forEach(nodeId -> {
                                if (nodeId < range.fromInclusive()) {
                                    outsideLower.add(nodeId);
                                } else if (nodeId < range.toExclusive()) {
                                    insideRange.add(nodeId);
                                } else {
                                    outsideUpper.add(nodeId);
                                }
                            });

                    for (long nodeId : outsideUpper) {
                        progressor.skipUntil(nodeId);
                    }

                    for (long nodeId : insideRange) {
                        progressor.skipUntil(nodeId);
                        assertThat(progressor.next()).isTrue();
                    }

                    for (long nodeId : outsideLower) {
                        progressor.skipUntil(nodeId);
                    }

                    assertThat(client.observedIds).containsExactlyElementsOf(insideRange);
                });
    }

    @Test
    void shouldRespectRequestedRangeSeekAscending() {
        runSeekTest(
                IndexOrder.ASCENDING,
                label -> {
                    long maxNodeId = label.getMaxNodeId();
                    long minNodeId = label.getMinNodeId();
                    long lower = random.nextLong(minNodeId, maxNodeId + 1);
                    long upper = random.nextLong(lower, maxNodeId + 1);
                    return new EntityRange(lower, upper);
                },
                (label, client, progressor, range) -> {
                    List<Long> outsideLower = Lists.mutable.empty();
                    List<Long> insideRange = Lists.mutable.empty();
                    List<Long> outsideUpper = Lists.mutable.empty();

                    for (long nodeId : label.getNodeIds()) {
                        if (nodeId < range.fromInclusive()) {
                            outsideLower.add(nodeId);
                        } else if (nodeId < range.toExclusive()) {
                            insideRange.add(nodeId);
                        } else {
                            outsideUpper.add(nodeId);
                        }
                    }

                    for (long nodeId : outsideLower) {
                        progressor.skipUntil(nodeId);
                    }

                    for (long nodeId : insideRange) {
                        progressor.skipUntil(nodeId);
                        assertThat(progressor.next()).isTrue();
                    }

                    for (long nodeId : outsideUpper) {
                        progressor.skipUntil(nodeId);
                    }

                    assertThat(client.observedIds).containsExactlyElementsOf(insideRange);
                });
    }

    @Test
    void shouldSeekSeveralTimesDescending() {
        runSeekTest(IndexOrder.DESCENDING, (label, client, progressor, range) -> {
            List<Long> orderedSubset = LongStream.of(label.getNodeIds())
                    .filter(ignored -> random.nextBoolean() && random.nextBoolean())
                    .boxed()
                    .sorted(Collections.reverseOrder())
                    .toList();

            for (long nodeId : orderedSubset) {
                progressor.skipUntil(nodeId);
                assertThat(progressor.next()).isTrue();
            }
            assertThat(client.observedIds).containsExactlyElementsOf(orderedSubset);
        });
    }

    @Test
    void shouldSeekSeveralTimesAscending() {
        runSeekTest(IndexOrder.ASCENDING, (label, client, progressor, range) -> {
            List<Long> orderedSubset = LongStream.of(label.getNodeIds())
                    .filter(ignored -> random.nextBoolean() && random.nextBoolean())
                    .boxed()
                    .toList();

            for (long nodeId : orderedSubset) {
                progressor.skipUntil(nodeId);
                assertThat(progressor.next()).isTrue();
            }
            assertThat(client.observedIds).containsExactlyElementsOf(orderedSubset);
        });
    }

    @Test
    void shouldSeekToLastDescending() {
        runSeekTest(IndexOrder.DESCENDING, (label, client, progressor, range) -> {
            // seek to max id
            long minId = label.getMinNodeId();
            progressor.skipUntil(minId);
            while (progressor.next()) {}
            assertThat(client.observedIds).containsExactly(minId);
        });
    }

    @Test
    void shouldSeekToLastAscending() {
        runSeekTest(IndexOrder.ASCENDING, (label, client, progressor, range) -> {
            // seek to max id
            long maxId = label.getMaxNodeId();
            progressor.skipUntil(maxId);
            while (progressor.next()) {}
            assertThat(client.observedIds).containsExactly(maxId);
        });
    }

    @Test
    void shouldSeekToFirstAscending() {
        runSeekTest(IndexOrder.ASCENDING, (label, client, progressor, range) -> {
            long[] nodeIds = label.getNodeIds();
            progressor.skipUntil(Long.MIN_VALUE);
            while (progressor.next()) {}
            assertThat(client.observedIds)
                    .as("Label: " + label.getId())
                    .containsExactly(LongStream.of(nodeIds).boxed().toArray(Long[]::new));
        });
    }

    @Test
    void shouldSeekToFirstDescending() {
        runSeekTest(IndexOrder.DESCENDING, (label, client, progressor, range) -> {
            long[] nodeIds = label.getNodeIds();
            progressor.skipUntil(Long.MAX_VALUE);
            while (progressor.next()) {}
            assertThat(client.observedIds)
                    .containsExactly(LongStream.of(nodeIds)
                            .boxed()
                            .sorted(Collections.reverseOrder())
                            .toArray(Long[]::new));
        });
    }

    @Test
    void shouldSeekToRandomInRangeDescending() {
        runSeekTest(IndexOrder.DESCENDING, (label, client, progressor, range) -> {
            long maxId = label.getMaxNodeId();
            long randId = random.nextLong(0, maxId + 1);
            progressor.skipUntil(randId);
            while (progressor.next()) {}
            assertThat(client.observedIds).allMatch(observed -> observed <= randId);
        });
    }

    @Test
    void shouldSeekToRandomInRangeAscending() {
        runSeekTest(IndexOrder.ASCENDING, (label, client, progressor, range) -> {
            long maxId = label.getMaxNodeId();
            long randId = random.nextLong(0, maxId + 1);
            progressor.skipUntil(randId);
            while (progressor.next()) {}
            assertThat(client.observedIds).allMatch(observed -> observed >= randId);
        });
    }

    @Test
    void shouldSeekToOutsideBound() {
        runSeekTest(IndexOrder.ASCENDING, (label, client, progressor, range) -> {
            long maxId = label.getMaxNodeId();
            progressor.skipUntil(maxId + 1);
            while (progressor.next()) {}
            assertThat(client.observedIds).isEmpty();
        });
    }

    private void runSeekTest(IndexOrder order, SeekTest test) {
        runSeekTest(order, ignored -> EntityRange.FULL, test);
    }

    private void runSeekTest(IndexOrder order, Function<Labels, EntityRange> labelToRange, SeekTest test) {
        var idLayout = new DefaultTokenIndexIdLayout();
        List<Labels> labels = randomData(random, idLayout);

        for (Labels label : labels) {
            MyClient client = new MyClient();
            EntityRange range = labelToRange.apply(label);
            TokenScanValueIndexProgressor progressor = new TokenScanValueIndexProgressor(
                    order != IndexOrder.DESCENDING ? label.cursor() : label.descendingCursor(),
                    client,
                    order,
                    range,
                    idLayout,
                    label.getId());

            test.run(label, client, progressor, range);
        }
    }

    private interface SeekTest {
        void run(Labels label, MyClient client, TokenScanValueIndexProgressor progressor, EntityRange range);
    }
}
