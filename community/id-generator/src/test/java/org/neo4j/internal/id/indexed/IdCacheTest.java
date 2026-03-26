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
package org.neo4j.internal.id.indexed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.internal.id.IdGenerator.NO_ID;
import static org.neo4j.internal.id.IdSlotDistribution.slotDistribution;
import static org.neo4j.internal.id.indexed.IdCache.DYNAMIC_CHUNK_SIZE;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.LARGE_CACHE_CAPACITY;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.SMALL_CACHE_CAPACITY;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.id.IdSequence.ConsecutiveId;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.internal.id.indexed.IdCache.SlotSizeFallback;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class IdCacheTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldReportCorrectSlotsByAvailableSpace() {
        // given
        var cache = new IdCache(
                new IdSlotDistribution.Slot(8, 1),
                new IdSlotDistribution.Slot(8, 2),
                new IdSlotDistribution.Slot(4, 4));
        assertThat(cache.slotsByAvailableSpace()).isEqualTo(new IdSlotDistribution.Slot[] {
            new IdSlotDistribution.Slot(DYNAMIC_CHUNK_SIZE * 2, 1),
            new IdSlotDistribution.Slot(DYNAMIC_CHUNK_SIZE * 2, 2),
            new IdSlotDistribution.Slot(DYNAMIC_CHUNK_SIZE * 2, 4)
        });

        // when
        cache.offer(1, 4, NO_MONITOR);
        cache.offer(10, 6, NO_MONITOR);

        // then
        assertThat(cache.slotsByAvailableSpace()).isEqualTo(new IdSlotDistribution.Slot[] {
            new IdSlotDistribution.Slot(DYNAMIC_CHUNK_SIZE * 2, 1),
            new IdSlotDistribution.Slot(DYNAMIC_CHUNK_SIZE * 2 - 1, 2),
            new IdSlotDistribution.Slot(DYNAMIC_CHUNK_SIZE * 2 - 2, 4)
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {LARGE_CACHE_CAPACITY, SMALL_CACHE_CAPACITY})
    void drainRangeShouldNotLooseIds(int capacity) {
        IdCache cache = new IdCache(IdSlotDistribution.SINGLE_IDS.slots(capacity));
        var half = capacity / 2;
        var rangeSize = capacity / 5;
        for (int i = 0; i < half; i++) {
            cache.offer(i + 1000, 1, NO_MONITOR);
        }
        for (int i = 0; i < capacity - half; i++) {
            cache.offer(i, 1, NO_MONITOR);
        }
        int drained = 0;
        long[] ids;
        do {
            ids = cache.drainRange(rangeSize);
            assertIdsInSameRange(ids, rangeSize);
            drained += ids.length;
        } while (ids.length != 0);
        assertThat(drained).isEqualTo(capacity);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldTakeIdFromBiggerSlotIfPossible(boolean allowSmaller) {
        // given
        long higherSizeId = 100;
        int requestedIdSize = 8;
        int higherSizeIdSize = requestedIdSize * 2;
        int lowerSizeIdSize = requestedIdSize / 2;
        var cache = new IdCache(
                IdSlotDistribution.slotDistribution(new int[] {1, 2, 4, 8, 16}).slots(128));
        cache.offer(higherSizeId, higherSizeIdSize, NO_MONITOR);
        cache.offer(higherSizeId + higherSizeIdSize, lowerSizeIdSize, NO_MONITOR);

        // when
        var remainderId = new MutableLong();
        var remainderIdSize = new MutableInt();
        var id = cache.takeOrDefault(
                NO_ID,
                requestedIdSize,
                new IndexedIdGenerator.Monitor() {
                    @Override
                    public void cached(long cachedId, int numberOfIds) {
                        remainderId.setValue(cachedId);
                        remainderIdSize.setValue(numberOfIds);
                    }
                },
                (id1, size) -> {},
                allowSmaller ? SlotSizeFallback.full : SlotSizeFallback.none);

        // then
        assertThat(id).isEqualTo(new ConsecutiveId(higherSizeId, requestedIdSize));
        assertThat(remainderId.longValue()).isEqualTo(higherSizeId + requestedIdSize);
        assertThat(remainderIdSize.intValue()).isEqualTo(higherSizeIdSize - requestedIdSize);
    }

    @Test
    void shouldTakeIdFromSmallerSlotIfToldTo() {
        // given
        long lowerSizeId = 100;
        int lowerSizeIdSize = 2;
        var cache = new IdCache(
                IdSlotDistribution.slotDistribution(new int[] {1, 2, 4, 8, 16}).slots(128));
        cache.offer(lowerSizeId, lowerSizeIdSize, NO_MONITOR);

        // when
        int requestedIdSize = 8;
        var id = cache.takeOrDefault(
                NO_ID, requestedIdSize, NO_MONITOR, (ignore1, ignore2) -> {}, SlotSizeFallback.full);

        // then
        assertThat(id).isEqualTo(new ConsecutiveId(lowerSizeId, lowerSizeIdSize));
    }

    private void assertIdsInSameRange(long[] ids, int rangeSize) {
        if (ids.length == 0) {
            return;
        }
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long id : ids) {
            if (id > max) {
                max = id;
            }
            if (id < min) {
                min = id;
            }
        }
        assertThat(max / rangeSize).isEqualTo(min / rangeSize);
    }

    @MethodSource("sizes")
    @ParameterizedTest
    void shouldAcceptIdsOfVariousSizes(int slotSize) {
        // given
        var slotSizes = new int[] {1, 2, 4, 8};
        var cache = new IdCache(slotDistribution(slotSizes).slots(128));

        // when
        var actual = new BitSet();
        var monitor = new IndexedIdGenerator.Monitor() {
            @Override
            public void cached(long cachedId, int numberOfIds) {
                for (var i = 0; i < numberOfIds; i++) {
                    actual.set((int) (cachedId + i));
                }
            }
        };
        var id = random.nextInt(1_000);
        var accepted = cache.offer(id, slotSize, monitor);
        var expected = new BitSet();
        for (var i = 0; i < slotSize; i++) {
            expected.set(id + i);
        }

        // then
        assertThat(accepted).isEqualTo(slotSize);
        assertThat(actual).isEqualTo(expected);
    }

    private static Stream<Arguments> sizes() {
        List<Arguments> permutations = new ArrayList<>();
        for (int i = 1; i < 128; i++) {
            permutations.add(arguments(i));
        }
        return permutations.stream();
    }
}
