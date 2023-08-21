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
import static org.neo4j.internal.id.IdSlotDistribution.diminishingSlotDistribution;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class PendingIdQueueTest {
    @Inject
    private RandomSupport random;

    @MethodSource("sizes")
    @ParameterizedTest
    void shouldAcceptIdsOfVariousSizes(int slotSize) {
        // given
        int[] slotSizes = {1, 2, 4, 8};
        PendingIdQueue cache =
                new PendingIdQueue(diminishingSlotDistribution(slotSizes).slots(128));

        // when
        int id = random.nextInt(1_000);
        int accepted = cache.offer(id, slotSize);
        BitSet expected = new BitSet();
        for (int i = 0; i < slotSize; i++) {
            expected.set(id + i);
        }

        // then
        BitSet actual = new BitSet();
        cache.accept((slotIndex, size, ids) -> ids.forEach(cachedId -> {
            for (int i = 0; i < size; i++) {
                actual.set((int) (cachedId + i));
            }
        }));
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
