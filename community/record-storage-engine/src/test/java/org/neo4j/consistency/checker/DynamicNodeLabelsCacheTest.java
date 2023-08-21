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
package org.neo4j.consistency.checker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class DynamicNodeLabelsCacheTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldPutAndGetLabels() {
        // given
        DynamicNodeLabelsCache cache = new DynamicNodeLabelsCache(INSTANCE);
        long[] indexes = new long[1_000];
        long[][] expectedLabels = new long[indexes.length][];

        // when
        for (int i = 0; i < indexes.length; i++) {
            long[] labels = expectedLabels[i] = randomSortedLabels();
            indexes[i] = cache.put(labels);
        }

        // then
        for (int i = 0; i < indexes.length; i++) {
            long[] expected = expectedLabels[i];
            long[] actual = cache.get(indexes[i], new long[expected.length]);
            assertArrayEquals(expected, actual);
        }
    }

    private long[] randomSortedLabels() {
        long[] labels = new long[random.nextInt(1, 10)];
        int strider = 0;
        for (int ii = 0; ii < labels.length; ii++) {
            strider += random.nextInt(1, 100);
            labels[ii] = strider;
        }
        return labels;
    }
}
