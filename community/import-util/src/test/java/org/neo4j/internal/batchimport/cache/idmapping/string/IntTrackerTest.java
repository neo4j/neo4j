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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class IntTrackerTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldKeepIdsAndMarkDuplicates() {
        // given
        int length = 10_000;
        try (IntTracker tracker =
                new IntTracker(NumberArrayFactories.HEAP.newIntArray(length, IntTracker.DEFAULT_VALUE, INSTANCE))) {
            // when
            long[] values = new long[length];
            boolean[] marks = new boolean[length];
            for (int i = 0; i < length; i++) {
                tracker.set(i, values[i] = random.nextLong(IntTracker.ID_MASK));
                if (random.nextBoolean()) {
                    tracker.markAsDuplicate(i);
                    marks[i] = true;
                }
            }

            // then
            for (int i = 0; i < length; i++) {
                assertEquals(values[i], tracker.get(i));
                assertEquals(marks[i], tracker.isMarkedAsDuplicate(i));
            }
        }
    }
}
