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
package org.neo4j.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.HeapEstimator.OBJECT_ALIGNMENT_BYTES;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstanceWithObjectReferences;

import org.junit.jupiter.api.Test;

class HeapEstimatorTest {
    @Test
    void alignObjectSize() {
        for (int i = 0; i <= 1024; i++) {
            long aligned = HeapEstimator.alignObjectSize(i);
            assertEquals(0, aligned % OBJECT_ALIGNMENT_BYTES);
        }
    }

    @Test
    void shouldNotOverflowOnInsanelyBigClass() {
        assertThat(shallowSizeOfInstanceWithObjectReferences(536870912)).isGreaterThan(0L);
    }

    @Test
    void shouldEstimateShallowSizeOfInstanceTheSame() {
        assertThat(shallowSizeOfInstanceWithObjectReferences(3)).isEqualTo(shallowSizeOfInstance(DummyClass.class));
    }

    private static class DummyClass {
        Object ref1;
        Object ref2;
        Object ref3;
    }
}
